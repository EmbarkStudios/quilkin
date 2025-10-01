/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mod config;
mod metrics;

use crate::{filters::prelude::*, net::endpoint::metadata};

use self::metrics::Metrics;

pub use self::config::{Branch, Config, DirectionalConfig, Fallthrough};
use crate::generated::quilkin::filters::matches::v1alpha1 as proto;

struct ConfigInstance {
    metadata_key: metadata::Key,
    branches: Vec<(metadata::Value, (metadata::Key, FilterInstance))>,
    fallthrough: (metadata::Key, FilterInstance),
}

impl ConfigInstance {
    fn new(config: config::DirectionalConfig) -> Result<Self, CreationError> {
        let map_to_instance =
            |filter: String, config_type: Option<serde_json::Value>| -> Result<_, CreationError> {
                let instance = crate::filters::FilterRegistry::get(
                    &filter,
                    CreateFilterArgs::new(config_type.map(From::from)),
                )?;
                Ok((filter.into(), instance))
            };

        let branches = config
            .branches
            .into_iter()
            .map(|branch| {
                map_to_instance(branch.filter.name, branch.filter.config.clone())
                    .map(|instance| (branch.value.clone(), instance))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            metadata_key: config.metadata_key,
            branches,
            fallthrough: map_to_instance(config.fallthrough.0.name, config.fallthrough.0.config)?,
        })
    }
}

pub struct Match {
    metrics: Metrics,
    on_read_filters: Option<ConfigInstance>,
    on_write_filters: Option<ConfigInstance>,
}

impl Match {
    fn new(config: Config, metrics: Metrics) -> Result<Self, CreationError> {
        let on_read_filters = config.on_read.map(ConfigInstance::new).transpose()?;
        let on_write_filters = config.on_write.map(ConfigInstance::new).transpose()?;

        if on_read_filters.is_none() && on_write_filters.is_none() {
            return Err(CreationError::MissingConfig(Self::NAME));
        }

        Ok(Self {
            metrics,
            on_read_filters,
            on_write_filters,
        })
    }

    pub fn testing(config: Config) -> Self {
        Self::new(config, Metrics::new()).unwrap()
    }
}

fn match_filter<'config, 'ctx, Ctx>(
    config: &'config Option<ConfigInstance>,
    metrics: &'config Metrics,
    ctx: &'ctx mut Ctx,
    get_metadata: impl for<'value> Fn(
        &'value Ctx,
        &'config metadata::Key,
    ) -> Option<&'value metadata::Value>,
    and_then: impl Fn(&'ctx mut Ctx, &'config FilterInstance) -> Result<(), FilterError>,
) -> Result<(), FilterError> {
    match config {
        Some(config) => {
            let value =
                (get_metadata)(ctx, &config.metadata_key).ok_or(FilterError::MatchNoMetadata)?;

            if let Some((value, instance)) = config.branches.iter().find(|(key, _)| key == value) {
                tracing::trace!(key=%config.metadata_key, %value, filter=%instance.0, "Matched against branch");
                metrics.packets_matched_total.inc();
                (and_then)(ctx, &instance.1)
            } else {
                tracing::trace!(
                    key = %config.metadata_key,
                    fallthrough = %config.fallthrough.0,
                    "No match found, calling fallthrough"
                );
                metrics.packets_fallthrough_total.inc();
                (and_then)(ctx, &config.fallthrough.1)
            }
        }
        None => Ok(()),
    }
}

impl Filter for Match {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        tracing::trace!(metadata=?ctx.metadata);
        match_filter(
            &self.on_read_filters,
            &self.metrics,
            ctx,
            |ctx, metadata_key| ctx.metadata.get(metadata_key),
            |ctx, instance| instance.filter().read(ctx),
        )
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        match_filter(
            &self.on_write_filters,
            &self.metrics,
            ctx,
            |ctx, metadata_key| ctx.metadata.get(metadata_key),
            |ctx, instance| instance.filter().write(ctx),
        )
    }
}

impl StaticFilter for Match {
    const NAME: &'static str = "quilkin.filters.match.v1alpha1.Match";
    type Configuration = Config;
    type BinaryConfiguration = proto::Match;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Self::new(Self::ensure_config_exists(config)?, Metrics::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        filters::*,
        net::endpoint::{Endpoint, metadata},
        test::alloc_buffer,
    };

    #[tokio::test]
    async fn metrics() {
        let metrics = Metrics::new();
        let key = metadata::Key::from_static("myapp.com/token");
        let config = Config {
            on_read: Some(DirectionalConfig {
                metadata_key: key,
                branches: vec![Branch {
                    value: "abc".into(),
                    filter: Pass::as_filter_config(None).unwrap(),
                }],
                fallthrough: <_>::default(),
            }),
            on_write: None,
        };
        let filter = Match::new(config, metrics).unwrap();
        let endpoint: Endpoint = Default::default();
        let contents = b"hello";

        // no config, so should make no change.
        filter
            .write(&mut WriteContext::new(
                endpoint.address,
                "127.0.0.1:70".parse().unwrap(),
                alloc_buffer(contents),
            ))
            .unwrap();

        assert_eq!(0, filter.metrics.packets_fallthrough_total.get());
        assert_eq!(0, filter.metrics.packets_matched_total.get());

        // config so we can test match and fallthrough.
        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut dest = Vec::new();
        let mut ctx = ReadContext::new(
            &endpoints,
            ([127, 0, 0, 1], 7000).into(),
            alloc_buffer(contents),
            &mut dest,
        );
        ctx.metadata.insert(key, "abc".into());

        filter.read(&mut ctx).unwrap();
        assert_eq!(1, filter.metrics.packets_matched_total.get());
        assert_eq!(0, filter.metrics.packets_fallthrough_total.get());

        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut dest = Vec::new();
        let mut ctx = ReadContext::new(
            &endpoints,
            ([127, 0, 0, 1], 7000).into(),
            alloc_buffer(contents),
            &mut dest,
        );
        ctx.metadata.insert(key, "xyz".into());

        let result = filter.read(&mut ctx);
        assert!(result.is_err());
        assert_eq!(1, filter.metrics.packets_matched_total.get());
        assert_eq!(1, filter.metrics.packets_fallthrough_total.get());
    }
}
