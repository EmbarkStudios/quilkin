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

use crate::xds as envoy;

crate::include_proto!("quilkin.filters.matches.v1alpha1");

mod config;
mod metrics;

use crate::{filters::prelude::*, metadata::Value};

use self::quilkin::filters::matches::v1alpha1 as proto;
use crate::filters::r#match::metrics::Metrics;

pub use self::config::{Branch, Config, DirectionalConfig, Fallthrough};

struct ConfigInstance {
    metadata_key: String,
    branches: Vec<(Value, (String, FilterInstance))>,
    fallthrough: (String, FilterInstance),
}

impl ConfigInstance {
    fn new(config: config::DirectionalConfig) -> Result<Self, Error> {
        let map_to_instance =
            |filter: String, config_type: Option<serde_json::Value>| -> Result<_, Error> {
                let instance = crate::filters::FilterRegistry::get(
                    &filter,
                    CreateFilterArgs::new(config_type.map(From::from)),
                )?;
                Ok((filter, instance))
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
    fn new(config: Config, metrics: Metrics) -> Result<Self, Error> {
        let on_read_filters = config.on_read.map(ConfigInstance::new).transpose()?;
        let on_write_filters = config.on_write.map(ConfigInstance::new).transpose()?;

        if on_read_filters.is_none() && on_write_filters.is_none() {
            return Err(Error::MissingConfig(Self::NAME));
        }

        Ok(Self {
            metrics,
            on_read_filters,
            on_write_filters,
        })
    }
}

fn match_filter<'config, Ctx, R>(
    config: &'config Option<ConfigInstance>,
    metrics: &'config Metrics,
    ctx: Ctx,
    get_metadata: impl for<'ctx> Fn(&'ctx Ctx, &'config String) -> Option<&'ctx Value>,
    and_then: impl Fn(Ctx, &'config FilterInstance) -> Option<R>,
) -> Option<R>
where
    Ctx: Into<R>,
{
    match config {
        Some(config) => {
            let value = (get_metadata)(&ctx, &config.metadata_key)?;

            match config.branches.iter().find(|(key, _)| key == value) {
                Some((value, instance)) => {
                    tracing::trace!(key=&*config.metadata_key, value=?value, filter=&*instance.0, "Matched against branch");
                    metrics.packets_matched_total.inc();
                    (and_then)(ctx, &instance.1)
                }
                None => {
                    tracing::trace!(
                        key = &*config.metadata_key,
                        fallthrough = &*config.fallthrough.0,
                        "No match found, calling fallthrough"
                    );
                    metrics.packets_fallthrough_total.inc();
                    (and_then)(ctx, &config.fallthrough.1)
                }
            }
        }
        None => Some(ctx.into()),
    }
}

impl Filter for Match {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn read(&self, ctx: ReadContext) -> Option<ReadResponse> {
        tracing::trace!(metadata=?ctx.metadata);
        match_filter(
            &self.on_read_filters,
            &self.metrics,
            ctx,
            |ctx, metadata_key| ctx.metadata.get(metadata_key),
            |ctx, instance| instance.filter.read(ctx),
        )
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn write(&self, ctx: WriteContext) -> Option<WriteResponse> {
        match_filter(
            &self.on_write_filters,
            &self.metrics,
            ctx,
            |ctx, metadata_key| ctx.metadata.get(metadata_key),
            |ctx, instance| instance.filter.write(ctx),
        )
    }
}

impl StaticFilter for Match {
    const NAME: &'static str = "quilkin.filters.match.v1alpha1.Match";
    type Configuration = Config;
    type BinaryConfiguration = proto::Match;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, Error> {
        Self::new(Self::ensure_config_exists(config)?, Metrics::new()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{endpoint::Endpoint, filters::*};
    use std::sync::Arc;

    #[test]
    fn metrics() {
        let metrics = Metrics::new().unwrap();
        let key = "myapp.com/token";
        let config = Config {
            on_read: Some(DirectionalConfig {
                metadata_key: key.into(),
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
        let contents = "hello".to_string().into_bytes();

        // no config, so should make no change.
        filter
            .write(WriteContext::new(
                &endpoint,
                endpoint.address.clone(),
                "127.0.0.1:70".parse().unwrap(),
                contents.clone(),
            ))
            .unwrap();

        assert_eq!(0, filter.metrics.packets_fallthrough_total.get());
        assert_eq!(0, filter.metrics.packets_matched_total.get());

        // config so we can test match and fallthrough.
        let mut ctx = ReadContext::new(
            vec![Default::default()],
            ([127, 0, 0, 1], 7000).into(),
            contents.clone(),
        );
        ctx.metadata.insert(Arc::new(key.into()), "abc".into());

        filter.read(ctx).unwrap();
        assert_eq!(1, filter.metrics.packets_matched_total.get());
        assert_eq!(0, filter.metrics.packets_fallthrough_total.get());

        let mut ctx = ReadContext::new(
            vec![Default::default()],
            ([127, 0, 0, 1], 7000).into(),
            contents,
        );
        ctx.metadata.insert(Arc::new(key.into()), "xyz".into());

        let result = filter.read(ctx);
        assert!(result.is_none());
        assert_eq!(1, filter.metrics.packets_matched_total.get());
        assert_eq!(1, filter.metrics.packets_fallthrough_total.get());
    }
}
