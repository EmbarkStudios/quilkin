/*
 * Copyright 2020 Google LLC
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

use prometheus::{Histogram, exponential_buckets};

use crate::{
    config::filter::Filter as FilterConfig,
    filters::{FilterRegistry, prelude::*},
    metrics::{CollectorExt, histogram_opts},
};

const FILTER_LABEL: &str = "filter";

/// Start the histogram bucket at an eighth of a millisecond, as we bucketed the full filter
/// chain processing starting at a quarter of a millisecond, so we we will want finer granularity
/// here.
const BUCKET_START: f64 = 0.000125;

const BUCKET_FACTOR: f64 = 2.5;

/// At an exponential factor of 2.5 ([`BUCKET_FACTOR`]), 11 iterations gets us to just over half a
/// second. Any processing that occurs over half a second is far too long, so we end
/// the bucketing there as we don't care about granularity past this value.
const BUCKET_COUNT: usize = 11;

/// A chain of [`Filter`]s to be executed in order.
///
/// Executes each filter, passing the [`ReadContext`] and [`WriteContext`]
/// between each filter's execution, returning the result of data that has gone
/// through all of the filters in the chain. If any of the filters in the chain
/// return `None`, then the chain is broken, and `None` is returned.
#[derive(Clone, Default)]
pub struct FilterChain {
    filters: Vec<(String, FilterInstance)>,
    filter_read_duration_seconds: Vec<Histogram>,
    filter_write_duration_seconds: Vec<Histogram>,
}

impl FilterChain {
    pub fn new(filters: Vec<(String, FilterInstance)>) -> Result<Self, CreationError> {
        let subsystem = "filter";

        Ok(Self {
            filter_read_duration_seconds: filters
                .iter()
                .map(|(name, _)| {
                    Histogram::with_opts(
                        histogram_opts(
                            "read_duration_seconds",
                            subsystem,
                            "Seconds taken to execute a given filter's `read`.",
                            Some(
                                exponential_buckets(BUCKET_START, BUCKET_FACTOR, BUCKET_COUNT)
                                    .unwrap(),
                            ),
                        )
                        .const_label(FILTER_LABEL, name),
                    )
                    .and_then(|histogram| histogram.register_if_not_exists())
                })
                .collect::<Result<_, prometheus::Error>>()?,
            filter_write_duration_seconds: filters
                .iter()
                .map(|(name, _)| {
                    Histogram::with_opts(
                        histogram_opts(
                            "write_duration_seconds",
                            subsystem,
                            "Seconds taken to execute a given filter's `write`.",
                            Some(exponential_buckets(0.000125, 2.5, 11).unwrap()),
                        )
                        .const_label(FILTER_LABEL, name),
                    )
                    .and_then(|histogram| histogram.register_if_not_exists())
                })
                .collect::<Result<_, prometheus::Error>>()?,
            filters,
        })
    }

    pub fn testing<const N: usize>(filters: [FilterInstance; N]) -> Self {
        let filters = filters.into_iter().map(|f| (String::new(), f)).collect();
        Self::new(filters).unwrap()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = FilterConfig> + '_ {
        self.filters.iter().map(|(name, instance)| FilterConfig {
            name: name.clone(),
            label: instance.label().map(String::from),
            config: match instance.config() {
                serde_json::Value::Null => None,
                value => Some(value.clone()),
            },
        })
    }

    /// Validates the filter configurations in the provided config and constructs
    /// a [`Self`] if all configurations are valid, including the conversion
    /// into a [`Filter`]
    pub fn try_create_fallible<Item>(
        filter_configs: impl IntoIterator<Item = Item>,
    ) -> Result<Self, CreationError>
    where
        Item: TryInto<FilterConfig, Error = CreationError>,
    {
        let mut filters = Vec::new();

        for filter_config in filter_configs {
            let filter_config = filter_config.try_into()?;
            let filter = FilterRegistry::get(
                &filter_config.name,
                CreateFilterArgs::fixed(filter_config.config),
            )?;

            filters.push((filter_config.name, filter));
        }

        Self::new(filters)
    }

    /// Validates the filter configurations in the provided config and constructs
    /// a [`Self`] if all configurations are valid.
    pub fn try_create(
        filter_configs: impl IntoIterator<Item = FilterConfig>,
    ) -> Result<Self, CreationError> {
        let mut filters = Vec::new();

        for filter_config in filter_configs {
            let filter = FilterRegistry::get(
                &filter_config.name,
                CreateFilterArgs::fixed(filter_config.config),
            )?;

            filters.push((filter_config.name, filter));
        }

        Self::new(filters)
    }
}

impl std::fmt::Debug for FilterChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut filters = f.debug_struct("Filters");

        for (id, instance) in &self.filters {
            filters.field(id, instance.config());
        }

        filters.finish()
    }
}

impl PartialEq for FilterChain {
    fn eq(&self, rhs: &Self) -> bool {
        if self.filters.len() != rhs.filters.len() {
            return false;
        }

        self.filters.iter().zip(&rhs.filters).all(
            |((lhs_name, lhs_instance), (rhs_name, rhs_instance))| {
                lhs_name == rhs_name
                    && lhs_instance.config() == rhs_instance.config()
                    && lhs_instance.label() == rhs_instance.label()
            },
        )
    }
}

use crate::generated::envoy::config::listener::v3::FilterChain as EnvoyFilterChain;

impl TryFrom<FilterChain> for EnvoyFilterChain {
    type Error = CreationError;

    fn try_from(chain: FilterChain) -> Result<Self, Self::Error> {
        Self::try_from(&chain)
    }
}

impl TryFrom<&'_ FilterChain> for EnvoyFilterChain {
    type Error = CreationError;

    fn try_from(chain: &FilterChain) -> Result<Self, Self::Error> {
        Ok(Self {
            filters: chain
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, Self::Error>>()?,
            ..<_>::default()
        })
    }
}

impl TryFrom<&'_ FilterChain> for crate::net::cluster::proto::FilterChain {
    type Error = CreationError;

    fn try_from(value: &'_ FilterChain) -> Result<Self, Self::Error> {
        Ok(Self {
            filters: value
                .iter()
                .map(|filter| crate::net::cluster::proto::Filter {
                    name: filter.name,
                    label: filter.label,
                    config: filter.config.map(|v| v.to_string()),
                })
                .collect(),
        })
    }
}

impl std::ops::Index<usize> for FilterChain {
    type Output = (String, FilterInstance);

    fn index(&self, index: usize) -> &Self::Output {
        &self.filters[index]
    }
}

impl<'de> serde::Deserialize<'de> for FilterChain {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let filters = <Vec<FilterConfig>>::deserialize(de)?;

        Self::try_create(filters).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for FilterChain {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let filters = self
            .filters
            .iter()
            .map(|(name, instance)| FilterConfig {
                name: name.clone(),
                label: instance.label().map(String::from),
                config: Some(serde_json::Value::clone(instance.config())),
            })
            .collect::<Vec<_>>();

        filters.serialize(ser)
    }
}

impl schemars::JsonSchema for FilterChain {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        <Vec<FilterConfig>>::schema_name()
    }

    fn json_schema(sg: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        <Vec<FilterConfig>>::json_schema(sg)
    }
}

impl Filter for FilterChain {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        for ((id, instance), histogram) in self
            .filters
            .iter()
            .zip(self.filter_read_duration_seconds.iter())
        {
            tracing::trace!(%id, "read filtering packet");
            let timer = histogram.start_timer();
            let result = instance.filter().read(ctx);
            timer.stop_and_record();
            match result {
                Ok(()) => tracing::trace!(%id, "read passing packet"),
                Err(error) => {
                    tracing::trace!(%id, "read dropping packet");
                    return Err(error);
                }
            }
        }

        // Special case to handle to allow for pass-through, if no filter
        // has rejected, and the destinations is empty, we passthrough to all.
        // Which mimics the old behaviour while avoid clones in most cases.
        if ctx.destinations.is_empty() {
            ctx.destinations
                .extend(ctx.endpoints.endpoints().into_iter().map(|ep| ep.address));
        }

        Ok(())
    }

    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        for ((id, instance), histogram) in self
            .filters
            .iter()
            .rev()
            .zip(self.filter_write_duration_seconds.iter().rev())
        {
            tracing::trace!(%id, "write filtering packet");
            let timer = histogram.start_timer();
            let result = instance.filter().write(ctx);
            timer.stop_and_record();
            match result {
                Ok(()) => tracing::trace!(%id, "write passing packet"),
                Err(error) => {
                    tracing::trace!(%id, "write dropping packet");
                    return Err(error);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        filters::Debug,
        net::endpoint::Endpoint,
        test::{TestConfig, TestFilter, alloc_buffer},
    };

    use super::*;

    #[test]
    fn from_config() {
        let provider = Debug::factory();

        // everything is fine
        let filter_configs = [FilterConfig {
            name: provider.name().into(),
            label: None,
            config: Some(serde_json::Map::default().into()),
        }];

        let chain = FilterChain::try_create(filter_configs).unwrap();
        assert_eq!(1, chain.filters.len());

        // uh oh, something went wrong
        let filter_configs = [FilterConfig {
            name: "this is so wrong".into(),
            label: None,
            config: Default::default(),
        }];
        let result = FilterChain::try_create(filter_configs);
        assert!(result.is_err());
    }

    fn endpoints() -> std::sync::Arc<crate::net::cluster::ClusterMap> {
        crate::net::cluster::ClusterMap::new_default(
            [
                Endpoint::new("127.0.0.1:80".parse().unwrap()),
                Endpoint::new("127.0.0.1:90".parse().unwrap()),
            ]
            .into(),
        )
        .into()
    }

    #[tokio::test]
    async fn chain_single_test_filter() {
        crate::test::load_test_filters();
        let config = TestConfig::new();
        let endpoints_fixture = endpoints();
        let mut dest = Vec::new();
        let mut context = ReadContext::new(
            &endpoints_fixture,
            "127.0.0.1:70".parse().unwrap(),
            alloc_buffer(b"hello"),
            &mut dest,
        );

        config.filters.read(&mut context).unwrap();
        let expected = endpoints_fixture.clone();

        assert_eq!(&*expected.endpoints(), &*context.destinations);
        assert_eq!(
            "hello:odr:127.0.0.1:70",
            std::str::from_utf8(&context.contents).unwrap()
        );
        assert_eq!(
            "receive",
            context.metadata[&"downstream".into()].as_string().unwrap()
        );

        let mut context = WriteContext::new(
            endpoints_fixture
                .endpoints()
                .first()
                .unwrap()
                .address
                .clone(),
            "127.0.0.1:70".parse().unwrap(),
            alloc_buffer(b"hello"),
        );
        config.filters.write(&mut context).unwrap();

        assert_eq!(
            "receive",
            context.metadata[&"upstream".into()].as_string().unwrap()
        );
        assert_eq!(b"hello:our:127.0.0.1:80:127.0.0.1:70", &*context.contents,);
    }

    #[tokio::test]
    async fn chain_double_test_filter() {
        let chain = FilterChain::new(vec![
            (
                TestFilter::NAME.into(),
                FilterInstance::new(serde_json::json!(null), TestFilter.into()),
            ),
            (
                TestFilter::NAME.into(),
                FilterInstance::new(serde_json::json!(null), TestFilter.into()),
            ),
        ])
        .unwrap();

        let endpoints_fixture = endpoints();
        let mut dest = Vec::new();

        let (contents, metadata) = {
            let mut context = ReadContext::new(
                &endpoints_fixture,
                "127.0.0.1:70".parse().unwrap(),
                alloc_buffer(b"hello"),
                &mut dest,
            );
            chain.read(&mut context).unwrap();
            (context.contents, context.metadata)
        };
        let expected = endpoints_fixture.clone();
        assert_eq!(expected.endpoints(), dest);
        assert_eq!(b"hello:odr:127.0.0.1:70:odr:127.0.0.1:70", &*contents);
        assert_eq!(
            "receive:receive",
            metadata[&"downstream".into()].as_string().unwrap()
        );

        let mut context = WriteContext::new(
            endpoints_fixture
                .endpoints()
                .first()
                .unwrap()
                .address
                .clone(),
            "127.0.0.1:70".parse().unwrap(),
            alloc_buffer(b"hello"),
        );

        chain.write(&mut context).unwrap();
        assert_eq!(
            "hello:our:127.0.0.1:80:127.0.0.1:70:our:127.0.0.1:80:127.0.0.1:70",
            std::str::from_utf8(&context.contents).unwrap(),
        );
        assert_eq!(
            "receive:receive",
            context.metadata[&"upstream".into()].as_string().unwrap()
        );
    }

    #[test]
    fn get_configs() {
        let filter_chain = FilterChain::new(vec![(
            "TestFilter".into(),
            FilterInstance::new(serde_json::json!(null), TestFilter.into()),
        )])
        .unwrap();

        let configs = filter_chain.iter().collect::<Vec<_>>();
        assert_eq!(
            vec![FilterConfig {
                name: "TestFilter".into(),
                label: None,
                config: None,
            },],
            configs
        );
    }
}
