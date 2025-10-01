use crate::filters::{CreationError, FilterChain, FilterInstance};
use std::sync::Arc;

pub type FilterChangedSubscriber = tokio::sync::broadcast::Receiver<()>;
pub type CachedFilterChain = arc_swap::Cache<Arc<arc_swap::ArcSwap<FilterChain>>, Arc<FilterChain>>;

#[derive(Clone, Debug)]
pub struct FilterChainConfig {
    chain: Arc<arc_swap::ArcSwap<FilterChain>>,
    channel: tokio::sync::broadcast::Sender<()>,
}

impl FilterChainConfig {
    pub fn new(chain: FilterChain) -> Self {
        Self {
            chain: Arc::new(arc_swap::ArcSwap::new(Arc::new(chain))),
            channel: tokio::sync::broadcast::channel(1).0,
        }
    }

    #[inline]
    pub fn store(&self, new_chain: FilterChain) {
        {
            let cur = self.chain.load();
            if (*cur).as_ref() == &new_chain {
                return;
            }
        }

        self.chain.store(Arc::new(new_chain));
        tracing::debug!("sending new FilterChain notification");
        let _ = self.channel.send(());
    }

    #[inline]
    pub fn load(&self) -> arc_swap::Guard<Arc<FilterChain>> {
        self.chain.load()
    }

    #[inline]
    pub fn subscribe(&self) -> FilterChangedSubscriber {
        self.channel.subscribe()
    }

    /// Cached the filter chain, only reloading it if the filter changes
    #[inline]
    pub fn cached(&self) -> CachedFilterChain {
        arc_swap::Cache::new(self.chain.clone())
    }
}

impl Default for FilterChainConfig {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl PartialEq for FilterChainConfig {
    fn eq(&self, other: &Self) -> bool {
        *self.chain.load() == *other.chain.load()
    }
}

impl typemap_rev::TypeMapKey for FilterChain {
    type Value = FilterChainConfig;
}

impl super::DynamicConfig {
    pub fn filters(&self) -> Option<&FilterChainConfig> {
        self.typemap.get::<FilterChain>()
    }

    pub fn cached_filter_chain(&self) -> Option<CachedFilterChain> {
        self.typemap.get::<FilterChain>().map(|fc| fc.cached())
    }

    pub fn subscribe_filter_changes(&self) -> Option<FilterChangedSubscriber> {
        self.typemap.get::<FilterChain>().map(|fc| fc.subscribe())
    }
}

/// Filter is the configuration for a single filter
#[derive(
    Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub name: String,
    pub label: Option<String>,
    pub config: Option<serde_json::Value>,
}

impl TryFrom<crate::net::cluster::proto::Filter> for Filter {
    type Error = CreationError;

    fn try_from(value: crate::net::cluster::proto::Filter) -> Result<Self, Self::Error> {
        let config = if let Some(cfg) = value.config {
            Some(
                serde_json::from_str(&cfg)
                    .map_err(|err| CreationError::DeserializeFailed(err.to_string()))?,
            )
        } else {
            None
        };

        Ok(Self {
            name: value.name,
            label: value.label,
            config,
        })
    }
}

impl From<(String, FilterInstance)> for Filter {
    fn from((name, instance): (String, FilterInstance)) -> Self {
        Self {
            name,
            label: instance.label().map(String::from),
            config: Some(serde_json::Value::clone(instance.config())),
        }
    }
}

use crate::generated::envoy::config::listener::v3 as listener;

impl TryFrom<listener::Filter> for Filter {
    type Error = CreationError;

    fn try_from(filter: listener::Filter) -> Result<Self, Self::Error> {
        use listener::filter::ConfigType;

        let config = if let Some(config_type) = filter.config_type {
            let config = match config_type {
                ConfigType::TypedConfig(any) => any,
                ConfigType::ConfigDiscovery(_) => {
                    return Err(CreationError::FieldInvalid {
                        field: "config_type".into(),
                        reason: "ConfigDiscovery is currently unsupported".into(),
                    });
                }
            };
            Some(
                crate::filters::FilterRegistry::get_factory(&filter.name)
                    .ok_or_else(|| CreationError::NotFound(filter.name.clone()))?
                    .encode_config_to_json(config)?,
            )
        } else {
            None
        };

        Ok(Self {
            name: filter.name,
            // TODO: keep the label across xDS
            label: None,
            config,
        })
    }
}

impl TryFrom<Filter> for listener::Filter {
    type Error = CreationError;

    fn try_from(filter: Filter) -> Result<Self, Self::Error> {
        use listener::filter::ConfigType;

        let config = if let Some(config) = filter.config {
            Some(
                crate::filters::FilterRegistry::get_factory(&filter.name)
                    .ok_or_else(|| CreationError::NotFound(filter.name.clone()))?
                    .encode_config_to_protobuf(config)?,
            )
        } else {
            None
        };

        Ok(Self {
            name: filter.name,
            config_type: config.map(ConfigType::TypedConfig),
        })
    }
}
