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

use crate::generated::quilkin::filters::debug::v1alpha1 as proto;

use crate::filters::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Debug logs all incoming and outgoing packets
#[derive(Debug)]
pub struct Debug {
    config: Config,
}

impl Debug {
    /// Constructor for the Debug. Pass in a "id" to append a string to your log messages from this
    /// Filter.
    fn new(config: Option<Config>) -> Self {
        Self {
            config: config.unwrap_or_default(),
        }
    }

    pub fn testing(config: Option<Config>) -> Self {
        Self::new(config)
    }
}

impl Filter for Debug {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        info!(id = ?self.config.id, source = ?&ctx.source, contents = ?String::from_utf8_lossy(ctx.contents.as_slice()), "Read filter event");
        Ok(())
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        info!(
            id = ?self.config.id,
            source = ?&ctx.source,
            dest = ?&ctx.dest,
            contents = ?String::from_utf8_lossy(ctx.contents.as_slice()), "Write filter event"
        );
        Ok(())
    }
}

impl StaticFilter for Debug {
    const NAME: &'static str = "quilkin.filters.debug.v1alpha1.Debug";
    type Configuration = Config;
    type BinaryConfiguration = proto::Debug;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Debug::new(config))
    }
}

/// A Debug filter's configuration.
#[derive(Serialize, Default, Deserialize, Debug, schemars::JsonSchema)]
pub struct Config {
    /// Identifier that will be optionally included with each log message.
    pub id: Option<String>,
}

impl From<Config> for proto::Debug {
    fn from(config: Config) -> Self {
        Self { id: config.id }
    }
}

impl TryFrom<proto::Debug> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::Debug) -> Result<Self, Self::Error> {
        Ok(Config { id: p.id })
    }
}

#[cfg(test)]
mod tests {
    use crate::test::{assert_filter_read_no_change, assert_write_no_change};
    use tracing_test::traced_test;

    use super::*;

    #[traced_test]
    #[tokio::test]
    async fn read() {
        let df = Debug::new(None);
        assert_filter_read_no_change(&df);
        assert!(logs_contain("Read filter event"));
    }

    #[traced_test]
    #[tokio::test]
    async fn write() {
        let df = Debug::new(None);
        assert_write_no_change(&df);
        assert!(logs_contain("Write filter event"));
        assert!(logs_contain("quilkin::filters::debug")); // the given name to the the logger by tracing
    }

    #[test]
    fn from_config_with_id() {
        let config = serde_json::json!({ "id": "name", });
        Debug::from_config(Some(serde_json::from_value(config).unwrap()));
    }

    #[test]
    fn from_config_without_id() {
        let config = serde_json::json!({});
        Debug::from_config(Some(serde_json::from_value(config).unwrap()));
    }

    #[test]
    fn from_config_should_error() {
        serde_json::from_value::<Config>(serde_json::json!({ "id": {} })).unwrap_err();
    }
}
