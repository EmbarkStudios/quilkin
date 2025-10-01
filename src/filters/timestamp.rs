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

use std::sync::Arc;

use crate::{
    filters::prelude::*,
    metrics::Direction,
    net::endpoint::metadata::{self, Value},
};

use crate::generated::quilkin::filters::timestamp::v1alpha1 as proto;

/// A filter that reads a metadata value as a timestamp to be observed in
/// a histogram.
#[derive(Debug, Clone)]
pub struct Timestamp {
    config: Arc<Config>,
}

impl Timestamp {
    /// Observes the duration since a timestamp stored in `metadata` and now,
    /// if present.
    pub fn observe(&self, metadata: &metadata::DynamicMetadata, direction: Direction) {
        let Some(value) = metadata
            .get(&self.config.metadata_key)
            .and_then(|item| match item {
                Value::Number(item) => Some(*item as i64),
                Value::Bytes(vec) => Some(i64::from_be_bytes((**vec).try_into().ok()?)),
                _ => None,
            })
        else {
            return;
        };

        let Ok(datetime) = time::OffsetDateTime::from_unix_timestamp(value) else {
            tracing::warn!(
                timestamp = value,
                metadata_key = %self.config.metadata_key,
                "invalid unix timestamp"
            );
            return;
        };

        let seconds = (time::OffsetDateTime::now_utc() - datetime).as_seconds_f64();
        self.metric(direction).observe(seconds);
    }

    fn metric(&self, direction: Direction) -> prometheus::Histogram {
        crate::filters::metrics::histogram(
            Timestamp::NAME,
            "duration",
            "duration in seconds of time since the timestamp in metadata compared to now",
            direction,
            &[&*self.config.metadata_key.to_string()],
        )
    }
}

impl Timestamp {
    fn new(config: Config) -> Result<Self, CreationError> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl TryFrom<Config> for Timestamp {
    type Error = CreationError;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        Self::new(config)
    }
}

impl Filter for Timestamp {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        self.observe(&ctx.metadata, Direction::Read);
        Ok(())
    }

    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        self.observe(&ctx.metadata, Direction::Write);
        Ok(())
    }
}

impl StaticFilter for Timestamp {
    const NAME: &'static str = "quilkin.filters.timestamp.v1alpha1.Timestamp";
    type Configuration = Config;
    type BinaryConfiguration = proto::Timestamp;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Self::new(Self::ensure_config_exists(config)?)
    }
}

/// Config represents a [self]'s configuration.
#[derive(
    Clone, Debug, Eq, PartialEq, schemars::JsonSchema, serde::Serialize, serde::Deserialize,
)]
pub struct Config {
    /// The metadata key to read the UTC UNIX Timestamp from.
    #[serde(rename = "metadataKey")]
    pub metadata_key: metadata::Key,
}

impl Config {
    pub fn new(metadata_key: impl AsRef<str>) -> Self {
        Self {
            metadata_key: metadata_key.as_ref().into(),
        }
    }
}

impl From<Config> for proto::Timestamp {
    fn from(config: Config) -> Self {
        Self {
            metadata_key: Some(config.metadata_key.to_string()),
        }
    }
}

impl TryFrom<proto::Timestamp> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::Timestamp) -> Result<Self, Self::Error> {
        p.metadata_key
            .map(Self::new)
            .ok_or_else(|| ConvertProtoConfigError::missing_field("metadata_key"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        filters::capture::{self, Capture},
        test::alloc_buffer,
    };

    #[tokio::test]
    async fn basic() {
        const TIMESTAMP_KEY: &str = "BASIC";
        let filter = Timestamp::from_config(Config::new(TIMESTAMP_KEY).into());
        let mut dest = Vec::new();
        let cm = Default::default();
        let mut ctx = ReadContext::new(
            &cm,
            (std::net::Ipv4Addr::UNSPECIFIED, 0).into(),
            alloc_buffer(b"hello"),
            &mut dest,
        );
        ctx.metadata.insert(
            TIMESTAMP_KEY.into(),
            Value::Number(crate::time::UtcTimestamp::now().unix() as u64),
        );

        filter.read(&mut ctx).unwrap();

        assert_eq!(1, filter.metric(Direction::Read).get_sample_count());
    }

    #[tokio::test]
    async fn with_capture() {
        const TIMESTAMP_KEY: &str = "WITH_CAPTURE";
        let capture = Capture::from_config(
            capture::Config {
                metadata_key: TIMESTAMP_KEY.into(),
                strategy: capture::Suffix {
                    remove: true,
                    size: 8,
                }
                .into(),
            }
            .into(),
        );
        let timestamp = Timestamp::from_config(Config::new(TIMESTAMP_KEY).into());
        let source = (std::net::Ipv4Addr::UNSPECIFIED, 0);
        let mut dest = Vec::new();
        let cm = Default::default();
        let mut ctx = ReadContext::new(
            &cm,
            source.into(),
            alloc_buffer([0, 0, 0, 0, 99, 81, 55, 181]),
            &mut dest,
        );

        capture.read(&mut ctx).unwrap();
        timestamp.read(&mut ctx).unwrap();

        assert_eq!(1, timestamp.metric(Direction::Read).get_sample_count());
    }
}
