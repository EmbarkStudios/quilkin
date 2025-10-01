/*
 * Copyright 2021 Google LLC
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

use serde::{Deserialize, Serialize};

use super::proto;
use crate::{
    config::filter::Filter,
    filters::{ConvertProtoConfigError, StaticFilter},
};

/// Configuration for [`Match`][super::Match].
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Configuration for [`Filter::read`][crate::filters::Filter::read].
    pub on_read: Option<DirectionalConfig>,
    /// Configuration for [`Filter::write`][crate::filters::Filter::write].
    pub on_write: Option<DirectionalConfig>,
}

impl TryFrom<Config> for proto::Match {
    type Error = crate::filters::CreationError;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        Ok(Self {
            on_read: config.on_read.map(TryFrom::try_from).transpose()?,
            on_write: config.on_write.map(TryFrom::try_from).transpose()?,
        })
    }
}

impl TryFrom<proto::Match> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(value: proto::Match) -> Result<Self, Self::Error> {
        Ok(Self {
            on_read: value
                .on_read
                .map(proto::r#match::Config::try_into)
                .transpose()
                .map_err(|error: eyre::Report| {
                    ConvertProtoConfigError::new(error, Some("on_read".into()))
                })?,
            on_write: value
                .on_write
                .map(proto::r#match::Config::try_into)
                .transpose()
                .map_err(|error: eyre::Report| {
                    ConvertProtoConfigError::new(error, Some("on_write".into()))
                })?,
        })
    }
}

/// Configuration for a specific direction.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, schemars::JsonSchema)]
pub struct DirectionalConfig {
    /// The key for the metadata to compare against.
    #[serde(rename = "metadataKey")]
    pub metadata_key: crate::net::endpoint::metadata::Key,
    /// List of filters to compare and potentially run if any match.
    pub branches: Vec<Branch>,
    /// The behaviour for when none of the `branches` match.
    #[serde(default)]
    pub fallthrough: Fallthrough,
}

impl TryFrom<DirectionalConfig> for proto::r#match::Config {
    type Error = crate::filters::CreationError;

    fn try_from(config: DirectionalConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            metadata_key: Some(config.metadata_key.to_string()),
            branches: config
                .branches
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            fallthrough: config.fallthrough.try_into().map(Some)?,
        })
    }
}

impl TryFrom<proto::r#match::Config> for DirectionalConfig {
    type Error = eyre::Report;

    fn try_from(value: proto::r#match::Config) -> Result<Self, Self::Error> {
        Ok(Self {
            metadata_key: value.metadata_key.map(From::from).ok_or_else(|| {
                ConvertProtoConfigError::new("Missing", Some("metadata_key".into()))
            })?,
            branches: value
                .branches
                .into_iter()
                .map(proto::r#match::Branch::try_into)
                .collect::<Result<_, _>>()?,
            fallthrough: value
                .fallthrough
                .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("fallthrough".into())))?
                .try_into()
                .map(Fallthrough)?,
        })
    }
}

/// A specific match branch. The filter is run when `value` matches the value
/// defined in `metadata_key`.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, schemars::JsonSchema)]
pub struct Branch {
    /// The value to compare against the dynamic metadata.
    pub value: crate::net::endpoint::metadata::Value,
    /// The filter to run on successful matches.
    #[serde(flatten)]
    pub filter: Filter,
}

impl TryFrom<Branch> for proto::r#match::Branch {
    type Error = crate::filters::CreationError;

    fn try_from(branch: Branch) -> Result<Self, Self::Error> {
        Ok(Self {
            value: Some(branch.value.into()),
            filter: branch.filter.try_into().map(Some)?,
        })
    }
}

impl TryFrom<proto::r#match::Branch> for Branch {
    type Error = eyre::Report;

    fn try_from(branch: proto::r#match::Branch) -> Result<Self, Self::Error> {
        Ok(Self {
            value: branch
                .value
                .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("value".into())))?
                .try_into()?,
            filter: branch
                .filter
                .map(|filter| filter.try_into())
                .transpose()?
                .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("filter".into())))?,
        })
    }
}

/// The behaviour when the none of branches match. Defaults to dropping packets.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct Fallthrough(pub Filter);

impl Default for Fallthrough {
    fn default() -> Self {
        Self(crate::filters::Drop::as_filter_config(None).unwrap())
    }
}

impl TryFrom<Fallthrough> for crate::generated::envoy::config::listener::v3::Filter {
    type Error = crate::filters::CreationError;
    fn try_from(fallthrough: Fallthrough) -> Result<Self, Self::Error> {
        fallthrough.0.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        let matches_yaml = "
on_read:
    metadataKey: quilkin.dev/captured_bytes
    branches:
        - value: abc
          name: quilkin.filters.debug.v1alpha1.Debug
        ";

        let config = serde_yaml::from_str::<Config>(matches_yaml).unwrap();

        assert_eq!(
            config,
            Config {
                on_read: Some(DirectionalConfig {
                    metadata_key: "quilkin.dev/captured_bytes".into(),
                    branches: vec![Branch {
                        value: String::from("abc").into(),
                        filter: crate::filters::Debug::as_filter_config(None).unwrap(),
                    }],
                    fallthrough: <_>::default(),
                }),
                on_write: None,
            }
        );
    }
}
