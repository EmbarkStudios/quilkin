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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::proto;
use crate::config::Base64Standard;

#[derive(Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum Strategy {
    #[serde(rename = "APPEND")]
    Append,
    #[serde(rename = "PREPEND")]
    Prepend,
    #[serde(rename = "DO_NOTHING")]
    #[default]
    DoNothing,
}

impl From<Strategy> for proto::concatenate::Strategy {
    fn from(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Append => Self::Append,
            Strategy::Prepend => Self::Prepend,
            Strategy::DoNothing => Self::DoNothing,
        }
    }
}

impl From<proto::concatenate::Strategy> for Strategy {
    fn from(strategy: proto::concatenate::Strategy) -> Self {
        match strategy {
            proto::concatenate::Strategy::Append => Self::Append,
            proto::concatenate::Strategy::Prepend => Self::Prepend,
            proto::concatenate::Strategy::DoNothing => Self::DoNothing,
        }
    }
}

impl From<Strategy> for proto::concatenate::StrategyValue {
    fn from(strategy: Strategy) -> Self {
        Self {
            value: proto::concatenate::Strategy::from(strategy) as i32,
        }
    }
}

/// Config represents a `Concatenate` filter configuration.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, JsonSchema)]
pub struct Config {
    /// Whether or not to `append` or `prepend` or `do nothing` on Filter `Read`
    #[serde(default)]
    pub on_read: Strategy,
    /// Whether or not to `append` or `prepend` or `do nothing` on Filter `Write`
    #[serde(default)]
    pub on_write: Strategy,

    #[serde(
        deserialize_with = "Base64Standard::deserialize",
        serialize_with = "Base64Standard::serialize"
    )]
    pub bytes: Vec<u8>,
}

impl From<Config> for proto::Concatenate {
    fn from(config: Config) -> Self {
        Self {
            on_read: Some(config.on_read.into()),
            on_write: Some(config.on_write.into()),
            bytes: config.bytes,
        }
    }
}

impl From<proto::Concatenate> for Config {
    fn from(p: proto::Concatenate) -> Self {
        let on_read = p
            .on_read
            .map(|p| p.value())
            .map(Strategy::from)
            .unwrap_or_default();

        let on_write = p
            .on_write
            .map(|p| p.value())
            .map(Strategy::from)
            .unwrap_or_default();

        Self {
            on_read,
            on_write,
            bytes: p.bytes,
        }
    }
}
