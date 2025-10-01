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

use crate::filters::prelude::*;
use serde::{Deserialize, Serialize};

use crate::generated::quilkin::filters::drop::v1alpha1 as proto;

pub const NAME: &str = Drop::NAME;

/// Always drops a packet, mostly useful in combination with other filters.
pub struct Drop;

impl Drop {
    fn new() -> Self {
        Self
    }
}

impl Filter for Drop {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip_all))]
    fn read<P: PacketMut>(&self, _: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        Err(FilterError::Dropped)
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(skip_all))]
    fn write<P: PacketMut>(&self, _: &mut WriteContext<P>) -> Result<(), FilterError> {
        Err(FilterError::Dropped)
    }
}

impl StaticFilter for Drop {
    const NAME: &'static str = "quilkin.filters.drop.v1alpha1.Drop";
    type Configuration = Config;
    type BinaryConfiguration = proto::Drop;

    fn try_from_config(_: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Drop::new())
    }
}

/// `pass` filter's configuration.
#[derive(Serialize, Deserialize, Debug, schemars::JsonSchema)]
pub struct Config;

impl From<Config> for proto::Drop {
    fn from(_config: Config) -> Self {
        Self {}
    }
}

impl TryFrom<proto::Drop> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(_: proto::Drop) -> Result<Self, Self::Error> {
        Ok(Config)
    }
}
