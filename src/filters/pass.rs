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

use crate::generated::quilkin::filters::pass::v1alpha1 as proto;

/// Allows a packet to pass through, mostly useful in combination with
/// other filters.
pub struct Pass;

impl Pass {
    fn new() -> Self {
        Self
    }
}

impl Filter for Pass {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip_all))]
    fn read<P: PacketMut>(&self, _: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        Ok(())
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(skip_all))]
    fn write<P: PacketMut>(&self, _: &mut WriteContext<P>) -> Result<(), FilterError> {
        Ok(())
    }
}

impl StaticFilter for Pass {
    const NAME: &'static str = "quilkin.filters.pass.v1alpha1.Pass";
    type Configuration = Config;
    type BinaryConfiguration = proto::Pass;

    fn try_from_config(_config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Pass::new())
    }
}

/// `pass` filter's configuration.
#[derive(Serialize, Deserialize, Debug, schemars::JsonSchema)]
pub struct Config;

impl From<Config> for proto::Pass {
    fn from(_config: Config) -> Self {
        Self {}
    }
}

impl TryFrom<proto::Pass> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(_: proto::Pass) -> Result<Self, Self::Error> {
        Ok(Config)
    }
}
