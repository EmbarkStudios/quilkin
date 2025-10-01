/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

use crate::generated::quilkin::filters::concatenate::v1alpha1 as proto;

mod config;

use crate::filters::prelude::*;

pub use config::{Config, Strategy};

/// The `Concatenate` filter's job is to add a byte packet to either the
/// beginning or end of each UDP packet that passes through. This is commonly
/// used to provide an auth token to each packet, so they can be
/// routed appropriately.
pub struct Concatenate {
    on_read: Strategy,
    on_write: Strategy,
    bytes: Vec<u8>,
}

impl Concatenate {
    pub fn new(config: Config) -> Self {
        Self {
            on_read: config.on_read,
            on_write: config.on_write,
            bytes: config.bytes,
        }
    }

    pub fn testing(config: Config) -> Self {
        Self::new(config)
    }
}

impl Filter for Concatenate {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        match self.on_read {
            Strategy::Append => {
                ctx.contents.extend_tail(&self.bytes);
            }
            Strategy::Prepend => {
                ctx.contents.extend_head(&self.bytes);
            }
            Strategy::DoNothing => {}
        }

        Ok(())
    }

    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        match self.on_write {
            Strategy::Append => {
                ctx.contents.extend_tail(&self.bytes);
            }
            Strategy::Prepend => {
                ctx.contents.extend_head(&self.bytes);
            }
            Strategy::DoNothing => {}
        }

        Ok(())
    }
}

impl StaticFilter for Concatenate {
    const NAME: &'static str = "quilkin.filters.concatenate.v1alpha1.Concatenate";
    type Configuration = Config;
    type BinaryConfiguration = proto::Concatenate;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Concatenate::new(Self::ensure_config_exists(config)?))
    }
}
