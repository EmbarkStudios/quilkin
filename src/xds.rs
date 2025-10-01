/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use ::quilkin_xds::generated::quilkin::config::v1alpha1 as proto;
use prost::Message;
use prost_types::Any;

pub const CLUSTER_TYPE: &str = "type.googleapis.com/quilkin.config.v1alpha1.Cluster";
pub const DATACENTER_TYPE: &str = "type.googleapis.com/quilkin.config.v1alpha1.Datacenter";
pub const FILTER_CHAIN_TYPE: &str = "type.googleapis.com/quilkin.config.v1alpha1.FilterChain";
const PREFIX: &str = "type.googleapis.com/quilkin.config.v1alpha1.";

pub enum Resource {
    Cluster(proto::Cluster),
    Datacenter(proto::Datacenter),
    FilterChain(proto::FilterChain),
}

impl Resource {
    #[inline]
    pub fn try_decode(any: Any) -> Result<Self, eyre::Error> {
        let Some(suffix) = any.type_url.strip_prefix(PREFIX) else {
            eyre::bail!("unknown resource type '{}'", any.type_url);
        };

        Ok(match suffix {
            "Cluster" => Self::Cluster(proto::Cluster::decode(&*any.value)?),
            "Datacenter" => Self::Datacenter(proto::Datacenter::decode(&*any.value)?),
            "FilterChain" => Self::FilterChain(proto::FilterChain::decode(&*any.value)?),
            _ => eyre::bail!("unknown resource type '{}'", any.type_url),
        })
    }

    #[inline]
    pub fn try_encode(&self) -> Result<Any, prost::EncodeError> {
        let (value, type_url) = match self {
            Self::Cluster(c) => {
                let mut value = Vec::with_capacity(c.encoded_len());
                c.encode(&mut value)?;
                (value, CLUSTER_TYPE)
            }
            Self::Datacenter(d) => {
                let mut value = Vec::with_capacity(d.encoded_len());
                d.encode(&mut value)?;
                (value, DATACENTER_TYPE)
            }
            Self::FilterChain(f) => {
                let mut value = Vec::with_capacity(f.encoded_len());
                f.encode(&mut value)?;
                (value, FILTER_CHAIN_TYPE)
            }
        };

        Ok(Any {
            type_url: type_url.into(),
            value,
        })
    }

    #[inline]
    pub fn type_url(&self) -> &'static str {
        match self {
            Self::Cluster(_) => CLUSTER_TYPE,
            Self::Datacenter(_) => DATACENTER_TYPE,
            Self::FilterChain(_) => FILTER_CHAIN_TYPE,
        }
    }
}

#[derive(Copy, Clone)]
pub enum ResourceType {
    Cluster,
    Datacenter,
    FilterChain,
}

impl std::str::FromStr for ResourceType {
    type Err = eyre::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.strip_prefix(PREFIX) {
            Some("Cluster") => Self::Cluster,
            Some("Datacenter") => Self::Datacenter,
            Some("FilterChain") => Self::FilterChain,
            _ => eyre::bail!("unknown resource type '{s}'"),
        })
    }
}
