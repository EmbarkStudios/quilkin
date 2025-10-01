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

use super::endpoint_chooser::{
    EndpointChooser, HashEndpointChooser, RandomEndpointChooser, RoundRobinEndpointChooser,
};
use super::proto;

/// The configuration for [`load_balancer`][super].
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, JsonSchema)]
pub struct Config {
    #[serde(default)]
    pub policy: Policy,
}

impl From<Config> for super::proto::LoadBalancer {
    fn from(config: Config) -> Self {
        Self {
            policy: Some(config.policy.into()),
        }
    }
}

impl From<proto::LoadBalancer> for Config {
    fn from(p: proto::LoadBalancer) -> Self {
        Self {
            policy: p
                .policy
                .map(|p| p.value())
                .map(Policy::from)
                .unwrap_or_default(),
        }
    }
}

/// Policy represents how a [`load_balancer`][super] distributes
/// packets across endpoints.
#[derive(Debug, Default, Deserialize, Serialize, Eq, PartialEq, JsonSchema)]
pub enum Policy {
    /// Send packets to endpoints in turns.
    #[serde(rename = "ROUND_ROBIN")]
    #[default]
    RoundRobin,
    /// Send packets to endpoints chosen at random.
    #[serde(rename = "RANDOM")]
    Random,
    /// Send packets to endpoints based on hash of source IP and port.
    #[serde(rename = "HASH")]
    Hash,
}

impl Policy {
    pub fn as_endpoint_chooser(&self) -> Box<dyn EndpointChooser> {
        match self {
            Policy::RoundRobin => Box::new(RoundRobinEndpointChooser::new()),
            Policy::Random => Box::new(RandomEndpointChooser),
            Policy::Hash => Box::new(HashEndpointChooser),
        }
    }
}

impl From<Policy> for proto::load_balancer::Policy {
    fn from(policy: Policy) -> Self {
        match policy {
            Policy::RoundRobin => Self::RoundRobin,
            Policy::Random => Self::Random,
            Policy::Hash => Self::Hash,
        }
    }
}

impl From<proto::load_balancer::Policy> for Policy {
    fn from(policy: proto::load_balancer::Policy) -> Self {
        match policy {
            proto::load_balancer::Policy::RoundRobin => Self::RoundRobin,
            proto::load_balancer::Policy::Random => Self::Random,
            proto::load_balancer::Policy::Hash => Self::Hash,
        }
    }
}

impl From<Policy> for proto::load_balancer::PolicyValue {
    fn from(policy: Policy) -> Self {
        Self {
            value: proto::load_balancer::Policy::from(policy) as i32,
        }
    }
}
