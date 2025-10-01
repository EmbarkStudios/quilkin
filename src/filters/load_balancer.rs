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

use crate::generated::quilkin::filters::load_balancer::v1alpha1 as proto;

mod config;
mod endpoint_chooser;

use crate::filters::prelude::*;
use endpoint_chooser::EndpointChooser;

pub use config::{Config, Policy};

/// Balances packets over the upstream endpoints.
pub struct LoadBalancer {
    endpoint_chooser: Box<dyn EndpointChooser>,
}

impl LoadBalancer {
    fn new(config: Config) -> Self {
        Self {
            endpoint_chooser: config.policy.as_endpoint_chooser(),
        }
    }

    pub fn testing(config: Config) -> Self {
        Self::new(config)
    }
}

impl Filter for LoadBalancer {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        self.endpoint_chooser
            .choose_endpoints(ctx.destinations, ctx.endpoints, &ctx.source);
        Ok(())
    }
}

impl StaticFilter for LoadBalancer {
    const NAME: &'static str = "quilkin.filters.load_balancer.v1alpha1.LoadBalancer";
    type Configuration = Config;
    type BinaryConfiguration = proto::LoadBalancer;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(LoadBalancer::new(Self::ensure_config_exists(config)?))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::Ipv4Addr};

    use super::*;
    use crate::{
        net::endpoint::{Endpoint, EndpointAddress},
        test::alloc_buffer,
    };

    fn get_response_addresses(
        filter: &LoadBalancer,
        input_addresses: &[EndpointAddress],
        source: EndpointAddress,
    ) -> Vec<EndpointAddress> {
        let endpoints = input_addresses
            .iter()
            .cloned()
            .map(Endpoint::new)
            .collect::<std::collections::BTreeSet<_>>();
        let endpoints = crate::net::cluster::ClusterMap::new_default(endpoints);
        let mut dest = Vec::new();
        {
            let mut context = ReadContext::new(&endpoints, source, alloc_buffer([]), &mut dest);
            filter.read(&mut context).unwrap();
        }

        dest
    }

    #[tokio::test]
    async fn round_robin_load_balancer_policy() {
        let addresses: Vec<EndpointAddress> = vec![
            ([127, 0, 0, 1], 8080).into(),
            ([127, 0, 0, 2], 8080).into(),
            ([127, 0, 0, 3], 8080).into(),
        ];

        let yaml = "policy: ROUND_ROBIN";
        let filter = LoadBalancer::from_config(serde_yaml::from_str(yaml).unwrap());

        // Check that we repeat the same addresses in sequence forever.
        let expected_sequence = addresses
            .iter()
            .map(|addr| vec![addr.clone()])
            .collect::<Vec<_>>();

        for _ in 0..10 {
            assert_eq!(expected_sequence, {
                let mut responses = Vec::new();
                for _ in 0..addresses.len() {
                    responses.push(get_response_addresses(
                        &filter,
                        &addresses,
                        "127.0.0.1:8080".parse().unwrap(),
                    ));
                }
                responses
            });
        }
    }

    #[tokio::test]
    async fn random_load_balancer_policy() {
        let addresses = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.2:8080".parse().unwrap(),
            "127.0.0.3:8080".parse().unwrap(),
        ];

        let yaml = "
policy: RANDOM
";
        let filter = LoadBalancer::from_config(serde_yaml::from_str(yaml).unwrap());

        // Run a few selection rounds through the addresses.
        let mut result_sequences = vec![];
        for _ in 0..10 {
            for _ in 0..addresses.len() {
                result_sequences.push(get_response_addresses(
                    &filter,
                    &addresses,
                    "127.0.0.1:8080".parse().unwrap(),
                ));
            }
        }

        // Check that every address was chosen at least once.
        assert_eq!(
            addresses.into_iter().collect::<HashSet<_>>(),
            result_sequences
                .clone()
                .into_iter()
                .flatten()
                .collect::<HashSet<_>>(),
        );

        // Check that there is at least one different sequence of addresses.
        assert!(
            &result_sequences[1..]
                .iter()
                .any(|seq| seq != &result_sequences[0]),
            "the same sequence of addresses were chosen for random load balancer"
        );
    }

    #[tokio::test]
    async fn hash_load_balancer_policy() {
        let addresses: Vec<EndpointAddress> = vec![
            ([127, 0, 0, 1], 8080).into(),
            ([127, 0, 0, 2], 8080).into(),
            ([127, 0, 0, 3], 8080).into(),
        ];
        let source_ips = vec![[127u8, 1, 1, 1], [127, 2, 2, 2], [127, 3, 3, 3]];
        let source_ports = [11111u16, 22222, 33333, 44444, 55555];

        let yaml = "policy: HASH";
        let filter = LoadBalancer::from_config(serde_yaml::from_str(yaml).unwrap());

        // Run a few selection rounds through the addresses.
        let mut result_sequences = vec![];
        for _ in 0..10 {
            for _ in 0..addresses.len() {
                result_sequences.push(get_response_addresses(
                    &filter,
                    &addresses,
                    (Ipv4Addr::LOCALHOST, 8080).into(),
                ));
            }
        }

        // Verify that all packets went the same way
        assert_eq!(
            1,
            result_sequences
                .into_iter()
                .flatten()
                .collect::<HashSet<_>>()
                .len(),
        );

        // Run a few selection rounds through the address
        // this time vary the port for a single IP
        let mut result_sequences = vec![];
        for port in source_ports.iter().copied() {
            result_sequences.push(vec![
                get_response_addresses(
                    &filter,
                    &addresses,
                    (Ipv4Addr::LOCALHOST, port).into()
                );
                addresses.len()
            ]);
        }

        // Verify that more than 1 path was picked
        assert_ne!(
            1,
            result_sequences
                .into_iter()
                .flatten()
                .flatten()
                .collect::<HashSet<_>>()
                .len(),
        );

        // Run a few selection rounds through the addresses
        // This time vary the source IP and port
        let mut result_sequences = vec![];
        for ip in source_ips {
            for port in source_ports.iter().copied() {
                result_sequences.push(vec![
                    get_response_addresses(
                        &filter,
                        &addresses,
                        (ip, port).into()
                    );
                    addresses.len()
                ]);
            }
        }

        // Check that every address was chosen at least once.
        assert_eq!(
            addresses.into_iter().collect::<HashSet<_>>(),
            result_sequences
                .clone()
                .into_iter()
                .flatten()
                .flatten()
                .collect::<HashSet<_>>(),
        );

        // Check that there is at least one different sequence of addresses.
        assert!(
            &result_sequences[1..]
                .iter()
                .any(|seq| seq != &result_sequences[0]),
            "the same sequence of addresses were chosen for hash load balancer"
        );
    }
}
