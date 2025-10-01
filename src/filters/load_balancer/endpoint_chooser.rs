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

use std::sync::atomic::{AtomicUsize, Ordering};

use rand::Rng;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::net::{ClusterMap, EndpointAddress};

/// Chooses from a set of endpoints that a proxy is connected to.
pub trait EndpointChooser: Send + Sync {
    /// Asks for the next endpoint(s) to use.
    fn choose_endpoints(
        &self,
        destinations: &mut Vec<EndpointAddress>,
        endpoints: &ClusterMap,
        src: &EndpointAddress,
    );
}

/// Chooses endpoints in round-robin order.
pub struct RoundRobinEndpointChooser {
    next_endpoint: AtomicUsize,
}

impl RoundRobinEndpointChooser {
    pub fn new() -> Self {
        RoundRobinEndpointChooser {
            next_endpoint: AtomicUsize::new(0),
        }
    }
}

impl EndpointChooser for RoundRobinEndpointChooser {
    fn choose_endpoints(
        &self,
        destinations: &mut Vec<EndpointAddress>,
        endpoints: &ClusterMap,
        _src: &EndpointAddress,
    ) {
        let count = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        // Note: The index is guaranteed to be in range.
        destinations.push(
            endpoints
                .nth_endpoint(count % endpoints.num_of_endpoints())
                .unwrap()
                .address
                .clone(),
        );
    }
}

/// Chooses endpoints in random order.
pub struct RandomEndpointChooser;

impl EndpointChooser for RandomEndpointChooser {
    fn choose_endpoints(
        &self,
        destinations: &mut Vec<EndpointAddress>,
        endpoints: &ClusterMap,
        _src: &EndpointAddress,
    ) {
        // The index is guaranteed to be in range.
        let index = rand::rng().random_range(0..endpoints.num_of_endpoints());
        destinations.push(endpoints.nth_endpoint(index).unwrap().address.clone());
    }
}

/// Chooses endpoints based on a hash of source IP and port.
pub struct HashEndpointChooser;

impl EndpointChooser for HashEndpointChooser {
    fn choose_endpoints(
        &self,
        destinations: &mut Vec<EndpointAddress>,
        endpoints: &ClusterMap,
        src: &EndpointAddress,
    ) {
        let mut hasher = DefaultHasher::new();
        src.hash(&mut hasher);
        destinations.push(
            endpoints
                .nth_endpoint(hasher.finish() as usize % endpoints.num_of_endpoints())
                .unwrap()
                .address
                .clone(),
        );
    }
}
