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

use rand::{thread_rng, Rng};

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::filters::ReadContext;

/// EndpointChooser chooses from a set of endpoints that a proxy is connected to.
pub trait EndpointChooser: Send + Sync {
    /// choose_endpoints asks for the next endpoint(s) to use.
    fn choose_endpoints(&self, endpoints: &mut ReadContext);
}

/// RoundRobinEndpointChooser chooses endpoints in round-robin order.
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
    fn choose_endpoints(&self, ctx: &mut ReadContext) {
        let count = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        // Note: The index is guaranteed to be in range.
        ctx.endpoints = vec![ctx.endpoints[count % ctx.endpoints.len()].clone()];
    }
}

/// RandomEndpointChooser chooses endpoints in random order.
pub struct RandomEndpointChooser;

impl EndpointChooser for RandomEndpointChooser {
    fn choose_endpoints(&self, ctx: &mut ReadContext) {
        // The index is guaranteed to be in range.
        let index = thread_rng().gen_range(0..ctx.endpoints.len());
        ctx.endpoints = vec![ctx.endpoints[index].clone()];
    }
}

/// HashEndpointChooser chooses endpoints based on a hash of source IP and port.
pub struct HashEndpointChooser;

impl EndpointChooser for HashEndpointChooser {
    fn choose_endpoints(&self, ctx: &mut ReadContext) {
        let mut hasher = DefaultHasher::new();
        ctx.source.hash(&mut hasher);
        ctx.endpoints = vec![ctx.endpoints[hasher.finish() as usize % ctx.endpoints.len()].clone()];
    }
}
