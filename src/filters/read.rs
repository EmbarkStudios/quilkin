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

#[cfg(doc)]
use crate::filters::Filter;
use crate::net::{
    ClusterMap,
    endpoint::{EndpointAddress, metadata::DynamicMetadata},
};

/// The input arguments to [`Filter::read`].
pub struct ReadContext<'ctx, P> {
    /// The upstream endpoints that the packet will be forwarded to.
    pub endpoints: &'ctx ClusterMap,
    /// The upstream endpoints that the packet will be forwarded to.
    pub destinations: &'ctx mut Vec<EndpointAddress>,
    /// The source of the received packet.
    pub source: EndpointAddress,
    /// Contents of the received packet.
    pub contents: P,
    /// Arbitrary values that can be passed from one filter to another.
    pub metadata: DynamicMetadata,
}

impl<'ctx, P: super::PacketMut> ReadContext<'ctx, P> {
    /// Creates a new [`ReadContext`].
    #[inline]
    pub fn new(
        endpoints: &'ctx ClusterMap,
        source: EndpointAddress,
        contents: P,
        destinations: &'ctx mut Vec<EndpointAddress>,
    ) -> Self {
        Self {
            endpoints,
            destinations,
            source,
            contents,
            metadata: <_>::default(),
        }
    }
}
