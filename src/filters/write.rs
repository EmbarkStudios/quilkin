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

use crate::net::endpoint::{DynamicMetadata, EndpointAddress};

#[cfg(doc)]
use crate::filters::Filter;

/// The input arguments to [`Filter::write`].
#[non_exhaustive]
pub struct WriteContext<P> {
    /// The source of the received packet.
    pub source: EndpointAddress,
    /// The destination of the received packet.
    pub dest: EndpointAddress,
    /// Contents of the received packet.
    pub contents: P,
    /// Arbitrary values that can be passed from one filter to another
    pub metadata: DynamicMetadata,
}

impl<P: super::PacketMut> WriteContext<P> {
    /// Creates a new [`WriteContext`]
    #[inline]
    pub fn new(source: EndpointAddress, dest: EndpointAddress, contents: P) -> Self {
        Self {
            source,
            dest,
            contents,
            metadata: <_>::default(),
        }
    }
}
