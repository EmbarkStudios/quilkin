/*
 * Copyright 2020 Google LLC
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

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;

use crate::filters::{
    CreateFilterArgs, CreationError, DynFilterFactory, FilterInstance, FilterSet,
};

static REGISTRY: Lazy<ArcSwap<FilterSet>> =
    Lazy::new(|| ArcSwap::new(std::sync::Arc::new(FilterSet::default())));

/// Registry of all [`Filter`][crate::filters::Filter]s that can be applied in the system.
///
/// **Note:** Cloning [`FilterRegistry`], clones a new reference to the data and
/// does not clone the data itself. In other words the clone is "shallow" and
/// not deep.
#[derive(Debug)]
pub struct FilterRegistry;

impl FilterRegistry {
    /// Loads the provided [`FilterSet`] into the registry of available filters.
    pub fn register(factories: impl IntoIterator<Item = DynFilterFactory>) {
        let mut registry = FilterSet::clone(&REGISTRY.load_full());
        for factory in factories {
            registry.insert(factory);
        }

        REGISTRY.store(std::sync::Arc::from(registry));
    }

    /// Creates and returns a new dynamic instance of [`Filter`][crate::filters::Filter] for a given
    /// `key`. Errors if the filter cannot be found, or if there is a
    /// configuration issue.
    pub fn get(key: &str, args: CreateFilterArgs) -> Result<FilterInstance, CreationError> {
        match REGISTRY.load().get(key).map(|p| p.create_filter(args)) {
            None => Err(CreationError::NotFound(key.to_owned())),
            Some(filter) => filter,
        }
    }

    /// Returns a [`DynFilterFactory`] for a given `key`. Returning `None` if the
    /// factory cannot be found.
    pub fn get_factory(key: &str) -> Option<std::sync::Arc<DynFilterFactory>> {
        REGISTRY.load().get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use crate::test::{alloc_buffer, load_test_filters};

    use super::*;
    use crate::filters::{
        Filter, FilterError, FilterRegistry, PacketMut, ReadContext, WriteContext,
    };
    use crate::net::endpoint::{Endpoint, EndpointAddress};

    #[allow(dead_code)]
    struct TestFilter {}

    impl Filter for TestFilter {
        fn read<P: PacketMut>(&self, _: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
            Err(FilterError::Custom("test error"))
        }

        fn write<P: PacketMut>(&self, _: &mut WriteContext<P>) -> Result<(), FilterError> {
            Err(FilterError::Custom("test error"))
        }
    }

    #[tokio::test]
    async fn insert_and_get() {
        load_test_filters();

        match FilterRegistry::get(&String::from("not.found"), CreateFilterArgs::fixed(None)) {
            Ok(_) => unreachable!("should not be filter"),
            Err(err) => assert_eq!(CreationError::NotFound("not.found".to_string()), err),
        };

        assert!(
            FilterRegistry::get(&String::from("TestFilter"), CreateFilterArgs::fixed(None)).is_ok()
        );

        let instance =
            FilterRegistry::get(&String::from("TestFilter"), CreateFilterArgs::fixed(None))
                .unwrap();
        let filter = instance.filter();

        let addr: EndpointAddress = (Ipv4Addr::LOCALHOST, 8080).into();
        let endpoint = Endpoint::new(addr.clone());

        let endpoints = crate::net::cluster::ClusterMap::new_default([endpoint.clone()].into());
        let mut dest = Vec::new();
        assert!(
            filter
                .read(&mut ReadContext::new(
                    &endpoints,
                    addr.clone(),
                    alloc_buffer([]),
                    &mut dest,
                ))
                .is_ok()
        );
        assert!(
            filter
                .write(&mut WriteContext::new(addr.clone(), addr, alloc_buffer([])))
                .is_ok()
        );
    }
}
