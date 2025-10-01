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

use std::{iter::FromIterator, sync::Arc};

use crate::filters::{self, DynFilterFactory, StaticFilter};

#[cfg(doc)]
use crate::filters::{FilterFactory, FilterRegistry};

/// A map of [`FilterFactory::name`]s to [`DynFilterFactory`] values.
pub type FilterMap = std::collections::HashMap<&'static str, Arc<DynFilterFactory>>;

/// A set of filters to be registered with a [`FilterRegistry`].
///
/// Current default filters:
/// - [`debug`][filters::debug]
/// - [`local_rate_limit`][filters::local_rate_limit]
/// - [`concatenate`][filters::concatenate]
/// - [`load_balancer`][filters::load_balancer]
/// - [`capture`][filters::capture]
/// - [`token_router`][filters::token_router]
/// - [`hashed_token_router`][filters::token_router]
#[derive(Clone)]
pub struct FilterSet(FilterMap);

impl FilterSet {
    /// Returns a `FilterSet` with the filters provided through `filters` in
    /// addition to the defaults. Any filter factories provided by `filters`
    /// will override any defaults with a matching name.
    pub fn default_with(filters: impl IntoIterator<Item = DynFilterFactory>) -> Self {
        Self::with(
            [
                filters::Capture::factory(),
                filters::Concatenate::factory(),
                filters::Debug::factory(),
                filters::Drop::factory(),
                filters::Firewall::factory(),
                filters::HashedTokenRouter::factory(),
                filters::LoadBalancer::factory(),
                filters::LocalRateLimit::factory(),
                filters::Match::factory(),
                filters::Pass::factory(),
                filters::Timestamp::factory(),
                filters::TokenRouter::factory(),
            ]
            .into_iter()
            .chain(filters),
        )
    }

    /// Creates a new [`FilterSet`] with the set of `filter_factories` without
    /// any defaults.
    pub fn with(filters: impl IntoIterator<Item = DynFilterFactory>) -> Self {
        Self::from_iter(filters)
    }

    /// Returns a [`DynFilterFactory`] if one matches `id`, otherwise returns
    /// `None`.
    pub fn get(&self, key: &str) -> Option<&Arc<DynFilterFactory>> {
        self.0.get(key)
    }

    /// Inserts factory for the specified [`FilterFactory`], returning any
    /// previous filter stored at that location if present.
    pub fn insert(&mut self, value: DynFilterFactory) -> Option<Arc<DynFilterFactory>> {
        self.0.insert(value.name(), Arc::new(value))
    }

    /// Returns a by reference iterator over the set of filters.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            inner: self.0.iter(),
        }
    }
}

impl Default for FilterSet {
    fn default() -> Self {
        Self::default_with(Option::into_iter(None))
    }
}

impl<I: Iterator<Item = DynFilterFactory>> From<I> for FilterSet {
    fn from(iter: I) -> Self {
        Self::with(iter)
    }
}

impl From<FilterSet> for FilterMap {
    fn from(set: FilterSet) -> Self {
        set.0
    }
}

impl FromIterator<DynFilterFactory> for FilterSet {
    fn from_iter<I: IntoIterator<Item = DynFilterFactory>>(iter: I) -> Self {
        let mut set = Self(Default::default());

        for factory in iter {
            set.0.insert(factory.name(), Arc::new(factory));
        }

        set
    }
}

impl IntoIterator for FilterSet {
    type IntoIter = IntoIter;
    type Item = Arc<DynFilterFactory>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.0.into_iter(),
        }
    }
}

/// Iterator over a set of [`DynFilterFactory`]s.
pub struct IntoIter {
    inner: std::collections::hash_map::IntoIter<&'static str, Arc<DynFilterFactory>>,
}

impl Iterator for IntoIter {
    type Item = Arc<DynFilterFactory>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

/// Iterator over a set of [`DynFilterFactory`]s.
pub struct Iter<'r> {
    inner: std::collections::hash_map::Iter<'r, &'static str, Arc<DynFilterFactory>>,
}

impl<'r> Iterator for Iter<'r> {
    type Item = &'r Arc<DynFilterFactory>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}
