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

use dashmap::DashMap;
use dashmap::mapref::entry::Entry as DashMapEntry;
use dashmap::mapref::one::{Ref, RefMut};
use tracing::warn;

use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::oneshot::{Receiver, Sender, channel};

pub use dashmap::try_result::TryResult;

// Clippy isn't recognizing that these imports are used conditionally.
#[allow(unused_imports)]
use std::time::{SystemTime, UNIX_EPOCH};
#[allow(unused_imports)]
use tokio::time::Instant;

/// A wrapper around the value of an entry in the map.
/// It contains the value's ttl.
pub struct Value<V> {
    pub value: V,
    expires_at: Arc<AtomicU64>,
    clock: Clock,
}

impl<V> Value<V> {
    fn new(value: V, ttl: Duration, clock: Clock) -> Value<V> {
        let value = Value {
            value,
            expires_at: Arc::new(AtomicU64::new(0)),
            clock,
        };
        value.update_expiration(ttl);
        value
    }

    /// Get the expiration time for this value. The returned value is the
    /// number of seconds relative to some reference point (e.g `UNIX_EPOCH`), based
    /// on the clock being used.
    #[inline]
    fn expiration_secs(&self) -> u64 {
        self.expires_at.load(Ordering::Relaxed)
    }

    /// Update the value's expiration time to (now + TTL).
    #[inline]
    fn update_expiration(&self, ttl: Duration) {
        match self.clock.compute_expiration_secs(ttl) {
            Ok(new_expiration_time) => {
                self.expires_at
                    .store(new_expiration_time, Ordering::Relaxed);
            }
            Err(err) => {
                warn!("failed to increment key expiration: {}", err);
            }
        }
    }
}

impl<V: std::fmt::Debug> std::fmt::Debug for Value<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Value")
            .field("value", &self.value)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl<V> std::ops::Deref for Value<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// Map contains the hash map implementation.
struct Map<K, V> {
    inner: DashMap<K, Value<V>>,
    ttl: Duration,
    clock: Clock,
    shutdown_tx: Option<Sender<()>>,
}

impl<K: std::fmt::Debug + std::hash::Hash + std::cmp::Eq, V: std::fmt::Debug> std::fmt::Debug
    for Map<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map")
            .field("inner", &self.inner)
            .field("ttl", &self.ttl)
            .field("shutdown_tx", &self.shutdown_tx)
            .finish()
    }
}

impl<K, V> Drop for Map<K, V> {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            shutdown_tx.send(()).ok();
        }
    }
}

/// A key value hash map where entries are associated with a TTL.
///
/// When the TTL for an entry elapses, the entry is removed from the map.
///
/// The TTL is reset each time the entry is (re)inserted or read via [`TtlMap::get`],
/// [`TtlMap::get_mut`] functions, or via the [`TtlMap::entry`] interface.
///
/// During tests, the internal clock implementation is driven by [`tokio::time`] so
/// functions like [`tokio::time::pause`] and [`tokio::time::advance`] can be used.
pub struct TtlMap<K, V>(Arc<Map<K, V>>);

impl<K, V> TtlMap<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(ttl: Duration, poll_interval: Duration) -> Self {
        Self::initialize(DashMap::new(), ttl, poll_interval)
    }

    #[allow(dead_code)]
    pub fn with_capacity(ttl: Duration, poll_interval: Duration, capacity: usize) -> Self {
        Self::initialize(DashMap::with_capacity(capacity), ttl, poll_interval)
    }

    fn initialize(inner: DashMap<K, Value<V>>, ttl: Duration, poll_interval: Duration) -> Self {
        let (shutdown_tx, shutdown_rx) = channel();
        let map = TtlMap(Arc::new(Map {
            inner,
            shutdown_tx: Some(shutdown_tx),
            ttl,
            clock: Clock::new(),
        }));
        spawn_cleanup_task(
            map.0.clone(),
            poll_interval,
            map.0.clock.clone(),
            shutdown_rx,
        );
        map
    }

    /// Returns the current time as the number of seconds relative to some initial
    /// reference point (e.g `UNIX_EPOCH`), based on the clock implementation being used.
    /// In tests, this will be driven by [`tokio::time`]
    #[inline]
    pub(crate) fn now_relative_secs(&self) -> u64 {
        self.0.clock.now_relative_secs().unwrap_or_default()
    }
}

#[allow(dead_code)]
impl<K, V> TtlMap<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync,
{
    /// Returns a reference to value corresponding to key.
    #[inline]
    pub fn get(&self, key: &K) -> Option<Ref<'_, K, Value<V>>> {
        let value = self.0.inner.get(key);
        if let Some(value) = &value {
            value.update_expiration(self.0.ttl);
        }

        value
    }

    /// Returns a reference to value corresponding to key.
    #[inline]
    pub fn try_get(&self, key: &K) -> TryResult<Ref<'_, K, Value<V>>> {
        let value = self.0.inner.try_get(key);
        if let TryResult::Present(value) = &value {
            value.update_expiration(self.0.ttl);
        }

        value
    }

    /// Returns a mutable reference to value corresponding to key.
    /// The value will be reset to expire at the configured TTL after the time of retrieval.
    #[inline]
    pub fn get_mut(&self, key: &K) -> Option<RefMut<'_, K, Value<V>>> {
        let value = self.0.inner.get_mut(key);
        if let Some(ref value) = value {
            value.update_expiration(self.0.ttl);
        }

        value
    }

    /// Returns the number of entries currently in the map.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.inner.len()
    }

    /// Returns whether the map currently contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether the map currently contains any entries.
    #[inline]
    pub fn is_not_empty(&self) -> bool {
        !self.is_empty()
    }

    /// Returns true if the map contains a value for the specified key.
    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.0.inner.contains_key(key)
    }

    /// Inserts a key-value pair into the map.
    /// The value will be set to expire at the configured TTL after the time of insertion.
    /// If a previous value existed for this key, that value is returned.
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.0
            .inner
            .insert(key, Value::new(value, self.0.ttl, self.0.clock.clone()))
            .map(|value| value.value)
    }

    /// Removes a key-value pair from the map.
    #[inline]
    pub fn remove(&self, key: K) -> bool {
        self.0.inner.remove(&key).is_some()
    }

    /// Removes all entries from the map
    #[inline]
    pub fn clear(&self) {
        self.0.inner.clear();
    }

    /// Returns an entry for in-place updates of the specified key-value pair.
    /// Note: This acquires a write lock on the map's shard that corresponds
    /// to the entry.
    #[inline]
    pub fn entry(&self, key: K) -> Entry<'_, K, Value<V>> {
        let ttl = self.0.ttl;
        match self.0.inner.entry(key) {
            inner @ DashMapEntry::Occupied(_) => Entry::Occupied(OccupiedEntry {
                inner,
                ttl,
                clock: self.0.clock.clone(),
            }),
            inner @ DashMapEntry::Vacant(_) => Entry::Vacant(VacantEntry {
                inner,
                ttl,
                clock: self.0.clock.clone(),
            }),
        }
    }
}

impl<K: std::fmt::Debug + std::hash::Hash + std::cmp::Eq, V: std::fmt::Debug> std::fmt::Debug
    for TtlMap<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TtlMap").field("inner", &self.0).finish()
    }
}

impl<K, V> Clone for TtlMap<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, V> Default for TtlMap<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        const DEFAULT_TIMEOUT_SECONDS: Duration = Duration::from_secs(60);
        const DEFAULT_EXPIRY_POLL_INTERVAL: Duration = Duration::from_secs(60);
        Self::new(DEFAULT_TIMEOUT_SECONDS, DEFAULT_EXPIRY_POLL_INTERVAL)
    }
}

/// A view into an occupied entry in the map.
pub struct OccupiedEntry<'a, K, V> {
    inner: DashMapEntry<'a, K, V>,
    ttl: Duration,
    clock: Clock,
}

/// A view into a vacant entry in the map.
pub struct VacantEntry<'a, K, V> {
    inner: DashMapEntry<'a, K, V>,
    ttl: Duration,
    clock: Clock,
}

/// A view into an entry in the map.
/// It may either be [`VacantEntry`] or [`OccupiedEntry`]
pub enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

impl<K, V> OccupiedEntry<'_, K, Value<V>>
where
    K: Eq + Hash,
{
    /// Returns a reference to the entry's value.
    /// The value will be reset to expire at the configured TTL after the time of retrieval.
    pub fn get(&self) -> &Value<V> {
        match &self.inner {
            DashMapEntry::Occupied(entry) => {
                let value = entry.get();
                value.update_expiration(self.ttl);
                value
            }
            DashMapEntry::Vacant(_) => unreachable!("BUG: entry type should be occupied"),
        }
    }

    #[allow(dead_code)]
    /// Returns a mutable reference to the entry's value.
    /// The value will be reset to expire at the configured TTL after the time of retrieval.
    pub fn get_mut(&mut self) -> &mut Value<V> {
        match &mut self.inner {
            DashMapEntry::Occupied(entry) => {
                let value = entry.get_mut();
                value.update_expiration(self.ttl);
                value
            }
            DashMapEntry::Vacant(_) => unreachable!("BUG: entry type should be occupied"),
        }
    }

    #[allow(dead_code)]
    /// Replace the entry's value with a new value, returning the old value.
    /// The value will be set to expire at the configured TTL after the time of insertion.
    pub fn insert(&mut self, value: V) -> Value<V> {
        match &mut self.inner {
            DashMapEntry::Occupied(entry) => {
                entry.insert(Value::new(value, self.ttl, self.clock.clone()))
            }
            DashMapEntry::Vacant(_) => unreachable!("BUG: entry type should be occupied"),
        }
    }
}

impl<'a, K, V> VacantEntry<'a, K, Value<V>>
where
    K: Eq + Hash,
{
    /// Set an entry's value.
    /// The value will be set to expire at the configured TTL after the time of insertion.
    pub fn insert(self, value: V) -> RefMut<'a, K, Value<V>> {
        match self.inner {
            DashMapEntry::Vacant(entry) => {
                entry.insert(Value::new(value, self.ttl, self.clock.clone()))
            }
            DashMapEntry::Occupied(_) => unreachable!("BUG: entry type should be vacant"),
        }
    }
}

fn spawn_cleanup_task<K, V>(
    map: Arc<Map<K, V>>,
    poll_interval: Duration,
    clock: Clock,
    mut shutdown_rx: Receiver<()>,
) where
    K: Send + Sync + Hash + Eq + 'static,
    V: Send + Sync + 'static,
{
    let mut interval = tokio::time::interval(poll_interval);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    prune_entries( &map, &clock).await;
                }
                _ = &mut shutdown_rx => {
                    return;
                }
            }
        }
    });
}

async fn prune_entries<K, V>(map: &Arc<Map<K, V>>, clock: &Clock)
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let now_secs = if let Ok(now_secs) = clock.now_relative_secs() {
        now_secs
    } else {
        warn!("Failed to get current time when pruning sessions");
        return;
    };

    // Take a read lock first and check if there is at least 1 item to remove.
    let has_expired_keys = map
        .inner
        .iter()
        .filter(|entry| entry.value().expiration_secs() <= now_secs)
        .take(1)
        .next()
        .is_some();

    // If we have work to do then, take a write lock.
    if has_expired_keys {
        // Go over the whole map in case anything expired
        // since acquiring the write lock.
        map.inner
            .retain(|_, value| value.expiration_secs() > now_secs);
    }
}

/// A wrapper over functions to generate relative timestamps and ttl.
/// During test it is driven via [`tokio::time`], otherwise it uses system time.
#[derive(Clone)]
struct Clock {
    #[cfg(test)]
    base: Instant,
}

impl Clock {
    fn new() -> Clock {
        #[cfg(not(test))]
        return Clock {};

        #[cfg(test)]
        return Clock {
            base: Instant::now(),
        };
    }

    /// Returns the current time in seconds, relative to some base time instant.
    /// For non test cases, relative to `UNIX_EPOCH`, while during test, a random
    /// point in the past is used.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn now_relative_secs(&self) -> Result<u64, String> {
        #[cfg(not(test))]
        return SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_err| {
                String::from("duration_since was called with time later than the current time")
            })
            .map(|t| t.as_secs());

        #[cfg(test)]
        return Ok((Instant::now()).duration_since(self.base).as_secs());
    }

    /// Returns the expiration time from now in seconds for the given ttl.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn compute_expiration_secs(&self, ttl: Duration) -> Result<u64, String> {
        #[cfg(not(test))]
        return (SystemTime::now() + ttl)
            .duration_since(UNIX_EPOCH)
            .map_err(|_err| {
                String::from("duration_since was called with time later than the current time")
            })
            .map(|t| t.as_secs());

        #[cfg(test)]
        return Ok((Instant::now() + ttl).duration_since(self.base).as_secs());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::endpoint::EndpointAddress;
    use std::net::Ipv4Addr;

    use tokio::time;

    fn address_pair() -> (EndpointAddress, EndpointAddress) {
        (
            (Ipv4Addr::LOCALHOST, 8080).into(),
            ([127, 0, 0, 2], 8080).into(),
        )
    }

    #[tokio::test]
    async fn len() {
        let (one, two) = address_pair();
        const TTL: Duration = Duration::from_millis(50);
        const POLL: Duration = Duration::from_millis(10);

        let map = TtlMap::<EndpointAddress, usize>::new(TTL, POLL);
        map.insert(one, 1);
        assert_eq!(map.len(), 1);
        map.insert(two, 2);
        assert_eq!(map.len(), 2);

        // add POLL to allow for if it JUST polls right before TTL.
        tokio::time::sleep(TTL + POLL).await;
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn insert_and_get() {
        let (one, two) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);
        map.insert(two.clone(), 2);

        assert_eq!(map.get(&one).unwrap().value, 1);
        assert_eq!(map.get(&two).unwrap().value, 2);
    }

    #[tokio::test]
    async fn insert_and_get_expiration() {
        // Test that when we insert or retrieve an item, we update its expiration.
        time::pause();

        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);

        let exp1 = map.get(&one).unwrap().expiration_secs();

        time::advance(Duration::from_secs(2)).await;
        let exp2 = map.get(&one).unwrap().expiration_secs();

        time::advance(Duration::from_secs(3)).await;
        let exp3 = map.get(&one).unwrap().expiration_secs();

        assert!(exp1 < exp2);
        assert_eq!(2, exp2 - exp1);
        assert!(exp2 < exp3);
        assert_eq!(3, exp3 - exp2);
    }

    #[tokio::test]
    async fn contains_key() {
        let (one, two) = address_pair();
        let three = ([127, 0, 0, 3], 8080).into();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);
        map.insert(two.clone(), 2);

        assert!(map.contains_key(&one));
        assert!(!map.contains_key(&three));
        assert!(map.contains_key(&two));
    }

    #[tokio::test]
    async fn entry_occupied_insert_and_get() {
        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);

        match map.entry(one.clone()) {
            Entry::Occupied(mut entry) => {
                assert_eq!(entry.get().value, 1);
                entry.insert(5);
            }
            Entry::Vacant(_) => unreachable!("expected occupied entry"),
        }

        assert_eq!(map.get(&one).unwrap().value, 5);
    }

    #[tokio::test]
    async fn entry_occupied_get_mut() {
        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);

        match map.entry(one.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().value = 5;
            }
            Entry::Vacant(_) => unreachable!("expected occupied entry"),
        }

        assert_eq!(map.get(&one).unwrap().value, 5);
    }

    #[tokio::test]
    async fn entry_vacant_insert() {
        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );

        match map.entry(one.clone()) {
            Entry::Vacant(entry) => {
                let mut e = entry.insert(1);
                assert_eq!(e.value, 1);
                e.value = 5;
            }
            Entry::Occupied(_) => unreachable!("expected occupied entry"),
        }

        assert_eq!(map.get(&one).unwrap().value, 5);
    }

    #[tokio::test]
    async fn entry_occupied_get_expiration() {
        // Test that when we get a value via OccupiedEntry, we update its expiration.
        time::pause();

        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);

        let exp1 = map.get(&one).unwrap().expiration_secs();

        time::advance(Duration::from_secs(2)).await;

        let exp2 = match map.entry(one.clone()) {
            Entry::Occupied(entry) => entry.get().expiration_secs(),
            Entry::Vacant(_) => unreachable!("expected occupied entry"),
        };

        assert!(exp1 < exp2);
        assert_eq!(2, exp2 - exp1);
    }

    #[tokio::test]
    async fn entry_occupied_get_mut_expiration() {
        // Test that when we get_mut a value via OccupiedEntry, we update its expiration.
        time::pause();

        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);

        let exp1 = map.get(&one).unwrap().expiration_secs();

        time::advance(Duration::from_secs(2)).await;

        let exp2 = match map.entry(one) {
            Entry::Occupied(mut entry) => entry.get_mut().expiration_secs(),
            Entry::Vacant(_) => unreachable!("expected occupied entry"),
        };

        assert!(exp1 < exp2);
        assert_eq!(2, exp2 - exp1);
    }

    #[tokio::test]
    async fn entry_occupied_insert_expiration() {
        // Test that when we replace a value via OccupiedEntry, we update its expiration.
        time::pause();

        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );
        map.insert(one.clone(), 1);

        let exp1 = map.get(&one).unwrap().expiration_secs();

        time::advance(Duration::from_secs(2)).await;

        let old_exp1 = match map.entry(one.clone()) {
            Entry::Occupied(mut entry) => entry.insert(9).expiration_secs(),
            Entry::Vacant(_) => unreachable!("expected occupied entry"),
        };

        let exp2 = map.get(&one).unwrap().expiration_secs();

        assert_eq!(exp1, old_exp1);
        assert!(exp1 < exp2);
        assert_eq!(2, exp2 - exp1);
    }

    #[tokio::test]
    async fn entry_occupied_vacant_expiration() {
        // Test that when we insert a value via VacantEntry, we update its expiration.
        time::pause();

        let (one, _) = address_pair();

        let map = TtlMap::<EndpointAddress, usize>::new(
            Duration::from_secs(10),
            Duration::from_millis(10),
        );

        let exp1 = match map.entry(one.clone()) {
            Entry::Vacant(entry) => entry.insert(9).expiration_secs(),
            Entry::Occupied(_) => unreachable!("expected vacant entry"),
        };

        time::advance(Duration::from_secs(2)).await;

        let exp2 = map.get(&one).unwrap().expiration_secs();

        // Initial expiration should be set at our configured ttl.
        assert_eq!(10, exp1);

        assert!(exp1 < exp2);
        assert_eq!(2, exp2 - exp1);
    }

    #[tokio::test]
    async fn expiration_ttl() {
        // Test that when we expire entries at our configured ttl.
        time::pause();

        let (one, _) = address_pair();

        let ttl = Duration::from_secs(12);
        let map = TtlMap::<EndpointAddress, usize>::new(ttl, Duration::from_millis(10));

        let exp = match map.entry(one) {
            Entry::Vacant(entry) => entry.insert(9).expiration_secs(),
            Entry::Occupied(_) => unreachable!("expected vacant entry"),
        };

        // Check that it expires at our configured TTL.
        assert_eq!(12, exp);
    }

    #[tokio::test]
    async fn cleanup_expired_entries() {
        // Test that we delete expired entries from the ttl map.
        time::pause();

        let (one, two) = address_pair();

        let map =
            TtlMap::<EndpointAddress, usize>::new(Duration::from_secs(5), Duration::from_secs(1));
        map.insert(one.clone(), 1);
        map.insert(two.clone(), 2);

        assert!(map.contains_key(&one));
        assert!(map.contains_key(&two));

        time::advance(Duration::from_secs(4)).await;

        // Read one key so that it does not expire at the original ttl.
        map.get(&two).unwrap();

        // Check that only the un-read key is deleted.
        time::advance(Duration::from_secs(4)).await;
        assert!(!map.contains_key(&one));
        assert!(map.contains_key(&two));
        assert_eq!(map.len(), 1);

        // Check that the second key is eventually deleted.
        time::advance(Duration::from_secs(3)).await;
        assert!(!map.contains_key(&one));
        assert!(!map.contains_key(&two));
        assert_eq!(map.len(), 0);
    }
}
