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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
};

use corrosion::pubsub::ChangeId;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::net::endpoint::{Endpoint, EndpointAddress, EndpointMetadata, Locality, Metadata};

const HASH_SEED: i64 = 0xdeadbeef;

pub use crate::generated::quilkin::config::v1alpha1 as proto;

pub(crate) fn active_clusters() -> &'static prometheus::IntGauge {
    static ACTIVE_CLUSTERS: Lazy<prometheus::IntGauge> = Lazy::new(|| {
        crate::metrics::register(
            prometheus::IntGauge::with_opts(prometheus::Opts::new(
                "quilkin_cluster_active",
                "Number of currently active clusters.",
            ))
            .unwrap(),
        )
    });

    &ACTIVE_CLUSTERS
}

pub(crate) fn active_endpoints(cluster: &str) -> prometheus::IntGauge {
    static ACTIVE_ENDPOINTS: Lazy<prometheus::IntGaugeVec> = Lazy::new(|| {
        prometheus::register_int_gauge_vec_with_registry! {
            prometheus::opts! {
                "quilkin_active_endpoints",
                "Number of currently available endpoints across clusters",
            },
            &["quilkin_cluster"],
            crate::metrics::registry(),
        }
        .unwrap()
    });

    ACTIVE_ENDPOINTS.with_label_values(&[cluster])
}

pub type TokenAddressMap = gxhash::HashMap<u64, gxhash::HashSet<EndpointAddress>>;

#[derive(Copy, Clone)]
pub struct Token(u64);

impl Token {
    #[inline]
    pub fn new(token: &[u8]) -> Self {
        Self(gxhash::gxhash64(token, HASH_SEED))
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct EndpointSetVersion(u64);

impl EndpointSetVersion {
    pub fn from_number(version: u64) -> Self {
        Self(version)
    }

    pub fn number(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for EndpointSetVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::Debug for EndpointSetVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl std::str::FromStr for EndpointSetVersion {
    type Err = eyre::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str_radix(s, 16)?))
    }
}

type InnerMap = BTreeMap<EndpointAddress, EndpointMetadata>;

#[derive(Debug, Clone)]
pub struct EndpointSet {
    pub endpoints: InnerMap,
    pub token_map: TokenAddressMap,
    /// The hash of all of the endpoints in this set
    hash: u64,
    change_id: ChangeId,
}

impl EndpointSet {
    /// Creates a new endpoint set, calculating a unique version hash for it
    #[inline]
    pub fn new(endpoints: BTreeSet<Endpoint>) -> Self {
        let mut this = Self::from_set(endpoints);

        this.update();
        this
    }

    /// Creates a new endpoint set with the provided version hash, skipping
    /// calculation of it
    ///
    /// This hash _must_ be calculated with [`Self::update`] to be consistent
    /// across machines
    #[inline]
    pub fn with_version(endpoints: BTreeSet<Endpoint>, hash: EndpointSetVersion) -> Self {
        let mut this = Self::from_set(endpoints);
        this.hash = hash.number();

        this.build_token_map();
        this
    }

    #[inline]
    fn from_set(endpoints: BTreeSet<Endpoint>) -> Self {
        Self {
            endpoints: endpoints
                .into_iter()
                .map(|ep| (ep.address, ep.metadata))
                .collect(),
            token_map: Default::default(),
            hash: 0,
            change_id: ChangeId(0),
        }
    }

    /// For testing, converts to an endpoint -> tokenset map
    #[inline]
    pub fn to_map(
        &self,
    ) -> std::collections::BTreeMap<quilkin_types::Endpoint, quilkin_types::TokenSet> {
        self.endpoints
            .iter()
            .map(|(addr, md)| {
                (
                    quilkin_types::Endpoint {
                        address: addr.host.clone(),
                        port: addr.port,
                    },
                    md.known.tokens.clone(),
                )
            })
            .collect()
    }

    #[inline]
    pub fn endpoint_iter(&self) -> impl Iterator<Item = Endpoint> {
        self.endpoints.iter().map(|(addr, md)| Endpoint {
            address: addr.clone(),
            metadata: md.clone(),
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    #[inline]
    pub fn contains(&self, ep: &Endpoint) -> bool {
        self.endpoints.contains_key(&ep.address)
    }

    /// Unique version for this endpoint set
    #[inline]
    pub fn version(&self) -> EndpointSetVersion {
        EndpointSetVersion::from_number(self.hash)
    }

    /// Returns the latest change id seen from a remote server
    #[inline]
    pub fn change_id(&self) -> ChangeId {
        self.change_id
    }

    /// Bumps the version, calculating a hash for the entire endpoint set
    ///
    /// This is extremely expensive
    #[inline]
    pub fn update(&mut self) -> TokenAddressMap {
        use std::hash::{Hash, Hasher};
        let mut hasher = gxhash::GxHasher::with_seed(HASH_SEED);
        let mut token_map = TokenAddressMap::default();

        for (addr, md) in &self.endpoints {
            addr.hash(&mut hasher);
            md.known.tokens.hash(&mut hasher);

            for tok in md.known.tokens.iter() {
                let hash = gxhash::gxhash64(tok, HASH_SEED);
                token_map.entry(hash).or_default().insert(addr.clone());
            }
        }

        self.hash = hasher.finish();
        std::mem::replace(&mut self.token_map, token_map)
    }

    /// Creates a map of tokens -> address for the current set
    #[inline]
    pub fn build_token_map(&mut self) -> TokenAddressMap {
        let mut token_map = TokenAddressMap::default();

        // This is only called on proxies, so calculate a token map
        for (addr, md) in &self.endpoints {
            for tok in md.known.tokens.iter() {
                let hash = gxhash::gxhash64(tok, HASH_SEED);
                token_map.entry(hash).or_default().insert(addr.clone());
            }
        }

        std::mem::replace(&mut self.token_map, token_map)
    }

    #[inline]
    pub fn replace(
        &mut self,
        replacement: Self,
    ) -> (
        usize,
        std::collections::HashMap<u64, Option<BTreeSet<EndpointAddress>>>,
    ) {
        let old_len = std::mem::replace(&mut self.endpoints, replacement.endpoints).len();

        let old_tm = if replacement.hash == 0 {
            self.update()
        } else {
            self.hash = replacement.hash;
            self.build_token_map()
        };

        let mut hm = std::collections::HashMap::new();

        for (token, addrs) in &old_tm {
            match self.token_map.get(token) {
                Some(naddrs) => {
                    if addrs.symmetric_difference(naddrs).count() > 0 {
                        hm.insert(*token, Some(naddrs.iter().cloned().collect()));
                    }
                }
                _ => {
                    hm.insert(*token, None);
                }
            }
        }

        for (token, addrs) in &self.token_map {
            if !hm.contains_key(token) {
                hm.insert(*token, Some(addrs.iter().cloned().collect()));
            }
        }

        (old_len, hm)
    }
}

impl EndpointSet {
    pub fn corrosion_apply(
        &mut self,
        ss: corrosion::pubsub::SubscriptionStream,
        token_map: &DashMap<u64, BTreeSet<EndpointAddress>>,
    ) -> ChangeId {
        use corrosion::{
            api::{TypedQueryEvent as tqe, sqlite::ChangeType},
            db::read::{self, FromSqlValue},
        };

        for eve in ss {
            let eve = match eve {
                Ok(e) => e,
                Err(error) => {
                    tracing::warn!(%error, "received error in server subscription stream");
                    continue;
                }
            };

            let (cty, row, row_id) = match eve {
                tqe::Change(cty, rid, row, cid) => {
                    // It's possible? to get a change id that is smaller than the current one,
                    // we still apply it, but we only ever keep the highest one
                    self.change_id = ChangeId(cid.0.max(self.change_id.0));

                    (cty, row, rid)
                }
                tqe::Row(rid, row) => (ChangeType::Insert, row, rid),
                _ => continue,
            };

            let row = match read::ServerRow::from_sql(&row) {
                Ok(sr) => sr,
                Err(error) => {
                    tracing::warn!(%error, row_id = row_id.0, "failed to deserialize server row");
                    continue;
                }
            };

            let insert = |this: &mut TokenAddressMap, addr: &EndpointAddress, tok: &[u8]| {
                let tok = Token::new(tok);

                {
                    let mut tm = token_map.entry(tok.0).or_default();
                    tm.insert(addr.clone());
                }

                this.entry(tok.0).or_default().insert(addr.clone());
            };

            let remove = |this: &mut TokenAddressMap, addr: &EndpointAddress, tok: &[u8]| {
                let tok = Token::new(tok);

                let remove = if let Some(mut tm) = token_map.get_mut(&tok.0) {
                    tm.remove(addr);
                    tm.is_empty()
                } else {
                    false
                };

                if remove {
                    token_map.remove(&tok.0);
                }

                if let Some(old) = this.get_mut(&tok.0) {
                    old.remove(addr);
                    if old.is_empty() {
                        this.remove(&tok.0);
                    }
                }
            };

            match cty {
                ChangeType::Insert => {
                    let address = EndpointAddress {
                        host: row.endpoint.address,
                        port: row.endpoint.port,
                    };

                    for tok in row.tokens.iter() {
                        insert(&mut self.token_map, &address, tok);
                    }

                    self.endpoints.insert(
                        address,
                        EndpointMetadata {
                            known: Metadata { tokens: row.tokens },
                            unknown: Default::default(),
                        },
                    );
                }
                ChangeType::Update => {
                    let address = EndpointAddress {
                        host: row.endpoint.address,
                        port: row.endpoint.port,
                    };

                    let Some(tokens) = self
                        .endpoints
                        .get_mut(&address)
                        .map(|md| &mut md.known.tokens.0)
                    else {
                        tracing::warn!(%address, "address not found for update");
                        continue;
                    };

                    // Remove old tokens
                    for old in tokens.difference(&row.tokens.0) {
                        remove(&mut self.token_map, &address, old);
                    }

                    // Add new tokens
                    for new in row.tokens.0.difference(tokens) {
                        insert(&mut self.token_map, &address, new);
                    }

                    drop(std::mem::replace(tokens, row.tokens.0));
                }
                ChangeType::Delete => {
                    let address = EndpointAddress {
                        host: row.endpoint.address,
                        port: row.endpoint.port,
                    };

                    let Some(md) = self.endpoints.remove(&address) else {
                        tracing::warn!(%address, "address not found for removal");
                        continue;
                    };

                    // We could pedantically check if the local token set matches
                    // the one in the deletion

                    for tok in md.known.tokens {
                        remove(&mut self.token_map, &address, &tok);
                    }
                }
            }
        }

        self.change_id
    }
}

/// Represents a full snapshot of all clusters.
pub struct ClusterMap<S = gxhash::GxBuildHasher> {
    map: DashMap<Option<Locality>, EndpointSet, S>,
    localities: DashMap<Option<Locality>, Option<std::net::IpAddr>>,
    token_map: DashMap<u64, BTreeSet<EndpointAddress>>,
    num_endpoints: AtomicUsize,
    version: AtomicU64,
}

type DashMapRef<'inner> = dashmap::mapref::one::Ref<'inner, Option<Locality>, EndpointSet>;

impl ClusterMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_default(cluster: BTreeSet<Endpoint>) -> Self {
        let this = Self::default();
        this.insert_default(cluster);
        this
    }
}

impl<S> ClusterMap<S> {
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Relaxed)
    }
}

impl<S> ClusterMap<S>
where
    S: Default + std::hash::BuildHasher + Clone,
{
    pub fn benchmarking(capacity: usize, hasher: S) -> Self {
        Self {
            map: DashMap::with_capacity_and_hasher(capacity, hasher),
            ..Self::default()
        }
    }

    #[inline]
    pub fn insert(
        &self,
        remote_addr: Option<std::net::IpAddr>,
        locality: Option<Locality>,
        cluster: BTreeSet<Endpoint>,
    ) {
        let _res = self.apply(remote_addr, locality, EndpointSet::new(cluster));
    }

    pub fn apply(
        &self,
        remote_addr: Option<std::net::IpAddr>,
        locality: Option<Locality>,
        cluster: EndpointSet,
    ) -> crate::Result<()> {
        if let Some(raddr) = self.localities.get(&locality) {
            if *raddr != remote_addr {
                eyre::bail!(
                    "skipping cluster apply, '{locality:?}' is managed by '{raddr:?}', not '{remote_addr:?}'"
                );
            }
        } else {
            self.localities.insert(locality.clone(), remote_addr);
        }

        let new_len = cluster.len();
        if let Some(mut current) = self.map.get_mut(&locality) {
            let current = current.value_mut();

            let (old_len, token_map_diff) = current.replace(cluster);

            if new_len >= old_len {
                self.num_endpoints.fetch_add(new_len - old_len, Relaxed);
            } else {
                self.num_endpoints.fetch_sub(old_len - new_len, Relaxed);
            }

            self.version.fetch_add(1, Relaxed);

            for (token_hash, addrs) in token_map_diff {
                if let Some(addrs) = addrs {
                    self.token_map.insert(token_hash, addrs);
                } else {
                    self.token_map.remove(&token_hash);
                }
            }
        } else {
            for (token_hash, addrs) in &cluster.token_map {
                self.token_map
                    .insert(*token_hash, addrs.iter().cloned().collect());
            }

            self.map.insert(locality, cluster);
            self.num_endpoints.fetch_add(new_len, Relaxed);
            self.version.fetch_add(1, Relaxed);
        }

        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[inline]
    pub fn get(&self, key: &Option<Locality>) -> Option<DashMapRef<'_>> {
        self.map.get(key)
    }

    #[inline]
    pub fn insert_default(&self, endpoints: BTreeSet<Endpoint>) {
        self.insert(None, None, endpoints);
    }

    #[inline]
    pub fn remove_endpoint(&self, needle: &Endpoint) -> bool {
        let locality = 'l: {
            for mut entry in self.map.iter_mut() {
                let set = entry.value_mut();

                if set.endpoints.remove(&needle.address).is_some() {
                    set.update();
                    self.num_endpoints.fetch_sub(1, Relaxed);
                    self.version.fetch_add(1, Relaxed);

                    if set.is_empty() {
                        break 'l entry.key().clone();
                    }

                    return true;
                }
            }
            return false;
        };

        self.do_remove_locality(&locality);
        true
    }

    #[inline]
    pub fn remove_endpoint_if(&self, closure: impl Fn(&EndpointMetadata) -> bool) -> bool {
        let locality = 'l: {
            for mut entry in self.map.iter_mut() {
                let set = entry.value_mut();
                if let Some(address) = set
                    .endpoints
                    .iter()
                    .find_map(|(addr, md)| (closure)(md).then(|| addr.clone()))
                {
                    set.endpoints.remove(&address);
                    set.update();
                    self.num_endpoints.fetch_sub(1, Relaxed);
                    self.version.fetch_add(1, Relaxed);

                    if set.is_empty() {
                        break 'l entry.key().clone();
                    }
                    return true;
                }
            }
            return false;
        };

        self.do_remove_locality(&locality);
        true
    }

    #[inline]
    pub fn iter(&self) -> dashmap::iter::Iter<'_, Option<Locality>, EndpointSet, S> {
        self.map.iter()
    }

    #[inline]
    pub fn replace(
        &self,
        remote_addr: Option<std::net::IpAddr>,
        locality: Option<Locality>,
        endpoint: Endpoint,
    ) -> Option<Endpoint> {
        if let Some(raddr) = self.localities.get(&locality)
            && *raddr != remote_addr
        {
            tracing::trace!("not replacing locality endpoints");
            return None;
        }

        if let Some(mut set) = self.map.get_mut(&locality) {
            let replaced = set
                .endpoints
                .remove(&endpoint.address)
                .map(|metadata| Endpoint {
                    address: endpoint.address.clone(),
                    metadata,
                });
            set.endpoints.insert(endpoint.address, endpoint.metadata);
            set.update();
            self.version.fetch_add(1, Relaxed);

            if replaced.is_none() {
                self.num_endpoints.fetch_add(1, Relaxed);
            }

            replaced
        } else {
            self.insert(remote_addr, locality, [endpoint].into());
            None
        }
    }

    #[inline]
    pub fn endpoints(&self) -> Vec<Endpoint> {
        let mut endpoints = Vec::with_capacity(self.num_of_endpoints());

        for set in self.map.iter() {
            endpoints.extend(
                set.value()
                    .endpoints
                    .iter()
                    .map(|(address, metadata)| Endpoint {
                        address: address.clone(),
                        metadata: metadata.clone(),
                    }),
            );
        }

        endpoints
    }

    pub fn nth_endpoint(&self, mut index: usize) -> Option<Endpoint> {
        for set in self.iter() {
            let set = &set.value().endpoints;
            if index < set.len() {
                return set.iter().nth(index).map(|(address, metadata)| Endpoint {
                    address: address.clone(),
                    metadata: metadata.clone(),
                });
            } else {
                index -= set.len();
            }
        }

        None
    }

    #[inline]
    pub fn num_of_endpoints(&self) -> usize {
        self.num_endpoints.load(Relaxed)
    }

    #[inline]
    pub fn has_endpoints(&self) -> bool {
        self.num_of_endpoints() != 0
    }

    #[inline]
    pub fn update_unlocated_endpoints(
        &self,
        remote_addr: Option<std::net::IpAddr>,
        locality: Locality,
    ) {
        if let Some(raddr) = self.localities.get(&None)
            && *raddr != remote_addr
        {
            tracing::trace!("not updating locality");
            return;
        }

        self.localities.remove(&None);
        self.localities.insert(Some(locality.clone()), remote_addr);

        if let Some((_, set)) = self.map.remove(&None) {
            self.version.fetch_add(1, Relaxed);
            if let Some(replaced) = self.map.insert(Some(locality), set) {
                self.num_endpoints.fetch_sub(replaced.len(), Relaxed);
            }
        }
    }

    #[inline]
    fn do_remove_locality(&self, locality: &Option<Locality>) -> Option<EndpointSet> {
        self.localities.remove(locality);

        let ret = self.map.remove(locality).map(|(_k, v)| v);
        if let Some(ret) = &ret {
            self.version.fetch_add(1, Relaxed);
            self.num_endpoints.fetch_sub(ret.len(), Relaxed);
        }
        ret
    }

    #[inline]
    pub fn remove_contributor(&self, remote_addr: Option<std::net::IpAddr>) {
        self.localities.retain(|k, v| {
            let keep = *v != remote_addr;
            if !keep {
                tracing::debug!(locality=?k, ?remote_addr, "removing locality contributor");
            }
            keep
        });
    }

    #[inline]
    pub fn remove_locality(
        &self,
        remote_addr: Option<std::net::IpAddr>,
        locality: &Option<Locality>,
    ) -> Option<EndpointSet> {
        {
            if let Some(raddr) = self.localities.get(locality)
                && *raddr != remote_addr
            {
                tracing::trace!("skipping locality removal");
                return None;
            }
        }

        self.do_remove_locality(locality)
    }

    pub fn addresses_for_token(&self, token: Token, addrs: &mut Vec<EndpointAddress>) {
        if let Some(ma) = self.token_map.get(&token.0) {
            addrs.extend(ma.value().iter().cloned());
        }
    }
}

impl<S> ClusterMap<S>
where
    S: Default + std::hash::BuildHasher + Clone,
{
    /// Applies a stream of events to this cluster map
    ///
    /// This adds, updates, and/or removes endpoints from a 'corrosion' locality
    /// that is temporarily used during the transition period
    pub fn corrosion_apply(&self, ss: corrosion::pubsub::SubscriptionStream) -> ChangeId {
        static CORRO: std::sync::LazyLock<Locality> =
            std::sync::LazyLock::new(|| Locality::new("corrosion", "", ""));

        self.map
            .entry(Some((*CORRO).clone()))
            .or_insert_with(|| EndpointSet::new(BTreeSet::default()))
            .corrosion_apply(ss, &self.token_map)
    }
}

impl<S> crate::config::watch::Watchable for ClusterMap<S> {
    #[inline]
    fn mark(&self) -> crate::config::watch::Marker {
        crate::config::watch::Marker::Version(self.version())
    }

    #[inline]
    #[allow(irrefutable_let_patterns)]
    fn has_changed(&self, marker: crate::config::watch::Marker) -> bool {
        let crate::config::watch::Marker::Version(marked) = marker else {
            return false;
        };
        self.version() != marked
    }
}

impl<S> fmt::Debug for ClusterMap<S>
where
    S: Default + std::hash::BuildHasher + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterMap")
            .field("map", &self.map)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl<S> Default for ClusterMap<S>
where
    S: Default + std::hash::BuildHasher + Clone,
{
    fn default() -> Self {
        Self {
            map: <DashMap<Option<Locality>, EndpointSet, S>>::default(),
            localities: Default::default(),
            token_map: Default::default(),
            version: <_>::default(),
            num_endpoints: <_>::default(),
        }
    }
}

impl Clone for ClusterMap {
    fn clone(&self) -> Self {
        let map = self.map.clone();
        Self::from(map)
    }
}

#[cfg(test)]
impl<S> PartialEq for ClusterMap<S>
where
    S: Default + std::hash::BuildHasher + Clone,
{
    fn eq(&self, rhs: &Self) -> bool {
        for a in self.iter() {
            match rhs
                .get(a.key())
                .filter(|b| a.value().endpoints == b.endpoints)
            {
                Some(_) => {}
                None => return false,
            }
        }

        true
    }
}

#[derive(Default, Debug, Deserialize, Serialize, PartialEq, Clone, Eq, schemars::JsonSchema)]
pub(crate) struct EndpointWithLocality {
    pub endpoints: BTreeSet<Endpoint>,
    pub locality: Option<Locality>,
    pub version: Option<u64>,
}

impl From<(Option<Locality>, &EndpointSet)> for EndpointWithLocality {
    fn from((locality, endpoint_set): (Option<Locality>, &EndpointSet)) -> Self {
        Self {
            locality,
            endpoints: endpoint_set.endpoint_iter().collect(),
            version: Some(endpoint_set.version().number()),
        }
    }
}

impl schemars::JsonSchema for ClusterMap {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        <Vec<EndpointWithLocality>>::schema_name()
    }
    fn json_schema(sg: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        <Vec<EndpointWithLocality>>::json_schema(sg)
    }
}

pub struct ClusterMapDeser {
    pub(crate) endpoints: Vec<EndpointWithLocality>,
}

impl<'de> Deserialize<'de> for ClusterMapDeser {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut endpoints = Vec::<EndpointWithLocality>::deserialize(deserializer)?;

        endpoints.sort_by(|a, b| a.locality.cmp(&b.locality));

        for window in endpoints.windows(2) {
            if window[0] == window[1] {
                return Err(serde::de::Error::custom(
                    "duplicate localities found in cluster map",
                ));
            }
        }

        Ok(Self { endpoints })
    }
}

impl<'de> Deserialize<'de> for ClusterMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let cmd = ClusterMapDeser::deserialize(deserializer)?;
        let map = cmd
            .endpoints
            .into_iter()
            .map(
                |EndpointWithLocality {
                     locality,
                     endpoints,
                     version,
                 }| {
                    let eps = if let Some(version) = version {
                        EndpointSet::with_version(
                            endpoints,
                            EndpointSetVersion::from_number(version),
                        )
                    } else {
                        EndpointSet::new(endpoints)
                    };

                    (locality, eps)
                },
            )
            .collect::<DashMap<_, _, _>>();
        Ok(Self::from(map))
    }
}

impl Serialize for ClusterMap {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.map
            .iter()
            .map(|entry| EndpointWithLocality::from((entry.key().clone(), entry.value())))
            .collect::<Vec<_>>()
            .serialize(ser)
    }
}

impl<S> From<DashMap<Option<Locality>, EndpointSet, S>> for ClusterMap<S>
where
    S: Default + std::hash::BuildHasher + Clone,
{
    fn from(map: DashMap<Option<Locality>, EndpointSet, S>) -> Self {
        let num_endpoints = AtomicUsize::new(map.iter().map(|kv| kv.value().len()).sum());

        let token_map = DashMap::<u64, BTreeSet<EndpointAddress>>::default();
        let localities = DashMap::default();
        for es in &map {
            for (token_hash, addrs) in &es.value().token_map {
                token_map.insert(*token_hash, addrs.iter().cloned().collect());
            }

            localities.insert(es.key().clone(), None);
        }

        Self {
            map,
            localities,
            token_map,
            num_endpoints,
            version: AtomicU64::new(1),
        }
    }
}

impl From<&'_ Endpoint> for proto::Endpoint {
    fn from(endpoint: &Endpoint) -> Self {
        Self {
            host: endpoint.address.host.to_string(),
            port: endpoint.address.port.into(),
            metadata: Some((&endpoint.metadata).into()),
            host2: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn merge() {
        let nl1 = Locality::with_region("nl-1");
        let de1 = Locality::with_region("de-1");

        let mut endpoint = Endpoint::new((Ipv4Addr::LOCALHOST, 7777).into());
        let cluster1 = ClusterMap::new();

        cluster1.insert(None, Some(nl1.clone()), [endpoint.clone()].into());
        cluster1.insert(None, Some(de1.clone()), [endpoint.clone()].into());

        assert_eq!(cluster1.get(&Some(nl1.clone())).unwrap().len(), 1);
        assert!(
            cluster1
                .get(&Some(nl1.clone()))
                .unwrap()
                .contains(&endpoint)
        );
        assert_eq!(cluster1.get(&Some(de1.clone())).unwrap().len(), 1);
        assert!(
            cluster1
                .get(&Some(de1.clone()))
                .unwrap()
                .contains(&endpoint)
        );

        endpoint.address.port = 8080;

        cluster1.insert(None, Some(de1.clone()), [endpoint.clone()].into());

        assert_eq!(cluster1.get(&Some(nl1.clone())).unwrap().len(), 1);
        assert_eq!(cluster1.get(&Some(de1.clone())).unwrap().len(), 1);
        assert!(
            cluster1
                .get(&Some(de1.clone()))
                .unwrap()
                .contains(&endpoint)
        );

        cluster1.insert(None, Some(de1.clone()), <_>::default());

        assert_eq!(cluster1.get(&Some(nl1.clone())).unwrap().len(), 1);
        assert!(cluster1.get(&Some(de1.clone())).unwrap().is_empty());
    }

    #[test]
    fn reject_duplicate_localities() {
        let nl1 = Locality::with_region("nl-1");

        let nl01 = Ipv4Addr::new(1, 1, 1, 1);
        let nl02 = Ipv6Addr::new(1, 1, 1, 1, 1, 1, 1, 1);

        let expected: std::collections::BTreeSet<_> = [
            Endpoint::new((Ipv4Addr::new(1, 2, 3, 4), 1234).into()),
            Endpoint::new((Ipv4Addr::new(4, 3, 2, 1), 1234).into()),
        ]
        .into();

        let cluster = ClusterMap::new();
        cluster.insert(Some(nl01.into()), Some(nl1.clone()), expected.clone());

        let not_expected: std::collections::BTreeSet<_> =
            [Endpoint::new((Ipv4Addr::new(20, 20, 20, 20), 1234).into())].into();

        fn set_to_map(set: BTreeSet<Endpoint>) -> BTreeMap<EndpointAddress, EndpointMetadata> {
            set.into_iter()
                .map(|ep| (ep.address, ep.metadata))
                .collect()
        }

        cluster.insert(Some(nl02.into()), Some(nl1.clone()), not_expected.clone());
        assert_eq!(
            cluster.get(&Some(nl1.clone())).unwrap().endpoints,
            set_to_map(expected)
        );

        cluster.remove_locality(Some(nl01.into()), &Some(nl1.clone()));

        cluster.insert(Some(nl02.into()), Some(nl1.clone()), not_expected.clone());
        assert_eq!(
            cluster.get(&Some(nl1)).unwrap().endpoints,
            set_to_map(not_expected)
        );
    }

    #[test]
    fn remove_avoids_deadlocks() {
        let endpoints: std::collections::BTreeSet<_> = [
            Endpoint::new((Ipv4Addr::new(1, 2, 3, 4), 1234).into()),
            Endpoint::new((Ipv6Addr::new(1, 2, 3, 4, 5, 6, 7, 8), 1234).into()),
            Endpoint::new((Ipv4Addr::new(4, 3, 2, 1), 1234).into()),
        ]
        .into();

        let cluster = ClusterMap::new();
        {
            cluster.insert(None, None, endpoints.clone());
            for ep in &endpoints {
                assert!(cluster.remove_endpoint(ep));
            }

            assert!(cluster.get(&None).is_none());
        }

        {
            cluster.insert(None, None, endpoints.clone());
            for _ in 0..endpoints.len() {
                assert!(cluster.remove_endpoint_if(|_ep| true));
            }

            assert!(cluster.get(&None).is_none());
        }
    }
}
