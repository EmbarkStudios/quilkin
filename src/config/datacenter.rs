use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, SocketAddr},
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct DatacenterMap {
    map: dashmap::DashMap<IpAddr, Datacenter>,
    #[serde(skip)]
    removed: parking_lot::Mutex<Vec<SocketAddr>>,
    version: AtomicU64,
}

impl DatacenterMap {
    #[inline]
    pub fn insert(&self, ip: IpAddr, datacenter: Datacenter) -> Option<Datacenter> {
        let old = self.map.insert(ip, datacenter);
        self.version.fetch_add(1, Relaxed);
        old
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
    pub fn version(&self) -> u64 {
        self.version.load(Relaxed)
    }

    #[inline]
    pub fn exists(&self, key: &IpAddr) -> bool {
        self.map.get(key).is_some()
    }

    /// Iterates over the entries in the `DatacenterMap` with the given func
    ///
    /// This ensures that the dashmap entry references are never held across await boundaries as
    /// the func cannot be async.
    #[inline]
    pub fn iter_with<F, T>(&self, func: F) -> Vec<T>
    where
        F: for<'a> Fn(&'a IpAddr, &'a Datacenter) -> T,
    {
        let mut results: Vec<T> = Vec::new();
        for entry in self.map.iter() {
            results.push(func(entry.key(), entry.value()));
        }
        results
    }

    #[inline]
    pub fn remove(&self, ip: IpAddr) {
        let mut lock = self.removed.lock();
        let mut version = 0;

        let Some((_k, v)) = self.map.remove(&ip) else {
            return;
        };

        lock.push((ip, v.qcmp_port).into());
        version += 1;

        self.version.fetch_add(version, Relaxed);
    }

    #[inline]
    pub fn removed(&self) -> Vec<SocketAddr> {
        std::mem::take(&mut self.removed.lock())
    }
}

impl Clone for DatacenterMap {
    fn clone(&self) -> Self {
        let map = self.map.clone();
        Self {
            map,
            version: <_>::default(),
            removed: Default::default(),
        }
    }
}

impl crate::config::watch::Watchable for DatacenterMap {
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

impl schemars::JsonSchema for DatacenterMap {
    fn schema_name() -> String {
        <std::collections::HashMap<IpAddr, Datacenter>>::schema_name()
    }
    fn json_schema(r#gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        <std::collections::HashMap<IpAddr, Datacenter>>::json_schema(r#gen)
    }

    fn is_referenceable() -> bool {
        <std::collections::HashMap<IpAddr, Datacenter>>::is_referenceable()
    }
}

impl PartialEq for DatacenterMap {
    fn eq(&self, rhs: &Self) -> bool {
        if self.map.len() != rhs.map.len() {
            return false;
        }

        for a in self.map.iter() {
            match rhs.map.get(a.key()).filter(|b| *a.value() == **b) {
                Some(_) => {}
                None => return false,
            }
        }

        true
    }
}

#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub struct Datacenter {
    pub qcmp_port: u16,
    pub icao_code: super::IcaoCode,
}
