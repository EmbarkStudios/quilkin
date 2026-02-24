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
    pub fn get(&self, key: &IpAddr) -> Option<dashmap::mapref::one::Ref<'_, IpAddr, Datacenter>> {
        self.map.get(key)
    }

    #[inline]
    pub fn iter(&self) -> dashmap::iter::Iter<'_, IpAddr, Datacenter> {
        self.map.iter()
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
    fn schema_name() -> std::borrow::Cow<'static, str> {
        <std::collections::HashMap<IpAddr, Datacenter>>::schema_name()
    }
    fn json_schema(sg: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        <std::collections::HashMap<IpAddr, Datacenter>>::json_schema(sg)
    }
}

impl PartialEq for DatacenterMap {
    fn eq(&self, rhs: &Self) -> bool {
        if self.map.len() != rhs.map.len() {
            return false;
        }

        for a in self.iter() {
            match rhs.get(a.key()).filter(|b| *a.value() == **b) {
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
