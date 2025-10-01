use crate::{
    config::{
        Datacenter, IcaoCode,
        crdt::collections::{self, Cmrdt, ContentEqual, map::Op as MapOp},
    },
    generated::envoy::service::discovery::v3::Resource as XdsResource,
    net::{NodeAddress, NodeIp, UnknownIp},
};
use std::{collections::HashSet, sync::Arc};

pub type DatacenterMapCollection =
    collections::Map<IcaoCode, collections::Orswot<NodeAddress, NodeIp>, NodeIp>;
pub type InnerDatacenterMapOp = MapOp<IcaoCode, collections::Orswot<NodeAddress, NodeIp>, NodeIp>;

impl typemap_rev::TypeMapKey for DatacenterMap {
    type Value = Arc<DatacenterMap>;
}

impl crate::config::DynamicConfig {
    #[inline]
    pub fn xds_datacenters(&self) -> Option<&Arc<DatacenterMap>> {
        self.typemap.get::<DatacenterMap>()
    }
}

#[derive(Debug)]
pub struct DatacenterMap {
    dm: parking_lot::Mutex<DatacenterMapCollection>,
    local_ip: NodeIp,
    remote: parking_lot::Mutex<NodeIp>,
    tx: DatacenterMapOpSender,
}

impl DatacenterMap {
    pub fn new(capacity: usize) -> Result<Self, UnknownIp> {
        let local_ip = NodeIp::local_ip()?;
        let (tx, _rx) = tokio::sync::broadcast::channel(capacity);

        Ok(Self {
            dm: Default::default(),
            remote: parking_lot::Mutex::new(NodeIp::default()),
            local_ip,
            tx,
        })
    }

    pub fn serialize_snapshot(
        &self,
        resources: &mut Vec<XdsResource>,
        removed: &mut HashSet<String>,
        cs: &quilkin_xds::config::ClientState,
    ) {
        let dm = self.dm.lock();
        for (icao, endpoints) in &dm.entries {
            let icaos = icao.to_string();
            for (endpoint, clock) in &endpoints.val.entries {
                let resource = {
                    let res =
                        crate::xds::Resource::Datacenter(crate::net::cluster::proto::Datacenter {
                            qcmp_port: endpoint.port as u32,
                            icao_code: icaos.clone(),
                            host: endpoint.ip.to_string(),
                        });

                    match res.try_encode() {
                        Ok(res) => Some(res),
                        Err(error) => {
                            tracing::error!(%icao, %error, "failed to encode DatacenterMap xDS resource");
                            continue;
                        }
                    }
                };

                match serde_json::to_string(clock) {
                    Ok(version) => {
                        let name = format!("{icao}-{endpoint}");
                        if cs.version_matches(&name, &version) {
                            continue;
                        }

                        resources.push(XdsResource {
                            name,
                            resource,
                            version,
                            aliases: Vec::new(),
                            ttl: None,
                            cache_control: None,
                        });
                    }
                    Err(error) => {
                        tracing::error!(%icao, %error, "failed to serialize DatacenterMap xDS resource version");
                    }
                }
            }
        }

        {
            for key in cs.versions.keys() {
                let Some((icao, endpoint)) = parse_crdt_key(key) else {
                    continue;
                };
                if let Some(endpoints) = dm.entries.get(&icao) {
                    if endpoints.val.entries.contains_key(&endpoint) {
                        continue;
                    }
                }

                removed.insert(key.clone());
            }
        }
    }

    #[inline]
    pub fn upsert_datacenters(&self, resources: Vec<XdsResource>, ip: std::net::IpAddr) {
        let ip = ip.into();
        self.remove_ip(ip);

        for res in resources {
            let Some(resource) = res.resource else {
                tracing::error!(
                    res.name,
                    "a datacenter resource could not be applied because it didn't contain an actual payload"
                );
                continue;
            };

            let dc = match crate::xds::Resource::try_decode(resource) {
                Ok(crate::xds::Resource::Datacenter(dc)) => dc,
                Ok(other) => {
                    tracing::error!(
                        type_url = other.type_url(),
                        "a datacenter resource could not be applied because the resource payload was not the correct type"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::error!(%error, "a datacenter resource could not be applied because the resource payload could not be decoded");
                    continue;
                }
            };

            let parse_payload = || -> crate::Result<(IcaoCode, NodeAddress)> {
                use eyre::Context;
                let port = dc
                    .qcmp_port
                    .try_into()
                    .context("unable to parse datacenter QCMP port")?;
                let icao = dc
                    .icao_code
                    .parse()
                    .context("unable to parse datacenter ICAO")?;

                Ok((icao, NodeAddress { ip, port }))
            };

            let (icao, node_address) = match parse_payload() {
                Ok(dc) => dc,
                Err(error) => {
                    tracing::error!(%error, "failed to parse Datacenter protobuf");
                    continue;
                }
            };

            let mut dm = self.dm.lock();
            if let Some(eps) = dm.entries.get(&icao) {
                if eps.val.entries.contains_key(&node_address) {
                    continue;
                }
            }

            let op = dm.update(icao, dm.add_ctx(self.local_ip), |v, ctx| {
                v.add(node_address, ctx)
            });

            self.send(DatacenterMapOp::Add {
                icao,
                ip: node_address.ip,
                port: node_address.port,
            });

            dm.apply(op);
        }
    }

    #[inline]
    pub fn remove_datacenter(&self, ip: NodeIp) {
        self.remove_ip(ip);
    }

    #[inline]
    fn remove_ip(&self, ip: NodeIp) {
        let mut dm = self.dm.lock();
        for (key, entry) in &dm.entries {
            for addr in entry.val.entries.keys() {
                if addr.ip == ip {
                    let rm = entry.val.rm(*addr, entry.val.rm_ctx());
                    let rm_op = dm.up(*key, rm, self.local_ip);

                    self.send(DatacenterMapOp::Rm {
                        icao: *key,
                        ip,
                        port: addr.port,
                    });

                    dm.apply(rm_op);
                    return;
                }
            }
        }
    }

    #[inline]
    pub fn watch(&self) -> tokio::sync::broadcast::Receiver<DatacenterMapOp> {
        self.tx.subscribe()
    }

    #[inline]
    pub fn get_by_ip(&self, ip: std::net::IpAddr) -> Option<Datacenter> {
        self.dm.lock().entries.iter().find_map(|(key, entry)| {
            entry.val.entries.iter().find_map(|(addr, _)| {
                (addr.ip == ip).then_some(Datacenter {
                    icao_code: *key,
                    qcmp_port: addr.port,
                })
            })
        })
    }

    #[inline]
    pub fn reset(&self, snapshot: DatacenterMapCollection) {
        self.send(DatacenterMapOp::Clear);

        for (key, val) in &snapshot.entries {
            let icao = *key;
            for node in val.val.entries.keys() {
                self.send(DatacenterMapOp::Add {
                    icao,
                    ip: node.ip,
                    port: node.port,
                });
            }
        }

        *self.dm.lock() = snapshot;
    }

    pub fn apply_xds(&self, resources: Vec<XdsResource>, removed: &[String], remote: NodeIp) {
        let mut dc_map = self.dm.lock();

        // For now since proxies are only ever connected to one relay, we reset
        // the map if we connect to a new relay
        {
            let mut cur_remote = self.remote.lock();
            if *cur_remote != remote {
                self.send(DatacenterMapOp::Clear);
                dc_map.remove_actor(*cur_remote, |_key| {});
                *cur_remote = remote;
            }
        }

        for rm in removed {
            let (icao, node_addr_or_port) = if let Some((icao, ep)) = parse_crdt_key(rm) {
                (icao, either::Left(ep))
            } else if let Some((icao, port)) = parse_old_key(rm) {
                (icao, either::Right(port))
            } else {
                continue;
            };

            if let Some(entry) = dc_map.entries.get(&icao) {
                if entry.val.len() == 1 {
                    let op = dc_map.rm(icao, dc_map.rm_ctx());
                    self.send(DatacenterMapOp::RmAll { icao });
                    dc_map.apply(op);
                } else if let either::Left(node_addr) = node_addr_or_port {
                    let rm = entry.val.rm(node_addr, entry.val.rm_ctx());
                    let rm_op = dc_map.up(icao, rm, remote);
                    self.send(DatacenterMapOp::Rm {
                        icao,
                        ip: node_addr.ip,
                        port: node_addr.port,
                    });
                    dc_map.apply(rm_op);
                }
            }
        }

        for res in resources {
            let Some(resource) = res.resource else {
                tracing::error!(
                    res.name,
                    "a datacenter resource could not be applied because it didn't contain an actual payload"
                );
                continue;
            };

            let dc = match crate::xds::Resource::try_decode(resource) {
                Ok(crate::xds::Resource::Datacenter(dc)) => dc,
                Ok(other) => {
                    tracing::error!(
                        type_url = other.type_url(),
                        "a datacenter resource could not be applied because the resource payload was not the correct type"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::error!(%error, "a datacenter resource could not be applied because the resource payload could not be decoded");
                    continue;
                }
            };

            let host = match dc.host.parse() {
                Ok(host) => host,
                Err(error) => {
                    tracing::error!(%error, host = dc.host, "failed to parse datacenter host");
                    remote
                }
            };

            let parse_payload = || -> crate::Result<(IcaoCode, NodeAddress)> {
                use eyre::Context;
                let port = dc
                    .qcmp_port
                    .try_into()
                    .context("unable to parse datacenter QCMP port")?;
                let icao = dc
                    .icao_code
                    .parse()
                    .context("unable to parse datacenter ICAO")?;

                Ok((icao, NodeAddress { ip: host, port }))
            };

            let (icao, node_address) = match parse_payload() {
                Ok(dc) => dc,
                Err(error) => {
                    tracing::error!(%error, "failed to parse Datacenter protobuf");
                    continue;
                }
            };

            if let Some(eps) = dc_map.entries.get(&icao) {
                if eps.val.entries.contains_key(&node_address) {
                    continue;
                }
            }

            let op = dc_map.update(icao, dc_map.add_ctx(remote), |v, ctx| {
                v.add(node_address, ctx)
            });
            self.send(DatacenterMapOp::Add {
                icao,
                ip: node_address.ip,
                port: node_address.port,
            });
            dc_map.apply(op);
        }
    }

    #[inline]
    fn send(&self, op: DatacenterMapOp) {
        let _ = self.tx.send(op);
    }

    #[inline]
    pub fn inner(&self) -> parking_lot::MutexGuard<'_, DatacenterMapCollection> {
        self.dm.lock()
    }
}

#[inline]
fn parse_crdt_key(s: &str) -> Option<(IcaoCode, NodeAddress)> {
    let (icao, ep) = s.split_once('-')?;
    let icao = icao.parse().ok()?;
    let endpoint = ep.parse().ok()?;

    Some((icao, endpoint))
}

#[inline]
fn parse_old_key(s: &str) -> Option<(IcaoCode, u16)> {
    let (icao, port) = s.split_once('-')?;
    Some((icao.parse().ok()?, port.parse().ok()?))
}

impl PartialEq for DatacenterMap {
    fn eq(&self, other: &Self) -> bool {
        self.dm.lock().content_equal(&other.dm.lock())
    }
}

impl serde::Serialize for DatacenterMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.dm.lock().serialize(serializer)
    }
}

#[cfg(test)]
impl Default for DatacenterMap {
    fn default() -> Self {
        Self {
            dm: Default::default(),
            local_ip: NodeIp(std::net::Ipv6Addr::from_bits(0)),
            remote: Default::default(),
            tx: tokio::sync::broadcast::channel(1).0,
        }
    }
}

#[derive(Clone)]
pub enum DatacenterMapOp {
    Clear,
    Add {
        icao: IcaoCode,
        ip: NodeIp,
        port: u16,
    },
    Rm {
        icao: IcaoCode,
        ip: NodeIp,
        port: u16,
    },
    RmAll {
        icao: IcaoCode,
    },
}

pub type DatacenterMapOpReceiver = tokio::sync::broadcast::Receiver<DatacenterMapOp>;
pub type DatacenterMapOpSender = tokio::sync::broadcast::Sender<DatacenterMapOp>;
