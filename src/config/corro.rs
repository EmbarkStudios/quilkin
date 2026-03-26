use std::net::IpAddr;

pub struct BridgeInner {
    peer: corrosion::Peer,
    tx: super::DbTx,
    statements: Vec<corrosion::api::Statement>,
}

impl BridgeInner {
    fn new(config: &super::Config, ip: Option<IpAddr>) -> Option<Self> {
        let peer = ip.map(corrosion::ip_to_peer)?;
        let tx = config.dyn_cfg.db_tx()?;

        Some(Self {
            peer,
            tx,
            statements: Vec::new(),
        })
    }
}

impl Drop for BridgeInner {
    fn drop(&mut self) {
        let statements = std::mem::take(&mut self.statements);

        if !statements.is_empty() {
            let _rx_dropped = self.tx.tx.send(statements);
        }
    }
}

pub struct Bridge<'m> {
    maps: Option<(
        super::watch::WatchGuard<'m, super::DatacenterMap>,
        super::watch::WatchGuard<'m, crate::net::ClusterMap>,
    )>,
    b: Option<BridgeInner>,
}

impl<'m> Bridge<'m> {
    pub fn new(config: &'m super::Config, ip: Option<IpAddr>) -> Self {
        Self {
            maps: config
                .dyn_cfg
                .datacenters()
                .map(|dc| dc.write())
                .zip(config.dyn_cfg.clusters().map(|c| c.write())),
            b: BridgeInner::new(config, ip),
        }
    }

    pub fn insert_dc(&mut self, ip: IpAddr, dc: super::Datacenter) {
        let Some((wdc, wsrv)) = &self.maps else {
            return;
        };

        wdc.insert(ip, dc);

        let Some(bri) = &mut self.b else {
            return;
        };

        let peer = corrosion::ip_to_peer(ip);

        let mut sv = corrosion::SmallVec::<[_; 100]>::new();
        {
            let mut d = corrosion::db::write::Datacenter(&mut sv);
            d.insert(peer, dc.qcmp_port, dc.icao_code);
        }

        // We may have inserted clusters already from this DC, if so, insert all of them
        let Some(locality) = wsrv.locality_for_ip(ip) else {
            return;
        };

        let Some(endpoints) = wsrv.get(&Some(locality)) else {
            return;
        };

        {
            let mut s = corrosion::db::write::Server::for_peer(peer, &mut sv);

            for ep in endpoints.value().endpoint_iter() {
                s.upsert(
                    &quilkin_types::Endpoint {
                        address: ep.address.host.clone(),
                        port: ep.address.port,
                    },
                    dc.icao_code,
                    &ep.metadata.known.tokens,
                );
            }
        }

        bri.statements.extend(sv);
    }

    pub fn remove_dc(&mut self, ip: IpAddr) {
        let Some((wdc, _wsrv)) = &self.maps else {
            return;
        };

        wdc.remove(ip);

        let Some(bri) = &mut self.b else {
            return;
        };

        let peer = corrosion::ip_to_peer(ip);

        let mut sv = corrosion::SmallVec::<[_; 1]>::new();
        {
            let mut d = corrosion::db::write::Datacenter(&mut sv);
            d.remove(peer, None);
        }

        bri.statements.extend(sv);
    }

    pub fn remove_locality(
        &mut self,
        ip: Option<IpAddr>,
        locality: &Option<quilkin_xds::locality::Locality>,
    ) {
        let Some((_wdc, wsrv)) = &self.maps else {
            return;
        };

        let Some(es) = wsrv.remove_locality(ip, locality) else {
            return;
        };

        let Some((bri, ip)) = self.b.as_mut().zip(ip) else {
            return;
        };

        let peer = corrosion::ip_to_peer(ip);

        let mut statements = corrosion::SmallVec::<[_; 100]>::new();
        {
            let mut stx = corrosion::db::write::Server::for_peer(peer, &mut statements);

            for rm_srv in es.into_endpoints() {
                stx.remove_deferred(&rm_srv);
            }
        }

        bri.statements.extend(statements);
    }

    pub fn apply_changes(
        &mut self,
        ip: Option<IpAddr>,
        locality: Option<quilkin_xds::locality::Locality>,
        icao: Option<quilkin_types::IcaoCode>,
        es: crate::config::cluster::EndpointSet,
    ) {
        let Some((_wdc, wsrv)) = &self.maps else {
            return;
        };

        let mut rm = Vec::new();
        let mut up = Vec::new();

        if let Err(error) = wsrv.apply(ip, locality, es, &mut rm, &mut up) {
            tracing::error!(%error, "failed to apply xDS endpoint update");
            return;
        }

        let Some((bridge, icao)) = self.b.as_mut().zip(icao) else {
            return;
        };

        let mut statements = corro::SmallVec::<[_; 100]>::new();
        {
            let mut stx = corro::db::write::Server::for_peer(*peer, &mut statements);

            if let Some((r, u)) = STATEMENTS.lock().remove(peer.ip()) {
                if !r.is_empty() {
                    for rm_srv in r {
                        stx.remove_deferred(&rm_srv);
                    }
                }

                if !u.is_empty() {
                    for (ep, ts) in u {
                        stx.upsert(&ep, icao, &ts);
                    }
                }
            }

            if !rm.is_empty() {
                for rm_srv in rm {
                    stx.remove_deferred(&rm_srv);
                }
            }

            if !up.is_empty() {
                for (ep, ts) in up {
                    stx.upsert(&ep, icao, &ts);
                }
            }
        }

        self.1.extend(statements);
    }
}
