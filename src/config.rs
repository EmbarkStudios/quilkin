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

//! Quilkin configuration.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};

use base64_serde::base64_serde_type;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    generated::envoy::service::discovery::v3::Resource as XdsResource,
    net::cluster::{self, ClusterMap},
    xds::{self, ResourceType},
};

pub use self::{
    config_type::ConfigType,
    datacenter::{Datacenter, DatacenterMap},
    error::ValidationError,
    icao::{IcaoCode, NotifyingIcaoCode},
    watch::Watch,
};

mod config_type;
mod datacenter;
mod error;
pub mod filter;
mod icao;
pub mod qcmp;
mod serialization;
pub mod watch;

const ETC_CONFIG_PATH: &str = "/etc/quilkin/quilkin.yaml";
pub(crate) const BACKOFF_INITIAL_DELAY: Duration = Duration::from_millis(500);

pub type ConfigMap = typemap_rev::TypeMap<dyn typemap_rev::CloneDebuggableStorage>;

#[derive(Debug, Clone, Default)]
#[repr(transparent)]
pub(crate) struct LeaderLock(Arc<Lazy<Arc<AtomicBool>>>);

impl LeaderLock {
    pub(crate) fn load(&self) -> bool {
        self.0.load(Relaxed)
    }

    pub(crate) fn store(&self, is_leader: bool) {
        crate::metrics::leader_election(is_leader);
        self.0.store(is_leader, Relaxed);
    }
}

pub type BadNodeInformer = tokio::sync::mpsc::UnboundedSender<std::net::SocketAddr>;

base64_serde_type!(pub Base64Standard, base64::engine::general_purpose::STANDARD);
#[derive(Clone)]
#[cfg_attr(test, derive(Debug))]
pub struct Config {
    pub dyn_cfg: DynamicConfig,
    bad_node_informer: Option<BadNodeInformer>,
    cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

#[cfg(test)]
impl PartialEq for Config {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_cfg == other.dyn_cfg
    }
}

/// Configuration for a component
#[derive(Clone)]
pub struct DynamicConfig {
    pub id: Arc<parking_lot::Mutex<String>>,
    pub version: Version,
    pub icao_code: icao::NotifyingIcaoCode,
    pub typemap: ConfigMap,
}

impl typemap_rev::TypeMapKey for ClusterMap {
    type Value = Watch<ClusterMap>;
}

impl typemap_rev::TypeMapKey for DatacenterMap {
    type Value = Watch<DatacenterMap>;
}

impl typemap_rev::TypeMapKey for LeaderLock {
    type Value = LeaderLock;
}

impl DynamicConfig {
    pub fn clusters(&self) -> Option<&Watch<ClusterMap>> {
        self.typemap.get::<ClusterMap>()
    }

    pub fn datacenters(&self) -> Option<&Watch<DatacenterMap>> {
        self.typemap.get::<DatacenterMap>()
    }

    pub(crate) fn init_leader_lock(&self) -> LeaderLock {
        self.typemap.get::<LeaderLock>().unwrap().clone()
    }

    pub(crate) fn leader_lock(&self) -> Option<&LeaderLock> {
        self.typemap
            .get::<LeaderLock>()
            .filter(|ll| Lazy::get(&*ll.0).is_some())
    }
}

#[cfg(test)]
mod test_impls {
    use super::*;
    use crate::filters::FilterChain;

    impl PartialEq for DynamicConfig {
        fn eq(&self, other: &Self) -> bool {
            if self.id.lock().as_str() != other.id.lock().as_str() || self.version != other.version
            {
                return false;
            }

            fn compare<T>(a: &ConfigMap, b: &ConfigMap) -> bool
            where
                T: typemap_rev::TypeMapKey,
                T::Value: PartialEq + Clone + std::fmt::Debug,
            {
                match (a.get::<T>(), b.get::<T>()) {
                    (None, None) => true,
                    (None, Some(_)) | (Some(_), None) => false,
                    (Some(a), Some(b)) => a == b,
                }
            }

            compare::<FilterChain>(&self.typemap, &other.typemap)
                && compare::<qcmp::QcmpPort>(&self.typemap, &other.typemap)
                && compare::<ClusterMap>(&self.typemap, &other.typemap)
                && compare::<DatacenterMap>(&self.typemap, &other.typemap)
        }
    }

    use std::fmt;

    // typemap uses a HashMap<> for storage, which means that two typemaps won't
    // be ordered the same, resulting in messy diffs
    impl fmt::Debug for DynamicConfig {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let mut ds = f.debug_struct("DynamicConfig");
            ds.field("id", &self.id.lock());
            ds.field("version", &self.version);
            ds.field("icao_code", &self.icao_code.load());

            let tm = self.typemap.clone();
            ds.field(
                "typemap",
                &tm.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
            );

            ds.finish()
        }
    }
}

impl quilkin_xds::config::Configuration for Config {
    fn identifier(&self) -> String {
        String::clone(&self.id())
    }

    fn is_leader(&self) -> Option<bool> {
        self.dyn_cfg.leader_lock().map(|ll| ll.load())
    }

    fn allow_request_processing(&self, resource_type: &str) -> bool {
        resource_type.parse::<ResourceType>().is_ok()
    }

    fn apply_delta(
        &self,
        type_url: &str,
        resources: Vec<XdsResource>,
        removed_resources: &[String],
        remote_addr: Option<std::net::IpAddr>,
    ) -> quilkin_xds::Result<()> {
        self.apply_delta(type_url, resources, removed_resources, remote_addr)
    }

    fn delta_discovery_request(
        &self,
        client_state: &quilkin_xds::config::ClientState,
    ) -> quilkin_xds::Result<DeltaDiscoveryRes> {
        self.delta_discovery_request(client_state)
    }

    fn interested_resources(
        &self,
        _server_version: &str,
    ) -> impl Iterator<Item = (&'static str, Vec<String>)> {
        [
            (xds::CLUSTER_TYPE, Vec::new()),
            (xds::DATACENTER_TYPE, Vec::new()),
        ]
        .into_iter()
    }

    fn on_changed(
        &self,
        control_plane: quilkin_xds::server::ControlPlane<Self>,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        tracing::trace!("waiting for changes");

        async move {
            let clusters = control_plane.config.dyn_cfg.clusters();
            let datacenters = control_plane.config.dyn_cfg.datacenters();
            let filters = control_plane.config.dyn_cfg.subscribe_filter_changes();
            let qcmp_port = control_plane.config.dyn_cfg.qcmp_port();

            let indefinite = clusters.is_none()
                && datacenters.is_none()
                && filters.is_none()
                && qcmp_port.is_none();
            let mut ls = tokio::task::JoinSet::new();

            if let Some(clusters) = clusters {
                let mut cw = clusters.watch();
                let cp = control_plane.clone();

                ls.spawn(async move {
                    loop {
                        match cw.changed().await {
                            Ok(()) => cp.push_update(xds::CLUSTER_TYPE),
                            Err(error) => tracing::error!(%error, "error watching cluster changes"),
                        }
                    }
                });
            }

            if let Some(datacenters) = datacenters {
                let mut dcw = datacenters.watch();
                let cp = control_plane.clone();

                ls.spawn(async move {
                    loop {
                        match dcw.changed().await {
                            Ok(()) => cp.push_update(xds::DATACENTER_TYPE),
                            Err(error) => {
                                tracing::error!(%error, "error watching datacenter changes");
                            }
                        }
                    }
                });
            }

            if let Some(mut filters) = filters {
                let cp = control_plane.clone();

                ls.spawn(async move {
                    loop {
                        match filters.recv().await {
                            Ok(()) => cp.push_update(xds::FILTER_CHAIN_TYPE),
                            Err(error) => {
                                tracing::error!(%error, "error watching FilterChain changes");
                            }
                        }
                    }
                });
            }

            if let Some(qcmp) = qcmp_port {
                let mut icao_rx = control_plane.config.dyn_cfg.icao_code.subscribe();
                let mut qcmp_rx = qcmp.subscribe();
                let cp = control_plane;

                ls.spawn(async move { loop {
                    tokio::select! {
                        i = icao_rx.recv() => {
                            match i {
                                Ok(()) => cp.push_update(xds::DATACENTER_TYPE),
                                Err(error) => tracing::error!(%error, "error watching ICAO changes"),
                            }
                        }
                        q = qcmp_rx.recv() => {
                            match q {
                                Ok(_) => cp.push_update(xds::DATACENTER_TYPE),
                                Err(error) => tracing::error!(%error, "error watching QCMP port changes"),
                            }
                        }
                    }
                } });
            }

            if indefinite {
                ls.spawn(async {
                    tokio::time::sleep(std::time::Duration::from_secs(u64::MAX)).await;
                });
            }

            drop(shutdown.changed().await);
            ls.abort_all();

            // join_all panics if a task is cancelled, we don't care
            while ls.join_next().await.is_some() {}
        }
    }

    fn client_disconnected(&self, ip: std::net::IpAddr) {
        if let Some(dc) = self.dyn_cfg.datacenters() {
            dc.modify(|dc| {
                tracing::debug!(%ip, "removing agent");
                dc.remove(ip);
            });
        }
    }
}

use crate::net::xds::config::DeltaDiscoveryRes;

fn resolve_id(id: Option<String>) -> String {
    fn uuid() -> String {
        Uuid::new_v4().as_hyphenated().to_string()
    }

    fn hostname() -> Option<String> {
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                sys_info::hostname().ok()
            } else {
                None
            }
        }
    }

    id.filter(|v| !v.is_empty())
        .or_else(hostname)
        .unwrap_or_else(uuid)
}

impl Drop for Config {
    fn drop(&mut self) {
        if let Some(token) = &self.cancellation_token {
            token.cancel();
        }
    }
}

impl Config {
    /// Creates and initializes a new Config
    ///
    /// This constructor is mainly intended for tests
    pub fn new(
        id: Option<String>,
        icao_code: IcaoCode,
        providers: &crate::Providers,
        service: &crate::Service,
    ) -> Self {
        let mut config = Config {
            dyn_cfg: DynamicConfig {
                id: Arc::new(parking_lot::Mutex::new(resolve_id(id))),
                version: Version::default(),
                icao_code: NotifyingIcaoCode::new(icao_code),
                typemap: default_typemap(),
            },
            bad_node_informer: None,
            cancellation_token: None,
        };
        providers.init_config(&mut config);
        service.init_config(&mut config);

        config
    }

    /// Creates and initializes a new Arc<Config>
    ///
    /// Spawns a tokio task as a side effect that will be stopped when the cancellation token is
    /// cancelled. The token _will_ be cancelled when Config is dropped, so make sure to pass in a
    /// child token if the token is intended to live longer than the Config.
    pub fn new_rc(
        id: Option<String>,
        icao_code: IcaoCode,
        providers: &crate::Providers,
        service: &crate::Service,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<std::net::SocketAddr>();

        let mut config = Self::new(id, icao_code, providers, service);
        config.cancellation_token = Some(cancellation_token);
        config.bad_node_informer = Some(tx);

        let config = Arc::new(config);
        config
            .spawn_janitor(rx)
            .expect("spawn_janitor() from new_rc() should not fail");

        config
    }

    /// Spawns a janitor task to help out with cleaning stale Datacenter entries from the Config
    fn spawn_janitor(
        self: &Arc<Config>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<std::net::SocketAddr>,
    ) -> eyre::Result<()> {
        let cancellation_token = self
            .cancellation_token
            .as_ref()
            .ok_or_else(|| eyre::eyre!("cancellation token not set"))?
            .clone();

        // Ensure that we don't keep an owning reference to Arc<Config> so that Drop can occurr
        let janitor_config_ref = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                    node = rx.recv() => {
                        if let Some(node) = node {
                            let ip = node.ip();
                            if let Some(config) = janitor_config_ref.upgrade() {
                                if let Some(datacenters) = config.dyn_cfg.datacenters() {
                                    datacenters.modify(|wg| {
                                        tracing::warn!(%ip, "removing datacenter from local state");
                                        wg.remove(ip);
                                    });
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            tracing::trace!("Stopping janitor task");
        });
        Ok(())
    }

    pub fn read_config(
        self: &Arc<Self>,
        config_path: &std::path::Path,
        locality: Option<crate::net::endpoint::Locality>,
    ) -> Result<(), eyre::Error> {
        let paths = [config_path, std::path::Path::new(ETC_CONFIG_PATH)];
        let mut paths = paths.iter();

        let file = loop {
            let Some(path) = paths.next() else {
                return Ok(());
            };

            match std::fs::File::open(path) {
                Ok(file) => break file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path = %path.display(), "config path not found");
                    continue;
                }
                Err(err) => {
                    tracing::error!(path = %path.display(), error = ?err, "failed to read path");
                    eyre::bail!(err);
                }
            }
        };

        let json = serde_yaml::from_reader(file)?;
        self.update_from_json(json, locality)?;

        Ok(())
    }

    pub fn bad_node_informer(&self) -> Option<BadNodeInformer> {
        self.bad_node_informer.clone()
    }

    /// Given a list of subscriptions and the current state of the calling client,
    /// construct a response with the current state of our resources that differ
    /// from those of the client
    pub fn delta_discovery_request(
        &self,
        client_state: &quilkin_xds::config::ClientState,
    ) -> crate::Result<DeltaDiscoveryRes> {
        let mut resources = Vec::new();
        let mut removed = std::collections::HashSet::new();

        let resource_type = client_state.resource_type.parse::<ResourceType>()?;

        'append: {
            match resource_type {
                ResourceType::FilterChain => {
                    let Some(filters) = self.dyn_cfg.filters() else {
                        break 'append;
                    };

                    let resource = xds::Resource::FilterChain(
                        crate::net::cluster::proto::FilterChain::try_from(filters.load().as_ref())?,
                    );
                    let any = resource.try_encode()?;
                    let version = gxhash::gxhash64(&any.value, 0xdeadbeef);

                    let vstr = version.to_string();

                    if client_state.version_matches("filter_chain", &vstr) {
                        break 'append;
                    }

                    resources.push(XdsResource {
                        name: "filter_chain".into(),
                        version: vstr,
                        resource: Some(any),
                        aliases: Vec::new(),
                        ttl: None,
                        cache_control: None,
                    });
                }
                ResourceType::Datacenter => {
                    // The Datacenter xDS resource has two distinct propagation
                    // scenarios.
                    //
                    // Normally, the xDS resource `name` will be set to the ip
                    // address of the datacenter. However, when quilkin wants
                    // to propagate 'self' as a Datacenter, it doesn't have
                    // access to its own external IP address. Then the
                    // QUILKIN_SERVICE_ID (defaults to hostname) is used as the
                    // `name` instead.
                    //
                    // The `name` is used as the key in the version map on the
                    // xDS client, so as long as they are unique this should be
                    // ok.

                    #[inline]
                    fn resource_version(icao_code: &str, qcmp_port: u16) -> String {
                        format!("{icao_code}-{qcmp_port}")
                    }

                    if let Some(qport) = self.dyn_cfg.qcmp_port() {
                        let id = self.dyn_cfg.id.lock().to_string();
                        let icao_code = self.dyn_cfg.icao_code.load().to_string();
                        let qcmp_port = qport.load();
                        let version = resource_version(icao_code.as_str(), qcmp_port);

                        if !client_state.version_matches(&id, &version) {
                            let resource =
                                xds::Resource::Datacenter(crate::net::cluster::proto::Datacenter {
                                    qcmp_port: qcmp_port as _,
                                    icao_code: icao_code.clone(),
                                    host: String::new(),
                                });

                            resources.push(XdsResource {
                                name: id,
                                version,
                                resource: Some(resource.try_encode()?),
                                aliases: Vec::new(),
                                ttl: None,
                                cache_control: None,
                            });
                        }
                    }

                    if let Some(datacenters) = self.dyn_cfg.datacenters() {
                        for entry in datacenters.read().iter() {
                            let host = entry.key().to_string();
                            let qcmp_port = entry.qcmp_port;
                            let version =
                                resource_version(entry.icao_code.to_string().as_str(), qcmp_port);

                            if client_state.version_matches(&host, &version) {
                                continue;
                            }

                            let resource = crate::xds::Resource::Datacenter(
                                crate::net::cluster::proto::Datacenter {
                                    qcmp_port: qcmp_port as _,
                                    icao_code: entry.icao_code.to_string(),
                                    host: host.clone(),
                                },
                            );

                            resources.push(XdsResource {
                                name: host,
                                version,
                                resource: Some(resource.try_encode()?),
                                aliases: Vec::new(),
                                ttl: None,
                                cache_control: None,
                            });
                        }

                        {
                            let dc = datacenters.read();
                            for key in client_state.versions.keys() {
                                let Ok(addr) = key.parse() else {
                                    continue;
                                };
                                if dc.get(&addr).is_none() {
                                    removed.insert(key.clone());
                                }
                            }
                        }
                    }
                }
                ResourceType::Cluster => {
                    let mut push = |key: &Option<crate::net::endpoint::Locality>,
                                    value: &crate::net::cluster::EndpointSet|
                     -> crate::Result<()> {
                        let version = value.version().to_string();
                        let key_s = key.as_ref().map(|k| k.to_string()).unwrap_or_default();

                        if client_state.version_matches(&key_s, &version) {
                            return Ok(());
                        }

                        let resource = crate::xds::Resource::Cluster(
                            quilkin_xds::generated::quilkin::config::v1alpha1::Cluster {
                                locality: key.clone().map(|l| l.into()),
                                endpoints: value.endpoints.iter().map(|ep| ep.into()).collect(),
                            },
                        );

                        resources.push(XdsResource {
                            name: key_s,
                            version,
                            resource: Some(resource.try_encode()?),
                            ..Default::default()
                        });

                        Ok(())
                    };

                    let Some(clusters) = self.dyn_cfg.clusters() else {
                        break 'append;
                    };

                    if client_state.subscribed.is_empty() {
                        for cluster in clusters.read().iter() {
                            push(cluster.key(), cluster.value())?;
                        }
                    } else {
                        for locality in client_state.subscribed.iter().filter_map(|name| {
                            if name.is_empty() {
                                Some(None)
                            } else {
                                name.parse().ok().map(Some)
                            }
                        }) {
                            if let Some(cluster) = clusters.read().get(&locality) {
                                push(cluster.key(), cluster.value())?;
                            }
                        }
                    };

                    // Currently, we have exactly _one_ special case for removed resources, which
                    // is when ClusterMap::update_unlocated_endpoints is called to move the None
                    // locality endpoints to another one, so we just detect that case manually
                    if client_state.versions.contains_key("")
                        && clusters.read().get(&None).is_none()
                    {
                        removed.insert("".into());
                    }
                }
            }
        }

        Ok(DeltaDiscoveryRes { resources, removed })
    }

    #[tracing::instrument(skip_all, fields(response = type_url))]
    pub fn apply_delta(
        &self,
        type_url: &str,
        mut resources: Vec<XdsResource>,
        removed_resources: &[String],
        remote_addr: Option<std::net::IpAddr>,
    ) -> crate::Result<()> {
        let resource_type = type_url.parse::<ResourceType>()?;

        match resource_type {
            ResourceType::FilterChain => {
                let Some(filters) = self.dyn_cfg.filters() else {
                    return Ok(());
                };

                if resources.is_empty() && !removed_resources.is_empty() {
                    tracing::info!("ignoring filter chain removal");
                    return Ok(());
                }

                // Server should only ever send exactly one filter chain, more or less indicates a bug
                let Some(res) = resources.pop() else {
                    eyre::bail!("no resources in delta response");
                };

                eyre::ensure!(
                    resources.is_empty(),
                    "additional filter chain resources were present in delta response"
                );

                let Some(resource) = res.resource else {
                    eyre::bail!("filter chain response did not contain a resource payload");
                };

                let resource = match crate::xds::Resource::try_decode(resource)? {
                    crate::xds::Resource::FilterChain(r) => r,
                    res => {
                        eyre::bail!(
                            "filter chain response contained a {} resource payload",
                            res.type_url()
                        );
                    }
                };

                let fc =
                    crate::filters::FilterChain::try_create_fallible(resource.filters.into_iter())?;

                filters.store(fc);
            }
            ResourceType::Datacenter => {
                let Some(datacenters) = self.dyn_cfg.datacenters() else {
                    return Ok(());
                };

                datacenters.modify(|wg| {
                    if let Some(ip) = remote_addr.filter(|_| !removed_resources.is_empty()) {
                        wg.remove(ip);
                    }

                    for res in resources {
                        let Some(resource) = res.resource else {
                            eyre::bail!("a datacenter resource could not be applied because it didn't contain an actual payload");
                        };

                        let dc = match crate::xds::Resource::try_decode(resource) {
                            Ok(crate::xds::Resource::Datacenter(dc)) => dc,
                            Ok(other) => {
                                eyre::bail!("a datacenter resource could not be applied because the resource payload was '{}'", other.type_url());
                            }
                            Err(error) => {
                                return Err(error.wrap_err("a datacenter resource could not be applied because the resource payload could not be decoded"));
                            }
                        };

                        let host = if dc.host.is_empty() {
                            if let Some(ra) = remote_addr {
                                ra
                            } else {
                                continue;
                            }
                        } else {
                            match dc.host.parse() {
                                Ok(host) => host,
                                Err(_err) => {
                                    tracing::warn!("datacenter host not set, and there is not remote address");
                                    continue;
                                }
                            }
                        };

                        let parse_payload = || -> crate::Result<Datacenter> {
                            use eyre::Context;
                            let dc = Datacenter {
                                qcmp_port: dc.qcmp_port.try_into().context("unable to parse datacenter QCMP port")?,
                                icao_code: dc.icao_code.parse().context("unable to parse datacenter ICAO")?,
                            };

                            Ok(dc)
                        };

                        let datacenter = parse_payload()?;
                        wg.insert(
                            host,
                            datacenter,
                        );
                    }

                    Ok(())
                })?;
            }
            ResourceType::Cluster => {
                let Some(clusters) = self.dyn_cfg.clusters() else {
                    return Ok(());
                };

                clusters.modify(|guard| -> crate::Result<()> {
                    for removed in removed_resources {
                        let locality = if removed.is_empty() {
                            None
                        } else {
                            Some(removed.parse()?)
                        };
                        guard.remove_locality(remote_addr, &locality);
                    }

                    for res in resources {
                        let Some(resource) = res.resource else {
                            eyre::bail!("a cluster resource could not be applied because it didn't contain an actual payload");
                        };

                        let cluster = match crate::xds::Resource::try_decode(resource) {
                            Ok(crate::xds::Resource::Cluster(c)) => c,
                            Ok(other) => {
                                eyre::bail!("a cluster resource could not be applied because the resource payload was '{}'", other.type_url());
                            }
                            Err(error) => {
                                return Err(error.wrap_err("a cluster resource could not be applied because the resource payload could not be decoded"));
                            }
                        };

                        let parsed_version = res.version.parse()?;

                        let endpoints = match cluster
                                .endpoints
                                .into_iter()
                                .map(crate::net::endpoint::Endpoint::try_from)
                                .collect::<Result<_, _>>() {
                            Ok(eps) => eps,
                            Err(error) => {
                                return Err(error.wrap_err("a cluster resource could not be applied because one or more endpoints could not be parsed"));
                            }
                        };

                        let endpoints = crate::config::cluster::EndpointSet::with_version(
                            endpoints,
                            parsed_version,
                        );

                        let locality = cluster.locality.map(crate::net::endpoint::Locality::from);
                        guard.apply(remote_addr, locality, endpoints)?;
                    }

                    Ok(())
                })?;

                self.apply_metrics();
            }
        }

        Ok(())
    }

    pub fn cluster(
        self,
        remote_addr: Option<std::net::IpAddr>,
        locality: Option<quilkin_xds::locality::Locality>,
        cluster: BTreeSet<crate::net::Endpoint>,
    ) -> Self {
        let Some(clusters) = self.dyn_cfg.clusters() else {
            return self;
        };

        clusters.modify(|clusters| {
            clusters.insert(remote_addr, locality, cluster);
        });
        self
    }

    #[inline]
    pub fn apply_metrics(&self) {
        let Some(clusters) = self.dyn_cfg.clusters() else {
            return;
        };
        crate::metrics::apply_clusters(clusters);
    }

    #[inline]
    pub fn id(&self) -> String {
        self.dyn_cfg.id.lock().clone()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Serialize, JsonSchema, PartialEq)]
pub enum Version {
    #[serde(rename = "v1alpha1")]
    V1Alpha1,
}

impl Default for Version {
    fn default() -> Self {
        Self::V1Alpha1
    }
}

pub(crate) fn default_typemap() -> ConfigMap {
    typemap_rev::TypeMap::custom()
}

pub(crate) fn insert_default<T>(tm: &mut ConfigMap)
where
    T: typemap_rev::TypeMapKey,
    T::Value: Default + Clone + std::fmt::Debug,
{
    tm.insert::<T>(T::Value::default());
}

#[derive(Clone, Debug)]
pub struct AddressSelector {
    pub name: String,
    pub kind: AddrKind,
}

#[derive(Copy, Clone, Debug)]
pub enum AddrKind {
    Ipv4,
    Ipv6,
    Any,
}

impl clap::ValueEnum for crate::config::AddrKind {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Ipv4, Self::Ipv6, Self::Any]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        use clap::builder::PossibleValue as pv;
        Some(match self {
            Self::Ipv4 => pv::new("v4"),
            Self::Ipv6 => pv::new("v6"),
            Self::Any => pv::new("any"),
        })
    }
}
