/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

pub mod corrosion;
pub mod fs;
pub mod http;
pub mod k8s;

use std::{net::SocketAddr, sync::Arc};

use crate::{
    config,
    metrics::provider_task_failures_total,
    net::EndpointAddress,
    providers::{corrosion::CorrosionMode, k8s::EventProcessor},
};
use eyre::Context;
use futures::TryStreamExt;
use quilkin_graceful::{TaskSpawner, health::ChecksInit};

/// Functionally infinite retries as provider tasks are long running tasks
/// that we continually want to retry and Quilkin can run for days or weeks.
const RETRIES: u32 = u32::MAX;
const BACKOFF_STEP: std::time::Duration = std::time::Duration::from_millis(250);
const MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// The available xDS source provider.
#[derive(Clone, Debug, Default, clap::Args)]
#[command(next_help_heading = "Provider Options")]
pub struct Providers {
    /// Watches Agones' game server CRDs for `Allocated` game server endpoints,
    /// and for a `ConfigMap` that specifies the filter configuration.
    #[arg(
        long = "provider.k8s",
        env = "QUILKIN_PROVIDERS_K8S",
        default_value_t = false
    )]
    k8s_enabled: bool,

    /// The namespace that quilkin and related configuration has been set in.
    #[arg(
        long = "provider.k8s.namespace",
        env = "QUILKIN_PROVIDERS_K8S_NAMESPACE",
        default_value_t = From::from("default"),
        requires("k8s_enabled"),
    )]
    k8s_namespace: String,

    /// Enable leader election via k8s lease
    #[arg(
        long = "provider.k8s.leader-election",
        env = "QUILKIN_PROVIDERS_K8S_LEADER_ELECTION",
        default_value_t = false
    )]
    k8s_leader_election: bool,

    /// Override the ID used for k8s leader election (defaults to service.id)
    #[arg(
        long = "provider.k8s.leader-election.id",
        env = "QUILKIN_PROVIDERS_K8S_LEADER_ELECTION_ID",
        requires("k8s_leader_election")
    )]
    k8s_leader_id: Option<String>,

    /// The name of the k8s Lease resource used for leader election
    #[arg(
        long = "provider.k8s.leader-election.lease-name",
        env = "QUILKIN_PROVIDERS_K8S_LEADER_ELECTION_LEASE_NAME",
        default_value_t = String::from("quilkin"),
    )]
    k8s_leader_lease_name: String,

    #[arg(
        long = "provider.k8s.agones",
        env = "QUILKIN_PROVIDERS_K8S_AGONES",
        default_value_t = false
    )]
    agones_enabled: bool,

    #[arg(
        long = "provider.k8s.agones.namespace",
        env = "QUILKIN_PROVIDERS_K8S_AGONES_NAMESPACE",
        default_value_t = String::default(),
        requires("agones_enabled"),
    )]
    agones_namespace: String,

    /// The list of namespaces to watch for `GameServer` CRD events.
    #[arg(
        long = "provider.k8s.agones.namespaces",
        env = "QUILKIN_PROVIDERS_K8S_AGONES_NAMESPACES",
        default_values_t = [String::from("default")],
        value_delimiter = ',',
        requires("agones_enabled"),
        conflicts_with("agones_namespace"),
    )]
    agones_namespaces: Vec<String>,

    /// If specified, filters the available gameserver addresses to the one that
    /// matches the specified type
    #[arg(
        long = "provider.k8s.agones.address-type",
        env = "QUILKIN_PROVIDERS_K8S_AGONES_ADDRESS_TYPE",
        requires("agones_enabled")
    )]
    pub address_type: Option<String>,
    /// If specified, additionally filters the gameserver address by its ip kind
    #[arg(
        long = "provider.k8s.agones.ip-kind",
        env = "QUILKIN_PROVIDERS_K8S_AGONES_IP_KIND",
        requires("address_type"),
        value_enum
    )]
    pub ip_kind: Option<crate::config::AddrKind>,

    #[arg(
        long = "provider.fs",
        env = "QUILKIN_PROVIDERS_FS",
        conflicts_with("k8s_enabled"),
        default_value_t = false
    )]
    fs_enabled: bool,

    #[arg(
        long = "provider.fs.path",
        env = "QUILKIN_PROVIDERS_FS_PATH",
        requires("fs_enabled"),
        default_value = "/etc/quilkin/config.yaml"
    )]
    fs_path: std::path::PathBuf,
    /// One or more `quilkin relay` endpoints to push configuration changes to.
    #[clap(
        long = "provider.mds.endpoints",
        env = "QUILKIN_PROVIDERS_MDS_ENDPOINTS",
        value_delimiter = ',',
        hide = true
    )]
    relay: Vec<tonic::transport::Endpoint>,
    /// The remote URL or local file path to retrieve the Maxmind database.
    #[clap(
        long = "provider.mmdb.endpoints",
        env = "QUILKIN_PROVIDERS_MMDB_ENDPOINTS"
    )]
    mmdb: Option<crate::net::maxmind_db::Source>,
    /// One or more socket addresses to forward packets to.
    #[clap(
        long = "provider.static.endpoints",
        env = "QUILKIN_PROVIDERS_STATIC_ENDPOINTS",
        value_delimiter = ','
    )]
    endpoints: Vec<EndpointAddress>,
    /// Assigns dynamic tokens to each address in the `--to` argument
    ///
    /// Format is `<number of unique tokens>:<length of token suffix for each packet>`
    #[clap(
        long = "provider.static.endpoint-tokens",
        env = "QUILKIN_PROVIDERS_STATIC_ENDPOINT_TOKENS",
        requires("endpoints")
    )]
    endpoint_tokens: Option<String>,
    /// One or more xDS service endpoints to listen for config changes.
    #[clap(
        long = "provider.xds.endpoints",
        env = "QUILKIN_PROVIDERS_XDS_ENDPOINTS",
        value_delimiter = ',',
        hide = true
    )]
    xds_endpoints: Vec<tonic::transport::Endpoint>,
    /// One or more `quilkin relay` endpoints to push or pull configuration changes to/from
    #[clap(
        long = "provider.corrosion.endpoints",
        env = "QUILKIN_PROVIDERS_CORROSION_ENDPOINTS",
        value_delimiter = ','
    )]
    corrosion_endpoints: Vec<EndpointAddress>,
    /// What mode to run corrosion in
    #[clap(
        long = "provider.corrosion.mode",
        env = "QUILKIN_PROVIDERS_CORROSION_MODE"
    )]
    corrosion_mode: Option<corrosion::CorrosionMode>,
    /// Enable the HTTP provider, which exposes a REST API for managing endpoints
    /// and the filter chain.
    #[arg(
        long = "provider.http",
        env = "QUILKIN_PROVIDERS_HTTP",
        default_value_t = false
    )]
    http_enabled: bool,
    /// The socket address the HTTP provider listens on.
    #[arg(
        long = "provider.http.address",
        env = "QUILKIN_PROVIDERS_HTTP_ADDRESS",
        requires("http_enabled")
    )]
    http_address: Option<SocketAddr>,
}

#[derive(Clone)]
pub struct FiltersAndClusters {
    pub filters: crate::config::filter::FilterChainConfig,
    pub clusters: config::Watch<crate::net::ClusterMap>,
}

impl FiltersAndClusters {
    pub fn new(config: &crate::Config) -> Option<Self> {
        Some(Self {
            filters: config.dyn_cfg.filters()?.clone(),
            clusters: config.dyn_cfg.clusters()?.clone(),
        })
    }
}

impl Providers {
    #[allow(clippy::type_complexity)]
    const SUBS: &[(&str, &[(&str, Vec<String>)])] = &[
        (
            "9",
            &[
                (crate::xds::CLUSTER_TYPE, Vec::new()),
                (crate::xds::DATACENTER_TYPE, Vec::new()),
                (crate::xds::FILTER_CHAIN_TYPE, Vec::new()),
            ],
        ),
        (
            "",
            &[
                (crate::xds::CLUSTER_TYPE, Vec::new()),
                (crate::xds::DATACENTER_TYPE, Vec::new()),
            ],
        ),
    ];

    pub fn agones(mut self) -> Self {
        self.agones_enabled = true;
        self
    }

    pub fn agones_namespace(mut self, ns: impl Into<String>) -> Self {
        self.agones_namespaces = vec![ns.into()];
        self
    }

    pub fn agones_namespaces(mut self, ns: impl Into<Vec<String>>) -> Self {
        self.agones_namespaces = ns.into();
        self
    }

    pub fn fs(mut self) -> Self {
        self.fs_enabled = true;
        self
    }

    pub fn fs_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.fs_path = path.into();
        self
    }

    pub fn k8s(mut self) -> Self {
        self.k8s_enabled = true;
        self
    }

    pub fn k8s_namespace(mut self, ns: impl Into<String>) -> Self {
        self.k8s_namespace = ns.into();
        self
    }

    fn static_enabled(&self) -> bool {
        !self.endpoints.is_empty()
    }

    pub fn grpc_push_endpoints(
        mut self,
        endpoints: impl Into<Vec<tonic::transport::Endpoint>>,
    ) -> Self {
        self.relay = endpoints.into();
        self
    }

    pub fn grpc_pull_endpoints(
        mut self,
        endpoints: impl Into<Vec<tonic::transport::Endpoint>>,
    ) -> Self {
        self.xds_endpoints = endpoints.into();
        self
    }

    pub fn corrosion_endpoints(mut self, endpoints: impl Into<Vec<EndpointAddress>>) -> Self {
        self.corrosion_endpoints = endpoints.into();
        self
    }

    pub fn corrosion_mode(mut self, mode: CorrosionMode) -> Self {
        self.corrosion_mode = Some(mode);
        self
    }

    pub fn http(mut self) -> Self {
        self.http_enabled = true;
        self
    }

    pub fn http_address(mut self, addr: SocketAddr) -> Self {
        self.http_address = Some(addr);
        self
    }

    pub fn spawn_static_provider(
        &self,
        config: FiltersAndClusters,
        locality: Option<crate::net::endpoint::Locality>,
        mutator: Option<crate::providers::corrosion::ServerMutator>,
        spawner: &mut TaskSpawner,
    ) -> crate::Result<()> {
        let endpoint_tokens = self
            .endpoint_tokens
            .as_ref()
            .map(|tt| {
                let Some((count, length)) = tt.split_once(':') else {
                    eyre::bail!("--to-tokens `{tt}` is invalid, it must have a `:` separator")
                };

                let count: usize = count.parse()?;
                let length: usize = length.parse()?;

                Ok((count, length))
            })
            .transpose()?;

        let endpoints: std::collections::BTreeSet<crate::net::Endpoint> =
            if let Some((count, length)) = endpoint_tokens {
                let (unique, overflow) = 256u64.overflowing_pow(length as _);
                if overflow {
                    panic!(
                        "can't generate {} tokens of length {length} maximum is {}",
                        self.endpoints.len() * count,
                        u64::MAX,
                    );
                }

                if unique < (self.endpoints.len() * count) as u64 {
                    panic!(
                        "we require {} unique tokens but only {unique} can be generated",
                        self.endpoints.len() * count,
                    );
                }

                {
                    use crate::filters::StaticFilter as _;
                    let filter_chain = crate::filters::FilterChain::try_create([
                        crate::filters::Capture::as_filter_config(
                            crate::filters::capture::Config {
                                metadata_key: crate::filters::capture::CAPTURED_BYTES.into(),
                                strategy: crate::filters::capture::Strategy::Suffix(
                                    crate::filters::capture::Suffix {
                                        size: length as _,
                                        remove: true,
                                    },
                                ),
                            },
                        )?,
                        crate::filters::TokenRouter::as_filter_config(None)?,
                    ])?;
                    config.filters.store(filter_chain);
                }

                let count = count as u64;

                self.endpoints
                    .iter()
                    .enumerate()
                    .map(|(ind, addr)| {
                        let start = ind as u64 * count;

                        crate::net::endpoint::Endpoint::with_metadata(
                            addr.clone(),
                            crate::net::endpoint::Metadata {
                                tokens: (start..(start + count))
                                    .map(|i| i.to_le_bytes()[..length].to_vec())
                                    .collect(),
                            },
                        )
                    })
                    .collect()
            } else {
                self.endpoints
                    .iter()
                    .cloned()
                    .map(crate::net::endpoint::Endpoint::from)
                    .collect()
            };

        tracing::info!(
            provider = "static",
            endpoints = serde_json::to_string(&endpoints).unwrap(),
            "setting endpoints"
        );
        if let Some(mutator) = mutator.as_ref() {
            for endpoint in endpoints.iter() {
                mutator.upsert_server(
                    uuid::Uuid::new_v4(),
                    quilkin_types::Endpoint::new(
                        endpoint.address.host.clone(),
                        endpoint.address.port,
                    ),
                    endpoint.metadata.known.tokens.clone(),
                );
            }
        } else {
            config.clusters.modify(|clusters| {
                clusters.insert(None, locality, endpoints);
            });
        }

        let token = spawner.child();

        spawner.push_async("static", async move {
            // Take ownership of the mutator so we don't drop the Sender part of the channel
            let _mutator = mutator;

            token.cancelled().await;

            Ok(())
        });

        Ok(())
    }

    pub fn spawn_k8s_provider(
        &self,
        locality: Option<crate::net::endpoint::Locality>,
        config: &super::Config,
        mutator: Option<crate::providers::corrosion::ServerMutator>,
        spawner: &mut TaskSpawner,
        check_init: &mut ChecksInit,
    ) {
        if !self.agones_enabled() && !self.k8s_enabled() {
            return;
        }

        let agones_namespaces = if !self.agones_namespace.is_empty() {
            tracing::warn!(
                "`config.k8s.agones.namespace` is deprecated, use `config.k8s.agones.namespaces` instead"
            );
            vec![self.agones_namespace.clone()]
        } else {
            self.agones_namespaces.clone()
        };

        let ll = self
            .k8s_leader_election
            .then(|| config.dyn_cfg.init_leader_lock());
        let k8s_leader_id = self.k8s_leader_id.clone().unwrap_or_else(|| config.id());
        let k8s_leader_lease_name = self.k8s_leader_lease_name.clone();
        let k8s_namespace = self.k8s_namespace.clone();

        let selector = self
            .address_type
            .as_ref()
            .map(|at| config::AddressSelector {
                name: at.clone(),
                kind: self.ip_kind.unwrap_or(config::AddrKind::Any),
            });

        let health = check_init.add("k8s_provider");
        let token = spawner.child();

        let task = {
            let config = config.clone();
            let health_check = health.clone();
            let agones_namespaces = agones_namespaces.clone();
            let selector = selector.clone();
            let locality = locality.clone();
            let health_check = health_check.clone();
            let mutator = mutator.clone();
            let filters = config
                .dyn_cfg
                .filters()
                .filter(|_| self.k8s_enabled)
                .cloned();
            let clusters = config
                .dyn_cfg
                .clusters()
                .filter(|_| self.agones_enabled)
                .cloned();

            move || {
                let health_check = health_check.clone();
                let agones_namespaces = agones_namespaces.clone();
                let k8s_namespace: String = k8s_namespace.clone();
                let k8s_leader_id: String = k8s_leader_id.clone();
                let k8s_leader_lease_name: String = k8s_leader_lease_name.clone();
                let selector = selector.clone();
                let locality = locality.clone();
                let health_check = health_check.clone();
                let mutator = mutator.clone();
                let token = token.clone();
                let filters = filters.clone();
                let clusters = clusters.clone();
                let ll = ll.clone();

                async move {
                    let client =
                        tokio::time::timeout(std::time::Duration::from_secs(5), k8s::client())
                            .await??;

                    let mut js = tokio::task::JoinSet::new();

                    if let Some(filters) = filters {
                        js.spawn(Self::result_stream(
                            token.clone(),
                            k8s::update_filters_from_configmap(
                                client.clone(),
                                k8s_namespace.clone(),
                                filters,
                            ),
                        ));
                    }

                    if let Some(ll) = ll {
                        js.spawn(k8s::update_leader_lock(
                            client.clone(),
                            k8s_namespace,
                            k8s_leader_lease_name,
                            k8s_leader_id,
                            ll,
                            token.clone(),
                        ));
                    }

                    if let Some(clusters) = clusters {
                        for namespace in agones_namespaces {
                            let cluster_update_batcher =
                                crate::net::cluster::ClusterUpdateBatcher::spawn(
                                    clusters.clone(),
                                    locality.clone(),
                                    std::time::Duration::from_millis(500),
                                    token.clone(),
                                    &mut js,
                                );
                            let processor = EventProcessor {
                                namespace: namespace.clone(),
                                mutator: mutator.clone(),
                                address_selector: selector.clone(),
                                cluster_update_batcher: cluster_update_batcher.clone(),
                                servers: Default::default(),
                            };

                            js.spawn(Self::result_stream(
                                token.clone(),
                                k8s::update_endpoints_from_gameservers(
                                    client.clone(),
                                    namespace.clone(),
                                    processor,
                                ),
                            ));
                        }
                    }

                    health_check.ready();

                    tokio::select! {
                        Some(result) = js.join_next() => result.map_err(From::from).and_then(|result| result),
                        _ = token.cancelled() => {
                            js.join_next().await.unwrap().map_err(From::from).and_then(|result| result)
                        }
                    }
                }
            }
        };

        Self::task(spawner, health, task)
    }

    async fn result_stream<T>(
        token: quilkin_graceful::ChildToken,
        stream: impl futures::Stream<Item = crate::Result<T>>,
    ) -> crate::Result<()> {
        tokio::pin!(stream);
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return Ok(());
                }
                res = stream.try_next() => {
                    if !res?.is_some() {
                        eyre::bail!("kubernetes watch stream terminated");
                    }
                }
            }
        }
    }

    fn spawn_mmdb_provider(&self, spawner: &mut TaskSpawner) {
        let Some(source) = self.mmdb.clone() else {
            return;
        };

        let token = spawner.child();
        spawner.push_async("mmdb", async move {
            while let Err(error) = tryhard::retry_fn(|| crate::MaxmindDb::update(source.clone()))
                .retries(10)
                .exponential_backoff(crate::config::BACKOFF_INITIAL_DELAY)
                .await
            {
                tracing::warn!(%error, "error updating maxmind database");
            }

            // TODO: Keep task running for now, should be replaced with
            // checking for updates to the mmdb source.
            token.cancelled().await;
            Ok(())
        });
    }

    pub fn spawn_mds_provider(
        &self,
        config: Arc<config::Config>,
        locality: Option<crate::net::endpoint::Locality>,
        spawner: &mut TaskSpawner,
        check_init: &mut ChecksInit,
    ) {
        if !self.grpc_push_enabled() {
            return;
        }

        let config = config.clone();
        let endpoints = self.relay.clone();
        let control_plane_id = locality.map_or_else(|| config.id(), |l| l.region().to_string());
        let ss = spawner.sub_spawner();
        let health = check_init.add("mds_provider");

        Self::task(spawner, health.clone(), move || {
            let config = config.clone();
            let endpoints = endpoints.clone();
            let control_plane_id = control_plane_id.clone();
            let health_check = health.clone();
            let ss = ss.clone();

            async move {
                let stream =
                    crate::net::xds::client::MdsClient::connect(control_plane_id, endpoints)
                        .await?
                        .delta_stream(config.clone(), health_check.clone(), ss)
                        .await
                        .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                health_check.mark_healthiness(true, None);

                stream.await.wrap_err("join handle error")?
            }
        })
    }

    pub fn spawn_xds_provider(
        &self,
        config: Arc<config::Config>,
        notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
        spawner: &mut TaskSpawner,
        checks_init: &mut ChecksInit,
    ) {
        if !self.grpc_pull_enabled() {
            return;
        }

        let config = config.clone();
        let endpoints = self.xds_endpoints.clone();
        let health = checks_init.add("xds_provider");

        Self::task(spawner, health.clone(), move || {
            let config = config.clone();
            let endpoints = endpoints.clone();
            let health_check = health.clone();
            let tx = notifier.clone();

            async move {
                let identifier = config.id();
                let stream = crate::net::xds::delta_subscribe(
                    config,
                    identifier,
                    endpoints,
                    health_check.clone(),
                    tx,
                    Self::SUBS,
                )
                .await
                .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                health_check.mark_healthiness(true, None);

                stream.await.wrap_err("join handle error")?
            }
        })
    }

    pub fn grpc_push_enabled(&self) -> bool {
        !self.relay.is_empty()
    }

    pub fn grpc_pull_enabled(&self) -> bool {
        !self.xds_endpoints.is_empty()
    }

    pub fn k8s_enabled(&self) -> bool {
        self.k8s_enabled
    }

    pub fn agones_enabled(&self) -> bool {
        self.agones_enabled
    }

    pub fn fs_enabled(&self) -> bool {
        self.fs_enabled
    }

    pub fn mmdb_enabled(&self) -> bool {
        self.mmdb.is_some()
    }

    pub fn corrosion_enabled(&self) -> bool {
        self.corrosion_mode
            .is_some_and(|_cm| !self.corrosion_endpoints.is_empty())
    }

    pub fn http_enabled(&self) -> bool {
        self.http_enabled
    }

    pub fn any_provider_enabled(&self) -> bool {
        self.agones_enabled()
            || self.fs_enabled()
            || self.grpc_pull_enabled()
            || self.grpc_push_enabled()
            || self.http_enabled()
            || self.k8s_enabled()
            || self.mmdb_enabled()
            || self.static_enabled()
            || self.corrosion_enabled()
    }

    /// Adds the required typemap entries to the config depending on what providers are enabled
    pub fn init_config(&self, config: &mut config::Config) {
        use crate::config::insert_default;

        // TODO are these required by all providers or only some?
        if self.any_provider_enabled() {
            insert_default::<crate::filters::FilterChain>(&mut config.dyn_cfg.typemap);
            insert_default::<crate::net::ClusterMap>(&mut config.dyn_cfg.typemap);
        }
    }

    pub fn spawn_providers(
        self,
        config: &Arc<config::Config>,
        locality: Option<crate::net::endpoint::Locality>,
        notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
        spawner: &mut TaskSpawner,
        checks_init: &mut ChecksInit,
    ) {
        if !self.any_provider_enabled() {
            tracing::info!("no configuration providers specified");
            return;
        }

        tracing::info!(providers=?[
            self.agones_enabled().then_some("agones"),
            self.fs_enabled().then_some("fs"),
            self.grpc_pull_enabled().then_some("mDS"),
            self.grpc_push_enabled().then_some("xDS"),
            self.http_enabled().then_some("http"),
            self.k8s_enabled().then_some("k8s"),
            self.mmdb_enabled().then_some("mmdb"),
            self.static_enabled().then_some("static"),
            self.corrosion_enabled().then_some("corrosion"),
        ].into_iter().flatten().collect::<Vec<&str>>(), "starting configuration providers");

        self.spawn_mmdb_provider(spawner);
        let mutator = self.maybe_spawn_corrosion(config, spawner, checks_init);

        if mutator.is_some() && self.fs_enabled() {
            tracing::error!("corrosion mutation does not work with file system data");
        };

        self.spawn_mds_provider(config.clone(), locality.clone(), spawner, checks_init);

        self.spawn_k8s_provider(
            locality.clone(),
            config,
            mutator.clone(),
            spawner,
            checks_init,
        );

        self.spawn_xds_provider(config.clone(), notifier, spawner, checks_init);

        if self.fs_enabled() {
            let config = config.clone();
            let health = checks_init.add("fs_watch_provider");
            let token = spawner.child();
            let path = self.fs_path.clone();
            let locality = locality.clone();

            Self::task(spawner, health.clone(), move || {
                let path = path.clone();
                let health_check = health.clone();
                let locality = locality.clone();
                let token = token.clone();
                let config = config.clone();

                async move {
                    token
                        .run_until_cancelled(fs::watch(config, health_check, path, locality))
                        .await
                        .map_or(Ok(()), |r| r)
                }
            });
        }

        if self.http_enabled()
            && let Some(fc) = FiltersAndClusters::new(config)
        {
            let address = self
                .http_address
                .unwrap_or_else(|| (std::net::Ipv6Addr::UNSPECIFIED, http::DEFAULT_PORT).into());
            let health = checks_init.add("http_provider");
            let token = spawner.child();
            Self::task(spawner, health.clone(), move || {
                http::serve(fc.clone(), address, health.clone(), token.clone())
            });
        }

        if let Some(fc) = self
            .static_enabled()
            .then(|| FiltersAndClusters::new(config))
            .flatten()
        {
            self.spawn_static_provider(fc, locality.clone(), mutator, spawner)
                .expect("failed to initialize static provider");
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn task<F, T>(
        spawner: &mut TaskSpawner,
        health: quilkin_graceful::health::HealthToken,
        task: T,
    ) where
        F: std::future::Future<Output = crate::Result<()>> + Send + 'static,
        T: FnMut() -> F + Send + 'static,
    {
        let name = health.name();

        struct CancellableExponentialBackoff {
            delay: std::time::Duration,
            cancel: quilkin_graceful::ChildToken,
        }

        impl<'a, E> tryhard::backoff_strategies::BackoffStrategy<'a, E> for CancellableExponentialBackoff {
            type Output = tryhard::RetryPolicy;

            #[inline]
            fn delay(&mut self, _attempt: u32, _error: &'a E) -> Self::Output {
                if self.cancel.is_cancelled() {
                    tryhard::RetryPolicy::Break
                } else {
                    let prev_delay = self.delay;
                    self.delay = self.delay.saturating_mul(2).min(MAX_DELAY);
                    tryhard::RetryPolicy::Delay(prev_delay)
                }
            }
        }

        let backoff = CancellableExponentialBackoff {
            delay: BACKOFF_STEP,
            cancel: spawner.child(),
        };

        spawner.push_async(name, async move {
            tryhard::retry_fn(task)
                .retries(RETRIES)
                .custom_backoff(backoff)
                .on_retry(|attempt, _, error: &eyre::Error| {
                    let error = error.to_string();
                    health.mark_healthiness(false, Some(error.clone()));
                    async move {
                        provider_task_failures_total(&name).inc();
                        tracing::warn!(%attempt, error, task=%name, "provider task error, retrying");
                    }
                }).await
        });
    }
}
