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

pub mod fs;
pub mod k8s;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{config, metrics::provider_task_failures_total};
use eyre::Context;
use futures::TryStreamExt;

/// Functionally infinite retries as provider tasks are long running tasks
/// that we continually want to retry and Quilkin can run for days or weeks.
const RETRIES: u32 = u32::MAX;
const BACKOFF_STEP: std::time::Duration = std::time::Duration::from_millis(250);
const MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
pub(crate) const NO_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

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
        env = "QUILKIN_PROVIDERS_MDS_ENDPOINTS"
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
        env = "QUILKIN_PROVIDERS_STATIC_ENDPOINTS"
    )]
    endpoints: Vec<SocketAddr>,
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
        env = "QUILKIN_PROVIDERS_XDS_ENDPOINTS"
    )]
    xds_endpoints: Vec<tonic::transport::Endpoint>,
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

    pub fn spawn_static_provider(
        &self,
        config: FiltersAndClusters,
        health_check: &AtomicBool,
        locality: Option<crate::net::endpoint::Locality>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>> + 'static> {
        let endpoint_tokens = self
            .endpoint_tokens
            .as_ref()
            .map(|tt| {
                let Some((count, length)) = tt.split_once(':') else {
                    eyre::bail!("--to-tokens `{tt}` is invalid, it must have a `:` separator")
                };

                let count = count.parse()?;
                let length = length.parse()?;

                Ok(crate::components::proxy::ToTokens { count, length })
            })
            .transpose()?;

        let endpoints = if let Some(tt) = endpoint_tokens {
            let (unique, overflow) = 256u64.overflowing_pow(tt.length as _);
            if overflow {
                panic!(
                    "can't generate {} tokens of length {} maximum is {}",
                    self.endpoints.len() * tt.count,
                    tt.length,
                    u64::MAX,
                );
            }

            if unique < (self.endpoints.len() * tt.count) as u64 {
                panic!(
                    "we require {} unique tokens but only {unique} can be generated",
                    self.endpoints.len() * tt.count,
                );
            }

            {
                use crate::filters::StaticFilter as _;
                let filter_chain = crate::filters::FilterChain::try_create([
                    crate::filters::Capture::as_filter_config(crate::filters::capture::Config {
                        metadata_key: crate::filters::capture::CAPTURED_BYTES.into(),
                        strategy: crate::filters::capture::Strategy::Suffix(
                            crate::filters::capture::Suffix {
                                size: tt.length as _,
                                remove: true,
                            },
                        ),
                    })?,
                    crate::filters::TokenRouter::as_filter_config(None)?,
                ])?;
                config.filters.store(filter_chain);
            }

            let count = tt.count as u64;

            self.endpoints
                .iter()
                .enumerate()
                .map(|(ind, sa)| {
                    let start = ind as u64 * count;

                    crate::net::endpoint::Endpoint::with_metadata(
                        (*sa).into(),
                        crate::net::endpoint::Metadata {
                            tokens: (start..(start + count))
                                .map(|i| i.to_le_bytes()[..tt.length].to_vec())
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
        config.clusters.modify(|clusters| {
            clusters.insert(None, locality, endpoints);
        });

        health_check.store(true, Ordering::SeqCst);

        Ok(std::future::pending())
    }

    pub fn spawn_k8s_provider(
        &self,
        health_check: Arc<AtomicBool>,
        locality: Option<crate::net::endpoint::Locality>,
        config: &super::Config,
    ) -> impl Future<Output = crate::Result<()>> + 'static {
        let agones_namespaces = if !self.agones_namespace.is_empty() {
            tracing::warn!(
                "`config.k8s.agones.namespace` is deprecated, use `config.k8s.agones.namespaces` instead"
            );
            vec![self.agones_namespace.clone()]
        } else {
            self.agones_namespaces.clone()
        };

        let agones_enabled = self.agones_enabled;
        let k8s_enabled = self.k8s_enabled;
        let k8s_leader_election = self.k8s_leader_election;
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

        let task = {
            let config = config.clone();
            let health_check = health_check.clone();
            let agones_namespaces = agones_namespaces.clone();
            let selector = selector.clone();
            let locality = locality.clone();
            let health_check = health_check.clone();

            move || {
                let config = config.clone();
                let health_check = health_check.clone();
                let agones_namespaces = agones_namespaces.clone();
                let k8s_namespace: String = k8s_namespace.clone();
                let k8s_leader_id: String = k8s_leader_id.clone();
                let k8s_leader_lease_name: String = k8s_leader_lease_name.clone();
                let selector = selector.clone();
                let locality = locality.clone();
                let health_check = health_check.clone();

                async move {
                    let client = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        kube::Client::try_default(),
                    )
                    .await??;

                    let k8s_stream =
                        if let Some(Some(fc)) = k8s_enabled.then(|| config.dyn_cfg.filters()) {
                            either::Left(Self::result_stream(
                                health_check.clone(),
                                k8s::update_filters_from_configmap(
                                    client.clone(),
                                    k8s_namespace.clone(),
                                    fc.clone(),
                                ),
                            ))
                        } else {
                            either::Right(std::future::pending())
                        };

                    let k8s_leader_election_task = if k8s_leader_election {
                        let ll = config.dyn_cfg.init_leader_lock();
                        either::Left(tokio::spawn(k8s::update_leader_lock(
                            client.clone(),
                            k8s_namespace,
                            k8s_leader_lease_name,
                            k8s_leader_id,
                            ll,
                        )))
                    } else {
                        either::Right(std::future::pending())
                    };

                    let mut gs_streams = tokio::task::JoinSet::new();
                    if let Some(Some(clusters)) = agones_enabled.then(|| config.dyn_cfg.clusters())
                    {
                        for namespace in agones_namespaces {
                            gs_streams.spawn(Self::result_stream(
                                health_check.clone(),
                                k8s::update_endpoints_from_gameservers(
                                    client.clone(),
                                    namespace.clone(),
                                    clusters.clone(),
                                    locality.clone(),
                                    selector.clone(),
                                ),
                            ));
                        }
                    } else {
                        gs_streams.spawn(std::future::pending());
                    };

                    health_check.store(true, Ordering::SeqCst);
                    tokio::select! {
                        Some(result) = gs_streams.join_next() => result.map_err(From::from).and_then(|result| result),
                        result = k8s_leader_election_task => result.map_err(eyre::Error::from).and_then(|result| result),
                        result = k8s_stream => result,
                    }
                }
            }
        };

        Self::task("k8s_provider".into(), health_check.clone(), task)
    }

    async fn result_stream<T>(
        health_check: Arc<AtomicBool>,
        stream: impl futures::Stream<Item = crate::Result<T>>,
    ) -> crate::Result<()> {
        tokio::pin!(stream);
        loop {
            match stream.try_next().await {
                Ok(Some(_)) => health_check.store(true, Ordering::SeqCst),
                Ok(None) => break Err(eyre::eyre!("kubernetes watch stream terminated")),
                Err(error) => break Err(error),
            }
        }
    }

    fn spawn_mmdb_provider(&self) -> impl Future<Output = crate::Result<()>> + 'static {
        self.mmdb.as_ref().map_or_else(
            || either::Left(std::future::pending()),
            |source| {
                let source = source.clone();
                either::Right(async move {
                    while let Err(error) =
                        tryhard::retry_fn(|| crate::MaxmindDb::update(source.clone()))
                            .retries(10)
                            .exponential_backoff(crate::config::BACKOFF_INITIAL_DELAY)
                            .await
                    {
                        tracing::warn!(%error, "error updating maxmind database");
                    }

                    // TODO: Keep task running for now, should be replaced with
                    // checking for updates to the mmdb source.
                    std::future::pending().await
                })
            },
        )
    }

    pub fn spawn_mds_provider(
        &self,
        config: Arc<config::Config>,
        health_check: Arc<AtomicBool>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> impl Future<Output = crate::Result<()>> + 'static {
        let config = config.clone();
        let endpoints = self.relay.clone();
        Self::task("mds_provider".into(), health_check.clone(), move || {
            let config = config.clone();
            let endpoints = endpoints.clone();
            let health_check = health_check.clone();
            let shutdown = shutdown.clone();
            async move {
                let stream = crate::net::xds::client::MdsClient::connect(config.id(), endpoints)
                    .await?
                    .delta_stream(config.clone(), health_check.clone(), shutdown)
                    .await
                    .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                health_check.store(true, Ordering::SeqCst);

                stream.await.wrap_err("join handle error")?
            }
        })
    }

    pub fn spawn_xds_provider(
        &self,
        config: Arc<config::Config>,
        health_check: Arc<AtomicBool>,
        notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    ) -> impl Future<Output = crate::Result<()>> + 'static {
        let config = config.clone();
        let endpoints = self.xds_endpoints.clone();

        Self::task("xds_provider".into(), health_check.clone(), move || {
            let config = config.clone();
            let endpoints = endpoints.clone();
            let health_check = health_check.clone();
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

                health_check.store(true, Ordering::SeqCst);

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

    pub fn any_provider_enabled(&self) -> bool {
        self.agones_enabled()
            || self.fs_enabled()
            || self.grpc_pull_enabled()
            || self.grpc_push_enabled()
            || self.k8s_enabled()
            || self.mmdb_enabled()
            || self.static_enabled()
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
        health_check: Arc<AtomicBool>,
        locality: Option<crate::net::endpoint::Locality>,
        notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> tokio::task::JoinSet<crate::Result<()>> {
        let mut providers = tokio::task::JoinSet::new();

        if !self.any_provider_enabled() {
            tracing::info!("no configuration providers specified");
            health_check.store(true, std::sync::atomic::Ordering::Relaxed);
            return providers;
        }

        tracing::info!(providers=?[
            self.agones_enabled().then_some("agones"),
            self.fs_enabled().then_some("fs"),
            self.grpc_pull_enabled().then_some("mDS"),
            self.grpc_push_enabled().then_some("xDS"),
            self.k8s_enabled().then_some("k8s"),
            self.mmdb_enabled().then_some("mmdb"),
            self.static_enabled().then_some("static"),
        ].into_iter().flatten().collect::<Vec<&str>>(), "starting configuration providers");

        if self.mmdb_enabled() {
            providers.spawn(self.spawn_mmdb_provider());
        }

        if self.grpc_push_enabled() {
            providers.spawn(self.spawn_mds_provider(
                config.clone(),
                health_check.clone(),
                shutdown,
            ));
        }

        if self.k8s_enabled() || self.agones_enabled() {
            providers.spawn(self.spawn_k8s_provider(
                health_check.clone(),
                locality.clone(),
                config,
            ));
        }

        if self.grpc_pull_enabled() {
            providers.spawn(self.spawn_xds_provider(
                config.clone(),
                health_check.clone(),
                notifier,
            ));
        }

        if self.fs_enabled() {
            let config = config.clone();

            providers.spawn(Self::task(
                "fs_watch_provider".into(),
                health_check.clone(),
                {
                    let path = self.fs_path.clone();
                    let health_check = health_check.clone();
                    let locality = locality.clone();

                    move || {
                        fs::watch(
                            config.clone(),
                            health_check.clone(),
                            path.clone(),
                            locality.clone(),
                        )
                    }
                },
            ));
        }

        if let Some(fc) = self
            .static_enabled()
            .then(|| FiltersAndClusters::new(config))
            .flatten()
        {
            health_check.store(true, Ordering::SeqCst);
            providers.spawn(
                self.spawn_static_provider(fc, &health_check, locality.clone())
                    .unwrap(),
            );
        }

        assert!(
            !providers.is_empty(),
            "bug: no provider tasks running when {:?} was specified",
            providers
        );

        providers
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn task<F>(
        name: String,
        health_check: Arc<AtomicBool>,
        task: impl FnMut() -> F,
    ) -> crate::Result<()>
    where
        F: std::future::Future<Output = crate::Result<()>>,
    {
        tryhard::retry_fn(task)
            .retries(RETRIES)
            .exponential_backoff(BACKOFF_STEP)
            .max_delay(MAX_DELAY)
            .on_retry(|attempt, _, error: &eyre::Error| {
                health_check.store(false, Ordering::SeqCst);
                let name = name.clone();
                let error = error.to_string();
                async move {
                    provider_task_failures_total(&name).inc();
                    tracing::warn!(%attempt, %error, task=%name, "provider task error, retrying");
                }
            })
            .await
    }
}
