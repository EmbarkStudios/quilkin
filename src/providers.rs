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

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    config,
    metrics::provider_task_failures_total,
    net::EndpointAddress,
    providers::{corrosion::CorrosionMode, k8s::EventProcessor},
};
use eyre::Context;
use futures::TryStreamExt;

/// Functionally infinite retries as provider tasks are long running tasks
/// that we continually want to retry and Quilkin can run for days or weeks.
const RETRIES: u32 = u32::MAX;
const BACKOFF_STEP: std::time::Duration = std::time::Duration::from_millis(250);
const MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

// The long names of the client TLS flags, without the dashes. Shared so the
// errors that tell operators what is missing can't name a renamed flag.
const XDS_ENDPOINTS_FLAG: &str = "provider.xds.endpoints";
const XDS_TLS_CA_FLAG: &str = "provider.xds.tls.ca-path";
const XDS_TLS_SERVER_NAME_FLAG: &str = "provider.xds.tls.server-name";
const XDS_TLS_CERT_FLAG: &str = "provider.xds.tls.cert-path";
const XDS_TLS_KEY_FLAG: &str = "provider.xds.tls.key-path";
const MDS_ENDPOINTS_FLAG: &str = "provider.mds.endpoints";
const MDS_TLS_CA_FLAG: &str = "provider.mds.tls.ca-path";
const MDS_TLS_SERVER_NAME_FLAG: &str = "provider.mds.tls.server-name";
const MDS_TLS_CERT_FLAG: &str = "provider.mds.tls.cert-path";
const MDS_TLS_KEY_FLAG: &str = "provider.mds.tls.key-path";

/// The flags one provider's client TLS is configured from.
#[derive(Clone, Copy)]
struct TlsFlags {
    endpoints: &'static str,
    ca_path: &'static str,
    server_name: &'static str,
    cert_path: &'static str,
    key_path: &'static str,
}

const XDS_TLS_FLAGS: TlsFlags = TlsFlags {
    endpoints: XDS_ENDPOINTS_FLAG,
    ca_path: XDS_TLS_CA_FLAG,
    server_name: XDS_TLS_SERVER_NAME_FLAG,
    cert_path: XDS_TLS_CERT_FLAG,
    key_path: XDS_TLS_KEY_FLAG,
};

const MDS_TLS_FLAGS: TlsFlags = TlsFlags {
    endpoints: MDS_ENDPOINTS_FLAG,
    ca_path: MDS_TLS_CA_FLAG,
    server_name: MDS_TLS_SERVER_NAME_FLAG,
    cert_path: MDS_TLS_CERT_FLAG,
    key_path: MDS_TLS_KEY_FLAG,
};

/// What one provider was told to present and verify.
#[derive(Clone, Copy, Default)]
struct ClientTls<'a> {
    ca_path: Option<&'a std::path::Path>,
    server_name: Option<&'a str>,
    cert_path: Option<&'a std::path::Path>,
    key_path: Option<&'a std::path::Path>,
}

/// Attaches the trust an `https` endpoint is verified against.
///
/// tonic refuses to connect to an `https` endpoint that carries no TLS
/// configuration, so this has to be applied before the client is built rather
/// than left to the connection itself.
fn with_client_tls(
    endpoints: Vec<tonic::transport::Endpoint>,
    client: ClientTls<'_>,
    flags: TlsFlags,
) -> crate::Result<Vec<tonic::transport::Endpoint>> {
    use eyre::WrapErr as _;

    let ClientTls {
        ca_path,
        server_name,
        cert_path,
        key_path,
    } = client;

    let identity = match (cert_path, key_path) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => eyre::bail!(
            "--{} and --{} have to be supplied together",
            flags.cert_path,
            flags.key_path
        ),
    };

    let secure = endpoints
        .iter()
        .any(|endpoint| endpoint.uri().scheme_str() == Some("https"));

    let Some(ca_path) = ca_path else {
        if secure {
            eyre::bail!(
                "--{} has an https endpoint, so --{} is required to verify it",
                flags.endpoints,
                flags.ca_path
            );
        }
        if server_name.is_some() {
            eyre::bail!(
                "--{} only applies to an https endpoint, and --{} has none",
                flags.server_name,
                flags.endpoints
            );
        }
        if identity.is_some() {
            eyre::bail!(
                "--{} is only presented over TLS, so --{} is required as well",
                flags.cert_path,
                flags.ca_path
            );
        }
        return Ok(endpoints);
    };

    if !secure {
        eyre::bail!(
            "--{} was supplied, but no endpoint in --{} uses https",
            flags.ca_path,
            flags.endpoints
        );
    }

    let ca = crate::tls::read_pem_file(ca_path, flags.ca_path, crate::tls::validate_pem_cert)?;
    let mut tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca));

    if let Some(server_name) = server_name {
        tls = tls.domain_name(server_name);
    }

    if let Some((cert_path, key_path)) = identity {
        let cert =
            crate::tls::read_pem_file(cert_path, flags.cert_path, crate::tls::validate_pem_cert)?;
        let key =
            crate::tls::read_pem_file(key_path, flags.key_path, crate::tls::validate_pem_key)?;
        tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
    }

    endpoints
        .into_iter()
        .map(|endpoint| {
            let uri = endpoint.uri().clone();
            endpoint
                .tls_config(tls.clone())
                .with_context(|| format!("failed to configure TLS for {uri}"))
        })
        .collect()
}

/// A provider's endpoints, replaced in place when the certificate they present
/// is rotated.
type SharedEndpoints = Arc<arc_swap::ArcSwap<Vec<tonic::transport::Endpoint>>>;

/// Rebuilds one provider's endpoints when the files its TLS came from change.
struct ClientTlsReloader {
    /// The endpoints as configured, before any TLS was attached.
    base: Vec<tonic::transport::Endpoint>,
    client: OwnedClientTls,
    flags: TlsFlags,
    endpoints: SharedEndpoints,
    reconnect: Arc<tokio::sync::Notify>,
}

/// The owned form of [`ClientTls`], for re-reading after the flags are gone.
#[derive(Clone)]
struct OwnedClientTls {
    ca_path: std::path::PathBuf,
    server_name: Option<String>,
    cert_path: Option<std::path::PathBuf>,
    key_path: Option<std::path::PathBuf>,
}

impl ClientTlsReloader {
    /// The files this reads, which are the ones worth watching.
    fn paths(&self) -> Vec<std::path::PathBuf> {
        [
            Some(&self.client.ca_path),
            self.client.cert_path.as_ref(),
            self.client.key_path.as_ref(),
        ]
        .into_iter()
        .flatten()
        .cloned()
        .collect()
    }

    fn rebuild(&self) -> crate::Result<Vec<tonic::transport::Endpoint>> {
        with_client_tls(
            self.base.clone(),
            ClientTls {
                ca_path: Some(&self.client.ca_path),
                server_name: self.client.server_name.as_deref(),
                cert_path: self.client.cert_path.as_deref(),
                key_path: self.client.key_path.as_deref(),
            },
            self.flags,
        )
    }

    /// Reconnects with whatever is on disk whenever it changes.
    fn spawn(self, shutdown: tokio::sync::watch::Receiver<()>) -> crate::Result<()> {
        let paths = self.paths();
        let subject = self.flags.endpoints.to_owned();

        crate::tls::spawn_reload(subject, paths, shutdown, move || {
            self.endpoints.store(Arc::new(self.rebuild()?));
            // The provider reconnects through its ordinary retry path, so
            // nothing else has to know about rotation.
            self.reconnect.notify_waiters();

            // The certificate presented to the server, which is the one whose
            // expiry matters here. Without one there is nothing to report.
            Ok(match self.client.cert_path.as_deref() {
                Some(cert) => std::fs::read(cert)?,
                None => Vec::new(),
            })
        })
    }
}

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
    /// Path to a PEM encoded CA bundle the `quilkin relay` certificate is verified against.
    #[clap(
        long = MDS_TLS_CA_FLAG,
        env = "QUILKIN_PROVIDERS_MDS_TLS_CA_PATH",
        hide = true
    )]
    mds_tls_ca_path: Option<std::path::PathBuf>,
    /// The name to expect in the relay certificate, when it is not the endpoint's host.
    #[clap(
        long = MDS_TLS_SERVER_NAME_FLAG,
        env = "QUILKIN_PROVIDERS_MDS_TLS_SERVER_NAME",
        hide = true
    )]
    mds_tls_server_name: Option<String>,
    /// Path to a PEM encoded certificate presented to the server when it asks for one.
    #[clap(
        long = MDS_TLS_CERT_FLAG,
        env = "QUILKIN_PROVIDERS_MDS_TLS_CERT_PATH",
        hide = true
    )]
    mds_tls_cert_path: Option<std::path::PathBuf>,
    /// Path to the private key for the presented certificate.
    #[clap(
        long = MDS_TLS_KEY_FLAG,
        env = "QUILKIN_PROVIDERS_MDS_TLS_KEY_PATH",
        hide = true
    )]
    mds_tls_key_path: Option<std::path::PathBuf>,
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
    /// Path to a PEM encoded CA bundle the xDS server certificate is verified against.
    #[clap(
        long = XDS_TLS_CA_FLAG,
        env = "QUILKIN_PROVIDERS_XDS_TLS_CA_PATH",
        hide = true
    )]
    xds_tls_ca_path: Option<std::path::PathBuf>,
    /// The name to expect in the xDS server certificate, when it is not the endpoint's host.
    #[clap(
        long = XDS_TLS_SERVER_NAME_FLAG,
        env = "QUILKIN_PROVIDERS_XDS_TLS_SERVER_NAME",
        hide = true
    )]
    xds_tls_server_name: Option<String>,
    /// Path to a PEM encoded certificate presented to the server when it asks for one.
    #[clap(
        long = XDS_TLS_CERT_FLAG,
        env = "QUILKIN_PROVIDERS_XDS_TLS_CERT_PATH",
        hide = true
    )]
    xds_tls_cert_path: Option<std::path::PathBuf>,
    /// Path to the private key for the presented certificate.
    #[clap(
        long = XDS_TLS_KEY_FLAG,
        env = "QUILKIN_PROVIDERS_XDS_TLS_KEY_PATH",
        hide = true
    )]
    xds_tls_key_path: Option<std::path::PathBuf>,
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

    pub fn grpc_push_tls_ca_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.mds_tls_ca_path = Some(path.into());
        self
    }

    pub fn grpc_pull_tls_ca_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.xds_tls_ca_path = Some(path.into());
        self
    }

    pub fn grpc_push_tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.mds_tls_server_name = Some(name.into());
        self
    }

    pub fn grpc_pull_tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.xds_tls_server_name = Some(name.into());
        self
    }

    pub fn grpc_push_tls_identity_paths(
        mut self,
        cert: impl Into<std::path::PathBuf>,
        key: impl Into<std::path::PathBuf>,
    ) -> Self {
        self.mds_tls_cert_path = Some(cert.into());
        self.mds_tls_key_path = Some(key.into());
        self
    }

    pub fn grpc_pull_tls_identity_paths(
        mut self,
        cert: impl Into<std::path::PathBuf>,
        key: impl Into<std::path::PathBuf>,
    ) -> Self {
        self.xds_tls_cert_path = Some(cert.into());
        self.xds_tls_key_path = Some(key.into());
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
        health_check: &AtomicBool,
        locality: Option<crate::net::endpoint::Locality>,
        mutator: Option<crate::providers::corrosion::ServerMutator>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>> + 'static> {
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

        health_check.store(true, Ordering::SeqCst);

        Ok(async move {
            // Take ownership of the mutator so we don't drop the Sender part of the channel
            let _mutator = mutator;
            std::future::pending::<()>().await;
            Ok(())
        })
    }

    pub fn spawn_k8s_provider(
        &self,
        health_check: Arc<AtomicBool>,
        locality: Option<crate::net::endpoint::Locality>,
        config: &super::Config,
        mutator: Option<crate::providers::corrosion::ServerMutator>,
        shutdown: tokio::sync::watch::Receiver<()>,
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
            let mutator = mutator.clone();
            let shutdown = shutdown;
            let token = crate::signal::cancellation_token(shutdown.clone());

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
                let mutator = mutator.clone();
                let shutdown = shutdown.clone();
                let token = token.clone();

                async move {
                    let client =
                        tokio::time::timeout(std::time::Duration::from_secs(5), k8s::client())
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
                        either::Left(k8s::update_leader_lock(
                            client.clone(),
                            k8s_namespace,
                            k8s_leader_lease_name,
                            k8s_leader_id,
                            ll,
                            shutdown.clone(),
                        ))
                    } else {
                        either::Right(std::future::pending())
                    };

                    // Cancels this attempt's batcher tasks when the future is dropped
                    let batch_token = token.child_token();
                    let _batch_guard = batch_token.clone().drop_guard();

                    let mut gs_streams = tokio::task::JoinSet::new();
                    if let Some(Some(clusters)) = agones_enabled.then(|| config.dyn_cfg.clusters())
                    {
                        for namespace in agones_namespaces {
                            let cluster_update_batcher =
                                crate::net::cluster::ClusterUpdateBatcher::spawn(
                                    clusters.clone(),
                                    locality.clone(),
                                    std::time::Duration::from_millis(500),
                                    batch_token.child_token(),
                                );
                            let processor = EventProcessor {
                                namespace: namespace.clone(),
                                mutator: mutator.clone(),
                                address_selector: selector.clone(),
                                cluster_update_batcher: cluster_update_batcher.clone(),
                                servers: Default::default(),
                            };

                            gs_streams.spawn(Self::result_stream(
                                health_check.clone(),
                                k8s::update_endpoints_from_gameservers(
                                    client.clone(),
                                    namespace.clone(),
                                    processor,
                                ),
                            ));
                        }
                    } else {
                        gs_streams.spawn(std::future::pending());
                    };

                    health_check.store(true, Ordering::SeqCst);
                    tokio::select! {
                        Some(result) = gs_streams.join_next() => result.map_err(From::from).and_then(|result| result),
                        result = k8s_leader_election_task => result,
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
            if stream.try_next().await?.is_some() {
                health_check.store(true, Ordering::SeqCst);
            } else {
                eyre::bail!("kubernetes watch stream terminated");
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
        locality: Option<crate::net::endpoint::Locality>,
        shutdown: tokio::sync::watch::Receiver<()>,
        endpoints: SharedEndpoints,
        reconnect: Arc<tokio::sync::Notify>,
    ) -> impl Future<Output = crate::Result<()>> + 'static {
        let config = config.clone();
        let control_plane_id = locality.map_or_else(|| config.id(), |l| l.region().to_string());
        Self::task("mds_provider".into(), health_check.clone(), move || {
            let config = config.clone();
            let endpoints = endpoints.clone();
            let control_plane_id = control_plane_id.clone();
            let health_check = health_check.clone();
            let reconnect = reconnect.clone();
            let shutdown = shutdown.clone();
            async move {
                loop {
                    let mut stream = crate::net::xds::client::MdsClient::connect(
                        control_plane_id.clone(),
                        endpoints.load().as_ref().clone(),
                    )
                    .await?
                    .delta_stream(config.clone(), health_check.clone(), shutdown.clone())
                    .await
                    .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                    health_check.store(true, Ordering::SeqCst);

                    tokio::select! {
                        res = &mut stream => return res.wrap_err("join handle error")?,
                        // A rotated certificate only reaches the server on a
                        // new connection, so the current one is dropped and
                        // the ordinary connect path runs again.
                        () = reconnect.notified() => stream.abort(),
                    }
                }
            }
        })
    }

    pub fn spawn_xds_provider(
        &self,
        config: Arc<config::Config>,
        health_check: Arc<AtomicBool>,
        notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
        endpoints: SharedEndpoints,
        reconnect: Arc<tokio::sync::Notify>,
    ) -> impl Future<Output = crate::Result<()>> + 'static {
        let config = config.clone();

        Self::task("xds_provider".into(), health_check.clone(), move || {
            let config = config.clone();
            let endpoints = endpoints.clone();
            let health_check = health_check.clone();
            let reconnect = reconnect.clone();
            let tx = notifier.clone();
            async move {
                loop {
                    let identifier = config.id();
                    let mut stream = crate::net::xds::delta_subscribe(
                        config.clone(),
                        identifier,
                        endpoints.load().as_ref().clone(),
                        health_check.clone(),
                        tx.clone(),
                        Self::SUBS,
                    )
                    .await
                    .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                    health_check.store(true, Ordering::SeqCst);

                    tokio::select! {
                        res = &mut stream => return res.wrap_err("join handle error")?,
                        // A rotated certificate only reaches the server on a
                        // new connection, so the current one is dropped and
                        // the ordinary connect path runs again.
                        () = reconnect.notified() => stream.abort(),
                    }
                }
            }
        })
    }

    /// The mds client TLS flags, when they name files that can be rotated.
    fn owned_mds_tls(&self) -> Option<OwnedClientTls> {
        Some(OwnedClientTls {
            ca_path: self.mds_tls_ca_path.clone()?,
            server_name: self.mds_tls_server_name.clone(),
            cert_path: self.mds_tls_cert_path.clone(),
            key_path: self.mds_tls_key_path.clone(),
        })
    }

    /// The xds client TLS flags, when they name files that can be rotated.
    fn owned_xds_tls(&self) -> Option<OwnedClientTls> {
        Some(OwnedClientTls {
            ca_path: self.xds_tls_ca_path.clone()?,
            server_name: self.xds_tls_server_name.clone(),
            cert_path: self.xds_tls_cert_path.clone(),
            key_path: self.xds_tls_key_path.clone(),
        })
    }

    pub fn grpc_push_enabled(&self) -> bool {
        !self.relay.is_empty()
    }

    pub fn grpc_pull_enabled(&self) -> bool {
        !self.xds_endpoints.is_empty()
    }

    /// Whether this node is configured with somewhere to forward game traffic.
    ///
    /// Coarse on purpose: a peer cares that gameservers are reachable here, not
    /// which provider supplies them.
    pub fn gameservers(&self) -> bool {
        self.agones_enabled() || self.static_enabled() || self.http_enabled()
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

        // The cluster map can arrive before the filter chain, so populate with
        // FilterChain::pending() to drop packets until the filter chain has arrived
        if self.grpc_pull_enabled() {
            config
                .dyn_cfg
                .typemap
                .insert::<crate::filters::FilterChain>(
                    crate::config::filter::FilterChainConfig::new(
                        crate::filters::FilterChain::pending(),
                    ),
                );
        }
    }

    pub fn spawn_providers(
        mut self,
        config: &Arc<config::Config>,
        health_check: Arc<AtomicBool>,
        locality: Option<crate::net::endpoint::Locality>,
        notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> crate::Result<tokio::task::JoinSet<crate::Result<()>>> {
        // Resolved before anything spawns: provider tasks retry with backoff,
        // so a connection they can never make has to fail here instead.
        let relay = std::mem::take(&mut self.relay);
        let relay_base = relay.clone();
        self.relay = with_client_tls(
            relay,
            ClientTls {
                ca_path: self.mds_tls_ca_path.as_deref(),
                server_name: self.mds_tls_server_name.as_deref(),
                cert_path: self.mds_tls_cert_path.as_deref(),
                key_path: self.mds_tls_key_path.as_deref(),
            },
            MDS_TLS_FLAGS,
        )?;

        let xds_endpoints = std::mem::take(&mut self.xds_endpoints);
        let xds_base = xds_endpoints.clone();
        self.xds_endpoints = with_client_tls(
            xds_endpoints,
            ClientTls {
                ca_path: self.xds_tls_ca_path.as_deref(),
                server_name: self.xds_tls_server_name.as_deref(),
                cert_path: self.xds_tls_cert_path.as_deref(),
                key_path: self.xds_tls_key_path.as_deref(),
            },
            XDS_TLS_FLAGS,
        )?;

        // Shared so a rotated client certificate reaches the next connection
        // attempt without rebuilding the provider.
        let relay_endpoints: SharedEndpoints =
            Arc::new(arc_swap::ArcSwap::from_pointee(self.relay.clone()));
        let xds_endpoints: SharedEndpoints =
            Arc::new(arc_swap::ArcSwap::from_pointee(self.xds_endpoints.clone()));
        let relay_reconnect = Arc::new(tokio::sync::Notify::new());
        let xds_reconnect = Arc::new(tokio::sync::Notify::new());

        for (base, client, flags, endpoints, reconnect) in [
            (
                relay_base,
                self.owned_mds_tls(),
                MDS_TLS_FLAGS,
                &relay_endpoints,
                &relay_reconnect,
            ),
            (
                xds_base,
                self.owned_xds_tls(),
                XDS_TLS_FLAGS,
                &xds_endpoints,
                &xds_reconnect,
            ),
        ] {
            let Some(client) = client else { continue };

            if let Some(cert) = client.cert_path.as_deref()
                && let Ok(pem) = std::fs::read(cert)
            {
                crate::tls::report_certificate(flags.endpoints, &pem);
            }

            ClientTlsReloader {
                base,
                client,
                flags,
                endpoints: endpoints.clone(),
                reconnect: reconnect.clone(),
            }
            .spawn(shutdown.clone())?;
        }

        let mut providers = tokio::task::JoinSet::new();

        if !self.any_provider_enabled() {
            tracing::info!("no configuration providers specified");
            health_check.store(true, std::sync::atomic::Ordering::Relaxed);
            return Ok(providers);
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

        if self.mmdb_enabled() {
            providers.spawn(self.spawn_mmdb_provider());
        }

        let mutator = self.maybe_spawn_corrosion(config, &health_check, &mut providers);

        if mutator.is_some() && self.fs_enabled() {
            tracing::error!("corrosion mutation does not work with file system data");
        };

        if self.grpc_push_enabled() {
            providers.spawn(self.spawn_mds_provider(
                config.clone(),
                health_check.clone(),
                locality.clone(),
                shutdown.clone(),
                relay_endpoints.clone(),
                relay_reconnect.clone(),
            ));
        }

        if self.k8s_enabled() || self.agones_enabled() {
            providers.spawn(self.spawn_k8s_provider(
                health_check.clone(),
                locality.clone(),
                config,
                mutator.clone(),
                shutdown,
            ));
        }

        if self.grpc_pull_enabled() {
            providers.spawn(self.spawn_xds_provider(
                config.clone(),
                health_check.clone(),
                notifier,
                xds_endpoints.clone(),
                xds_reconnect.clone(),
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

        if self.http_enabled()
            && let Some(fc) = FiltersAndClusters::new(config)
        {
            let address = self
                .http_address
                .unwrap_or_else(|| (std::net::Ipv6Addr::UNSPECIFIED, http::DEFAULT_PORT).into());
            let health_check = health_check.clone();

            providers.spawn(Self::task(
                "http_provider".into(),
                health_check.clone(),
                move || http::serve(fc.clone(), address, health_check.clone()),
            ));
        }

        if let Some(fc) = self
            .static_enabled()
            .then(|| FiltersAndClusters::new(config))
            .flatten()
        {
            health_check.store(true, Ordering::SeqCst);
            providers.spawn(
                self.spawn_static_provider(fc, &health_check, locality.clone(), mutator)
                    .unwrap(),
            );
        }

        assert!(
            !providers.is_empty(),
            "bug: no provider tasks running when {:?} was specified",
            providers
        );

        Ok(providers)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn filters_are_pending(providers: &Providers) -> bool {
        let mut service = crate::Service::default();
        let config = crate::Config::new(None, Default::default(), providers, &mut service);

        config.dyn_cfg.filters().unwrap().load().is_pending()
    }

    #[test]
    fn grpc_pull_provider_starts_with_pending_filters() {
        let providers =
            Providers::default().grpc_pull_endpoints([tonic::transport::Endpoint::from_static(
                "http://127.0.0.1:7800",
            )]);

        assert!(filters_are_pending(&providers));
    }

    #[test]
    fn standalone_provider_starts_with_not_pending_filters() {
        assert!(!filters_are_pending(&Providers::default().fs()));
    }

    /// tonic refuses an `https` endpoint carrying no TLS configuration, so a
    /// missing CA is a connection that can never be made. The provider tasks
    /// retry with backoff, which would otherwise bury it.
    #[test]
    fn client_tls_flags_must_agree_with_the_endpoint_scheme() {
        let secure = || {
            vec![tonic::transport::Endpoint::from_static(
                "https://localhost:7800",
            )]
        };
        let plain = || {
            vec![tonic::transport::Endpoint::from_static(
                "http://localhost:7800",
            )]
        };
        let ca = std::path::Path::new("ca.pem");
        let rejected = |endpoints, client| {
            with_client_tls(endpoints, client, XDS_TLS_FLAGS)
                .expect_err("an unusable combination must not resolve")
                .to_string()
        };

        let error = rejected(secure(), ClientTls::default());
        assert!(error.contains(XDS_TLS_CA_FLAG), "{error}");

        let error = rejected(
            plain(),
            ClientTls {
                ca_path: Some(ca),
                ..Default::default()
            },
        );
        assert!(error.contains("no endpoint"), "{error}");

        let error = rejected(
            plain(),
            ClientTls {
                server_name: Some("relay"),
                ..Default::default()
            },
        );
        assert!(error.contains(XDS_TLS_SERVER_NAME_FLAG), "{error}");

        // A client certificate is only ever presented over TLS.
        let error = rejected(
            plain(),
            ClientTls {
                cert_path: Some(ca),
                key_path: Some(ca),
                ..Default::default()
            },
        );
        assert!(error.contains(XDS_TLS_CA_FLAG), "{error}");

        // Half an identity is a certificate that would never be presented.
        let error = rejected(
            secure(),
            ClientTls {
                ca_path: Some(ca),
                cert_path: Some(ca),
                ..Default::default()
            },
        );
        assert!(error.contains(XDS_TLS_KEY_FLAG), "{error}");

        // Plain endpoints and no TLS flags is the untouched default.
        assert!(with_client_tls(plain(), ClientTls::default(), XDS_TLS_FLAGS).is_ok());
    }
}
