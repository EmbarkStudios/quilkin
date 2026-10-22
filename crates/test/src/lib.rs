#![allow(clippy::unimplemented)]

use quilkin::{Config, signal::ShutdownTx, test::TestConfig};

pub use serde_json::json;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc;

#[cfg(target_os = "linux")]
pub mod xdp_util;

/// Upper bound for waiting on a packet that is expected to arrive.
///
/// Generous on purpose: shared CI runners have been observed stalling every
/// test process on the machine for ~500ms while sandboxes spin up, and this is
/// only a guard against hanging forever, not a latency assertion.
pub const RECV_TIMEOUT: u64 = 10_000;

pub const MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

#[inline]
pub fn alloc_buffer(data: impl AsRef<[u8]>) -> bytes::BytesMut {
    bytes::BytesMut::from(data.as_ref())
}

/// Macro that can get the function name of the function the macro is invoked
/// within
#[macro_export]
macro_rules! func_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

/// Creates a temporary file with the specified prefix in a directory named
/// after the calling function, ie using it within a test will place it in a
/// temporary directory named after the test
#[macro_export]
macro_rules! temp_file {
    ($prefix:expr) => {{
        let name = $crate::func_name!();
        let name = name.strip_suffix("::{{closure}}").unwrap_or(name);
        let mut name = name.replace("::", ".");
        name.push('-');
        name.push_str($prefix);
        name.push('-');
        tempfile::NamedTempFile::with_prefix(name).unwrap()
    }};
}

pub use tracing::{Level, subscriber::DefaultGuard};

/// Installs the rustls crypto provider the TLS paths need.
///
/// `quilkin` does this in `main`, which no test runs. Tests in a binary share
/// the process, so only the first call installs and the rest are no-ops.
pub fn install_crypto_provider() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        drop(rustls::crypto::ring::default_provider().install_default());
    });
}

/// Writes a client certificate for `named`, returning its cert and key paths.
fn write_client_identity(
    td: &std::path::Path,
    pail: &str,
    tls: &RelayTls,
    named: &str,
) -> (PathBuf, PathBuf) {
    let (cert_pem, key_pem) = tls.pki.issue_client(named);
    let cert = td.join(format!("{pail}-client.pem"));
    let key = td.join(format!("{pail}-client-key.pem"));
    std::fs::write(&cert, cert_pem).unwrap();
    std::fs::write(&key, key_pem).unwrap();
    (cert, key)
}

/// A CA and the server certificate it signed, for a relay serving TLS.
///
/// The leaf is signed by a separate CA rather than being its own anchor:
/// rustls rejects a self-signed leaf offered as its own trust root.
pub struct TestPki {
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    ca_params: rcgen::CertificateParams,
    ca_key: rcgen::KeyPair,
}

impl TestPki {
    pub fn generate() -> Self {
        use rcgen::{
            BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
            KeyPair, KeyUsagePurpose,
        };

        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "quilkin test ca");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        // The pails address the relay as `localhost`, so that is what the
        // certificate has to be valid for.
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "localhost");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().unwrap();
        let issuer = Issuer::from_params(&ca_params, &ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();

        Self {
            ca_pem: ca_cert.pem(),
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            ca_params,
            ca_key,
        }
    }

    /// Issues a client certificate whose only DNS name is `name`.
    ///
    /// The relay checks that name against the identifier the peer claims over
    /// xDS or mDS, so it has to be the pail's name.
    pub fn issue_client(&self, name: &str) -> (String, String) {
        use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, Issuer, KeyPair};

        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

        let key = KeyPair::generate().unwrap();
        let issuer = Issuer::from_params(&self.ca_params, &self.ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();

        (cert.pem(), key.serialize_pem())
    }
}

pub fn init_logging(level: Level, test_pkg: &'static str) {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};
    let layer = tracing_subscriber::fmt::layer()
        .with_test_writer()
        .with_filter(tracing_subscriber::filter::LevelFilter::from_level(level))
        .with_filter(tracing_subscriber::EnvFilter::new(format!(
            // NOTE: corrosion is _EXTREMELY_ spammy at the trace level, so don't enable these unless you are trying
            // to see everything that could go on, but it's so noisy it can actually make it harder to diagnose issues
            // corrosion=trace,corro_types=trace,tripwire=trace
            "{test_pkg}=trace,qt=trace,quilkin=debug"
        )));
    let sub = tracing_subscriber::Registry::default().with(layer);
    let disp = tracing::dispatcher::Dispatch::new(sub);
    // tests in the same binary share the process-wide dispatcher, so only the
    // first one gets to install it
    drop(tracing::dispatcher::set_global_default(disp));
}

#[macro_export]
macro_rules! trace_test {
    ($(#[$attr:meta])* $name:ident, $body:block) => {
        $(#[$attr])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            // Get the module name
            let fname = $crate::func_name!();
            let mname = fname.rsplit("::").nth(2).unwrap();

            let _guard = $crate::init_logging($crate::Level::TRACE, mname);

            $body
        }
    };
}

pub struct ServerPailConfig {
    pub packet_size: u16,
    pub num_packets: Option<usize>,
    pub echo: bool,
}

impl Default for ServerPailConfig {
    fn default() -> Self {
        Self {
            packet_size: 1024,
            num_packets: None,
            echo: false,
        }
    }
}

#[derive(Default)]
pub struct AdminPailConfig;

#[derive(Default)]
pub struct RelayPailConfig {
    pub config: Option<quilkin::test::TestConfig>,
    /// Serve xDS and mDS over TLS. Dependent pails verify it against the CA
    /// this generates, without having to be told about it.
    pub tls: bool,
    /// Require a certificate from dependent pails and check the identifier
    /// each one claims against it. Implies `tls`.
    pub mtls: bool,
}

/// What a relay serving TLS publishes to the pails that depend on it.
#[derive(Clone)]
pub struct RelayTls {
    pub ca_path: PathBuf,
    pub pki: Arc<TestPki>,
    /// The relay requires a client certificate naming the peer.
    pub mutual: bool,
}

#[derive(Default)]
pub struct ProxyPailConfig {
    pub config: Option<quilkin::test::TestConfig>,
    pub corrosion: bool,
    /// Verify the relay against a CA that did not sign its certificate.
    pub mismatched_tls_ca: bool,
    /// Present a client certificate naming something other than this pail,
    /// which is the identifier it claims over xDS.
    pub client_cert_name: Option<&'static str>,
    /// Present no client certificate, even to a relay that requires one.
    pub no_client_cert: bool,
}

#[derive(Default)]
pub struct ManagementPailConfig {}

pub struct AgentPailConfig {
    pub endpoints: Vec<(&'static str, &'static [&'static str])>,
    pub icao_code: quilkin::config::IcaoCode,
    pub corrosion: bool,
}

impl Default for AgentPailConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            icao_code: Default::default(),
            corrosion: true,
        }
    }
}

pub enum PailConfig {
    /// Creates a UDP socket with an ephemeral port that will send received
    /// packets to a channel
    Server(ServerPailConfig),
    // Creates an admin server that is applied to any following relay, proxy, or management pails
    //Admin(AdminPailConfig),
    Relay(RelayPailConfig),
    Proxy(ProxyPailConfig),
    //Management(ManagementPailConfig),
    Agent(AgentPailConfig),
}

impl From<ServerPailConfig> for PailConfig {
    fn from(value: ServerPailConfig) -> Self {
        Self::Server(value)
    }
}

impl From<RelayPailConfig> for PailConfig {
    fn from(value: RelayPailConfig) -> Self {
        Self::Relay(value)
    }
}

impl From<AgentPailConfig> for PailConfig {
    fn from(value: AgentPailConfig) -> Self {
        Self::Agent(value)
    }
}

impl From<ProxyPailConfig> for PailConfig {
    fn from(value: ProxyPailConfig) -> Self {
        Self::Proxy(value)
    }
}

pub struct ConfigFile {
    pub path: PathBuf,
    pub config: TestConfig,
}

impl ConfigFile {
    pub fn update(&mut self, update: impl FnOnce(&mut TestConfig)) {
        update(&mut self.config);
        self.config.write_to_file(&self.path);
    }
}

macro_rules! abort_task {
    ($pail:ty) => {
        impl Drop for $pail {
            fn drop(&mut self) {
                if let Some(task) = self.task.take() {
                    task.abort();
                }
            }
        }
    };
}

pub struct SandboxPailConfig {
    pub name: &'static str,
    pub config: PailConfig,
    pub dependencies: &'static [&'static str],
}

pub struct SandboxConfig {
    pub name: String,
    pub pails: Vec<SandboxPailConfig>,
}

pub type JoinHandle =
    tokio::task::JoinHandle<(quilkin::signal::ShutdownHandler, quilkin::Result<()>)>;
pub type JoinSet = tokio::task::JoinSet<quilkin::Result<()>>;

pub struct ServerPail {
    /// The server socket's ephemeral port
    pub port: u16,
    pub packet_rx: Option<mpsc::Receiver<String>>,
    /// The join handle to the task driving the socket. Used to both cancel the task
    /// and/or wait for it to finish
    pub task: Option<tokio::task::JoinHandle<usize>>,
}

abort_task!(ServerPail);

pub struct RelayPail {
    pub xds_port: u16,
    pub mds_port: u16,
    /// Set when this relay serves TLS.
    pub tls: Option<RelayTls>,
    pub corrosion_port: Option<u16>,
    pub task: Option<JoinHandle>,
    pub provider_task: JoinSet,
    pub healthy: Arc<std::sync::atomic::AtomicBool>,
    pub shutdown: ShutdownTx,
    pub config_file: Option<ConfigFile>,
    pub config: Arc<Config>,
}

abort_task!(RelayPail);

pub struct AgentPail {
    pub qcmp_port: u16,
    pub task: Option<JoinHandle>,
    pub provider_task: JoinSet,
    pub healthy: Arc<std::sync::atomic::AtomicBool>,
    pub shutdown: ShutdownTx,
    pub config_file: Option<ConfigFile>,
    pub config: Arc<Config>,
}

abort_task!(AgentPail);

pub struct ProxyPail {
    pub port: u16,
    pub qcmp_port: u16,
    pub phoenix_port: u16,
    pub task: Option<JoinHandle>,
    pub provider_task: JoinSet,
    pub healthy: Arc<std::sync::atomic::AtomicBool>,
    pub shutdown: ShutdownTx,
    pub config: Arc<Config>,
    pub delta_applies: Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
}

abort_task!(ProxyPail);

pub type Pails = std::collections::BTreeMap<&'static str, Pail>;

pub enum Pail {
    Server(ServerPail),
    Relay(RelayPail),
    Agent(AgentPail),
    Proxy(ProxyPail),
}

impl Pail {
    #[inline]
    pub fn config(&self) -> Arc<Config> {
        match self {
            Self::Relay(p) => p.config.clone(),
            Self::Agent(p) => p.config.clone(),
            Self::Proxy(p) => p.config.clone(),
            Self::Server(_) => panic!("no config"),
        }
    }

    pub fn is_healthy(&self) -> bool {
        match self {
            Pail::Relay(p) => p.healthy.load(std::sync::atomic::Ordering::Relaxed),
            Pail::Agent(p) => p.healthy.load(std::sync::atomic::Ordering::Relaxed),
            Pail::Proxy(p) => p.healthy.load(std::sync::atomic::Ordering::Relaxed),
            Pail::Server(_) => panic!("no health"),
        }
    }

    async fn stop(&mut self) {
        if let Self::Relay(rp) = self {
            if let Some(task) = rp.task.take() {
                let _ = rp.shutdown.send(());
                let result = task.await.unwrap();
                tracing::info!(result = ?result.1, "task finished");
            }
        } else {
            unimplemented!();
        }
    }

    async fn start(&mut self) {
        if let Self::Relay(rp) = self {
            if rp.task.is_some() {
                panic!("relay still running");
            }

            let svc = quilkin::Service::default()
                .xds()
                .xds_port(rp.xds_port)
                .mds()
                .mds_port(rp.mds_port)
                .grpc()
                .corrosion_port(0)
                .testing();
            let (tx, rx) = quilkin::signal::channel();
            let sh = quilkin::signal::ShutdownHandler::new(tx.clone(), rx);
            let (task, ports) = svc.spawn_services(&rp.config, sh).await.unwrap();

            rp.shutdown = tx;
            rp.corrosion_port = ports.corrosion;
            rp.task = Some(task);
        } else {
            unimplemented!();
        }
    }
}

impl Pail {
    pub async fn construct(spc: SandboxPailConfig, pails: &Pails, td: &std::path::Path) -> Self {
        match spc.config {
            PailConfig::Server(sspc) => {
                let (packet_tx, packet_rx) = mpsc::channel::<String>(10);
                let socket = quilkin::net::DualStackEpollSocket::new(0)
                    .expect("failed to create server socket");

                let port = socket
                    .local_addr()
                    .expect("failed to bind server socket")
                    .port();

                tracing::debug!(port, spc.name, "bound server socket");

                let packet_size = sspc.packet_size as usize;
                let mut num_packets = sspc.num_packets.unwrap_or(usize::MAX);

                let task = tokio::spawn(async move {
                    let mut buf = vec![0; packet_size];
                    let mut received = 0;

                    while num_packets > 0 {
                        let (size, addr) = socket
                            .recv_from(&mut buf)
                            .await
                            .expect("failed to receive packet");
                        received += size;

                        if sspc.echo {
                            socket
                                .send_to(&buf[..size], addr)
                                .await
                                .expect("failed to send packet");
                        } else {
                            let pstr = std::str::from_utf8(&buf[..size])
                                .expect("received non-utf8 string in packet")
                                .to_owned();

                            packet_tx.send(pstr).await.expect("packet receiver dropped");
                        }

                        num_packets -= 1;
                    }

                    received
                });

                Self::Server(ServerPail {
                    port,
                    task: Some(task),
                    packet_rx: Some(packet_rx),
                })
            }
            PailConfig::Relay(rpc) => {
                let path = td.join(spc.name);
                let mut tc = rpc.config.unwrap_or_default();
                tc.id = spc.name.into();
                tc.write_to_file(&path);

                let config_path = path.clone();

                let (shutdown, shutdown_rx) = quilkin::signal::channel();
                let pail_token = quilkin::signal::cancellation_token(shutdown_rx.clone());

                let tls = (rpc.tls || rpc.mtls).then(|| {
                    install_crypto_provider();
                    let pki = TestPki::generate();
                    let ca_path = td.join(format!("{}-ca.pem", spc.name));
                    std::fs::write(&ca_path, &pki.ca_pem).unwrap();
                    RelayTls {
                        ca_path,
                        pki: Arc::new(pki),
                        mutual: rpc.mtls,
                    }
                });

                let providers = quilkin::Providers::default().fs().fs_path(path);
                let mut svc = quilkin::Service::default()
                    .xds()
                    .xds_port(0)
                    .mds()
                    .mds_port(0)
                    .grpc()
                    .corrosion_port(0)
                    .testing();

                if let Some(tls) = &tls {
                    svc = svc.tls_pem(&tls.pki.cert_pem, &tls.pki.key_pem);

                    // The relay's own CA also signs the peers, so the same
                    // bundle verifies both directions.
                    if tls.mutual {
                        svc = svc.tls_client_ca_path(&tls.ca_path);
                    }
                }

                let config = crate::Config::new_rc(
                    Some("test-relay".into()),
                    Default::default(),
                    &providers,
                    &mut svc,
                    pail_token,
                );

                *config.dyn_cfg.id.lock() = spc.name.into();
                let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let provider_task = providers
                    .spawn_providers(&config, healthy.clone(), None, None, shutdown_rx.clone())
                    .expect("failed to configure providers");
                let (task, ports) = svc
                    .spawn_services(
                        &config,
                        quilkin::signal::ShutdownHandler::new(shutdown.clone(), shutdown_rx),
                    )
                    .await
                    .unwrap();

                Self::Relay(RelayPail {
                    xds_port: ports.xds.expect("xds not spawned"),
                    mds_port: ports.mds.expect("mds not spawned"),
                    tls,
                    corrosion_port: ports.corrosion,
                    task: Some(task),
                    provider_task,
                    healthy,
                    shutdown,
                    config_file: Some(ConfigFile {
                        path: config_path,
                        config: tc,
                    }),
                    config,
                })
            }
            PailConfig::Agent(apc) => {
                let mut endpoints = std::collections::BTreeSet::new();

                for (dep_name, tokens) in apc.endpoints {
                    let Pail::Server(server) = &pails[dep_name] else {
                        panic!("expected '{dep_name}' to be a server pail for endpoint");
                    };

                    endpoints.insert(quilkin::net::Endpoint::with_metadata(
                        (std::net::Ipv4Addr::UNSPECIFIED, server.port).into(),
                        quilkin::net::endpoint::Metadata {
                            tokens: tokens.iter().map(|t| Vec::from(*t)).collect(),
                        },
                    ));
                }

                let mut tc = TestConfig::new();
                tc.clusters.insert_default(endpoints);
                tc.id = spc.name.into();

                let path = td.join(spc.name);
                tc.write_to_file(&path);

                let mut relay_tls = None;
                let relay_servers = spc
                    .dependencies
                    .iter()
                    .filter_map(|dname| {
                        let Pail::Relay(RelayPail { mds_port, tls, .. }) = &pails[dname] else {
                            return None;
                        };
                        let scheme = if tls.is_some() {
                            relay_tls = tls.clone();
                            "https"
                        } else {
                            "http"
                        };
                        Some(
                            format!("{scheme}://localhost:{mds_port}")
                                .parse()
                                .expect("failed to parse endpoint"),
                        )
                    })
                    .collect::<Vec<_>>();

                let (shutdown, shutdown_rx) = quilkin::signal::channel();
                let pail_token = quilkin::signal::cancellation_token(shutdown_rx.clone());

                let config_path = path.clone();
                let mut svc = quilkin::Service::default().qcmp().qcmp_port(0);
                let mut providers = quilkin::Providers::default()
                    .fs()
                    .fs_path(path)
                    .grpc_push_endpoints(relay_servers);

                if let Some(tls) = relay_tls {
                    install_crypto_provider();
                    providers = providers.grpc_push_tls_ca_path(&tls.ca_path);

                    if tls.mutual {
                        let (cert, key) = write_client_identity(td, spc.name, &tls, spc.name);
                        providers = providers.grpc_push_tls_identity_paths(cert, key);
                    }
                }

                let corrosion_eps = spc
                    .dependencies
                    .iter()
                    .filter_map(|dname| {
                        let Pail::Relay(RelayPail { corrosion_port, .. }) = &pails[dname] else {
                            return None;
                        };
                        corrosion_port.filter(|_a| apc.corrosion).map(|p| {
                            quilkin::net::EndpointAddress::from(SocketAddr::V6(
                                std::net::SocketAddrV6::new(std::net::Ipv6Addr::LOCALHOST, p, 0, 0),
                            ))
                        })
                    })
                    .collect::<Vec<_>>();

                let providers = if !corrosion_eps.is_empty() {
                    providers
                        .corrosion_endpoints(corrosion_eps)
                        .corrosion_mode(quilkin::providers::corrosion::CorrosionMode::Push)
                } else {
                    providers
                };

                let config = crate::Config::new_rc(
                    Some("test-agent".into()),
                    apc.icao_code,
                    &providers,
                    &mut svc,
                    pail_token,
                );

                *config.dyn_cfg.id.lock() = spc.name.into();
                let acfg = config.clone();
                let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let provider_task = providers
                    .spawn_providers(&config, healthy.clone(), None, None, shutdown_rx.clone())
                    .expect("failed to configure providers");
                let (task, ports) = svc
                    .spawn_services(
                        &config,
                        quilkin::signal::ShutdownHandler::new(shutdown.clone(), shutdown_rx),
                    )
                    .await
                    .unwrap();

                Self::Agent(AgentPail {
                    qcmp_port: ports.qcmp.expect("failed to spawn QCMP"),
                    task: Some(task),
                    provider_task,
                    healthy,
                    shutdown,
                    config_file: Some(ConfigFile {
                        path: config_path,
                        config: tc,
                    }),
                    config: acfg,
                })
            }
            PailConfig::Proxy(ppc) => {
                let providers = if ppc.corrosion {
                    let corrosion_eps: Vec<_> = spc
                        .dependencies
                        .iter()
                        .filter_map(|dname| {
                            let Pail::Relay(RelayPail { corrosion_port, .. }) = &pails[dname]
                            else {
                                return None;
                            };
                            corrosion_port.map(|p| {
                                quilkin::net::EndpointAddress::from(SocketAddr::V6(
                                    std::net::SocketAddrV6::new(
                                        std::net::Ipv6Addr::LOCALHOST,
                                        p,
                                        0,
                                        0,
                                    ),
                                ))
                            })
                        })
                        .collect();

                    quilkin::Providers::default()
                        .corrosion_endpoints(corrosion_eps)
                        .corrosion_mode(quilkin::providers::corrosion::CorrosionMode::Pull)
                } else {
                    let mut relay_tls = None;
                    let management_servers: Vec<_> = spc
                        .dependencies
                        .iter()
                        .filter_map(|dname| {
                            let Pail::Relay(RelayPail { xds_port, tls, .. }) = &pails[dname] else {
                                return None;
                            };
                            let scheme = if tls.is_some() {
                                relay_tls = tls.clone();
                                "https"
                            } else {
                                "http"
                            };
                            Some(
                                format!("{scheme}://localhost:{xds_port}")
                                    .parse()
                                    .expect("failed to parse endpoint"),
                            )
                        })
                        .collect();

                    let mut providers =
                        quilkin::Providers::default().grpc_pull_endpoints(management_servers);

                    if let Some(tls) = relay_tls {
                        install_crypto_provider();

                        // A CA that signed nothing in this sandbox, for asserting
                        // that verification is actually performed.
                        let ca = if ppc.mismatched_tls_ca {
                            let path = td.join(format!("{}-wrong-ca.pem", spc.name));
                            std::fs::write(&path, TestPki::generate().ca_pem).unwrap();
                            path
                        } else {
                            tls.ca_path.clone()
                        };

                        providers = providers.grpc_pull_tls_ca_path(ca);

                        if tls.mutual && !ppc.no_client_cert {
                            // Naming anything but the pail makes the identifier it
                            // claims over xDS disagree with its certificate.
                            let named = ppc.client_cert_name.unwrap_or(spc.name);
                            let (cert, key) = write_client_identity(td, spc.name, &tls, named);
                            providers = providers.grpc_pull_tls_identity_paths(cert, key);
                        }
                    }

                    providers
                };

                let mut svc = quilkin::Service::default()
                    .udp()
                    .udp_port(0)
                    .qcmp()
                    .qcmp_port(0)
                    .phoenix()
                    .phoenix_port(0)
                    .termination_timeout(None);

                let (shutdown, shutdown_rx) = quilkin::signal::channel();
                let pail_token = quilkin::signal::cancellation_token(shutdown_rx.clone());

                let config = crate::Config::new_rc(
                    Some("test-proxy".into()),
                    Default::default(),
                    &Default::default(),
                    &mut svc,
                    pail_token,
                );

                if let Some(cfg) = ppc.config {
                    if !cfg.clusters.is_empty() {
                        panic!("not implemented");
                    }

                    if !cfg.filters.is_empty() {
                        config.dyn_cfg.filters().unwrap().store(cfg.filters);
                    }
                }

                let endpoints: std::collections::BTreeSet<_> = spc
                    .dependencies
                    .iter()
                    .filter_map(|dname| {
                        let Pail::Server(ServerPail { port, .. }) = &pails[dname] else {
                            return None;
                        };

                        Some(quilkin::net::Endpoint::new(
                            (std::net::Ipv6Addr::LOCALHOST, *port).into(),
                        ))
                    })
                    .collect();

                if !endpoints.is_empty() {
                    config
                        .dyn_cfg
                        .clusters()
                        .unwrap()
                        .modify(|clusters| clusters.insert_default(endpoints));
                }

                *config.dyn_cfg.id.lock() = spc.name.into();

                let (rttx, rtrx) = tokio::sync::mpsc::unbounded_channel();

                let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let provider_task = providers
                    .spawn_providers(
                        &config,
                        healthy.clone(),
                        None,
                        Some(rttx),
                        shutdown_rx.clone(),
                    )
                    .expect("failed to configure providers");
                let (task, ports) = svc
                    .spawn_services(
                        &config,
                        quilkin::signal::ShutdownHandler::new(shutdown.clone(), shutdown_rx),
                    )
                    .await
                    .unwrap();

                Self::Proxy(ProxyPail {
                    port: ports.udp.expect("failed to spawn proxy"),
                    qcmp_port: ports.qcmp.expect("failed to spawn qcmp"),
                    phoenix_port: ports.phoenix.expect("failed to spawn phoenix"),
                    shutdown,
                    task: Some(task),
                    provider_task,
                    healthy,
                    config,
                    delta_applies: Some(rtrx),
                })
            }
        }
    }
}

pub struct Sandbox {
    pub pails: Pails,
    pub td: tempfile::TempDir,
}

#[macro_export]
macro_rules! sandbox_config {
    () => {{
        let fname = $crate::func_name!();
        let mut name = String::new();
        for comp in fname.split("::") {
            if comp.starts_with("{{") {
                continue;
            }

            if !name.is_empty() {
                name.push('.');
            }

            name.push_str(comp);
        }
        $crate::SandboxConfig::new(name)
    }};
}

impl SandboxConfig {
    pub fn new(name: impl Into<String>) -> Self {
        quilkin_xds::metrics::set_registry(quilkin::metrics::registry());

        Self {
            name: name.into(),
            pails: Vec::new(),
        }
    }

    pub fn push(
        &mut self,
        name: &'static str,
        config: impl Into<PailConfig>,
        deps: &'static [&'static str],
    ) {
        self.pails.push(SandboxPailConfig {
            name,
            config: config.into(),
            dependencies: deps,
        });
    }

    pub async fn spinup(self) -> Sandbox {
        // Validate that every dependency is satisfied
        for i in 0..self.pails.len() {
            let deps = self.pails[i].dependencies;

            for dep in deps {
                if !self.pails[..i].iter().any(|sp| sp.name == *dep) {
                    panic!(
                        "failed to locate dependency '{dep}' for '{}'",
                        self.pails[i].name
                    );
                }
            }
        }

        tracing::trace!("dependencies resolved");

        // Create a channel that is used to communicate readiness of each pail we create
        //let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Not necessarily used in every test, but in most
        let td = match tempfile::TempDir::with_prefix(&self.name) {
            Ok(td) => td,
            Err(err) => {
                panic!("failed to create temp dir '{}': {err:#}", self.name);
            }
        };

        let mut pails = Pails::new();
        for pc in self.pails {
            let name = pc.name;
            let pail = Pail::construct(pc, &pails, td.path()).await;

            if pails.insert(name, pail).is_some() {
                panic!("{name} already existed");
            }
        }

        Sandbox { pails, td }
    }
}

impl Sandbox {
    #[inline]
    pub fn proxy(
        &mut self,
        name: &str,
    ) -> (SocketAddr, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let Some(Pail::Proxy(pp)) = self.pails.get_mut(name) else {
            unreachable!()
        };
        (
            SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, pp.port)),
            pp.delta_applies.take().unwrap(),
        )
    }

    #[inline]
    pub fn packet_rx(&mut self, name: &str) -> mpsc::Receiver<String> {
        let Some(Pail::Server(pp)) = self.pails.get_mut(name) else {
            unreachable!()
        };
        pp.packet_rx.take().unwrap()
    }

    #[inline]
    pub fn server(&mut self, name: &str) -> (mpsc::Receiver<String>, SocketAddr) {
        let Some(Pail::Server(pp)) = self.pails.get_mut(name) else {
            unreachable!()
        };
        (
            pp.packet_rx.take().unwrap(),
            SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, pp.port)),
        )
    }

    #[inline]
    pub fn config_file(&mut self, name: &str) -> ConfigFile {
        let pail = self.pails.get_mut(name).unwrap();

        match pail {
            Pail::Relay(rp) => rp.config_file.take().unwrap(),
            Pail::Agent(ap) => ap.config_file.take().unwrap(),
            _ => unreachable!("no config_file for this pail"),
        }
    }

    #[inline]
    pub fn socket(&self) -> (socket2::Socket, SocketAddr) {
        let socket = quilkin::net::raw_socket_with_reuse(0).unwrap();
        let port = quilkin::net::socket_port(&socket);

        (
            socket,
            SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, port)),
        )
    }

    /// Creates an ephemeral socket that can be used to send messages to sandbox
    /// pails
    #[inline]
    pub fn client(&self) -> quilkin::net::DualStackEpollSocket {
        quilkin::net::DualStackEpollSocket::new(0).unwrap()
    }

    /// Sleeps for the specified number of milliseconds
    #[inline]
    pub async fn sleep(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    /// Runs a future, expecting it complete before the specified timeout
    #[inline]
    pub async fn timeout<F>(&self, ms: u64, fut: F) -> (F::Output, Duration)
    where
        F: std::future::Future,
    {
        let start = tokio::time::Instant::now();
        let res = tokio::time::timeout(Duration::from_millis(ms), fut)
            .await
            .expect("operation timed out");

        (res, start.elapsed())
    }

    #[inline]
    pub async fn maybe_timeout<F>(&self, ms: u64, fut: F) -> Option<F::Output>
    where
        F: std::future::Future,
    {
        tokio::time::timeout(Duration::from_millis(ms), fut)
            .await
            .ok()
    }

    /// Runs a future, expecting it to timeout instead of resolving, panics if
    /// the future finishes before the timeout
    #[inline]
    pub async fn expect_timeout<F>(&self, ms: u64, fut: F)
    where
        F: std::future::Future,
        F::Output: std::fmt::Debug,
    {
        tokio::time::timeout(Duration::from_millis(ms), fut)
            .await
            .expect_err("expected future to timeout");
    }

    pub async fn restart(&mut self, which: &str, dead_time: Duration) {
        let Some((_, pail)) = self.pails.iter_mut().find(|(name, _)| **name == which) else {
            panic!("failed to find '{which}'");
        };

        const TIMEOUT: Duration = Duration::from_secs(10);

        tokio::time::timeout(TIMEOUT, pail.stop())
            .await
            .expect("failed to stop");
        tokio::time::sleep(dead_time).await;
        tokio::time::timeout(TIMEOUT, pail.start())
            .await
            .expect("failed to start");
    }

    pub async fn stop(&mut self, which: &str) {
        let Some((_, pail)) = self.pails.iter_mut().find(|(name, _)| **name == which) else {
            panic!("failed to find '{which}'");
        };
        tracing::info!("stopping {which}");
        pail.stop().await;
        tracing::info!("stopped {which}");
    }

    pub async fn start(&mut self, which: &str) {
        let Some((_, pail)) = self.pails.iter_mut().find(|(name, _)| **name == which) else {
            panic!("failed to find '{which}'");
        };
        tracing::info!("starting {which}");
        pail.start().await;
        tracing::info!("started {which}");
    }

    #[inline]
    pub async fn block_until_ready(&self, which: &str) {
        self.wait_for_health_state(which, true).await;
    }

    #[inline]
    pub async fn block_until_not_ready(&self, which: &str) {
        self.wait_for_health_state(which, false).await;
    }

    async fn wait_for_health_state(&self, which: &str, health_state: bool) {
        let Some((_, pail)) = self.pails.iter().find(|(name, _)| **name == which) else {
            panic!("failed to find '{which}'");
        };

        let start = tokio::time::Instant::now();
        while pail.is_healthy() != health_state {
            tracing::debug!(
                "waiting for '{}' to enter health state '{}'",
                which,
                health_state
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            if start.elapsed() > MAX_WAIT {
                panic!(
                    "timed out waiting for '{which}' to enter healthy state '{health_state}' after {MAX_WAIT:?}"
                );
            }
        }
    }

    pub async fn block_until_packet_gets_through(
        &self,
        msg: &[u8],
        expected: &str,
        client: &quilkin::net::DualStackEpollSocket,
        proxy_address: &std::net::SocketAddr,
        server_rx: &mut tokio::sync::mpsc::Receiver<String>,
    ) {
        const PACKET_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
        let start = tokio::time::Instant::now();
        loop {
            tracing::debug!(len = msg.len(), "sending packet");
            client.send_to(msg, *proxy_address).await.unwrap();

            if let Some(rmsg) = self
                .maybe_timeout(PACKET_TIMEOUT.as_millis() as u64, server_rx.recv())
                .await
            {
                tracing::debug!(len = msg.len(), "received packet");
                match rmsg {
                    Some(rmsg) => {
                        assert_eq!(expected, rmsg);
                    }
                    None => {
                        panic!("got None message, was server_rx channel closed?");
                    }
                }
                break;
            }
            tracing::warn!(len = msg.len(), "packet timeout, probably dropped");

            if start.elapsed() > MAX_WAIT {
                panic!("unable to pass message through proxy after {MAX_WAIT:?}");
            }
        }
    }
}

#[macro_export]
macro_rules! filter_chain {
    ([$($kind:ident => $filter:expr,)*]) => {
        quilkin::filters::FilterChain::testing([
            $(
                quilkin::filters::FilterInstance::testing(quilkin::filters::$kind::testing($filter))
            ),*
        ])
    };
    ([$($kind:ident => $filter:expr),*]) => {
        quilkin::filters::FilterChain::testing([
            $(
                quilkin::filters::FilterInstance::testing(quilkin::filters::$kind::testing($filter))
            ),*
        ])
    };
}
