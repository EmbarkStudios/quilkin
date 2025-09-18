#![allow(clippy::unimplemented)]

use quilkin::{
    Config,
    collections::{BufferPool, PoolBuffer},
    net::TcpListener,
    signal::ShutdownTx,
    test::TestConfig,
};
pub use serde_json::json;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc;

#[cfg(target_os = "linux")]
pub mod xdp_util;

pub const MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
pub static BUFFER_POOL: once_cell::sync::Lazy<Arc<BufferPool>> =
    once_cell::sync::Lazy::new(|| Arc::new(BufferPool::default()));

#[inline]
pub fn alloc_buffer(data: impl AsRef<[u8]>) -> PoolBuffer {
    BUFFER_POOL.clone().alloc_slice(data.as_ref())
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

pub fn init_logging(level: Level, test_pkg: &'static str) -> DefaultGuard {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};
    let layer = tracing_subscriber::fmt::layer()
        .with_test_writer()
        .with_filter(tracing_subscriber::filter::LevelFilter::from_level(level))
        .with_filter(tracing_subscriber::EnvFilter::new(format!(
            "{test_pkg}=trace,qt=trace,quilkin=trace,xds=trace"
        )));
    let sub = tracing_subscriber::Registry::default().with(layer);
    let disp = tracing::dispatcher::Dispatch::new(sub);
    tracing::dispatcher::set_default(&disp)
}

#[macro_export]
macro_rules! trace_test {
    ($(#[$attr:meta])* $name:ident, $body:block) => {
        $(#[$attr])*
        #[tokio::test]
        async fn $name() {
            // Get the module name
            let fname = $crate::func_name!();
            let mname = fname.rsplit("::").nth(2).unwrap();

            let _guard = init_logging($crate::Level::DEBUG, mname);

            $body
        }
    };
}

pub struct ServerPailConfig {
    pub packet_size: u16,
    pub num_packets: Option<usize>,
}

impl Default for ServerPailConfig {
    fn default() -> Self {
        Self {
            packet_size: 1024,
            num_packets: None,
        }
    }
}

#[derive(Default)]
pub struct AdminPailConfig;

#[derive(Default)]
pub struct RelayPailConfig {
    pub config: Option<quilkin::test::TestConfig>,
}

#[derive(Default)]
pub struct ProxyPailConfig {
    pub config: Option<quilkin::test::TestConfig>,
}

#[derive(Default)]
pub struct ManagementPailConfig {}

#[derive(Default)]
pub struct AgentPailConfig {
    pub endpoints: Vec<(&'static str, &'static [&'static str])>,
    pub icao_code: quilkin::config::IcaoCode,
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
    /// The server socket's ephmeral port
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
                .mds_port(rp.mds_port);
            let (tx, rx) = quilkin::signal::channel();
            let sh = quilkin::signal::ShutdownHandler::new(tx.clone(), rx);
            let task = svc.spawn_services(&rp.config, sh).unwrap();

            rp.shutdown = tx;
            rp.task = Some(task);
        } else {
            unimplemented!();
        }
    }
}

impl Pail {
    pub fn construct(spc: SandboxPailConfig, pails: &Pails, td: &std::path::Path) -> Self {
        let pail = match spc.config {
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
                        let (size, _) = socket
                            .recv_from(&mut buf)
                            .await
                            .expect("failed to receive packet");
                        received += size;
                        let pstr = std::str::from_utf8(&buf[..size])
                            .expect("received non-utf8 string in packet")
                            .to_owned();

                        packet_tx.send(pstr).await.expect("packet receiver dropped");

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
                let xds_port = TcpListener::bind(None).unwrap().port();
                let mds_port = TcpListener::bind(None).unwrap().port();

                let path = td.join(spc.name);
                let mut tc = rpc.config.unwrap_or_default();
                tc.id = spc.name.into();
                tc.write_to_file(&path);

                let config_path = path.clone();

                let (shutdown, shutdown_rx) = quilkin::signal::channel();
                let pail_token = quilkin::signal::cancellation_token(shutdown_rx.clone());

                let providers = quilkin::Providers::default().fs().fs_path(path);
                let svc = quilkin::Service::default()
                    .xds()
                    .xds_port(xds_port)
                    .mds()
                    .mds_port(mds_port);

                let config = crate::Config::new_rc(
                    Some("test-relay".into()),
                    Default::default(),
                    &providers,
                    &svc,
                    pail_token,
                );

                *config.dyn_cfg.id.lock() = spc.name.into();
                let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let provider_task = providers.spawn_providers(
                    &config,
                    healthy.clone(),
                    None,
                    None,
                    shutdown_rx.clone(),
                );
                let task = svc
                    .spawn_services(
                        &config,
                        quilkin::signal::ShutdownHandler::new(shutdown.clone(), shutdown_rx),
                    )
                    .unwrap();

                Self::Relay(RelayPail {
                    xds_port,
                    mds_port,
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

                let relay_servers = spc
                    .dependencies
                    .iter()
                    .filter_map(|dname| {
                        let Pail::Relay(RelayPail { mds_port, .. }) = &pails[dname] else {
                            return None;
                        };
                        Some(
                            format!("http://localhost:{mds_port}")
                                .parse()
                                .expect("failed to parse endpoint"),
                        )
                    })
                    .collect::<Vec<_>>();

                let (shutdown, shutdown_rx) = quilkin::signal::channel();
                let pail_token = quilkin::signal::cancellation_token(shutdown_rx.clone());

                let port = quilkin::net::socket_port(
                    &quilkin::net::raw_socket_with_reuse(0).expect("failed to bind qcmp socket"),
                );

                let config_path = path.clone();
                let svc = quilkin::Service::default().qcmp().qcmp_port(port);
                let providers = quilkin::Providers::default()
                    .fs()
                    .fs_path(path)
                    .grpc_push_endpoints(relay_servers);

                let config = crate::Config::new_rc(
                    Some("test-agent".into()),
                    apc.icao_code,
                    &providers,
                    &svc,
                    pail_token,
                );

                *config.dyn_cfg.id.lock() = spc.name.into();
                let acfg = config.clone();
                let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let provider_task = providers.spawn_providers(
                    &config,
                    healthy.clone(),
                    None,
                    None,
                    shutdown_rx.clone(),
                );
                let task = svc
                    .spawn_services(
                        &config,
                        quilkin::signal::ShutdownHandler::new(shutdown.clone(), shutdown_rx),
                    )
                    .unwrap();

                Self::Agent(AgentPail {
                    qcmp_port: port,
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
                let socket = quilkin::net::raw_socket_with_reuse(0).expect("failed to bind socket");
                let qcmp =
                    quilkin::net::raw_socket_with_reuse(0).expect("failed to bind qcmp socket");
                let qcmp_port = quilkin::net::socket_port(&qcmp);
                let phoenix_port = TcpListener::bind(None)
                    .expect("failed to bind phoenix socket")
                    .port();

                let port = quilkin::net::socket_port(&socket);

                let management_servers: Vec<_> = spc
                    .dependencies
                    .iter()
                    .filter_map(|dname| {
                        let Pail::Relay(RelayPail { xds_port, .. }) = &pails[dname] else {
                            return None;
                        };
                        Some(
                            format!("http://localhost:{xds_port}")
                                .parse()
                                .expect("failed to parse endpoint"),
                        )
                    })
                    .collect();

                let providers =
                    quilkin::Providers::default().grpc_pull_endpoints(management_servers);

                let svc = quilkin::Service::default()
                    .udp()
                    .udp_port(port)
                    .qcmp()
                    .qcmp_port(qcmp_port)
                    .phoenix()
                    .phoenix_port(phoenix_port)
                    .termination_timeout(None);

                let (shutdown, shutdown_rx) = quilkin::signal::channel();
                let pail_token = quilkin::signal::cancellation_token(shutdown_rx.clone());

                let config = crate::Config::new_rc(
                    Some("test-proxy".into()),
                    Default::default(),
                    &Default::default(),
                    &svc,
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
                let provider_task = providers.spawn_providers(
                    &config,
                    healthy.clone(),
                    None,
                    Some(rttx),
                    shutdown_rx.clone(),
                );
                let task = svc
                    .spawn_services(
                        &config,
                        quilkin::signal::ShutdownHandler::new(shutdown.clone(), shutdown_rx),
                    )
                    .unwrap();

                Self::Proxy(ProxyPail {
                    port,
                    qcmp_port,
                    phoenix_port,
                    shutdown,
                    task: Some(task),
                    provider_task,
                    healthy,
                    config,
                    delta_applies: Some(rtrx),
                })
            }
        };
        pail
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
            let pail = Pail::construct(pc, &pails, td.path());

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
            client.send_to(msg, &proxy_address).await.unwrap();

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
