use eyre::ContextCompat;
use std::sync::Arc;

use crate::{components::proxy::SessionPool, config::Config, signal::ShutdownHandler};

#[derive(Debug, clap::Parser)]
#[command(next_help_heading = "Service Options")]
pub struct Service {
    /// The identifier for an instance.
    #[arg(long = "service.id", env = "QUILKIN_SERVICE_ID")]
    pub id: Option<String>,
    /// Whether to serve mDS requests.
    #[arg(
        long = "service.mds",
        env = "QUILKIN_SERVICE_MDS",
        default_value_t = false
    )]
    mds_enabled: bool,
    /// The TCP port to listen to serve xDS requests.
    #[clap(
        long = "service.mds.port",
        env = "QUILKIN_SERVICE_MDS_PORT",
        default_value_t = 7900
    )]
    mds_port: u16,
    /// Whether to serve UDP requests.
    #[arg(
        long = "service.phoenix",
        env = "QUILKIN_SERVICE_PHOENIX",
        default_value_t = false
    )]
    phoenix_enabled: bool,
    /// The UDP port to listen for UDP packets.
    #[clap(
        long = "service.phoenix.port",
        env = "QUILKIN_SERVICE_PHOENIX_PORT",
        default_value_t = 7600
    )]
    phoenix_port: u16,
    /// Whether to serve UDP requests.
    #[arg(
        long = "service.qcmp",
        env = "QUILKIN_SERVICE_QCMP",
        default_value_t = false
    )]
    qcmp_enabled: bool,
    /// The UDP port to listen for UDP packets.
    #[clap(
        long = "service.qcmp.port",
        env = "QUILKIN_SERVICE_QCMP_PORT",
        default_value_t = 7600
    )]
    qcmp_port: u16,
    /// Whether to serve UDP requests.
    #[arg(
        long = "service.udp",
        env = "QUILKIN_SERVICE_UDP",
        default_value_t = false
    )]
    udp_enabled: bool,
    /// The UDP port to listen for UDP packets.
    #[clap(
        long = "service.udp.port",
        env = "QUILKIN_SERVICE_UDP_PORT",
        default_value_t = 7777
    )]
    udp_port: u16,
    #[clap(flatten)]
    pub xdp: XdpOptions,
    /// Amount of UDP workers to run.
    #[clap(long = "service.udp.workers", env = "QUILKIN_SERVICE_UDP_WORKERS", default_value_t = std::num::NonZeroUsize::new(num_cpus::get()).unwrap())]
    pub udp_workers: std::num::NonZeroUsize,
    /// Whether to serve xDS requests.
    #[arg(
        long = "service.xds",
        env = "QUILKIN_SERVICE_XDS",
        default_value_t = false
    )]
    xds_enabled: bool,
    /// The TCP port to listen to serve xDS requests.
    #[clap(
        long = "service.xds.port",
        env = "QUILKIN_SERVICE_XDS_PORT",
        default_value_t = 7800
    )]
    xds_port: u16,
    /// Whether to serve xDS and/or mDS requests.
    #[arg(
        long = "service.grpc",
        env = "QUILKIN_SERVICE_GRPC",
        default_value_t = false
    )]
    grpc_enabled: bool,
    /// A PEM encoded certificate, if supplied, applies to the mds and xds service(s)
    #[clap(
        long = "service.tls.cert",
        env = "QUILKIN_SERVICE_TLS_CERT",
        requires("tls_key")
    )]
    tls_cert: Option<Vec<u8>>,
    /// The private key for the cert
    #[clap(
        long = "service.tls.key",
        env = "QUILKIN_SERVICE_TLS_KEY",
        requires("tls_cert")
    )]
    tls_key: Option<Vec<u8>>,
    /// Path to a PEM encoded certificate, if supplied, applies to the mds and xds service(s)
    #[clap(
        long = "service.tls.cert-path",
        env = "QUILKIN_SERVICE_TLS_CERT_PATH",
        requires("tls_key_path"),
        conflicts_with("tls_cert")
    )]
    tls_cert_path: Option<std::path::PathBuf>,
    /// Path to the private key for the cert
    #[clap(
        long = "service.tls.key-path",
        env = "QUILKIN_SERVICE_TLS_KEY_PATH",
        requires("tls_cert_path"),
        conflicts_with("tls_key")
    )]
    tls_key_path: Option<std::path::PathBuf>,
    #[clap(long = "termination-timeout")]
    termination_timeout: Option<crate::cli::Duration>,
    #[clap(skip)]
    testing: bool,
}

pub type Finalizer = Box<dyn FnOnce() + Send>;

impl Default for Service {
    fn default() -> Self {
        Self {
            id: None,
            mds_enabled: <_>::default(),
            mds_port: 7900,
            phoenix_enabled: <_>::default(),
            phoenix_port: 7600,
            qcmp_enabled: <_>::default(),
            qcmp_port: 7600,
            udp_enabled: <_>::default(),
            udp_port: 7777,
            udp_workers: std::num::NonZeroUsize::new(num_cpus::get()).unwrap(),
            xds_enabled: <_>::default(),
            xds_port: 7800,
            grpc_enabled: false,
            xdp: <_>::default(),
            tls_cert: None,
            tls_key: None,
            tls_cert_path: None,
            tls_key_path: None,
            termination_timeout: None,
            testing: false,
        }
    }
}

impl Service {
    pub fn builder() -> Self {
        Self::default()
    }

    /// Enables the UDP service.
    pub fn udp(mut self) -> Self {
        self.udp_enabled = true;
        self
    }

    /// Sets the UDP service port.
    pub fn udp_port(mut self, port: u16) -> Self {
        self.udp_port = port;
        self
    }

    /// Enables the QCMP service.
    pub fn qcmp(mut self) -> Self {
        self.qcmp_enabled = true;
        self
    }

    /// Sets the UDP service port.
    pub fn qcmp_port(mut self, port: u16) -> Self {
        self.qcmp_port = port;
        self
    }

    /// Enables the mDS service.
    pub fn mds(mut self) -> Self {
        self.mds_enabled = true;
        self
    }

    /// Sets the mDS service port.
    pub fn mds_port(mut self, port: u16) -> Self {
        self.mds_port = port;
        self
    }

    /// Enables the Phoenix service.
    pub fn phoenix(mut self) -> Self {
        self.phoenix_enabled = true;
        self
    }

    /// Sets the Phoenix service port.
    pub fn phoenix_port(mut self, port: u16) -> Self {
        self.phoenix_port = port;
        self
    }

    /// Enables the xDS service.
    pub fn xds(mut self) -> Self {
        self.xds_enabled = true;
        self
    }

    /// Sets the xDS service port.
    pub fn xds_port(mut self, port: u16) -> Self {
        self.xds_port = port;
        self
    }

    pub fn grpc(mut self) -> Self {
        self.mds_enabled = true;
        self.xds_enabled = true;
        self.grpc_enabled = true;
        self
    }

    pub fn xdp(mut self, xdp_opts: XdpOptions) -> Self {
        self.xdp = xdp_opts;
        self
    }

    pub fn testing(mut self) -> Self {
        self.testing = true;
        self
    }

    /// Sets the xDS service port.
    pub fn any_service_enabled(&self) -> bool {
        self.udp_enabled
            || self.qcmp_enabled
            || self.phoenix_enabled
            || self.xds_enabled
            || self.mds_enabled
            || self.grpc_enabled
    }

    /// Adds the required typemap entries to the config depending on what services are enabled
    pub fn init_config(&self, config: &mut Config) {
        use crate::config::{self, insert_default};

        if self.udp_enabled || self.xds_enabled || self.mds_enabled {
            insert_default::<crate::filters::FilterChain>(&mut config.dyn_cfg.typemap);
            insert_default::<config::DatacenterMap>(&mut config.dyn_cfg.typemap);
        }

        if self.qcmp_enabled {
            config
                .dyn_cfg
                .typemap
                .insert::<config::qcmp::QcmpPort>(config::qcmp::QcmpPort::new(self.qcmp_port));
        }

        insert_default::<crate::net::ClusterMap>(&mut config.dyn_cfg.typemap);
    }

    pub fn termination_timeout(mut self, timeout: Option<crate::cli::Duration>) -> Self {
        self.termination_timeout = timeout;
        self
    }

    fn tls_identity(&self) -> crate::Result<Option<quilkin_xds::server::TlsIdentity>> {
        if let Some((cert, key)) = self.tls_cert.as_ref().zip(self.tls_key.as_ref()) {
            Ok(Some(quilkin_xds::server::TlsIdentity::from_raw(cert, key)))
        } else if let Some((certp, keyp)) =
            self.tls_cert_path.as_ref().zip(self.tls_key_path.as_ref())
        {
            Ok(Some(quilkin_xds::server::TlsIdentity::from_files(
                certp, keyp,
            )?))
        } else {
            Ok(None)
        }
    }

    /// The main entrypoint for listening network servers. When called will
    /// spawn any and all enabled services, if successful returning a future
    /// that can be await to wait on services to be cancelled.
    pub fn spawn_services(
        mut self,
        config: &Arc<Config>,
        mut shutdown: ShutdownHandler,
    ) -> crate::Result<tokio::task::JoinHandle<(ShutdownHandler, crate::Result<()>)>> {
        {
            let shutdown = &mut shutdown;
            self.publish_mds(config, shutdown)?;
            self.publish_phoenix(config, shutdown)?;
            // We need to call this before qcmp since if we use XDP we handle QCMP
            // internally without a separate task
            self.publish_udp(config, shutdown)?;
            self.publish_qcmp(config, shutdown)?;
            self.publish_xds(config, shutdown)?;
        }

        Ok(tokio::spawn(async move {
            let (tx, rx, results) = shutdown.await_any_then_shutdown().await;

            let mut errors = 0;
            for (task, res) in &results {
                if let Err(error) = res {
                    tracing::error!(task, %error, "service task failed");
                    errors += 1;
                }
            }

            let res = match errors {
                0 => Ok(()),
                1 => Err(results.into_iter().find_map(|(_, res)| res.err()).unwrap()),
                _ => {
                    use std::fmt::Write as _;
                    let mut err_str = String::new();
                    writeln!(&mut err_str, "encountered {errors} errors:").unwrap();

                    for (which, res) in results {
                        if let Err(error) = res {
                            writeln!(&mut err_str, "  {which}: {error:#}").unwrap();
                        }
                    }

                    Err(eyre::Report::msg(err_str))
                }
            };

            (ShutdownHandler::new(tx, rx), res)
        }))
    }

    /// Spawns an QCMP server if enabled, otherwise returns a future which never completes.
    fn publish_phoenix(
        &self,
        config: &Arc<Config>,
        shutdown: &mut ShutdownHandler,
    ) -> crate::Result<()> {
        if !self.phoenix_enabled {
            return Ok(());
        }

        let Some(datacenters) = config.dyn_cfg.datacenters() else {
            tracing::info!(
                "not starting phoenix service even though it was requested, datacenters were not configured"
            );
            return Ok(());
        };

        tracing::info!(port=%self.qcmp_port, "starting phoenix service");
        let phoenix = {
            let mut builder =
                crate::net::phoenix::Phoenix::builder(crate::codec::qcmp::QcmpTransceiver::new()?);
            if let Some(informer) = config.bad_node_informer() {
                builder = builder.inform_bad_nodes(informer);
            }
            builder.build()
        };
        let phoenix_listener = crate::net::TcpListener::bind(Some(self.phoenix_port))?;
        let finalizer = crate::net::phoenix::spawn(
            phoenix_listener,
            datacenters.clone(),
            phoenix,
            shutdown.shutdown_rx(),
        )?;

        let finished = shutdown.push("phoenix");
        let mut srx = shutdown.shutdown_rx();
        tokio::spawn(async move {
            let _ = srx.changed().await;

            tokio::task::spawn_blocking(|| {
                finalizer();
            });

            drop(finished.send(Ok(())));
        });

        Ok(())
    }

    /// Spawns an QCMP server if enabled, otherwise returns a future which never completes.
    fn publish_qcmp(&self, config: &Config, shutdown: &mut ShutdownHandler) -> crate::Result<()> {
        if !self.qcmp_enabled {
            return Ok(());
        }

        let qcmp_port = config
            .dyn_cfg
            .qcmp_port()
            .context("QCMP was enabled, but QCMP port was not inserted into typemap")?;

        qcmp_port.store(self.qcmp_port);

        tracing::info!(port=%self.qcmp_port, "starting qcmp service");
        let qcmp = crate::net::raw_socket_with_reuse(self.qcmp_port)?;

        crate::codec::qcmp::spawn(qcmp, qcmp_port.subscribe(), shutdown)?;

        Ok(())
    }

    /// Spawns an xDS server if enabled, otherwise returns a future which never completes.
    fn publish_xds(
        &self,
        config: &Arc<Config>,
        shutdown: &mut ShutdownHandler,
    ) -> crate::Result<()> {
        if !self.xds_enabled && !self.grpc_enabled {
            return Ok(());
        }

        let listener = crate::net::TcpListener::bind(Some(self.xds_port))?;

        let finished = shutdown.push("xds");
        let srx = shutdown.shutdown_rx();

        let xds_server = crate::net::xds::server::ControlPlane::from_arc(
            config.clone(),
            crate::components::admin::IDLE_REQUEST_INTERVAL,
            srx,
        )
        .management_server(listener, self.tls_identity()?)?;

        tokio::spawn(async move {
            let res = xds_server.await;
            drop(finished.send(res));
        });

        Ok(())
    }

    /// Spawns an xDS server if enabled, otherwise returns a future which never completes.
    fn publish_mds(
        &self,
        config: &Arc<Config>,
        shutdown: &mut ShutdownHandler,
    ) -> crate::Result<()> {
        if !self.mds_enabled && !self.grpc_enabled {
            return Ok(());
        }

        tracing::info!(port=%self.mds_port, "starting mds service");
        let listener = crate::net::TcpListener::bind(Some(self.mds_port))?;

        let finished = shutdown.push("mds");
        let srx = shutdown.shutdown_rx();

        let mds_server = crate::net::xds::server::ControlPlane::from_arc(
            config.clone(),
            crate::components::admin::IDLE_REQUEST_INTERVAL,
            srx,
        )
        .relay_server(listener, self.tls_identity()?)?;

        tokio::spawn(async move {
            let res = mds_server.await;
            drop(finished.send(res));
        });

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn publish_udp(
        &mut self,
        config: &Arc<Config>,
        shutdown: &mut ShutdownHandler,
    ) -> crate::Result<()> {
        if !self.udp_enabled && !self.qcmp_enabled {
            return Ok(());
        }

        tracing::info!(port=%self.udp_port, "starting udp service");

        #[cfg(target_os = "linux")]
        {
            match self.spawn_xdp(config.clone(), self.xdp.force_xdp) {
                Ok(xdp) => {
                    if let Some(xdp) = xdp {
                        // Disable this so that we don't create a separate user-space
                        // QCMP service since we are handling QCMP messsages in XDP
                        self.qcmp_enabled = false;

                        let finished = shutdown.push("xdp");
                        let mut srx = shutdown.shutdown_rx();
                        tokio::spawn(async move {
                            drop(srx.changed().await);

                            tokio::task::block_in_place(|| {
                                xdp();
                            });

                            drop(finished.send(Ok(())));
                        });

                        return Ok(());
                    } else if self.xdp.force_xdp {
                        eyre::bail!("XDP was forced on, but failed to initialize");
                    }
                }
                Err(err) => {
                    if self.xdp.force_xdp {
                        return Err(err);
                    }

                    tracing::warn!(
                        ?err,
                        "failed to spawn XDP I/O loop, falling back to io-uring"
                    );
                }
            }
        }

        if !self.udp_enabled {
            return Ok(());
        }

        self.spawn_user_space_router(config.clone(), shutdown)
    }

    /// Launches the user space implementation of the packet router using
    /// sockets. This implementation uses a pool of buffers and sockets to
    /// manage UDP sessions and sockets. On Linux this will use io-uring, where
    /// as it will use epoll interfaces on non-Linux platforms.
    #[allow(clippy::type_complexity)]
    pub fn spawn_user_space_router(
        &self,
        config: Arc<Config>,
        shutdown: &mut ShutdownHandler,
    ) -> crate::Result<()> {
        // If we're on linux, we're using io-uring, but we're probably running in a container
        // and may not be allowed to call io-uring related syscalls due to seccomp
        // profiles, so do a quick check here to validate that we can call io_uring_setup
        // https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html
        #[cfg(target_os = "linux")]
        {
            if let Err(err) = io_uring::IoUring::new(2) {
                fn in_container() -> bool {
                    let sched = match std::fs::read_to_string("/proc/1/sched") {
                        Ok(s) => s,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "unable to read /proc/1/sched to determine if quilkin is in a container"
                            );
                            return false;
                        }
                    };
                    let Some(line) = sched.lines().next() else {
                        tracing::warn!("/proc/1/sched was empty");
                        return false;
                    };
                    let Some(proc) = line.split(' ').next() else {
                        tracing::warn!("first line of /proc/1/sched was empty");
                        return false;
                    };
                    proc != "init" && proc != "systemd"
                }

                if err.kind() == std::io::ErrorKind::PermissionDenied && in_container() {
                    eyre::bail!(
                        "failed to call `io_uring_setup` due to EPERM ({err}), quilkin seems to be running inside a container meaning this is likely due to the seccomp profile not allowing the syscall"
                    );
                } else {
                    eyre::bail!("failed to call `io_uring_setup` due to {err}");
                }
            }
        }

        let socket = crate::net::raw_socket_with_reuse(self.udp_port)?;
        let workers = self.udp_workers.get();
        let buffer_pool = Arc::new(crate::collections::BufferPool::new(workers, 2 * 1024));

        let mut worker_sends = Vec::with_capacity(workers);
        let mut session_sends = Vec::with_capacity(workers);
        for _ in 0..workers {
            let queue = crate::net::queue(15)?;
            session_sends.push(queue.0.clone());
            worker_sends.push(queue);
        }

        let cached_filters = config
            .dyn_cfg
            .cached_filter_chain()
            .context("a cached FilterChain should have been configured")?;

        let sessions = SessionPool::new(session_sends, buffer_pool.clone(), cached_filters);
        crate::net::packet::spawn_receivers(config, socket, worker_sends, &sessions, buffer_pool)?;

        let finished = shutdown.push("udp");
        let mut srx = shutdown.shutdown_rx();
        let testing = self.testing;
        let termination_timeout = self.termination_timeout;

        tokio::spawn(async move {
            drop(srx.changed().await);

            if testing {
                drop(finished.send(Ok(())));
                return;
            }

            tracing::info!(sessions = %sessions.sessions().len(), "waiting for active sessions to expire");
            let start = std::time::Instant::now();

            let mut sessions_check = tokio::time::interval(std::time::Duration::from_millis(100));

            loop {
                sessions_check.tick().await;
                let elapsed = start.elapsed();
                if let Some(tt) = &termination_timeout {
                    if elapsed > **tt {
                        tracing::info!(
                            ?elapsed,
                            "termination timeout was reached before all sessions expired"
                        );
                        break;
                    }
                }

                if sessions.sessions().is_empty() {
                    tracing::info!(shutdown_duration = ?elapsed, "all sessions expired");
                    break;
                }
            }

            drop(finished.send(Ok(())));
        });

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn spawn_xdp(&self, config: Arc<Config>, force_xdp: bool) -> eyre::Result<Option<Finalizer>> {
        use crate::net::io::nic::xdp;
        use eyre::{Context as _, ContextCompat as _};

        // TODO: remove this once it's been more stabilized
        if !force_xdp {
            return Ok(None);
        }

        let filters = config
            .dyn_cfg
            .cached_filter_chain()
            .context("XDP requires a filter chain")?;
        let clusters = config
            .dyn_cfg
            .clusters()
            .context("XDP requires a cluster map")?
            .clone();

        let config = crate::net::io::nic::xdp::process::ConfigState { filters, clusters };

        let udp_port = if self.udp_enabled { self.udp_port } else { 0 };
        let qcmp_port = if self.qcmp_enabled { self.qcmp_port } else { 0 };

        tracing::info!(udp_port, qcmp_port, "setting up xdp module");
        let workers = xdp::setup_xdp_io(xdp::XdpConfig {
            nic: self
                .xdp
                .network_interface
                .as_deref()
                .map_or(xdp::NicConfig::Default, xdp::NicConfig::Name),
            external_port: udp_port,
            qcmp_port,
            maximum_packet_memory: self.xdp.maximum_memory,
            require_zero_copy: self.xdp.force_zerocopy,
            require_tx_checksum: self.xdp.force_tx_checksum_offload,
        })
        .context("failed to setup XDP")?;

        let io_loop = xdp::spawn(workers, config).context("failed to spawn XDP I/O loop")?;
        Ok(Some(Box::new(move || {
            io_loop.shutdown(true);
        })))
    }
}

/// XDP (eXpress Data Path) options
#[derive(clap::Args, Clone, Debug)]
pub struct XdpOptions {
    /// The name of the network interface to bind the XDP socket(s) to.
    ///
    /// If not specified quilkin will attempt to determine the most appropriate
    /// network interface to use. Quilkin will exit with an error if the network
    /// interface does not exist, or a suitable default cannot be determined.
    #[clap(
        long = "service.udp.xdp.network-interface",
        env = "QUILKIN_SERVICE_UDP_XDP_NETWORK_INTERFACE"
    )]
    pub network_interface: Option<String>,
    /// Forces the use of XDP.
    ///
    /// If XDP is not available on the chosen NIC, Quilkin exits with an error.
    /// If false, io-uring will be used as the fallback implementation.
    #[clap(long = "service.udp.xdp", env = "QUILKIN_SERVICE_UDP_XDP")]
    pub force_xdp: bool,
    /// Forces the use of [`XDP_ZEROCOPY`](https://www.kernel.org/doc/html/latest/networking/af_xdp.html#xdp-copy-and-xdp-zerocopy-bind-flags)
    ///
    /// If zero copy is not available on the chosen NIC, Quilkin exits with an error
    #[clap(
        long = "service.udp.xdp.zerocopy",
        env = "QUILKIN_SERVICE_UDP_XDP_ZEROCOPY"
    )]
    pub force_zerocopy: bool,
    /// Forces the use of [TX checksum offload](https://docs.kernel.org/6.8/networking/xsk-tx-metadata.html)
    ///
    /// TX checksum offload is an optional feature allowing the data portion of
    /// a packet to have its internet checksum calculation offloaded to the NIC,
    /// as otherwise this is done in software
    #[clap(long = "service.udp.xdp.tco", env = "QUILKIN_SERVICE_UDP_XDP_TCO")]
    pub force_tx_checksum_offload: bool,
    /// The maximum amount of memory mapped for packet buffers, in bytes
    ///
    /// If not specified, this defaults to 4MiB (2k allocated packets of 2k each at a time)
    /// per NIC queue, ie 128MiB on a 32 queue NIC
    #[clap(
        long = "service.udp.xdp.memory-limit",
        env = "QUILKIN_SERVICE_UDP_XDP_MEMORY_LIMIT"
    )]
    pub maximum_memory: Option<u64>,
}

#[allow(clippy::derivable_impls)]
impl Default for XdpOptions {
    fn default() -> Self {
        Self {
            network_interface: None,
            force_xdp: false,
            force_zerocopy: false,
            force_tx_checksum_offload: false,
            maximum_memory: None,
        }
    }
}
