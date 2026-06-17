use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

// Packet header: 8 bytes seq (u64 LE) + 8 bytes send_ns (u64 LE)
const HDR: usize = 16;

pub struct LoadParams {
    pub packets: u64,
    pub qps: u64,
    pub payload_size: usize,
    pub warmup: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ScenarioResult {
    pub name: String,
    pub results: Results,
}

// Fortio-compatible field names; the compare subcommand reads these directly.
#[derive(Serialize, Deserialize)]
pub struct Results {
    #[serde(rename = "ActualQPS")]
    pub actual_qps: f64,
    #[serde(rename = "BytesSent")]
    pub bytes_sent: u64,
    #[serde(rename = "BytesReceived")]
    pub bytes_received: u64,
    #[serde(rename = "DurationHistogram")]
    pub duration_histogram: DurationHistogram,
    #[serde(rename = "Sent")]
    pub sent: u64,
    #[serde(rename = "Received")]
    pub received: u64,
}

#[derive(Serialize, Deserialize)]
pub struct DurationHistogram {
    #[serde(rename = "Percentiles")]
    pub percentiles: Vec<Percentile>,
}

#[derive(Serialize, Deserialize)]
pub struct Percentile {
    #[serde(rename = "Percentile")]
    pub percentile: f64,
    #[serde(rename = "Value")]
    pub value: f64, // seconds
}

impl Results {
    pub fn loss_pct(&self) -> f64 {
        if self.sent == 0 {
            return 0.0;
        }
        (1.0 - self.received as f64 / self.sent as f64) * 100.0
    }

    pub fn percentile_ms(&self, p: f64) -> Option<f64> {
        self.duration_histogram
            .percentiles
            .iter()
            .find(|e| e.percentile >= p)
            .map(|e| e.value * 1000.0)
    }
}

pub async fn run_passthrough(
    backend: quilkin::net::io::UdpBackend,
    params: &LoadParams,
) -> eyre::Result<Results> {
    #[cfg(target_os = "linux")]
    if matches!(backend, quilkin::net::io::UdpBackend::Kernel) {
        return kernel::run(params).await;
    }

    let echo = EchoServer::spawn(Ipv4Addr::LOCALHOST).await;
    let proxy = ProxyHandle::spawn(echo.port(), backend).await?;
    run_with_warmup(proxy.addr(), params).await
}

async fn run_with_warmup(proxy_addr: SocketAddr, params: &LoadParams) -> eyre::Result<Results> {
    if params.warmup > 0 {
        run_load(
            proxy_addr,
            params.warmup,
            (params.qps / 10).max(1),
            params.payload_size,
        )
        .await?;
    }
    run_load(proxy_addr, params.packets, params.qps, params.payload_size).await
}

struct EchoServer {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl EchoServer {
    async fn spawn(bind: Ipv4Addr) -> Self {
        let socket = UdpSocket::bind(SocketAddr::from((bind, 0))).await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                let Ok((len, addr)) = socket.recv_from(&mut buf).await else {
                    break;
                };
                drop(socket.send_to(&buf[..len], addr).await);
            }
        });
        Self { port, task }
    }

    fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for EchoServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ProxyHandle {
    port: u16,
    _shutdown: quilkin::signal::ShutdownTx,
}

impl ProxyHandle {
    async fn spawn(
        upstream_port: u16,
        backend: quilkin::net::io::UdpBackend,
    ) -> eyre::Result<Self> {
        let providers = quilkin::Providers::default();
        let mut svc = quilkin::Service::builder()
            .udp()
            .udp_port(0)
            .udp_backend(backend)
            .testing();

        let config = Arc::new(quilkin::Config::new(
            None,
            Default::default(),
            &providers,
            &mut svc,
        ));
        let upstream = SocketAddr::from((Ipv4Addr::LOCALHOST, upstream_port));
        config.dyn_cfg.clusters().unwrap().modify(|clusters| {
            clusters.insert(
                None,
                None,
                std::iter::once(quilkin::net::Endpoint::new(upstream.into())).collect(),
            );
        });

        let (tx, rx) = quilkin::signal::channel();
        let shutdown = quilkin::signal::ShutdownHandler::new(tx, rx);
        let _shutdown = shutdown.shutdown_tx();
        let (_task, ports) = svc.spawn_services(&config, shutdown).await?;
        Ok(Self {
            port: ports.udp.expect("UDP port not allocated"),
            _shutdown,
        })
    }

    fn addr(&self) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.port))
    }
}

fn encode_packet(pkt: &mut [u8], seq: u64, send_ns: u64) {
    pkt[..8].copy_from_slice(&seq.to_le_bytes());
    pkt[8..16].copy_from_slice(&send_ns.to_le_bytes());
}

fn decode_rtt(buf: &[u8], now_ns: u64) -> Option<Duration> {
    if buf.len() < HDR {
        return None;
    }
    let send_ns = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    Some(Duration::from_nanos(now_ns.saturating_sub(send_ns)))
}

async fn receive_rtts(
    sock: Arc<UdpSocket>,
    epoch: Instant,
    done: tokio::sync::oneshot::Receiver<()>,
    capacity: usize,
) -> Vec<Duration> {
    let mut rtts = Vec::with_capacity(capacity);
    let mut buf = vec![0u8; 65535];
    let mut done = done;

    loop {
        tokio::select! {
            biased;
            result = sock.recv(&mut buf) => match result {
                Ok(len) => {
                    if let Some(rtt) = decode_rtt(&buf[..len], epoch.elapsed().as_nanos() as u64) {
                        rtts.push(rtt);
                    }
                }
                Err(_) => break,
            },
            _ = &mut done => {
                let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
                while let Ok(Ok(len)) = tokio::time::timeout_at(deadline, sock.recv(&mut buf)).await {
                    if let Some(rtt) = decode_rtt(&buf[..len], epoch.elapsed().as_nanos() as u64) {
                        rtts.push(rtt);
                    }
                }
                break;
            }
        }
    }
    rtts
}

async fn send_at_rate(
    sock: &UdpSocket,
    epoch: Instant,
    packets: u64,
    qps: u64,
    pkt_size: usize,
) -> Duration {
    let interval = Duration::from_nanos(1_000_000_000 / qps);
    let mut next_send = tokio::time::Instant::now();
    let start = Instant::now();
    let mut pkt = vec![0u8; pkt_size];

    for seq in 0u64..packets {
        tokio::time::sleep_until(next_send).await;
        encode_packet(&mut pkt, seq, epoch.elapsed().as_nanos() as u64);
        drop(sock.send(&pkt).await);
        next_send += interval;
    }
    start.elapsed()
}

async fn run_load(
    proxy_addr: SocketAddr,
    packets: u64,
    qps: u64,
    payload_size: usize,
) -> eyre::Result<Results> {
    let pkt_size = payload_size.max(HDR);
    let qps = qps.max(1);

    let sock = Arc::new(UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).await?);
    sock.connect(proxy_addr).await?;

    let epoch = Instant::now();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let receiver = tokio::spawn(receive_rtts(sock.clone(), epoch, done_rx, packets as usize));
    let elapsed = send_at_rate(&sock, epoch, packets, qps, pkt_size).await;

    let _ = done_tx.send(());
    let rtts = receiver.await?;
    let received = rtts.len() as u64;

    Ok(Results {
        actual_qps: packets as f64 / elapsed.as_secs_f64(),
        bytes_sent: packets * pkt_size as u64,
        bytes_received: received * pkt_size as u64,
        duration_histogram: compute_histogram(rtts),
        sent: packets,
        received,
    })
}

const PERCENTILES: &[f64] = &[50.0, 75.0, 90.0, 99.0, 99.9];

fn compute_histogram(mut rtts: Vec<Duration>) -> DurationHistogram {
    rtts.sort_unstable();
    let n = rtts.len();
    DurationHistogram {
        percentiles: if n == 0 {
            vec![]
        } else {
            PERCENTILES
                .iter()
                .map(|&p| {
                    let idx = (((p / 100.0) * n as f64) as usize).min(n - 1);
                    Percentile {
                        percentile: p,
                        value: rtts[idx].as_secs_f64(),
                    }
                })
                .collect()
        },
    }
}

// XDP attaches to a real NIC; loopback bypasses it. For the kernel backend we
// set up a veth pair so traffic flows through a real (virtual) interface:
//
//   default ns (client + echo): veth-ql-ext  10.100.0.1/24
//   ql-loadtest ns (quilkin):   veth-ql-int  10.100.0.2/24
//
// Requires CAP_NET_ADMIN (root).
#[cfg(target_os = "linux")]
mod kernel {
    use super::*;

    const NETNS: &str = "ql-loadtest";
    const EXT_IFACE: &str = "veth-ql-ext";
    const INT_IFACE: &str = "veth-ql-int";
    const EXT_IP: Ipv4Addr = Ipv4Addr::new(10, 100, 0, 1);
    const INT_IP: Ipv4Addr = Ipv4Addr::new(10, 100, 0, 2);
    const PROXY_PORT: u16 = 7777; // XDP does not support ephemeral ports

    struct VethSetup;

    impl VethSetup {
        fn create() -> eyre::Result<Self> {
            // Ignore errors — cleaning up leftover state from a prior run.
            drop(ip(&["netns", "del", NETNS]));
            drop(ip(&["link", "del", EXT_IFACE]));

            ip(&["netns", "add", NETNS])?;
            ip(&[
                "link", "add", EXT_IFACE, "type", "veth", "peer", "name", INT_IFACE,
            ])?;
            ip(&["link", "set", INT_IFACE, "netns", NETNS])?;
            ip(&["addr", "add", "10.100.0.1/24", "dev", EXT_IFACE])?;
            ip(&[
                "-n",
                NETNS,
                "addr",
                "add",
                "10.100.0.2/24",
                "dev",
                INT_IFACE,
            ])?;
            ip(&["link", "set", EXT_IFACE, "up"])?;
            ip(&["-n", NETNS, "link", "set", "lo", "up"])?;
            ip(&["-n", NETNS, "link", "set", INT_IFACE, "up"])?;
            // Newer kernels require an XDP program on both veth ends.
            ip(&[
                "link",
                "set",
                EXT_IFACE,
                "xdpgeneric",
                "obj",
                &dummy_bin()?,
                "sec",
                "xdp",
            ])?;
            Ok(Self)
        }
    }

    impl Drop for VethSetup {
        fn drop(&mut self) {
            drop(ip(&["link", "del", EXT_IFACE]));
            drop(ip(&["netns", "del", NETNS]));
        }
    }

    pub async fn run(params: &LoadParams) -> eyre::Result<Results> {
        let _veth = VethSetup::create()?;
        let echo = EchoServer::spawn(Ipv4Addr::UNSPECIFIED).await;

        let mut proc = std::process::Command::new("ip")
            .args(["netns", "exec", NETNS])
            .arg(quilkin_bin()?)
            .args([
                "--service.udp",
                &format!("--service.udp.port={PROXY_PORT}"),
                "--service.udp.backend=kernel",
                &format!("--service.udp.xdp.network-interface={INT_IFACE}"),
                &format!("--provider.static.endpoints={EXT_IP}:{}", echo.port()),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        tokio::time::sleep(Duration::from_secs(2)).await; // wait for XDP attach

        let result = run_with_warmup(SocketAddr::from((INT_IP, PROXY_PORT)), params).await;
        drop(proc.kill());
        result
    }

    fn ip(args: &[&str]) -> eyre::Result<()> {
        let st = std::process::Command::new("ip").args(args).status()?;
        eyre::ensure!(st.success(), "`ip {}` failed: {st}", args.join(" "));
        Ok(())
    }

    fn quilkin_bin() -> eyre::Result<std::path::PathBuf> {
        let path = std::env::current_exe()?.with_file_name("quilkin");
        eyre::ensure!(
            path.exists(),
            "quilkin not found at {path:?}; run `cargo build -p quilkin` first"
        );
        Ok(path)
    }

    fn dummy_bin() -> eyre::Result<String> {
        let path = std::env::current_exe()?
            .canonicalize()?
            .ancestors()
            .nth(3)
            .ok_or_else(|| eyre::eyre!("cannot determine workspace root from exe path"))?
            .join("crates/xdp/bin/dummy.bin");
        eyre::ensure!(path.exists(), "dummy.bin not found at {path:?}");
        Ok(path.to_string_lossy().into_owned())
    }
}
