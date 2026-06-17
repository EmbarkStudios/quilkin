//! UDP packet throughput benchmarks.
//!
//! - pipeline: `bench_process_packet` directly; no socket, no I/O loop.
//! - completion (Linux only): using the I/O uring backend.
//! - poll: using the polling backend.

use bytes::BytesMut;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use quilkin::{Config, Providers, filters::FilterChain, net::Endpoint, net::io::UdpBackend};

const SIZES: &[usize] = &[64, 256, 1024, 1400];

fn make_config(
    providers: &Providers,
    svc: &mut quilkin::Service,
    upstream: SocketAddr,
) -> Arc<Config> {
    let config = Arc::new(Config::new(None, Default::default(), providers, svc));
    let endpoint = Endpoint::new(upstream.into());
    config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| clusters.insert(None, None, std::iter::once(endpoint).collect()));
    config
}

async fn spawn_echo_server() -> (u16, tokio::task::AbortHandle) {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            let Ok((len, addr)) = socket.recv_from(&mut buf).await else {
                break;
            };
            drop(socket.send_to(&buf[..len], addr).await);
        }
    })
    .abort_handle();
    (port, handle)
}

async fn spawn_proxy(
    upstream_port: u16,
    backend: UdpBackend,
) -> (u16, quilkin::signal::ShutdownTx) {
    let providers = Providers::default();
    let mut svc = quilkin::Service::builder()
        .udp()
        .udp_port(0)
        .udp_backend(backend)
        .testing();

    let upstream = SocketAddr::from((Ipv4Addr::LOCALHOST, upstream_port));
    let config = make_config(&providers, &mut svc, upstream);

    let (tx, rx) = quilkin::signal::channel();
    let shutdown = quilkin::signal::ShutdownHandler::new(tx, rx);
    let stx = shutdown.shutdown_tx();

    let (_task, ports) = svc
        .spawn_services(&config, shutdown)
        .await
        .expect("failed to spawn proxy");

    (ports.udp.expect("UDP port not allocated"), stx)
}

fn bench_pipeline(c: &mut Criterion) {
    let providers = Providers::default();
    let mut svc = quilkin::Service::builder().udp();
    // Upstream address is never contacted -- NoopSessions discards all packets.
    let upstream = SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 7777u16));
    let config = make_config(&providers, &mut svc, upstream);
    let filter_chain = FilterChain::default();

    // Release builds reject loopback; use TEST-NET-3 (203.0.113.x/24) which
    // passes quilkin's source IP validation in all build profiles.
    let source: SocketAddr = "203.0.113.1:12345".parse().unwrap();

    let mut group = c.benchmark_group("pipeline/passthrough");
    for &size in SIZES {
        let payload = vec![0u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            let mut destinations = Vec::with_capacity(1);
            b.iter_batched(
                || BytesMut::from(payload.as_slice()),
                |buf| {
                    quilkin::net::packet::bench_process_packet(
                        black_box(buf),
                        source,
                        &config,
                        &filter_chain,
                        &mut destinations,
                    );
                    destinations.clear();
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn udp_roundtrip(client: &std::net::UdpSocket, payload: &[u8], buf: &mut [u8]) {
    loop {
        match client.send(payload) {
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("send: {e}"),
        }
    }
    loop {
        match client.recv(buf) {
            Ok(_) => return,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("recv: {e}"),
        }
    }
}

fn bench_io_backend(c: &mut Criterion, rt: &tokio::runtime::Runtime, backend: UdpBackend) {
    let name = format!("{backend:?}").to_lowercase();

    let (echo_port, _echo) = rt.block_on(spawn_echo_server());
    let (proxy_port, _shutdown) = rt.block_on(spawn_proxy(echo_port, backend));

    let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    client.connect(("127.0.0.1", proxy_port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut group = c.benchmark_group(format!("{name}/passthrough"));
    for &size in SIZES {
        let payload = vec![0u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.iter_batched(
                || vec![0u8; size + 64],
                |mut buf| {
                    udp_roundtrip(&client, black_box(payload), &mut buf);
                    black_box(buf)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn proxy_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    quilkin_xds::metrics::set_registry(quilkin::metrics::registry());

    bench_pipeline(c);

    #[cfg(target_os = "linux")]
    bench_io_backend(c, &rt, UdpBackend::Completion);

    bench_io_backend(c, &rt, UdpBackend::Poll);
}

criterion_group!(benches, proxy_throughput);
criterion_main!(benches);
