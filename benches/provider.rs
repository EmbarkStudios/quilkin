//! Provider benchmark — stresses endpoint add/remove across provider backends.
//!
//! Three harnesses run by default:
//!
//! * **HTTP** — drives the HTTP provider REST API via `axum_test` (no sockets).
//! * **Corrosion** — calls `ServerMutator` directly against in-memory
//!   `LocalState` (no external database required).
//! * **MDS+HTTP** — spins up a real Corrosion MDS service (`SQLite` in a temp
//!   directory, random UDP + gRPC ports) alongside the HTTP provider.  Both
//!   share the same in-memory `Config`.  This is the most realistic scenario:
//!   endpoint mutations are made through the HTTP provider while the Corrosion
//!   service is active in the background, exercising the full integrated stack.
//!
//! Run all scenarios:
//!   cargo bench --bench provider
//!
//! Run only the cycle scenario across all harnesses:
//!   cargo bench --bench provider -- `add_remove_cycle`

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use quilkin::{
    net::endpoint::{Endpoint, Metadata},
    providers::{FiltersAndClusters, corrosion::push::LocalState},
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

// ── ProviderHarness trait ────────────────────────────────────────────────────

trait ProviderHarness {
    async fn upsert(&self, addr: SocketAddr, tokens: Vec<Vec<u8>>);
    async fn remove(&self, addr: SocketAddr);
    async fn clear(&self);
    /// Number of endpoints currently visible to the provider's local state.
    async fn endpoint_count(&self) -> usize;

    /// Replace the entire endpoint set in a single operation.
    ///
    /// Providers with a native bulk API (HTTP `PUT /endpoints`) override this;
    /// others fall back to clear + individual upserts.
    async fn bulk_upsert(&self, addrs: &[SocketAddr]) {
        self.clear().await;
        for &addr in addrs {
            self.upsert(addr, vec![]).await;
        }
    }

    /// Remove a specific set of endpoints in a single operation.
    ///
    /// Providers with a native bulk API (HTTP `DELETE /endpoints` with body)
    /// override this; others fall back to individual removes.
    async fn bulk_remove(&self, addrs: &[SocketAddr]) {
        for &addr in addrs {
            self.remove(addr).await;
        }
    }
}

// ── Shared address generation ─────────────────────────────────────────────────

fn addrs(n: usize) -> Vec<SocketAddr> {
    assert!(n <= 0xffff, "at most 65535 addresses supported");
    (0..n)
        .map(|i| {
            let hi = (i >> 8) as u8;
            let lo = i as u8;
            format!("10.1.{hi}.{lo}:7777").parse().unwrap()
        })
        .collect()
}

// ── HTTP harness ──────────────────────────────────────────────────────────────

struct HttpHarness {
    server: axum_test::TestServer,
}

impl HttpHarness {
    fn new() -> Self {
        let providers = quilkin::Providers::default().http();
        let service = quilkin::Service::default();
        let config = quilkin::Config::new(None, Default::default(), &providers, &service);
        let fc = FiltersAndClusters::new(&config).unwrap();
        Self {
            server: axum_test::TestServer::new(quilkin::providers::http::make_router(fc)).unwrap(),
        }
    }
}

impl ProviderHarness for HttpHarness {
    async fn upsert(&self, addr: SocketAddr, tokens: Vec<Vec<u8>>) {
        let endpoint = Endpoint::with_metadata(
            addr.into(),
            Metadata {
                tokens: tokens.into_iter().collect(),
            },
        );
        self.server.post("/endpoints").json(&endpoint).await;
    }

    async fn remove(&self, addr: SocketAddr) {
        self.server.delete(&format!("/endpoints/{addr}")).await;
    }

    async fn clear(&self) {
        self.server.delete("/endpoints").await;
    }

    async fn endpoint_count(&self) -> usize {
        let body: Vec<serde_json::Value> = self.server.get("/endpoints").await.json();
        body.len()
    }

    async fn bulk_upsert(&self, addrs: &[SocketAddr]) {
        let endpoints: Vec<Endpoint> = addrs.iter().map(|&a| Endpoint::new(a.into())).collect();
        self.server.post("/endpoints/bulk").json(&endpoints).await;
    }

    async fn bulk_remove(&self, addrs: &[SocketAddr]) {
        let addresses: Vec<quilkin::net::endpoint::EndpointAddress> =
            addrs.iter().map(|&a| a.into()).collect();
        self.server.delete("/endpoints").json(&addresses).await;
    }
}

// ── Corrosion harness ─────────────────────────────────────────────────────────
//
// Uses `ServerMutator::testing` which wires up the mutator against an
// in-memory `LocalState` with no network connection required.

struct CorrosionHarness {
    mutator: quilkin::providers::corrosion::ServerMutator,
    state: Arc<LocalState>,
    /// Maps each `SocketAddr` to the UUID assigned at upsert time so that
    /// individual removes can target the right entry.
    ids: Mutex<HashMap<SocketAddr, Uuid>>,
}

impl CorrosionHarness {
    fn new() -> Self {
        let state = Arc::new(LocalState::default());
        let (mutator, _rx) = quilkin::providers::corrosion::ServerMutator::testing(state.clone());
        Self {
            mutator,
            state,
            ids: Mutex::new(HashMap::new()),
        }
    }

    fn make_endpoint(addr: SocketAddr) -> quilkin_types::Endpoint {
        quilkin_types::Endpoint::new(addr.ip().into(), addr.port())
    }
}

impl ProviderHarness for CorrosionHarness {
    async fn upsert(&self, addr: SocketAddr, tokens: Vec<Vec<u8>>) {
        let id = Uuid::new_v4();
        self.ids.lock().unwrap().insert(addr, id);
        self.mutator.upsert_server(
            id,
            Self::make_endpoint(addr),
            quilkin_types::TokenSet(tokens.into_iter().collect()),
        );
    }

    async fn remove(&self, addr: SocketAddr) {
        if let Some(id) = self.ids.lock().unwrap().remove(&addr) {
            self.mutator.remove_server(id);
        }
    }

    async fn clear(&self) {
        let ids: Vec<Uuid> = self.ids.lock().unwrap().drain().map(|(_, v)| v).collect();
        for id in ids {
            self.mutator.remove_server(id);
        }
    }

    async fn endpoint_count(&self) -> usize {
        self.state.to_map().len()
    }
}

// ── Combined harness (Corrosion MDS service + HTTP provider) ─────────────────
//
// Spins up the real Corrosion MDS service — SQLite database in a randomised
// temp directory, OS-chosen UDP and gRPC ports — alongside the HTTP provider
// so that both share the same in-memory Config.  Endpoint mutations are driven
// through the HTTP provider while the Corrosion service watches for filter-chain
// changes in the background, mirroring production topology.

struct CombinedHarness {
    /// HTTP provider (sans-IO via `axum_test`, no real socket).
    server: axum_test::TestServer,
    /// Kept alive so background service tasks keep running until the harness is
    /// dropped (which happens when the Criterion runtime tears down).
    _shutdown_tx: quilkin::signal::ShutdownTx,
}

impl CombinedHarness {
    async fn new() -> Self {
        let providers = quilkin::Providers::default().http();
        // mds()      — enables the Corrosion DB server + MDS relay
        // testing()  — uses a randomised temp directory for the SQLite DB
        // mds_port(0) / corrosion_port(0) — let the OS choose free ports
        let service = quilkin::Service::default()
            .mds()
            .mds_port(0)
            .corrosion_port(0)
            .testing();
        let config = Arc::new(quilkin::Config::new(
            None,
            Default::default(),
            &providers,
            &service,
        ));

        let (tx, rx) = quilkin::signal::channel();
        let shutdown = quilkin::signal::ShutdownHandler::new(tx, rx);
        // Clone the sender before moving the handler into spawn_services so we
        // can keep the channel open (and the service alive) for the benchmark.
        let shutdown_tx = shutdown.shutdown_tx();
        drop(
            service
                .spawn_services(&config, shutdown)
                .await
                .expect("failed to start MDS service for benchmark"),
        );

        let fc = FiltersAndClusters::new(&config).unwrap();
        Self {
            server: axum_test::TestServer::new(quilkin::providers::http::make_router(fc)).unwrap(),
            _shutdown_tx: shutdown_tx,
        }
    }
}

impl ProviderHarness for CombinedHarness {
    async fn upsert(&self, addr: SocketAddr, tokens: Vec<Vec<u8>>) {
        let endpoint = Endpoint::with_metadata(
            addr.into(),
            Metadata {
                tokens: tokens.into_iter().collect(),
            },
        );
        self.server.post("/endpoints").json(&endpoint).await;
    }

    async fn remove(&self, addr: SocketAddr) {
        self.server.delete(&format!("/endpoints/{addr}")).await;
    }

    async fn clear(&self) {
        self.server.delete("/endpoints").await;
    }

    async fn endpoint_count(&self) -> usize {
        let body: Vec<serde_json::Value> = self.server.get("/endpoints").await.json();
        body.len()
    }

    async fn bulk_upsert(&self, addrs: &[SocketAddr]) {
        let endpoints: Vec<Endpoint> = addrs.iter().map(|&a| Endpoint::new(a.into())).collect();
        self.server.post("/endpoints/bulk").json(&endpoints).await;
    }

    async fn bulk_remove(&self, addrs: &[SocketAddr]) {
        let addresses: Vec<quilkin::net::endpoint::EndpointAddress> =
            addrs.iter().map(|&a| a.into()).collect();
        self.server.delete("/endpoints").json(&addresses).await;
    }
}

// ── Benchmark scenarios ───────────────────────────────────────────────────────

/// Upsert N endpoints then clear — measures raw insert throughput.
async fn scenario_upsert<H: ProviderHarness>(h: &H, addrs: &[SocketAddr]) {
    for &addr in addrs {
        h.upsert(addr, vec![]).await;
    }
    h.clear().await;
}

/// Add N endpoints then remove them individually.
async fn scenario_add_remove_cycle<H: ProviderHarness>(h: &H, addrs: &[SocketAddr]) {
    for &addr in addrs {
        h.upsert(addr, vec![]).await;
    }
    for &addr in addrs {
        h.remove(addr).await;
    }
    debug_assert_eq!(
        h.endpoint_count().await,
        0,
        "all endpoints must be absent after individual removes — \
         a non-zero count indicates persisted state that was not cleaned up"
    );
}

/// Upsert the same N addresses three times — measures the update-in-place path.
async fn scenario_repeated_upsert<H: ProviderHarness>(h: &H, addrs: &[SocketAddr]) {
    for _ in 0..3 {
        for &addr in addrs {
            h.upsert(addr, vec![]).await;
        }
    }
    h.clear().await;
}

/// Add N endpoints in a single bulk call — measures batch insert throughput.
async fn scenario_bulk_upsert<H: ProviderHarness>(h: &H, addrs: &[SocketAddr]) {
    h.bulk_upsert(addrs).await;
    h.clear().await;
}

/// Add N endpoints in bulk then remove them in a single bulk call.
///
/// Complements `add_remove_cycle` (which removes individually) to show the
/// savings from batching.
async fn scenario_bulk_add_remove_cycle<H: ProviderHarness>(h: &H, addrs: &[SocketAddr]) {
    h.bulk_upsert(addrs).await;
    h.bulk_remove(addrs).await;
    debug_assert_eq!(
        h.endpoint_count().await,
        0,
        "all endpoints must be absent after bulk remove"
    );
}

// ── Criterion groups ──────────────────────────────────────────────────────────

const SIZES: &[usize] = &[10, 100, 1_000];

fn run_scenarios<H: ProviderHarness>(
    c: &mut Criterion,
    rt: &tokio::runtime::Runtime,
    harness: &H,
    name: &str,
) {
    // Scenarios that make N individual round-trips per iteration scale poorly
    // at N=1000.  Criterion's default is 100 samples; reduce that for large N
    // so the total bench time stays reasonable.
    let sample_size = |n: usize| if n >= 1_000 { 10 } else { 100 };

    let mut group = c.benchmark_group(format!("{name}/upsert"));
    for &n in SIZES {
        let addrs = addrs(n);
        group.throughput(Throughput::Elements(n as u64));
        group.sample_size(sample_size(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &addrs, |b, addrs| {
            b.to_async(rt).iter(|| scenario_upsert(harness, addrs));
        });
    }
    group.finish();

    let mut group = c.benchmark_group(format!("{name}/add_remove_cycle"));
    for &n in SIZES {
        let addrs = addrs(n);
        group.throughput(Throughput::Elements(n as u64));
        group.sample_size(sample_size(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &addrs, |b, addrs| {
            b.to_async(rt)
                .iter(|| scenario_add_remove_cycle(harness, addrs));
        });
    }
    group.finish();

    let mut group = c.benchmark_group(format!("{name}/repeated_upsert"));
    for &n in SIZES {
        let addrs = addrs(n);
        group.throughput(Throughput::Elements(n as u64));
        group.sample_size(sample_size(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &addrs, |b, addrs| {
            b.to_async(rt)
                .iter(|| scenario_repeated_upsert(harness, addrs));
        });
    }
    group.finish();

    let mut group = c.benchmark_group(format!("{name}/bulk_upsert"));
    for &n in SIZES {
        let addrs = addrs(n);
        group.throughput(Throughput::Elements(n as u64));
        group.sample_size(sample_size(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &addrs, |b, addrs| {
            b.to_async(rt).iter(|| scenario_bulk_upsert(harness, addrs));
        });
    }
    group.finish();

    let mut group = c.benchmark_group(format!("{name}/bulk_add_remove_cycle"));
    for &n in SIZES {
        let addrs = addrs(n);
        group.throughput(Throughput::Elements(n as u64));
        group.sample_size(sample_size(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &addrs, |b, addrs| {
            b.to_async(rt)
                .iter(|| scenario_bulk_add_remove_cycle(harness, addrs));
        });
    }
    group.finish();
}

fn provider_benchmarks(c: &mut Criterion) {
    // The Corrosion DB setup calls `block_in_place` internally, which requires
    // a multi-threaded runtime.  Use `new_multi_thread` for all harnesses so
    // that the same runtime works regardless of which harness is active.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // The MDS/xDS metrics module requires an explicit registry initialisation
    // that normally happens in the CLI entry point.  Do it here before any
    // harness that starts the MDS service.
    quilkin_xds::metrics::set_registry(quilkin::metrics::registry());

    run_scenarios(c, &rt, &HttpHarness::new(), "http");
    run_scenarios(c, &rt, &CorrosionHarness::new(), "corrosion");

    // Build the combined harness inside the runtime since spawn_services is async.
    let combined = rt.block_on(CombinedHarness::new());
    run_scenarios(c, &rt, &combined, "mds+http");
}

criterion_group!(benches, provider_benchmarks);
criterion_main!(benches);
