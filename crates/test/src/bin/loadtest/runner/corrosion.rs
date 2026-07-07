use corro_types::pubsub::SubsManager;
use corrosion::{
    db::{DBLimits, DBMaintenance, InitializedDb},
    persistent::{
        ExecResult, Metrics,
        client::{Client, MutationClient},
        mutator::BroadcastingTransactor,
        proto::v1::{ServerChange, ServerUpsert},
        server::Server,
    },
    pubsub::{PubsubContext, Trip},
    schema::SCHEMA,
};
use quilkin_types::{AddressKind, Endpoint, IcaoCode, TokenSet};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub struct CorrosionParams {
    pub writers: u32,
    pub transactions: u64,
    /// Transactions per second per writer; 0 = unlimited.
    pub rate: u64,
    /// Endpoints per transaction.
    pub batch: u32,
    /// `SQLite` `max_page_count` limit; None = no limit.
    pub max_pages: Option<u64>,
}

pub struct CorrosionResults {
    pub attempted: u64,
    pub succeeded: u64,
    /// QUIC/connection failures.
    pub errors_transport: u64,
    /// SQL execution failures (`SQLITE_FULL`, constraint violations, etc.).
    pub errors_exec: u64,
    pub actual_tps: f64,
    pub duration: Duration,
    /// Latency percentiles (ms) for succeeded transactions.
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Transactions taking longer than 1 s.
    pub stalls: u64,
}

impl CorrosionResults {
    pub fn failed(&self) -> u64 {
        self.errors_transport + self.errors_exec
    }
}

#[derive(Default)]
struct WriterStats {
    attempted: u64,
    succeeded: u64,
    errors_transport: u64,
    errors_exec: u64,
    latencies_ms: Vec<f64>,
}

struct CorrosionHandle {
    addr: SocketAddr,
    server: Server,
    trip: Trip,
    metrics: Metrics,
    _temp: tempfile::TempDir,
}

impl CorrosionHandle {
    async fn setup(params: CorrosionParams) -> eyre::Result<Self> {
        let temp = tempfile::TempDir::new()?;
        let root = camino::Utf8Path::from_path(temp.path()).expect("non-utf8 temp path");
        let db_path = root.join("db.sqlite");
        let sub_path = root.join("subs");
        // Pre-create so corrosion's async rmdir wins the race against TempDir's drop.
        std::fs::create_dir_all(sub_path.as_std_path())?;

        let maintenance = params.max_pages.map(|max_page_count| DBMaintenance {
            limits: DBLimits {
                max_page_count: Some(max_page_count),
                ..Default::default()
            },
            ..DBMaintenance::default()
        });

        let db = InitializedDb::setup(&db_path, SCHEMA, maintenance).await?;
        let subs = SubsManager::default();
        let btx = BroadcastingTransactor::new(
            db.actor_id,
            db.clock.clone(),
            db.pool.clone(),
            subs.clone(),
            None,
        )
        .await?;

        let trip = Trip::new();
        let pubsub_ctx = PubsubContext {
            schema: db.schema,
            subs,
            pool: db.pool,
            cache: Default::default(),
            path: sub_path,
            tripwire: trip.tripwire(),
        };

        let metrics = Metrics::new(quilkin::metrics::registry());
        let server = Server::new_unencrypted(
            "[::1]:0".parse::<SocketAddr>()?,
            btx,
            pubsub_ctx,
            metrics.clone(),
        )?;
        let addr = server.local_addr();

        Ok(Self {
            addr,
            server,
            trip,
            metrics,
            _temp: temp,
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn shutdown(self) {
        self.server.shutdown("loadtest complete").await;
        self.trip.shutdown().await;
    }
}

pub async fn run(params: CorrosionParams) -> eyre::Result<CorrosionResults> {
    let handle = CorrosionHandle::setup(params).await?;
    let addr = handle.addr();
    let metrics = handle.metrics.clone();

    let start = Instant::now();

    let handles: Vec<_> = (0..params.writers)
        .map(|i| {
            let m = metrics.clone();
            tokio::spawn(async move { run_writer(i, addr, m, params).await })
        })
        .collect();

    let mut agg = WriterStats::default();
    for h in handles {
        match h.await {
            Ok(Ok(ws)) => {
                agg.attempted += ws.attempted;
                agg.succeeded += ws.succeeded;
                agg.errors_transport += ws.errors_transport;
                agg.errors_exec += ws.errors_exec;
                agg.latencies_ms.extend(ws.latencies_ms);
            }
            Ok(Err(e)) => tracing::error!(%e, "writer task failed"),
            Err(e) => tracing::error!(%e, "writer task panicked"),
        }
    }

    let elapsed = start.elapsed();
    handle.shutdown().await;

    agg.latencies_ms.sort_unstable_by(f64::total_cmp);
    let stalls = agg.latencies_ms.iter().filter(|&&ms| ms >= 1000.0).count() as u64;
    let (p50_ms, p95_ms, p99_ms, max_ms) = compute_percentiles(&agg.latencies_ms);

    Ok(CorrosionResults {
        attempted: agg.attempted,
        succeeded: agg.succeeded,
        errors_transport: agg.errors_transport,
        errors_exec: agg.errors_exec,
        actual_tps: agg.attempted as f64 / elapsed.as_secs_f64(),
        duration: elapsed,
        p50_ms,
        p95_ms,
        p99_ms,
        max_ms,
        stalls,
    })
}

fn compute_percentiles(sorted: &[f64]) -> (f64, f64, f64, f64) {
    if sorted.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    let pct = |p: f64| {
        let idx = ((p / 100.0) * sorted.len() as f64) as usize;
        sorted[idx.min(sorted.len() - 1)]
    };
    (pct(50.0), pct(95.0), pct(99.0), *sorted.last().unwrap())
}

async fn run_writer(
    id: u32,
    addr: SocketAddr,
    metrics: Metrics,
    params: CorrosionParams,
) -> eyre::Result<WriterStats> {
    let client = Client::connect_insecure(addr, metrics).await?;
    let icao: IcaoCode = "LOAD".parse().expect("valid ICAO");
    let mc = MutationClient::connect(client, 0, icao).await?;

    let interval = (params.rate > 0).then(|| Duration::from_nanos(1_000_000_000 / params.rate));
    let mut stats = WriterStats {
        latencies_ms: Vec::with_capacity(params.transactions as usize),
        ..Default::default()
    };

    for seq in 0u64..params.transactions {
        if let Some(iv) = interval {
            // Schedule from now so a slow transaction doesn't trigger catch-up bursts.
            tokio::time::sleep(iv).await;
        }

        let upserts: Vec<ServerUpsert> = (0..params.batch)
            .map(|b| {
                let flat = seq * params.batch as u64 + b as u64;
                let endpoint = Endpoint::new(
                    AddressKind::Ip(IpAddr::V4(Ipv4Addr::new(
                        10,
                        (id % 256) as u8,
                        ((flat >> 8) & 0xff) as u8,
                        (flat & 0xff) as u8,
                    ))),
                    7777,
                );
                ServerUpsert {
                    endpoint,
                    icao,
                    tokens: TokenSet::default(),
                }
            })
            .collect();

        stats.attempted += 1;
        let t0 = Instant::now();
        match mc.transactions(&[ServerChange::Upsert(upserts)]).await {
            Ok(resp) => {
                let ms = t0.elapsed().as_secs_f64() * 1000.0;
                if resp
                    .results
                    .iter()
                    .any(|r| matches!(r, ExecResult::Error { .. }))
                {
                    stats.errors_exec += 1;
                } else {
                    stats.succeeded += 1;
                    stats.latencies_ms.push(ms);
                }
            }
            Err(e) => {
                tracing::debug!(%e, writer = id, seq, "transaction failed");
                stats.errors_transport += 1;
            }
        }
    }

    mc.shutdown().await;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(writers: u32, transactions: u64) -> CorrosionParams {
        CorrosionParams {
            writers,
            transactions,
            rate: 0,
            batch: 1,
            max_pages: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn smoke() {
        let r = run(params(1, 10)).await.expect("corrosion loadtest failed");
        assert_eq!(
            r.succeeded, 10,
            "all transactions succeeded (transport={}, exec={})",
            r.errors_transport, r.errors_exec
        );
    }

    /// Single-writer p50 must stay under 20 ms.
    #[tokio::test(flavor = "multi_thread")]
    async fn latency_single_writer() {
        let r = run(params(1, 50)).await.expect("corrosion loadtest failed");
        assert_eq!(
            r.succeeded, 50,
            "all transactions succeeded (transport={}, exec={})",
            r.errors_transport, r.errors_exec
        );
        eprintln!(
            "[single writer]  p50={:.2}ms p95={:.2}ms p99={:.2}ms stalls={}",
            r.p50_ms, r.p95_ms, r.p99_ms, r.stalls
        );
        assert!(r.p50_ms < 20.0, "p50 {:.1}ms exceeds 20ms", r.p50_ms);
    }

    /// 4-writer p50 must stay within 4× the single-writer baseline.
    #[tokio::test(flavor = "multi_thread")]
    async fn latency_concurrent_writers() {
        let single = run(params(1, 30)).await.expect("single writer failed");
        let multi = run(params(4, 30)).await.expect("multi writer failed");

        assert_eq!(
            single.succeeded, 30,
            "single: transport={} exec={}",
            single.errors_transport, single.errors_exec
        );
        assert_eq!(
            multi.succeeded, 120,
            "multi: transport={} exec={}",
            multi.errors_transport, multi.errors_exec
        );

        eprintln!(
            "[1 writer]  p50={:.2}ms  [4 writers]  p50={:.2}ms  ratio={:.1}x",
            single.p50_ms,
            multi.p50_ms,
            multi.p50_ms / single.p50_ms.max(0.01)
        );

        assert!(
            multi.p50_ms < single.p50_ms * 4.0 + 5.0,
            "4-writer p50 {:.1}ms is more than 4× single-writer p50 {:.1}ms — write serialisation has grown",
            multi.p50_ms,
            single.p50_ms
        );
    }
}
