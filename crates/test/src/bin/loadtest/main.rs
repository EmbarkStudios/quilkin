mod runner;

use clap::{Parser, Subcommand};
use quilkin::net::io::UdpBackend;
use std::{io::IsTerminal, path::PathBuf};

#[derive(Parser)]
#[command(about = "In-process UDP load test for quilkin")]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run one or more load test scenarios.
    ///
    /// TARGET: service, service.udp, provider, provider.corrosion. Omit to run all.
    Run {
        /// service, service.udp, provider, or provider.corrosion; omit for all
        #[arg(value_name = "TARGET")]
        target: Option<Target>,
        #[command(flatten)]
        udp: UdpArgs,
        #[command(flatten)]
        corrosion: CorrosionArgs,
    },

    /// Compare two JSON result files and print a per-scenario table
    Compare {
        /// Label shown in the top-level heading
        #[arg(long)]
        label: Option<String>,

        /// Main-branch results (baseline); may be absent on the first run
        baseline: Option<PathBuf>,

        /// PR results
        pr: Option<PathBuf>,
    },
}

#[derive(clap::Args)]
struct UdpArgs {
    /// Backends to benchmark; default is all supported on this platform.
    /// CI should pass an explicit list to avoid the kernel (XDP) backend.
    #[arg(long, value_delimiter = ',')]
    backends: Vec<UdpBackend>,

    /// Number of packets in the measured run
    #[arg(long, default_value_t = 50_000)]
    packets: u64,

    /// Target send rate in packets per second
    #[arg(long, default_value_t = 10_000)]
    qps: u64,

    /// Payload size in bytes (minimum 16)
    #[arg(long, default_value_t = 128)]
    payload_size: usize,

    /// Warmup packet count (results discarded)
    #[arg(long, default_value_t = 1_000)]
    warmup: u64,

    /// Write JSON results to this path
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(clap::Args)]
struct CorrosionArgs {
    /// Number of concurrent writer tasks
    #[arg(long, default_value_t = 12)]
    writers: u32,

    /// Transactions per writer
    #[arg(long, default_value_t = 1_000)]
    transactions: u64,

    /// Transactions per second per writer; 0 = unlimited
    #[arg(long, default_value_t = 0)]
    rate: u64,

    /// Endpoints per transaction
    #[arg(long, default_value_t = 1)]
    batch: u32,

    /// `SQLite` `max_page_count` limit; omit for no limit
    #[arg(long)]
    max_pages: Option<u64>,
}

/// All supported backends for this platform, most constrained first.
fn default_backends() -> Vec<UdpBackend> {
    #[cfg(target_os = "linux")]
    {
        vec![UdpBackend::Poll, UdpBackend::Completion, UdpBackend::Kernel]
    }
    #[cfg(not(target_os = "linux"))]
    {
        vec![UdpBackend::Poll]
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Progress to stderr; quilkin internals suppressed to warn by default.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .without_time()
        .with_level(false)
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,loadtest=info")),
        )
        .init();

    let args = Args::parse();
    match args.cmd {
        Cmd::Run {
            target,
            udp,
            corrosion,
        } => cmd_run(target, udp, corrosion).await,
        Cmd::Compare {
            label,
            baseline,
            pr,
        } => cmd_compare(label, baseline, pr),
    }
}

#[derive(Clone)]
enum Target {
    All,
    Service,
    ServiceUdp,
    Provider,
    ProviderCorrosion,
}

impl std::str::FromStr for Target {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "service" => Ok(Self::Service),
            "service.udp" => Ok(Self::ServiceUdp),
            "provider" => Ok(Self::Provider),
            "provider.corrosion" => Ok(Self::ProviderCorrosion),
            other => Err(format!(
                "unknown target {other:?}; valid: service, service.udp, provider, provider.corrosion"
            )),
        }
    }
}

impl Target {
    fn run_service_udp(&self) -> bool {
        matches!(self, Self::All | Self::Service | Self::ServiceUdp)
    }

    fn run_corrosion(&self) -> bool {
        matches!(self, Self::All | Self::Provider | Self::ProviderCorrosion)
    }
}

async fn cmd_run(
    target: Option<Target>,
    udp: UdpArgs,
    corrosion: CorrosionArgs,
) -> eyre::Result<()> {
    quilkin_xds::metrics::set_registry(quilkin::metrics::registry());

    let target = target.unwrap_or(Target::All);

    if target.run_service_udp() {
        let backends = if udp.backends.is_empty() {
            default_backends()
        } else {
            udp.backends.clone()
        };
        let params = runner::udp::LoadParams {
            packets: udp.packets,
            qps: udp.qps,
            payload_size: udp.payload_size,
            warmup: udp.warmup,
        };
        let mut results: Vec<runner::udp::ScenarioResult> = Vec::new();

        for backend in backends {
            let name = format!("{backend:?}").to_lowercase();
            tracing::info!(
                "==> [service.udp/{name}] load test (n={}, qps={}, payload={}B)",
                udp.packets,
                udp.qps,
                udp.payload_size
            );
            match runner::udp::run_passthrough(backend, &params).await {
                Ok(r) => {
                    tracing::info!(
                        "==> [service.udp/{name}] sent={} recv={} qps={:.0} loss={:.3}%",
                        r.sent,
                        r.received,
                        r.actual_qps,
                        r.loss_pct()
                    );
                    results.push(runner::udp::ScenarioResult { name, results: r });
                }
                Err(e) => tracing::warn!("==> [service.udp/{name}] skipped: {e:#}"),
            }
        }

        if let Some(path) = &udp.output {
            if let Some(dir) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::write(path, serde_json::to_string_pretty(&results)?)?;
            tracing::info!("==> written to {}", path.display());
        }

        if std::io::stdout().is_terminal() {
            print_multi_table(&results);
        } else {
            println!("{}", serde_json::to_string_pretty(&results)?);
        }
    }

    if target.run_corrosion() {
        let params = runner::corrosion::CorrosionParams {
            writers: corrosion.writers,
            transactions: corrosion.transactions,
            rate: corrosion.rate,
            batch: corrosion.batch,
            max_pages: corrosion.max_pages,
        };
        let rate_str = if corrosion.rate == 0 {
            "unlimited".into()
        } else {
            format!("{}/s", corrosion.rate)
        };
        tracing::info!(
            "==> [provider.corrosion] stress test (writers={}, txns={}, rate={rate_str}, batch={}{})",
            corrosion.writers,
            corrosion.transactions,
            corrosion.batch,
            corrosion
                .max_pages
                .map_or(String::new(), |p| format!(", max_pages={p}"))
        );
        match runner::corrosion::run(params).await {
            Ok(r) => {
                tracing::info!(
                    "==> [provider.corrosion] attempted={} succeeded={} transport_err={} exec_err={} tps={:.0} p99={:.0}ms stalls={} duration={:.1}s",
                    r.attempted,
                    r.succeeded,
                    r.errors_transport,
                    r.errors_exec,
                    r.actual_tps,
                    r.p99_ms,
                    r.stalls,
                    r.duration.as_secs_f64(),
                );
                if std::io::stdout().is_terminal() {
                    print_corrosion_table(&r);
                }
            }
            Err(e) => tracing::warn!("==> [provider.corrosion] failed: {e:#}"),
        }
    }

    Ok(())
}

fn print_multi_table(scenarios: &[runner::udp::ScenarioResult]) {
    use comfy_table::{Table, presets::UTF8_FULL};
    if scenarios.is_empty() {
        return;
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);

    let mut header = vec!["".to_string()];
    header.extend(scenarios.iter().map(|s| s.name.clone()));
    table.set_header(header);

    #[allow(clippy::type_complexity)]
    let metrics: &[(&str, fn(&runner::udp::Results) -> String)] = &[
        ("Sent", |r| format!("{}", r.sent)),
        ("Received", |r| format!("{}", r.received)),
        ("Packet loss", |r| format!("{:.3}%", r.loss_pct())),
        ("Throughput", |r| format!("{:.0} pkt/s", r.actual_qps)),
    ];
    for (label, f) in metrics {
        let mut row = vec![label.to_string()];
        row.extend(scenarios.iter().map(|s| f(&s.results)));
        table.add_row(row);
    }

    for (label, p) in [
        ("p50", 50.0f64),
        ("p75", 75.0),
        ("p90", 90.0),
        ("p99", 99.0),
        ("p99.9", 99.9),
    ] {
        let mut row = vec![label.to_string()];
        row.extend(scenarios.iter().map(|s| {
            s.results
                .percentile_ms(p)
                .map_or("—".into(), |v| format!("{v:.3} ms"))
        }));
        table.add_row(row);
    }

    println!("{table}");
}

fn print_corrosion_table(r: &runner::corrosion::CorrosionResults) {
    use comfy_table::{Cell, Color, Table, presets::UTF8_FULL};
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["Metric", "Value"]);

    let failed = r.failed();
    table.add_row(["Attempted", &r.attempted.to_string()]);
    table.add_row(["Succeeded", &r.succeeded.to_string()]);

    let fail_str = if failed == 0 {
        "0".to_owned()
    } else {
        format!(
            "{failed}  (transport: {}, exec: {})",
            r.errors_transport, r.errors_exec
        )
    };
    table.add_row([
        Cell::new("Failed"),
        if failed > 0 {
            Cell::new(fail_str).fg(Color::Red)
        } else {
            Cell::new(fail_str).fg(Color::Green)
        },
    ]);

    table.add_row(["Throughput", &format!("{:.0} txn/s", r.actual_tps)]);
    table.add_row(["p50 latency", &format!("{:.1} ms", r.p50_ms)]);
    table.add_row(["p95 latency", &format!("{:.1} ms", r.p95_ms)]);
    table.add_row(["p99 latency", &format!("{:.1} ms", r.p99_ms)]);
    table.add_row([
        "max latency",
        &format!("{:.1} ms  (incl. shutdown cleanup)", r.max_ms),
    ]);

    let stall_str = format!("{}", r.stalls);
    table.add_row([
        Cell::new("Stalls (>1 s)"),
        if r.stalls > 0 {
            Cell::new(stall_str).fg(Color::Yellow)
        } else {
            Cell::new(stall_str).fg(Color::Green)
        },
    ]);

    table.add_row(["Duration", &format!("{:.3} s", r.duration.as_secs_f64())]);

    println!("\nprovider.corrosion");
    println!("{table}");
}

fn delta_pct(b: Option<f64>, p: Option<f64>, higher_better: bool) -> (String, Option<bool>) {
    let (Some(b), Some(p)) = (b, p) else {
        return ("—".into(), None);
    };
    if b == 0.0 {
        return ("—".into(), None);
    }
    let up = p > b;
    (
        format!(
            "{} {:+.1}%",
            if up { "↑" } else { "↓" },
            (p - b) / b * 100.0
        ),
        Some(up == higher_better),
    )
}

fn delta_pp(b: Option<f64>, p: Option<f64>) -> (String, Option<bool>) {
    let (Some(b), Some(p)) = (b, p) else {
        return ("—".into(), None);
    };
    let diff = p - b;
    (
        format!("{} {:+.3} pp", if diff > 0.0 { "↑" } else { "↓" }, diff),
        Some(diff < 0.0),
    )
}

fn cmd_compare(
    label: Option<String>,
    baseline_path: Option<PathBuf>,
    pr_path: Option<PathBuf>,
) -> eyre::Result<()> {
    let load = |p: Option<&PathBuf>| -> Vec<runner::udp::ScenarioResult> {
        p.and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    };

    let label = label
        .or_else(|| {
            pr_path
                .as_ref()
                .and_then(|p| p.file_stem())
                .map(|s| s.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "unknown".into());

    let base = load(baseline_path.as_ref());
    let pr = load(pr_path.as_ref());

    println!("### Load test — `{label}`\n");
    if base.is_empty() {
        println!("> No baseline found — showing PR results only.\n");
    }

    let tty = std::io::stdout().is_terminal();

    for pr_scenario in &pr {
        let base_results = base
            .iter()
            .find(|b| b.name == pr_scenario.name)
            .map(|b| &b.results);
        compare_scenario(&pr_scenario.name, base_results, &pr_scenario.results, tty);
    }

    Ok(())
}

fn compare_scenario(
    name: &str,
    base: Option<&runner::udp::Results>,
    pr: &runner::udp::Results,
    tty: bool,
) {
    let f = |v: Option<f64>, u: &str| v.map_or("—".into(), |v| format!("{v:.3} {u}"));
    let qps = |v: Option<f64>| v.map_or("—".into(), |v| format!("{v:.0}"));

    let (bq, pq) = (base.map(|r| r.actual_qps), Some(pr.actual_qps));
    let (b50, p50) = (
        base.and_then(|r| r.percentile_ms(50.0)),
        pr.percentile_ms(50.0),
    );
    let (b99, p99) = (
        base.and_then(|r| r.percentile_ms(99.0)),
        pr.percentile_ms(99.0),
    );
    let (bl, pl) = (base.map(|r| r.loss_pct()), Some(pr.loss_pct()));

    let rows = [
        ("Actual QPS", qps(bq), qps(pq), delta_pct(bq, pq, true)),
        (
            "p50 latency",
            f(b50, "ms"),
            f(p50, "ms"),
            delta_pct(b50, p50, false),
        ),
        (
            "p99 latency",
            f(b99, "ms"),
            f(p99, "ms"),
            delta_pct(b99, p99, false),
        ),
        ("Packet loss", f(bl, "%"), f(pl, "%"), delta_pp(bl, pl)),
    ];

    if tty {
        use comfy_table::{Cell, Color, Table, presets::UTF8_FULL};
        println!("\n{name}");
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(["Metric", "main", "PR", "Δ"]);
        for (metric, base_val, pr_val, (delta_s, good)) in &rows {
            table.add_row([
                Cell::new(metric),
                Cell::new(base_val),
                Cell::new(pr_val),
                match good {
                    Some(true) => Cell::new(delta_s).fg(Color::Green),
                    Some(false) => Cell::new(delta_s).fg(Color::Red),
                    None => Cell::new(delta_s),
                },
            ]);
        }
        println!("{table}");
    } else {
        println!("#### {name}\n");
        println!("| Metric | main | PR | Δ |");
        println!("|--------|------|----|---|");
        for (metric, base_val, pr_val, (delta_s, _)) in &rows {
            println!("| {metric} | {base_val} | {pr_val} | {delta_s} |");
        }
        println!();
    }
}
