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
    /// Run one or more load test scenarios and display a comparison table
    Run {
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
            backends,
            packets,
            qps,
            payload_size,
            warmup,
            output,
        } => cmd_run(backends, packets, qps, payload_size, warmup, output).await,
        Cmd::Compare {
            label,
            baseline,
            pr,
        } => cmd_compare(label, baseline, pr),
    }
}

async fn cmd_run(
    backends: Vec<UdpBackend>,
    packets: u64,
    qps: u64,
    payload_size: usize,
    warmup: u64,
    output: Option<PathBuf>,
) -> eyre::Result<()> {
    quilkin_xds::metrics::set_registry(quilkin::metrics::registry());

    let backends = if backends.is_empty() {
        default_backends()
    } else {
        backends
    };
    let params = runner::LoadParams {
        packets,
        qps,
        payload_size,
        warmup,
    };
    let mut results: Vec<runner::ScenarioResult> = Vec::new();

    for backend in backends {
        let name = format!("{backend:?}").to_lowercase();
        tracing::info!("==> [{name}] load test (n={packets}, qps={qps}, payload={payload_size}B)");
        match runner::run_passthrough(backend, &params).await {
            Ok(r) => {
                tracing::info!(
                    "==> [{name}] sent={} recv={} qps={:.0} loss={:.3}%",
                    r.sent,
                    r.received,
                    r.actual_qps,
                    r.loss_pct()
                );
                results.push(runner::ScenarioResult { name, results: r });
            }
            Err(e) => tracing::warn!("==> [{name}] skipped: {e:#}"),
        }
    }

    if let Some(path) = &output {
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

    Ok(())
}

fn print_multi_table(scenarios: &[runner::ScenarioResult]) {
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
    let metrics: &[(&str, fn(&runner::Results) -> String)] = &[
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
    let load = |p: Option<&PathBuf>| -> Vec<runner::ScenarioResult> {
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

fn compare_scenario(name: &str, base: Option<&runner::Results>, pr: &runner::Results, tty: bool) {
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
