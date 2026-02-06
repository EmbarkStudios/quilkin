//! Implementation for a persistent connection between a client (mutating agent
//! or subscribing proxy) and server (relay).

pub mod client;
mod error;
pub mod mutator;
pub mod proto;
pub mod server;

pub use corro_api_types::{ExecResponse, ExecResult};
pub use error::ErrorCode;

#[derive(Clone)]
pub struct Metrics {
    active: prometheus::IntGaugeVec,
    tx_count: prometheus::IntCounterVec,
    tx_bytes: prometheus::IntCounterVec,
    rx_count: prometheus::IntCounterVec,
    rx_bytes: prometheus::IntCounterVec,
}

impl Metrics {
    pub fn new(registry: &'static prometheus::Registry) -> Self {
        static THIS: std::sync::OnceLock<Metrics> = std::sync::OnceLock::new();

        THIS.get_or_init(|| {
            let active = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_active_connections",
                    "Current number of active connections",
                },
                &[],
                registry,
            }
            .unwrap();
            let tx_count = prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_tx_count",
                    "Number of datagrams sent",
                },
                &[],
                registry,
            }
            .unwrap();
            let tx_bytes = prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_tx_bytes",
                    "Amount of bytes sent",
                },
                &[],
                registry,
            }
            .unwrap();
            let rx_count = prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_rx_count",
                    "Number of datagrams received",
                },
                &[],
                registry,
            }
            .unwrap();
            let rx_bytes = prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_rx_bytes",
                    "Amount of bytes received",
                },
                &[],
                registry,
            }
            .unwrap();

            Self {
                active,
                tx_count,
                tx_bytes,
                rx_count,
                rx_bytes,
            }
        })
        .clone()
    }
}

#[inline]
fn update_metric(metric: &prometheus::IntCounterVec, current: &mut u64, new: u64) {
    // I'd _assume_ that quinn will never decrement the stats, but just in case
    metric
        .with_label_values::<&str>(&[])
        .inc_by(new.saturating_sub(*current));
    *current = (*current).max(new);
}
