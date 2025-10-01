/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::net::maxmind_db::MetricsIpNetEntry;
use once_cell::sync::Lazy;
use prometheus::{
    DEFAULT_BUCKETS, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, core::Collector,
};

pub use prometheus::Result;

/// "event" is used as a label for Metrics that can apply to both Filter
/// `read` and `write` executions.
pub const DIRECTION_LABEL: &str = "event";

pub(crate) const READ: Direction = Direction::Read;
pub(crate) const WRITE: Direction = Direction::Write;
#[allow(dead_code)]
pub(crate) const ASN_LABEL: &str = "asn";

/// Label value for [`DIRECTION_LABEL`] for `read` events
pub const READ_DIRECTION_LABEL: &str = "read";
/// Label value for [`DIRECTION_LABEL`] for `write` events
pub const WRITE_DIRECTION_LABEL: &str = "write";

/// Returns the [`Registry`] containing all the metrics registered in Quilkin.
pub fn registry() -> &'static Registry {
    static REGISTRY: Lazy<Registry> =
        Lazy::new(|| Registry::new_custom(Some("quilkin".into()), None).unwrap());

    &REGISTRY
}

/// Start the histogram bucket at a quarter of a millisecond, as number below a millisecond are
/// what we are aiming for, but some granularity below a millisecond is useful for performance
/// profiling.
pub(crate) const BUCKET_START: f64 = 0.00025;

pub(crate) const BUCKET_FACTOR: f64 = 2.0;

/// At an exponential factor of 2.0 (`BUCKET_FACTOR`), 13 iterations gets us to just over 1 second.
/// Any processing that occurs over a second is far too long, so we end bucketing there as we don't
/// care about granularity past 1 second.
pub(crate) const BUCKET_COUNT: usize = 13;

pub(crate) fn leader_election(is_leader: bool) {
    static METRIC: Lazy<IntGauge> = Lazy::new(|| {
        prometheus::register_int_gauge_with_registry! {
            prometheus::opts! {
                "provider_leader_election",
                "Whether the current instance is considered the leader of the replicas.",
            },
            registry(),
        }
        .unwrap()
    });

    METRIC.set(is_leader as _);
}

pub(crate) mod k8s {
    use super::*;

    pub(crate) fn active(active: bool) {
        static METRIC: Lazy<IntGauge> = Lazy::new(|| {
            prometheus::register_int_gauge_with_registry! {
                prometheus::opts! {
                    "provider_k8s_active",
                    "Whether the kubernetes configuration provider is active or not (either 1 or 0).",
                },
                registry(),
            }
            .unwrap()
        });

        METRIC.set(active as _);
    }

    pub(crate) fn filters(active: bool) {
        static METRIC: Lazy<IntGauge> = Lazy::new(|| {
            prometheus::register_int_gauge_with_registry! {
                prometheus::opts! {
                    "provider_k8s_filters",
                    "Whether the kubernetes configuration provider has set the filter chain.",
                },
                registry(),
            }
            .unwrap()
        });

        METRIC.set(active as _);
    }

    pub(crate) fn events_total(kind: &'static str, ty: &'static str) -> IntCounter {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "provider_k8s_events_total",
                    "Total number of kubernetes events by `type` for a given resource (`kind`)",
                },
                &["kind", "type"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[kind, ty])
    }

    fn gameservers_total(kind: &'static str) -> IntCounter {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "provider_k8s_gameservers_total",
                    "Total number of gameservers applied (or failed to) by events and by `kind` (either `invalid`, `unallocated`, or `valid`) ",
                },
                &["kind"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[kind])
    }

    pub(crate) fn gameservers_total_invalid() {
        const KIND: &str = "invalid";
        gameservers_total(KIND).inc();
    }

    pub(crate) fn gameservers_total_valid() {
        const KIND: &str = "valid";
        gameservers_total(KIND).inc();
    }

    pub(crate) fn gameservers_total_unallocated() {
        const KIND: &str = "invalid";
        gameservers_total(KIND).inc();
    }

    pub(crate) fn gameservers_deletions_total(success: bool) {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "provider_k8s_gameservers_deletions_total",
                    "Total number of gameserver applied deletion events by `success` (either `true` or `false`) ",
                },
                &["kind"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[&success.to_string()]).inc();
    }

    pub(crate) fn errors_total(kind: &'static str, reason: &impl std::fmt::Display) -> IntCounter {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "providers_k8s_errors_total",
                    "total number of errors the kubernetes provider has encountered",
                },
                &["kind", "reason"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[kind, &reason.to_string()])
    }
}

pub(crate) mod qcmp {
    use super::*;

    pub(crate) fn active(active: bool) {
        static METRIC: Lazy<IntGauge> = Lazy::new(|| {
            prometheus::register_int_gauge_with_registry! {
                prometheus::opts! {
                    "service_qcmp_active",
                    "Whether the QCMP service is currently running, either 1 for running or 0 for not.",
                },
                registry(),
            }
            .unwrap()
        });

        METRIC.set(active as _);
    }

    fn bytes_total(kind: &'static str) -> IntCounter {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "service_qcmp_bytes_total",
                    "Total number of bytes processed through QCMP",
                },
                &["kind"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[kind])
    }

    pub(crate) fn errors_total(reason: &str) -> IntCounter {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "service_qcmp_errors_total",
                    "total number of errors QCMP has encountered",
                },
                &["reason"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[reason])
    }

    fn packets_total(kind: &'static str) -> IntCounter {
        static METRIC: Lazy<IntCounterVec> = Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "service_qcmp_packets_total",
                    "Total number of packets processed through QCMP",
                },
                &["kind"],
                registry(),
            }
            .unwrap()
        });

        METRIC.with_label_values(&[kind])
    }

    pub(crate) fn packets_total_invalid(size: usize) {
        const KIND: &str = "invalid";
        bytes_total(KIND).inc_by(size as u64);
        packets_total(KIND).inc();
    }

    pub(crate) fn packets_total_unsupported(size: usize) {
        const KIND: &str = "unsupported";
        bytes_total(KIND).inc_by(size as u64);
        packets_total(KIND).inc();
    }

    pub(crate) fn packets_total_valid(size: usize) {
        const KIND: &str = "valid";
        bytes_total(KIND).inc_by(size as u64);
        packets_total(KIND).inc();
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Direction {
    Read,
    Write,
}

impl Direction {
    pub(crate) const LABEL: &'static str = DIRECTION_LABEL;

    #[inline]
    pub fn label(self) -> &'static str {
        match self {
            Self::Read => READ_DIRECTION_LABEL,
            Self::Write => WRITE_DIRECTION_LABEL,
        }
    }
}

pub struct AsnInfo<'a> {
    pub asn: &'a str,
    pub prefix: &'a str,
}

impl AsnInfo<'static> {
    pub const EMPTY: AsnInfo<'static> = EMPTY;
}

pub const EMPTY: AsnInfo<'static> = AsnInfo {
    asn: "",
    prefix: "",
};

impl<'a> From<Option<&'a MetricsIpNetEntry>> for AsnInfo<'a> {
    #[inline]
    fn from(value: Option<&'a MetricsIpNetEntry>) -> Self {
        let Some(val) = value else {
            return EMPTY;
        };

        Self {
            prefix: val.prefix.as_str(),
            asn: val.asn.as_str(),
        }
    }
}

pub(crate) fn shutdown_initiated() -> &'static IntGauge {
    static SHUTDOWN_INITATED: Lazy<IntGauge> = Lazy::new(|| {
        prometheus::register_int_gauge_with_registry! {
            prometheus::opts! {
                "shutdown_initiated",
                "Shutdown process has been started",
            },
            registry(),
        }
        .unwrap()
    });

    &SHUTDOWN_INITATED
}

pub(crate) fn game_traffic_tasks() -> &'static IntCounter {
    static GAME_TRAFFIC_TASKS: Lazy<IntCounter> = Lazy::new(|| {
        prometheus::register_int_counter_with_registry! {
            prometheus::opts! {
                "game_traffic_tasks",
                "The amount of game traffic tasks that have spawned",
            },
            registry(),
        }
        .unwrap()
    });

    &GAME_TRAFFIC_TASKS
}

pub(crate) fn game_traffic_task_closed() -> &'static IntCounter {
    static GAME_TRAFFIC_TASK_CLOSED: Lazy<IntCounter> = Lazy::new(|| {
        prometheus::register_int_counter_with_registry! {
            prometheus::opts! {
                "game_traffic_task_closed",
                "The amount of game traffic tasks that have shutdown",
            },
            registry(),
        }
        .unwrap()
    });

    &GAME_TRAFFIC_TASK_CLOSED
}

pub(crate) fn phoenix_requests() -> &'static IntCounter {
    static PHOENIX_REQUESTS: Lazy<IntCounter> = Lazy::new(|| {
        prometheus::register_int_counter_with_registry! {
            prometheus::opts! {
                "phoenix_requests",
                "The amount of phoenix requests",
            },
            registry(),
        }
        .unwrap()
    });

    &PHOENIX_REQUESTS
}

pub(crate) fn phoenix_measurement_seconds(
    icao: crate::config::IcaoCode,
    direction: &str,
) -> Histogram {
    static PHOENIX_MEASUREMENT: Lazy<HistogramVec> = Lazy::new(|| {
        prometheus::register_histogram_vec_with_registry! {
            prometheus::histogram_opts! {
                "phoenix_measurement_seconds",
                "Histogram of phoenix measurements for a given node",
                prometheus::DEFAULT_BUCKETS.to_vec()
            },
            &["icao", "direction"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_MEASUREMENT.with_label_values(&[icao.as_ref(), direction])
}

pub(crate) fn phoenix_measurement_errors(icao: crate::config::IcaoCode) -> IntCounter {
    static PHOENIX_MEASUREMENT_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "phoenix_measurement_errors_total",
                "The number of measurement errors",
            },
            &["icao"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_MEASUREMENT_ERRORS.with_label_values(&[icao.as_ref()])
}

pub(crate) fn phoenix_distance(icao: crate::config::IcaoCode) -> Gauge {
    static PHOENIX_DISTANCE: Lazy<GaugeVec> = Lazy::new(|| {
        prometheus::register_gauge_vec_with_registry! {
            prometheus::opts! {
                "phoenix_distance",
                "The distance from this instance to another node in the network",
            },
            &["icao"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_DISTANCE.with_label_values(&[icao.as_ref()])
}

pub(crate) fn phoenix_coordinates(icao: crate::config::IcaoCode, axis: &str) -> Gauge {
    static PHOENIX_COORDINATES: Lazy<GaugeVec> = Lazy::new(|| {
        prometheus::register_gauge_vec_with_registry! {
            prometheus::opts! {
                "phoenix_coordinates",
                "The phoenix coordinates relative to this node",
            },
            &["icao", "axis"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_COORDINATES.with_label_values(&[icao.as_ref(), axis])
}

pub(crate) fn phoenix_coordinates_alpha(icao: crate::config::IcaoCode) -> Gauge {
    static PHOENIX_COORDINATES_ALPHA: Lazy<GaugeVec> = Lazy::new(|| {
        prometheus::register_gauge_vec_with_registry! {
            prometheus::opts! {
                "phoenix_coordinates_alpha",
                "The alpha used when adjusting coordinates",
            },
            &["icao"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_COORDINATES_ALPHA.with_label_values(&[icao.as_ref()])
}

pub(crate) fn phoenix_distance_error_estimate(icao: crate::config::IcaoCode) -> Gauge {
    static PHOENIX_DISTANCE_ERROR_ESTIMATE: Lazy<GaugeVec> = Lazy::new(|| {
        prometheus::register_gauge_vec_with_registry! {
            prometheus::opts! {
                "phoenix_distance_error_estimate",
                "The distance from this instance to another node in the network",
            },
            &["icao"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_DISTANCE_ERROR_ESTIMATE.with_label_values(&[icao.as_ref()])
}

pub(crate) fn phoenix_task_closed() -> &'static IntGauge {
    static PHOENIX_TASK_CLOSED: Lazy<IntGauge> = Lazy::new(|| {
        prometheus::register_int_gauge_with_registry! {
            prometheus::opts! {
                "phoenix_task_closed",
                "Whether the phoenix task has shutdown",
            },
            registry(),
        }
        .unwrap()
    });

    &PHOENIX_TASK_CLOSED
}

pub(crate) fn phoenix_server_errors(error: &str) -> IntCounter {
    static PHOENIX_SERVER_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "phoenix_server_errors",
                "The amount of errors attempting to spawn the phoenix HTTP server",
            },
            &["error"],
            registry(),
        }
        .unwrap()
    });

    PHOENIX_SERVER_ERRORS.with_label_values(&[error])
}

pub(crate) fn processing_time(direction: Direction) -> Histogram {
    static PROCESSING_TIME: Lazy<HistogramVec> = Lazy::new(|| {
        prometheus::register_histogram_vec_with_registry! {
            prometheus::histogram_opts! {
                "packets_processing_duration_seconds",
                "Total processing time for a packet",
                prometheus::exponential_buckets(BUCKET_START, BUCKET_FACTOR, BUCKET_COUNT).unwrap(),
            },
            &[Direction::LABEL],
            registry(),
        }
        .unwrap()
    });

    PROCESSING_TIME.with_label_values(&[direction.label()])
}

pub(crate) fn bytes_total(direction: Direction, _asn: &AsnInfo<'_>) -> IntCounter {
    static BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "bytes_total",
                "total number of bytes",
            },
            &[Direction::LABEL],
            registry(),
        }
        .unwrap()
    });

    BYTES_TOTAL.with_label_values(&[direction.label()])
}

pub(crate) fn errors_total(direction: Direction, display: &str, _asn: &AsnInfo<'_>) -> IntCounter {
    static ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "errors_total",
                "total number of errors sending packets",
            },
            &[Direction::LABEL, "display"],
            registry(),
        }
        .unwrap()
    });

    ERRORS_TOTAL.with_label_values(&[direction.label(), display])
}

pub(crate) fn packet_jitter(direction: Direction, _asn: &AsnInfo<'_>) -> IntGauge {
    static PACKET_JITTER: Lazy<IntGaugeVec> = Lazy::new(|| {
        prometheus::register_int_gauge_vec_with_registry! {
            prometheus::opts! {
                "packet_jitter",
                "The time between new packets",
            },
            &[Direction::LABEL],
            registry(),
        }
        .unwrap()
    });

    PACKET_JITTER.with_label_values(&[direction.label()])
}

pub(crate) fn packets_total(direction: Direction, _asn: &AsnInfo<'_>) -> IntCounter {
    static PACKETS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "packets_total",
                "Total number of packets",
            },
            &[Direction::LABEL],
            registry(),
        }
        .unwrap()
    });

    PACKETS_TOTAL.with_label_values(&[direction.label()])
}

pub(crate) fn packets_dropped_total(
    direction: Direction,
    source: &str,
    _asn: &AsnInfo<'_>,
) -> IntCounter {
    static PACKETS_DROPPED: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "packets_dropped_total",
                "Total number of dropped packets",
            },
            &[Direction::LABEL, "source"],
            registry(),
        }
        .unwrap()
    });

    PACKETS_DROPPED.with_label_values(&[direction.label(), source])
}

pub(crate) fn provider_task_failures_total(provider_task: &str) -> IntCounter {
    static PROVIDER_TASK_FAILURES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "provider_task_failures_total",
                "The number of times a provider task has failed and had to be restarted",
            },
            &["task"],
            registry(),
        }
        .unwrap()
    });

    PROVIDER_TASK_FAILURES_TOTAL.with_label_values(&[provider_task])
}

/// Create a generic metrics options.
/// Use `filter_opts` instead if the intended target is a filter.
pub fn opts(name: &str, subsystem: &str, description: &str) -> Opts {
    Opts::new(name, description).subsystem(subsystem)
}

pub fn histogram_opts(
    name: &str,
    subsystem: &str,
    description: &str,
    buckets: impl Into<Option<Vec<f64>>>,
) -> HistogramOpts {
    HistogramOpts {
        common_opts: opts(name, subsystem, description),
        buckets: buckets
            .into()
            .unwrap_or_else(|| Vec::from(DEFAULT_BUCKETS as &'static [f64])),
    }
}

/// Registers the current metric collector with the provided registry.
///
/// # Panics
/// A collector with the same name has already been registered.
pub fn register<T: Collector + Sized + Clone + 'static>(collector: T) -> T {
    let return_value = collector.clone();

    self::registry()
        .register(Box::from(collector))
        .map(|_| return_value)
        .unwrap()
}

pub trait CollectorExt: Collector + Clone + Sized + 'static {
    /// Registers the current metric collector with the provided registry
    /// if not already registered.
    fn register_if_not_exists(self) -> Result<Self> {
        match registry().register(Box::from(self.clone())) {
            Ok(_) | Err(prometheus::Error::AlreadyReg) => Ok(self),
            Err(err) => Err(err),
        }
    }
}

impl<C: Collector + Clone + 'static> CollectorExt for C {}

#[inline]
pub(crate) fn apply_clusters(clusters: &crate::config::Watch<crate::net::ClusterMap>) {
    let clusters = clusters.read();
    crate::net::cluster::active_clusters().set(clusters.len() as i64);

    for entry in clusters.iter() {
        crate::net::cluster::active_endpoints(
            &entry
                .key()
                .clone()
                .map(|key| key.to_string())
                .unwrap_or_default(),
        )
        .set(entry.value().len() as i64);
    }
}
