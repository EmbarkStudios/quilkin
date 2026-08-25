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

use once_cell::sync::Lazy;
use prometheus::{Histogram, IntCounter, IntGauge, IntGaugeVec, Opts};

use crate::metrics::{histogram_opts, register};

const SUBSYSTEM: &str = "session";
#[allow(dead_code)]
const AS_NAME_LABEL: &str = "organization";
const COUNTRY_CODE_LABEL: &str = "country_code";
#[allow(dead_code)]
const PREFIX_ENTITY_LABEL: &str = "prefix_entity";
#[allow(dead_code)]
const PREFIX_NAME_LABEL: &str = "prefix_name";

pub(crate) fn active_sessions(asn: Option<&crate::net::maxmind_db::IpNetEntry>) -> IntGauge {
    static ACTIVE_SESSIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
        prometheus::register_int_gauge_vec_with_registry! {
            Opts::new("active", "number of sessions currently active").subsystem(SUBSYSTEM).namespace("quilkin"),
            &[COUNTRY_CODE_LABEL],
            crate::metrics::registry(),
        }
        .unwrap()
    });

    if let Some(asnfo) = asn {
        ACTIVE_SESSIONS.with_label_values(&[&asnfo.as_cc])
    } else {
        ACTIVE_SESSIONS.with_label_values(&[""])
    }
}

pub(crate) fn total_sessions() -> &'static IntCounter {
    static TOTAL_SESSIONS: Lazy<IntCounter> = Lazy::new(|| {
        register(
            IntCounter::with_opts(
                Opts::new("total", "total number of established sessions")
                    .subsystem(SUBSYSTEM)
                    .namespace("quilkin"),
            )
            .unwrap(),
        )
    });

    &TOTAL_SESSIONS
}

pub(crate) fn sessions_rejected_total() -> &'static IntCounter {
    static SESSIONS_REJECTED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
        register(
            IntCounter::with_opts(
                Opts::new(
                    "rejected_total",
                    "total number of sessions rejected due to the session limit",
                )
                .subsystem(SUBSYSTEM)
                .namespace("quilkin"),
            )
            .unwrap(),
        )
    });

    &SESSIONS_REJECTED_TOTAL
}

/// Why a session ended.
///
/// A fall in session count looks the same whether players left or their
/// endpoints vanished, and those want different responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloseReason {
    /// No traffic within the session TTL. UDP has no close, so this is how a
    /// player leaving normally looks.
    IdleTimeout,
    /// The endpoint the session was routed to is no longer in the cluster map.
    EndpointGone,
    /// The proxy is shutting down.
    Shutdown,
}

impl CloseReason {
    fn label(self) -> &'static str {
        match self {
            Self::IdleTimeout => "idle_timeout",
            Self::EndpointGone => "endpoint_gone",
            Self::Shutdown => "shutdown",
        }
    }
}

pub(crate) fn sessions_closed_total(reason: CloseReason) -> prometheus::IntCounter {
    static SESSIONS_CLOSED: Lazy<prometheus::IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            Opts::new(
                "quilkin_sessions_closed_total",
                "total number of sessions closed, by the reason they ended",
            ),
            &[crate::metrics::REASON_LABEL],
            crate::metrics::registry(),
        }
        .unwrap()
    });

    SESSIONS_CLOSED.with_label_values(&[reason.label()])
}

pub(crate) fn duration_secs() -> &'static Histogram {
    static DURATION_SECS: Lazy<Histogram> = Lazy::new(|| {
        register(
            Histogram::with_opts(histogram_opts(
                "duration_secs",
                SUBSYSTEM,
                "duration of sessions",
                vec![
                    1f64, 5f64, 10f64, 25f64, 60f64, 300f64, 900f64, 1800f64, 3600f64,
                ],
            ))
            .unwrap(),
        )
    });

    &DURATION_SECS
}
