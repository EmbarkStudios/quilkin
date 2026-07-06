use prometheus::{
    self as p, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, opts,
};
use std::time::Duration;

use crate::gossip::transport::TrafficClass;

#[derive(Copy, Clone)]
pub enum Direction {
    /// Data received from a remote peer
    In,
    /// Data sent to a remote peer
    Out,
}

impl Direction {
    #[inline]
    fn to_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
        }
    }
}

pub struct GossipMetrics {
    total_conns: IntCounter,
    active_conns: IntGauge,
    failed_handshakes: IntCounterVec,
    total_datagrams: IntCounter,
    total_datagram_bytes: IntCounter,
    failed_datagrams: IntCounterVec,
    total_streams: IntCounterVec,
    active_streams: IntGaugeVec,
    total_stream_bytes: IntCounterVec,
    failed_streams: IntCounterVec,

    sync_changes: IntCounterVec,
    sync_requests: IntCounterVec,

    change_batches: IntCounterVec,
    change_batch_size: IntGauge,
    change_dropped: IntCounter,
    change_received: IntCounter,
    change_bx_duplicate: IntCounterVec,
    change_clock: IntCounterVec,
    change_bx_received: IntCounterVec,
    change_recv_lag: HistogramVec,
    changes_in_queue: IntGauge,
    changesets_in_queue: IntGauge,
    changes_committed: IntCounterVec,
    changes_processed_time: HistogramVec,
    changes_processed_chunk_size: Histogram,
    changes_queued_time: Histogram,
    channel_errors: IntCounterVec,

    broadcast_dropped: IntCounter,
    broadcast_remaining_burst: IntGauge,
    broadcast_rate_limited: IntCounter,
    broadcast_spawned: IntCounterVec,

    client_datagram_errors: IntCounterVec,
    client_chunks_sent: IntCounterVec,
    client_bytes_sent: IntCounterVec,
    client_connect_time: HistogramVec,
    client_connect_errors: IntCounterVec,

    member_removed: IntCounterVec,
    member_added: IntCounter,
    swim_notifications: IntCounterVec,
}

impl GossipMetrics {
    pub fn new(registry: &'static p::Registry) -> &'static Self {
        static THIS: std::sync::OnceLock<GossipMetrics> = std::sync::OnceLock::new();

        THIS.get_or_init(|| {
            let active_conns = p::register_int_gauge_with_registry! {
                opts! {
                    "corrosion_gossip_server_active_connections",
                    "Number of active server gossip connections",
                },
                registry,
            }
            .unwrap();
            let total_conns = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_server_total_connections",
                    "Number of total server gossip connections",
                },
                registry,
            }
            .unwrap();
            let failed_handshakes = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_server_failed_handshakes",
                    "Number of total failed server gossip handshakes",
                },
                &["error_kind"],
                registry,
            }
            .unwrap();
            let total_datagrams = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_server_total_datagrams",
                    "Number of total incoming server gossip foca datagrams",
                },
                registry,
            }
            .unwrap();
            let total_datagram_bytes = prometheus::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_server_total_datagram_bytes",
                    "Number of total incoming server gossip foca datagram bytes",
                },
                registry,
            }
            .unwrap();
            let failed_datagrams = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_failed_datagrams",
                    "Number of total failed gossip foca datagrams",
                },
                &["error_kind"],
                registry,
            }
            .unwrap();
            let total_streams = prometheus::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_server_total_streams",
                    "Number of total incoming server gossip streams",
                },
                &["stream_kind"],
                registry,
            }
            .unwrap();
            let active_streams = p::register_int_gauge_vec_with_registry! {
                opts! {
                    "corrosion_gossip_server_active_streams",
                    "Number of total active server gossip streams",
                },
                &["stream_kind"],
                registry,
            }
            .unwrap();
            let total_stream_bytes = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_server_total_stream_bytes",
                    "Number of total incoming server gossip foca datagram bytes",
                },
                &["stream_kind", "direction", "traffic"],
                registry,
            }
            .unwrap();
            let failed_streams = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_server_failed_streams",
                    "Number of total failed server gossip streams",
                },
                &["error_kind", "stream_kind"],
                registry,
            }
            .unwrap();
            let sync_changes = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_sync_changes",
                    "Number of total gossip sync changes",
                },
                &["direction"],
                registry,
            }
            .unwrap();
            let sync_requests = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_sync_requests",
                    "Number of total gossip sync requests",
                },
                &["direction"],
                registry,
            }
            .unwrap();
            let change_batches = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_change_batches",
                    "Number of total gossip change batches applied",
                },
                &["reason"],
                registry,
            }
            .unwrap();
            let change_batch_size = p::register_int_gauge_with_registry! {
                opts! {
                    "corrosion_gossip_change_batch_size",
                    "Size of the change batch currently being processed",
                },
                registry,
            }
            .unwrap();
            let change_dropped = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_change_dropped",
                    "Total number of changes that have been dropped due to overflowing the maximum change queue",
                },
                registry,
            }
            .unwrap();
            let change_received = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_change_received",
                    "Total number of changes that have been received by the chang processor",
                },
                registry,
            }
            .unwrap();
            let change_bx_duplicate = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_change_broadcast_duplicates",
                    "Number of total duplicate gossip changes broadcast",
                },
                &["from"],
                registry,
            }
            .unwrap();
            let change_clock = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_change_clock",
                    "Number of total gossip changes clock updates",
                },
                &["source"],
                registry,
            }
            .unwrap();
            let change_bx_received = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_change_broadcast_received",
                    "Number of total gossip broadcast changes received",
                },
                &["kind"],
                registry,
            }
            .unwrap();
            let change_recv_lag = p::register_histogram_vec_with_registry! {
                p::histogram_opts! {
                    "corrosion_gossip_change_recv_lag",
                    "Receive lag in seconds",
                },
                &["source"],
                registry,
            }.unwrap();
            let changes_in_queue = p::register_int_gauge_with_registry! {
                opts! {
                    "corrosion_gossip_change_in_queue",
                    "Number of changes in the queue",
                },
                registry,
            }
            .unwrap();
            let changesets_in_queue = p::register_int_gauge_with_registry! {
                opts! {
                    "corrosion_gossip_changesets_in_queue",
                    "Number of change sets in the queue",
                },
                registry,
            }
            .unwrap();
            let changes_committed = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_change_committed",
                    "Number of total gossip changes committed",
                },
                &["table", "source"],
                registry,
            }
            .unwrap();
            let changes_processed_time = p::register_histogram_vec_with_registry! {
                p::histogram_opts! {
                    "corrosion_gossip_changes_processed_time",
                    "Gossip changes processing time in seconds",
                },
                &["source"],
                registry,
            }.unwrap();
            let changes_processed_chunk_size = p::register_histogram_with_registry! {
                p::histogram_opts! {
                    "corrosion_gossip_changes_processed_chunk_size",
                    "Gossip changes processing chunk sizes",
                },
                registry,
            }.unwrap();
            let changes_queued_time = p::register_histogram_with_registry! {
                p::histogram_opts! {
                    "corrosion_gossip_changes_queued",
                    "Gossip changes queued time",
                },
                registry,
            }.unwrap();
            let channel_errors = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_channel_errors",
                    "Number of total gossip channel errors",
                },
                &["kind", "name"],
                registry,
            }
            .unwrap();
            let broadcast_dropped = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_broadcast_dropped",
                    "Total number of broadcasts that have been dropped due to overflowing the maximum broadcast queue",
                },
                registry,
            }
            .unwrap();
            let broadcast_remaining_burst = p::register_int_gauge_with_registry! {
                opts! {
                    "corrosion_gossip_broadcast_limiter_remaining_burst",
                    "Remaining burst capacity of the broadcast limiter",
                },
                registry,
            }
            .unwrap();
            let broadcast_rate_limited = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_broadcast_rate_limited",
                    "Total number of times that broadcasting has been rate limited",
                },
                registry,
            }
            .unwrap();
            let broadcast_spawned = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_broadcast_spawned",
                    "Total number of broadcast tasks that have been spawned",
                },
                &["kind"],
                registry,
            }
            .unwrap();
            let client_datagram_errors = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_client_datagram_errors",
                    "Number of total client datagram errors that have occurred",
                },
                &["error"],
                registry,
            }
            .unwrap();
            let client_chunks_sent = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_client_total_chunks",
                    "Number of total outgoing client gossip data chunks",
                },
                &["traffic"],
                registry,
            }
            .unwrap();
            let client_bytes_sent = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_client_total_bytes_sent",
                    "Number of total outgoing client bytes",
                },
                &["traffic"],
                registry,
            }
            .unwrap();
            let client_connect_time = p::register_histogram_vec_with_registry! {
                prometheus::histogram_opts! {
                    "corrosion_gossip_client_connect_time",
                    "Gossip client connection time in seconds",
                },
                &["traffic"],
                registry,
            }.unwrap();
            let client_connect_errors = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_client_connect_errors",
                    "Number of total gossip client connection errors",
                },
                &["traffic", "error"],
                registry,
            }
            .unwrap();
            let member_removed = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_member_removed",
                    "Number of total cluster membership removals",
                },
                &["reason"],
                registry,
            }
            .unwrap();
            let member_added = p::register_int_counter_with_registry! {
                opts! {
                    "corrosion_gossip_member_added",
                    "Number of total cluster membership additions",
                },
                registry,
            }
            .unwrap();
            let swim_notifications = p::register_int_counter_vec_with_registry! {
                opts! {
                    "corrosion_gossip_swim_notifications",
                    "Number of total SWIM notifications",
                },
                &["kind"],
                registry,
            }
            .unwrap();

            Self {
                active_conns,
                total_conns,
                failed_handshakes,
                total_datagrams,
                total_datagram_bytes,
                failed_datagrams,
                total_streams,
                active_streams,
                total_stream_bytes,
                failed_streams,
                sync_changes,
                sync_requests,
                change_batches,
                change_batch_size,
                change_dropped,
                change_received,
                change_bx_duplicate,
                change_clock,
                change_bx_received,
                change_recv_lag,
                changes_in_queue,
                changesets_in_queue,
                changes_committed,
                changes_processed_time,
                changes_processed_chunk_size,
                changes_queued_time,
                channel_errors,
                broadcast_dropped,
                broadcast_remaining_burst,
                broadcast_rate_limited,
                broadcast_spawned,
                client_datagram_errors,
                client_chunks_sent,
                client_bytes_sent,
                client_connect_time,
                client_connect_errors,
                member_removed,
                member_added,
                swim_notifications,
            }
        })
    }

    #[inline]
    pub fn server_conns_inc(&self) {
        self.active_conns.inc();
        self.total_conns.inc();
    }

    #[inline]
    pub fn server_conns_dec(&self) {
        self.active_conns.dec();
    }

    #[inline]
    pub fn server_handshakes_failed_inc(&self, error: &quinn::ConnectionError) {
        self.failed_handshakes
            .with_label_values(&[connection_error_to_string(error)])
            .inc();
    }

    #[inline]
    pub fn server_datagrams_inc(&self, datagram: &bytes::Bytes) {
        self.total_datagrams.inc();
        self.total_datagram_bytes.inc_by(datagram.len() as _);
    }

    #[inline]
    pub fn server_datagrams_failed_inc(&self, error: &quinn::ConnectionError) {
        self.failed_datagrams
            .with_label_values(&[connection_error_to_string(error)])
            .inc();
    }

    #[inline]
    pub fn server_streams_failed_inc(&self, error: &quinn::ConnectionError, kind: quinn::Dir) {
        self.failed_streams
            .with_label_values(&[connection_error_to_string(error), dir_to_string(kind)])
            .inc();
    }

    #[inline]
    pub fn server_streams_inc(&self, dir: quinn::Dir) {
        let dir = dir_to_string(dir);
        self.total_streams.with_label_values(&[dir]).inc();
        self.active_streams.with_label_values(&[dir]).inc();
    }

    #[inline]
    pub fn server_streams_dec(&self, dir: quinn::Dir) {
        self.active_streams
            .with_label_values(&[dir_to_string(dir)])
            .dec();
    }

    #[inline]
    pub fn stream_bytes_inc(
        &self,
        len: u64,
        stream_kind: quinn::Dir,
        dir: Direction,
        traffic: TrafficClass,
    ) {
        self.total_stream_bytes
            .with_label_values(&[dir_to_string(stream_kind), dir.to_str(), traffic.as_str()])
            .inc_by(len);
    }

    #[inline]
    pub fn sync_changes_inc(&self, len: u64, dir: Direction) {
        self.sync_changes
            .with_label_values(&[dir.to_str()])
            .inc_by(len);
    }

    #[inline]
    pub fn sync_requests_inc(&self, len: u64, dir: Direction) {
        self.sync_requests
            .with_label_values(&[dir.to_str()])
            .inc_by(len);
    }

    #[inline]
    pub fn change_batches_inc(&self, reason: &'static str) {
        self.change_batches.with_label_values(&[reason]).inc();
    }

    #[inline]
    pub fn change_batch_size(&self, size: usize) {
        self.change_batch_size.set(size as _);
    }

    #[inline]
    pub fn change_received_inc(&self, size: usize) {
        self.change_received.inc_by(size as _);
    }

    #[inline]
    pub fn change_dropped_inc(&self) {
        self.change_dropped.inc();
    }

    #[inline]
    pub fn change_broadcast_duplicate_inc(&self, from: &'static str) {
        self.change_bx_duplicate.with_label_values(&[from]).inc();
    }

    #[inline]
    pub fn change_clock_inc(&self, src: &'static str) {
        self.change_clock.with_label_values(&[src]).inc();
    }

    #[inline]
    pub fn change_broadcast_received_inc(&self, kind: &'static str) {
        self.change_bx_received.with_label_values(&[kind]).inc();
    }

    #[inline]
    pub fn change_recv_lag_sample(&self, lag: f64, src: &'static str) {
        self.change_recv_lag.with_label_values(&[src]).observe(lag);
    }

    #[inline]
    pub fn changes_queued(&self, cost: usize, changesets: usize) {
        self.changes_in_queue.set(cost as _);
        self.changesets_in_queue.set(changesets as _);
    }

    #[inline]
    pub fn changes_committed_inc(&self, table: &str, count: u64, source: &'static str) {
        self.changes_committed
            .with_label_values(&[table, source])
            .inc_by(count);
    }

    #[inline]
    pub fn changes_processed(&self, elapsed: Duration, source: &'static str, count: Option<usize>) {
        self.changes_processed_time
            .with_label_values(&[source])
            .observe(elapsed.as_secs_f64());
        if let Some(count) = count {
            self.changes_processed_chunk_size.observe(count as _);
        }
    }

    #[inline]
    pub fn changes_queued_time(&self, elapsed: Duration) {
        self.changes_queued_time.observe(elapsed.as_secs_f64());
    }

    #[inline]
    pub fn channel_error_inc(&self, kind: &'static str, name: &'static str) {
        self.channel_errors.with_label_values(&[kind, name]).inc();
    }

    #[inline]
    pub fn broadcast_dropped_inc(&self) {
        self.broadcast_dropped.inc();
    }

    #[inline]
    pub fn broadcast_remaining_burst(&self, remaining: u32) {
        self.broadcast_remaining_burst.set(remaining as _);
    }

    #[inline]
    pub fn broadcast_rate_limited_inc(&self) {
        self.broadcast_rate_limited.inc();
    }

    #[inline]
    pub fn broadcast_spawned_inc(&self, kind: &'static str, count: u32) {
        self.broadcast_spawned
            .with_label_values(&[kind])
            .inc_by(count as _);
    }

    #[inline]
    pub fn client_connect_time(&self, elapsed: Duration, traffic: TrafficClass) {
        self.client_connect_time
            .with_label_values(&[traffic.as_str()])
            .observe(elapsed.as_secs_f64());
    }

    #[inline]
    pub fn client_connect_error(&self, traffic: TrafficClass, error: &'static str) {
        self.client_connect_errors
            .with_label_values(&[traffic.as_str(), error])
            .inc();
    }

    #[inline]
    pub fn client_datagram_errors_inc(&self, error: &'static str) {
        self.client_datagram_errors
            .with_label_values(&[error])
            .inc();
    }

    #[inline]
    pub fn client_chunks_sent_inc(&self, len: usize, traffic: TrafficClass) {
        let ts = traffic.as_str();

        self.client_chunks_sent.with_label_values(&[ts]).inc();
        self.client_bytes_sent
            .with_label_values(&[ts])
            .inc_by(len as _);
    }

    #[inline]
    pub fn member_removed_inc(&self, reason: &str) {
        self.member_removed.with_label_values(&[reason]).inc();
    }

    #[inline]
    pub fn member_added_inc(&self) {
        self.member_added.inc();
    }

    #[inline]
    pub fn swim_notification_inc(&self, kind: &str) {
        self.swim_notifications.with_label_values(&[kind]).inc();
    }
}

#[inline]
fn dir_to_string(dir: quinn::Dir) -> &'static str {
    match dir {
        quinn::Dir::Uni => "uni",
        quinn::Dir::Bi => "bi",
    }
}

#[inline]
fn connection_error_to_string(error: &quinn::ConnectionError) -> &'static str {
    #[allow(clippy::enum_glob_use)]
    use quinn::ConnectionError::*;

    match error {
        VersionMismatch => "version_mismatch",
        TransportError(_) => "transport_error",
        ConnectionClosed(_) => "connection_closed",
        ApplicationClosed(_) => "application_closed",
        Reset => "reset",
        TimedOut => "timed_out",
        LocallyClosed => "locally_closed",
        CidsExhausted => "cids_exhausted",
    }
}
