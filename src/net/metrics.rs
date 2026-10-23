use std::sync::LazyLock;

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Unit,
};

use crate::metrics::{Direction, DropReason, LocalityLabel};

const PACKET_SIZE_BUCKETS: [f64; 9] = [32., 64., 128., 192., 256., 512., 768., 1024., 1280.];

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ForwardedLabels {
    direction: Direction,
    endpoint_locality: LocalityLabel,
}

// Smaller labelset for histogram stats to keep cardinality down
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ForwardedHistogramLabels {
    direction: Direction,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DroppedLabels {
    direction: Direction,
    reason: DropReason,
    filter: &'static str,
    endpoint_locality: LocalityLabel,
}

struct UdpMetrics {
    forwarded_packets: Family<ForwardedLabels, Counter>,
    forwarded_bytes: Family<ForwardedLabels, Counter>,
    forwarded_payload_size_bytes: Family<ForwardedHistogramLabels, Histogram>,
    dropped_packets: Family<DroppedLabels, Counter>,
    dropped_bytes: Family<DroppedLabels, Counter>,
}

#[inline]
fn get_udp_metrics() -> &'static UdpMetrics {
    static UDP_METRICS: LazyLock<UdpMetrics> = LazyLock::new(|| UdpMetrics {
        forwarded_packets: <_>::default(),
        forwarded_bytes: <_>::default(),
        forwarded_payload_size_bytes:
            Family::<ForwardedHistogramLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(PACKET_SIZE_BUCKETS)
            }),
        dropped_packets: <_>::default(),
        dropped_bytes: <_>::default(),
    });

    &UDP_METRICS
}

pub fn register_metrics(registry: &mut prometheus_client::registry::Registry) {
    let UdpMetrics {
        forwarded_packets,
        forwarded_bytes,
        forwarded_payload_size_bytes,
        dropped_packets,
        dropped_bytes,
    } = get_udp_metrics();

    registry.register(
        "quilkin_udp_forwarded_packets",
        "Total number of forwarded packets",
        forwarded_packets.clone(),
    );
    registry.register_with_unit(
        "quilkin_udp_forwarded",
        "Total number of forwarded bytes",
        Unit::Bytes,
        forwarded_bytes.clone(),
    );
    registry.register_with_unit(
        "quilkin_udp_forwarded_payload_size",
        "Histogram of the payload sizes of forwarded packets, in bytes",
        Unit::Bytes,
        forwarded_payload_size_bytes.clone(),
    );
    registry.register(
        "quilkin_udp_dropped_packets",
        "Total number of dropped packets",
        dropped_packets.clone(),
    );
    registry.register_with_unit(
        "quilkin_udp_dropped",
        "Total number of dropped bytes. Payload for valid UDP, invalid packets might include headers",
        Unit::Bytes,
        dropped_bytes.clone(),
    );
}

#[inline]
pub fn observe_forwarded_packet(
    direction: Direction,
    endpoint_locality: &str,
    payload_size: usize,
) {
    let udp = get_udp_metrics();
    let forwarded_labels = ForwardedLabels {
        direction,
        endpoint_locality: LocalityLabel(endpoint_locality.into()),
    };
    udp.forwarded_packets.get_or_create(&forwarded_labels).inc();
    udp.forwarded_bytes
        .get_or_create(&forwarded_labels)
        .inc_by(payload_size as u64);
    udp.forwarded_payload_size_bytes
        .get_or_create(&ForwardedHistogramLabels { direction })
        .observe(payload_size as f64);
}

#[inline]
pub fn observe_invalid_packet(direction: Direction, size: usize) {
    observe_dropped_packet_with_filter(
        direction,
        "",
        size,
        crate::metrics::DropReason::InvalidPacket,
        "",
    );
}

#[inline]
pub fn observe_dropped_packet(
    direction: Direction,
    endpoint_locality: &str,
    payload_size: usize,
    reason: DropReason,
) {
    observe_dropped_packet_with_filter(direction, endpoint_locality, payload_size, reason, "");
}

#[inline]
pub fn observe_dropped_packet_with_filter(
    direction: Direction,
    endpoint_locality: &str,
    payload_size: usize,
    reason: DropReason,
    filter: &'static str,
) {
    let udp = get_udp_metrics();
    let dropped_labels = DroppedLabels {
        direction,
        endpoint_locality: LocalityLabel(endpoint_locality.into()),
        reason,
        filter,
    };
    udp.dropped_packets.get_or_create(&dropped_labels).inc();
    udp.dropped_bytes
        .get_or_create(&dropped_labels)
        .inc_by(payload_size as u64);
}
