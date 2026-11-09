use super::{LinkLayer, PacketWrapper};
use crate::metrics::{self, DropReason};
use quilkin_xdp::{
    l2_cache,
    xdp::{
        self, Packet, Umem,
        packet::net_types::MacAddress,
        slab::{HeapSlab, Slab},
    },
};

pub struct Swap;

impl LinkLayer for Swap {
    #[inline]
    fn try_fill(
        &mut self,
        mut packet: PacketWrapper,
        tx_slab: &mut HeapSlab,
        _to_client: bool,
    ) -> Option<(Packet, DropReason)> {
        packet.headers.eth = packet.headers.eth.swapped();
        // All the packet headers have been written, so we only update the eth header here
        packet
            .buffer
            .write(0, packet.headers.eth)
            .expect("unreachable");
        tx_slab
            .push_front(packet.buffer)
            .map(|p| (p, DropReason::QueueFull))
    }
}

use l2_cache::router::CacheQueue;

pub(super) struct Local {
    cache: std::sync::Arc<l2_cache::L2Cache>,
    ipv6_gateway: MacAddress,
    ipv4_gateway: MacAddress,
}

impl Local {
    pub(super) fn new(cache: std::sync::Arc<l2_cache::L2Cache>) -> Self {
        Self {
            cache,
            ipv4_gateway: MacAddress([0; 6]),
            ipv6_gateway: MacAddress([0; 6]),
        }
    }

    #[inline]
    pub fn set_destination(
        packet: &mut Packet,
        eth: &mut xdp::packet::net_types::EthHdr,
        dest: MacAddress,
    ) {
        eth.source = eth.destination;
        eth.destination = dest;

        // All the packet headers have been written, so we only update the eth header here
        packet.write(0, *eth).expect("unreachable");
    }
}

impl LinkLayer for Local {
    #[inline]
    fn update_gateway(&mut self, addr: MacAddress, v4: bool) {
        if v4 {
            self.ipv4_gateway = addr;
        } else {
            self.ipv6_gateway = addr;
        }
    }

    fn try_fill(
        &mut self,
        mut packet: PacketWrapper,
        tx_slab: &mut HeapSlab,
        umem: &mut Umem,
        to_client: bool,
    ) -> Option<DropReason> {
        if to_client {
            let addr = if packet.headers.is_ipv4() {
                self.ipv4_gateway
            } else {
                self.ipv6_gateway
            };
            Self::set_destination(&mut packet.buffer, &mut packet.headers.eth, addr);
            return tx_slab.push_front(packet.buffer).map(|p| {
                umem.free_packet(p);
                DropReason::QueueFull
            });
        }

        let Some(lla) = self.cache.cached_or_send(&packet.buffer, &packet.headers) else {
            umem.free_packet(p);
            return None;
        };

        match lla {
            l2_cache::types::LinkLayerAddr::Known(addr) => {
                Self::set_destination(&mut packet.buffer, &mut packet.headers.eth, addr);
                tx_slab.push_front(packet.buffer).map(|p| {
                    umem.free_packet(p);
                    DropReason::QueueFull
                })
            }
            l2_cache::types::LinkLayerAddr::Unreachable => {
                umem.free_packet(p);
                Some(DropReason::UnreachableIp)
            }
        }
    }

    fn update(&mut self, tx_slab: &mut HeapSlab, umem: &mut xdp::Umem) {
        if !self.overflow.is_empty() && tx_slab.available() > 0 {
            while let Some(of) = self.overflow.pop_front() {
                if let Some(nospace) = tx_slab.push_front(of) {
                    self.overflow.push_front(nospace);
                    break;
                }
            }
        }

        // Call this even if the tx slab is full, it dequeues from a channel that we never want to reach capacity
        let before = self.overflow.len();
        self.queue.update(
            tx_slab,
            umem,
            |packet, _headers| {
                // We could emit a debug/trace log here noting the IP that's been resolved, but cannot yet be sent
                // due to the tx slab being full, but could be quite spammy
                self.overflow.push_back(packet);
            },
            |ip, count| {
                metrics::packets_dropped(
                    metrics::Direction::Read,
                    metrics::DropReason::UnreachableIp,
                )
                .inc_by(count as _);

                tracing::warn!(%ip, count, "failed to resolve layer 2 address for IP");
            },
        );

        // We only add to the queue so don't need to worry about wrapping
        let count = self.overflow.len() - before;
        if count > 0 {
            tracing::warn!(
                count,
                "layer 2 address resolved but packet were added to overflow due to full tx queue"
            );
        }
    }
}
