use super::*;
use xdp::{
    Packet,
    packet::net_types::{MacAddress, UdpHeaders},
    slab::Slab,
};

/// Queues packets whose destination IP has an unresolved layer 2 address
pub struct CacheQueue {
    cache: std::sync::Arc<L2Cache>,
    rx: super::CacheRx,
    queued: Vec<(Packet, UdpHeaders)>,
    rxid: u8,
}

pub enum Rejected {
    /// The link layer address could not be determined for the IP
    Unreachable,
    /// The TX slab was full
    Full,
}

impl CacheQueue {
    #[inline]
    pub fn new(cache: Arc<L2Cache>, rx: CacheRx, rxid: u8) -> Self {
        Self {
            cache,
            rx,
            rxid,
            queued: Vec::new(),
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

    /// Attempts to set the destination layer 2 address based on the destination IP if it is known, otherwise we queue
    /// the packet to be sent and initiate discovery of the layer 2 address
    pub fn try_fill<const N: usize>(
        &mut self,
        mut packet: Packet,
        mut headers: UdpHeaders,
        tx_slab: &mut xdp::slab::StackSlab<N>,
    ) -> Option<(Rejected, Packet)> {
        let Some(ll) = self
            .cache
            .mac_for_ip(headers.ip.destination_addr(), self.rxid)
        else {
            self.queued.push((packet, headers));
            return None;
        };

        let LinkLayerAddr::Known(mac) = ll else {
            return Some((Rejected::Unreachable, packet));
        };

        Self::set_destination(&mut packet, &mut headers.eth, mac);

        tx_slab.push_front(packet).map(|p| (Rejected::Full, p))
    }

    /// Dequeues notifications from the cache of resolved layer 2 addresses, attempting to enqueue packets for send that
    /// have been resolved, or freeing packets back to the umem if we failed to resolve the IP to a layer 2 address
    ///
    /// The `failed` callback is invoked when we fail to resolve the IP's layer 2 address, along with the number of
    /// individual packets that were thus freed without being enqueued
    ///
    /// Note that packets are not
    pub fn update<const N: usize>(
        &mut self,
        tx_slab: &mut xdp::slab::StackSlab<N>,
        umem: &mut xdp::Umem,
        overflow: impl Fn(Packet, UdpHeaders),
        failed: impl Fn(Ip, usize),
    ) {
        // Even if we don't have space in the slab for sends we want to dequeue all of the items currently in the channel
        // to avoid filling it
        while let Ok((ip, addr)) = self.rx.try_recv() {
            let addr = if let LinkLayerAddr::Known(mac) = addr {
                Some(mac)
            } else {
                None
            };
            let mut off = 0;
            let mut count = 0;

            while let Some(pos) = self
                .queued
                .iter()
                .skip(off)
                .position(|(_, hdrs)| ip == hdrs.ip.destination_addr())
            {
                let (mut packet, mut hdrs) = self.queued.swap_remove(pos + off);

                if let Some(dest) = addr {
                    Self::set_destination(&mut packet, &mut hdrs.eth, dest);

                    if let Some(buff) = tx_slab.push_front(packet) {
                        overflow(buff, hdrs);
                    }
                } else {
                    count += 1;
                    umem.free_packet(packet);
                }

                off += pos;
            }

            if count > 0 {
                failed(ip, count);
            }
        }
    }
}
