use super::{LinkLayer, PacketWrapper};

pub struct Swap;

impl LinkLayer for Swap {
    #[inline]
    fn try_fill<const N: usize>(
        &mut self,
        mut packet: PacketWrapper,
        tx_slab: &mut xdp::slab::StackSlab<N>,
        _to_client: bool,
    ) -> Option<Packet> {
        packet.headers.eth = packet.headers.eth.swapped();
        // All the packet headers have been written, so we only update the eth header here
        packet
            .buffer
            .write(0, packet.headers.eth)
            .expect("unreachable");
        tx_slab.push_front(packet.buffer)
    }
}

pub(super) struct Local {
    queue: quilkin_xdp::l2_cache::queue::CacheQueue,
    overflow: std::collections::VecDeque<Packet>,
    ipv6_gateway: MacAddress,
    ipv4_gateway: MacAddress,
}

impl Local {
    pub(super) fn new(cache: Arc<cache::L2Cache>, rx: cache::CacheRx, rxid: u8) -> Self {
        Self {
            queue: quilkin_xdp::l2_cache::queue::CacheQueue::new(cache, rx, rxid),
            overflow: Default::default(),
            ipv4_gateway: MacAddress([0; 6]),
            ipv6_gateway: MacAddress([0; 6]),
        }
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

    fn try_fill<const N: usize>(
        &mut self,
        mut packet: PacketWrapper,
        tx_slab: &mut xdp::slab::StackSlab<N>,
        to_client: bool,
    ) -> Option<Packet> {
        if to_client {
            let addr = if packet.headers.is_ipv4() {
                self.ipv4_gateway
            } else {
                self.ipv6_gateway
            };
            Self::set_destination(&mut packet, addr);
            return tx_slab.push_front(packet.buffer);
        }

        self.queue
            .try_fill(packet.buffer, packet.headers, tx_slab)
            .map(|(rejected, packet)| packet)
    }

    fn update<const N: usize>(
        &mut self,
        tx_slab: &mut xdp::slab::StackSlab<N>,
        umem: &mut xdp::Umem,
    ) {
        if !self.overflow.is_empty() && tx_slab.available() > 0 {
            while let Some(of) = self.overflow.pop_front() {
                if let Some(nospace) = tx_slab.push_front(of) {
                    self.overflow.push_front(nospace);
                    break;
                }
            }
        }
    }
}
