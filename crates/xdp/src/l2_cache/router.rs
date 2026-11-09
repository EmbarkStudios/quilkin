use super::*;
use xdp::{
    Packet,
    packet::net_types::{MacAddress, UdpHeaders},
    slab::{HeapSlab, Slab},
};

/// Caches layer 2 addresses, or invokes the kernel's normal routing paths via
/// a non-XDP send
pub struct Router {
    cache: L2Cache,
    socket: std::os::fd::OwnedFd,
}

pub enum Rejected {
    /// The link layer address could not be determined for the IP
    Unreachable,
    /// `sendto` failed
    SendTo(std::io::Error),
}

impl Router {
    #[inline]
    pub fn new(cache: L2Cache) -> std::io::Result<Self> {
        // SAFETY: raw syscalls, all arguments are valid and we check for errors
        let socket = unsafe {
            let socket = libc::socket(libc::AF_INET6, libc::SOCK_RAW, libc::IPPROTO_RAW);
            if socket < 0 {
                return Err(std::io::Error::last_os_error());
            }

            let enable = 1;
            if libc::setsockopt(
                socket,
                libc::IPPROTO_IPV6,
                libc::IPV6_HDRINCL,
                (&raw const enable).cast(),
                std::mem::size_of_val(&enable) as _,
            ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }

            use std::os::fd::FromRawFd;
            std::os::fd::OwnedFd::from_raw_fd(socket)
        };

        Ok(Self { cache, socket })
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
    pub fn try_fill(&mut self, packet: &mut Packet, headers: &mut UdpHeaders) -> Option<Rejected> {
        let Some(ll) = self.cache.mac_for_ip(headers.ip.destination_addr()) else {
            return None;
        };

        let LinkLayerAddr::Known(mac) = ll else {
            return Some(Rejected::Unreachable);
        };

        Self::set_destination(packet, &mut headers.eth, mac);

        None
    }
}
