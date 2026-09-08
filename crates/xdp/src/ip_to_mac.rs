use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

#[derive(Hash, Copy, Clone, PartialEq, Eq)]
pub struct Ip(pub Ipv6Addr);

impl fmt::Display for Ip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.octets() {
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d] => {
                write!(f, "{a}.{b}.{c}.{d}")
            }
            _ => {
                write!(f, "{}", self.0)
            }
        }
    }
}

impl PartialEq<IpAddr> for Ip {
    #[inline]
    fn eq(&self, other: &IpAddr) -> bool {
        let octs = self.0.octets();
        match other {
            IpAddr::V4(v4) => {
                octs[12..] == v4.octets()
                    && octs[..12] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff]
            }
            IpAddr::V6(v6) => octs == v6.octets(),
        }
    }
}

impl PartialEq<Ipv4Addr> for Ip {
    #[inline]
    fn eq(&self, other: &Ipv4Addr) -> bool {
        let octs = self.0.octets();
        octs[12..] == other.octets() && octs[..12] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff]
    }
}

impl PartialEq<Ipv6Addr> for Ip {
    #[inline]
    fn eq(&self, other: &Ipv6Addr) -> bool {
        &self.0 == other
    }
}

impl fmt::Debug for Ip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl From<IpAddr> for Ip {
    #[inline]
    fn from(value: IpAddr) -> Self {
        match value {
            IpAddr::V6(v6) => Self(v6),
            IpAddr::V4(v4) => Self(v4.to_ipv6_mapped()),
        }
    }
}

impl From<Ipv4Addr> for Ip {
    #[inline]
    fn from(value: Ipv4Addr) -> Self {
        Self(value.to_ipv6_mapped())
    }
}

impl From<Ipv6Addr> for Ip {
    #[inline]
    fn from(value: Ipv6Addr) -> Self {
        Self(value)
    }
}

#[repr(u16)]
pub enum Source {
    Icmp = 1,
    Icmpv6 = 58,
    NeighbourAdvertisement = 136,
    Arp = 0x0608,
}

#[repr(C)]
pub struct RingEntry {
    pub ip: Ip,
    pub mac: xdp::packet::net_types::MacAddress,
    pub source: Source,
}

#[inline]
pub fn read_entry(ring: &mut aya::maps::RingBuf<aya::maps::MapData>) -> Option<RingEntry> {
    ring.next().and_then(|item| {
        // We're the ones inserting from eBPF, there should never be anything else in here
        // SAFETY: we've verified the size, the caller is responsible for validating the actual contents
        (item.len() == std::mem::size_of::<RingEntry>())
            .then_some(unsafe { std::ptr::read_unaligned(item.as_ptr().cast()) })
    })
}
