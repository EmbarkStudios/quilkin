//! Next hop MACs for frames sent through `AF_XDP`, which bypasses the kernel's
//! neighbor resolution. They are read from the kernel's routing and neighbor
//! tables over rtnetlink, and the kernel is asked to resolve the ones it hasn't,
//! as nothing else on this path would trigger ARP or NDP for them. Until a next
//! hop resolves its frames are sent through the kernel's own stack instead.

use quilkin_xdp::xdp::packet::net_types::{EthHdr, MacAddress};
use std::{
    collections::HashMap,
    io,
    net::IpAddr,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

/// Entries are revalidated after this long, and dropped when unused for twice it
const REFRESH: i64 = 30 * 1_000_000_000;
/// How long to wait before asking the kernel about an unresolved entry again
const RETRY: i64 = 1_000_000_000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Route {
    /// The gateway, or the destination itself when it is on link
    pub next_hop: IpAddr,
    pub ifindex: u32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Neighbor {
    Resolved(MacAddress),
    /// Resolved, but the kernel is reconfirming it, so it may be about to change
    Stale(MacAddress),
    /// Entry without a link address, resolution is in progress or has failed
    Unresolved,
    Missing,
}

/// The kernel tables a next hop is read from
pub trait Tables {
    fn route(&mut self, dst: IpAddr) -> io::Result<Option<Route>>;
    fn neighbor(&mut self, ifindex: u32, ip: IpAddr) -> io::Result<Neighbor>;
    fn resolve(&mut self, ifindex: u32, ip: IpAddr) -> io::Result<()>;
}

struct RouteEntry {
    /// `None` when there is no usable route, so it isn't looked up per packet
    next_hop: Option<IpAddr>,
    refresh_at: i64,
    last_used: i64,
}

struct NeighborEntry {
    mac: Option<MacAddress>,
    refresh_at: i64,
}

pub struct NextHopCache<T> {
    tables: T,
    ifindex: u32,
    routes: HashMap<IpAddr, RouteEntry>,
    neighbors: HashMap<IpAddr, NeighborEntry>,
    next_sweep: i64,
}

impl<T: Tables> NextHopCache<T> {
    pub fn new(tables: T, ifindex: u32) -> Self {
        Self {
            tables,
            ifindex,
            routes: HashMap::new(),
            neighbors: HashMap::new(),
            next_sweep: 0,
        }
    }

    pub fn lookup(&mut self, dst: IpAddr, now: i64) -> Option<MacAddress> {
        self.sweep(now);
        let next_hop = self.route(dst, now)?;
        self.neighbor(next_hop, now)
    }

    fn route(&mut self, dst: IpAddr, now: i64) -> Option<IpAddr> {
        if let Some(entry) = self.routes.get_mut(&dst) {
            entry.last_used = now;
            if now < entry.refresh_at {
                return entry.next_hop;
            }
        }

        let (next_hop, refresh) = match self.tables.route(dst) {
            Ok(Some(route)) if route.ifindex == self.ifindex => (Some(route.next_hop), REFRESH),
            Ok(Some(route)) => {
                tracing::debug!(%dst, ifindex = route.ifindex, "route leaves through another interface");
                (None, RETRY)
            }
            Ok(None) => {
                tracing::debug!(%dst, "no route");
                (None, RETRY)
            }
            // A failed read keeps the last known next hop instead of dropping it
            Err(error) => {
                tracing::warn!(%dst, %error, "route lookup failed");
                (
                    self.routes.get(&dst).and_then(|entry| entry.next_hop),
                    RETRY,
                )
            }
        };

        self.routes.insert(
            dst,
            RouteEntry {
                next_hop,
                refresh_at: now + refresh,
                last_used: now,
            },
        );
        next_hop
    }

    fn neighbor(&mut self, ip: IpAddr, now: i64) -> Option<MacAddress> {
        if let Some(entry) = self.neighbors.get(&ip)
            && now < entry.refresh_at
        {
            return entry.mac;
        }

        let (mac, refresh) = match self.tables.neighbor(self.ifindex, ip) {
            Ok(Neighbor::Resolved(mac)) => (Some(mac), REFRESH),
            Ok(Neighbor::Stale(mac)) => (Some(mac), RETRY),
            Ok(_) => (None, RETRY),
            Err(error) => {
                tracing::warn!(%ip, %error, "neighbor lookup failed");
                (self.neighbors.get(&ip).and_then(|entry| entry.mac), RETRY)
            }
        };

        // Resolves a missing entry and keeps a resolved one fresh, the frames
        // sent here never touch the kernel's entry
        if let Err(error) = self.tables.resolve(self.ifindex, ip) {
            tracing::debug!(%ip, %error, "failed to ask the kernel to resolve neighbor");
        }
        if mac.is_none() {
            tracing::debug!(%ip, "next hop not resolved yet");
        }

        self.neighbors.insert(
            ip,
            NeighborEntry {
                mac,
                refresh_at: now + refresh,
            },
        );
        mac
    }

    fn sweep(&mut self, now: i64) {
        if now < self.next_sweep {
            return;
        }
        self.next_sweep = now + REFRESH;
        self.routes
            .retain(|_, entry| now - entry.last_used < REFRESH * 2);
        self.neighbors
            .retain(|_, entry| now < entry.refresh_at + REFRESH);
    }
}

pub fn mac_address(name: &str) -> io::Result<MacAddress> {
    parse_mac(&std::fs::read_to_string(format!(
        "/sys/class/net/{name}/address"
    ))?)
}

/// Parses the `aa:bb:cc:dd:ee:ff` form sysfs reports
fn parse_mac(text: &str) -> io::Result<MacAddress> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, text.trim().to_owned());
    let mut octets = text.trim().split(':');
    let mut mac = [0u8; 6];
    for byte in &mut mac {
        *byte = octets
            .next()
            .and_then(|octet| u8::from_str_radix(octet, 16).ok())
            .ok_or_else(invalid)?;
    }
    if octets.next().is_some() {
        return Err(invalid());
    }
    Ok(MacAddress(mac))
}

/// Sends the frames whose next hop is unresolved through the kernel's stack,
/// which routes them and holds them until it has resolved the next hop, as it
/// would for any socket
pub struct Kernel {
    v4: OwnedFd,
    /// `None` when IPv6 is disabled on the host
    v6: Option<OwnedFd>,
}

impl Kernel {
    pub fn new() -> io::Result<Self> {
        let v6 = raw_socket(libc::AF_INET6)
            .inspect_err(|error| tracing::debug!(%error, "no raw IPv6 socket"))
            .ok();
        Ok(Self {
            v4: raw_socket(libc::AF_INET)?,
            v6,
        })
    }

    /// Sends the IP packet in an ethernet frame, which must have a complete checksum
    pub fn send(&self, frame: &[u8]) -> io::Result<()> {
        let (packet, destination) = ip_packet(frame)?;

        // SAFETY: zeroed sockaddrs are valid
        let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let (fd, len) = match destination {
            IpAddr::V4(ip) => {
                // SAFETY: sockaddr_storage is large enough and aligned for any sockaddr
                let sin = unsafe { &mut *(&raw mut addr).cast::<libc::sockaddr_in>() };
                sin.sin_family = libc::AF_INET as _;
                sin.sin_addr.s_addr = u32::from_ne_bytes(ip.octets());
                (&self.v4, std::mem::size_of::<libc::sockaddr_in>())
            }
            IpAddr::V6(ip) => {
                // SAFETY: sockaddr_storage is large enough and aligned for any sockaddr
                let sin6 = unsafe { &mut *(&raw mut addr).cast::<libc::sockaddr_in6>() };
                sin6.sin6_family = libc::AF_INET6 as _;
                sin6.sin6_addr.s6_addr = ip.octets();
                let fd = self
                    .v6
                    .as_ref()
                    .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))?;
                (fd, std::mem::size_of::<libc::sockaddr_in6>())
            }
        };

        // SAFETY: syscall with a valid fd, buffer and address
        let sent = unsafe {
            libc::sendto(
                fd.as_raw_fd(),
                packet.as_ptr().cast(),
                packet.len(),
                libc::MSG_DONTWAIT,
                (&raw const addr).cast(),
                len as libc::socklen_t,
            )
        };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// The IP packet in an ethernet frame and its destination
fn ip_packet(frame: &[u8]) -> io::Result<(&[u8], IpAddr)> {
    let invalid = || io::Error::from(io::ErrorKind::InvalidInput);
    let packet = frame.get(EthHdr::LEN..).ok_or_else(invalid)?;
    let destination = match packet.first().map(|byte| byte >> 4) {
        Some(4) => {
            IpAddr::from(<[u8; 4]>::try_from(packet.get(16..20).ok_or_else(invalid)?).unwrap())
        }
        Some(6) => {
            IpAddr::from(<[u8; 16]>::try_from(packet.get(24..40).ok_or_else(invalid)?).unwrap())
        }
        _ => return Err(invalid()),
    };
    Ok((packet, destination))
}

/// A raw socket the caller writes complete IP packets to
fn raw_socket(family: i32) -> io::Result<OwnedFd> {
    // SAFETY: syscall with valid arguments
    let fd = unsafe {
        libc::socket(
            family,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            libc::IPPROTO_RAW,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the fd was just opened and is owned by nothing else
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };

    // IPv4 keeps the IP header we supply by default, IPv6 needs IPV6_HDRINCL
    if family == libc::AF_INET6 {
        let on: libc::c_int = 1;
        // SAFETY: syscall with a valid fd and an int option
        let set = unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_HDRINCL,
                (&raw const on).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if set < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(fd)
}

pub use netlink::Netlink;

mod netlink {
    use super::{MacAddress, Neighbor, Route, Tables};
    use std::{
        io,
        net::IpAddr,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
    };

    const HEADER_LEN: usize = 16;
    const ATTR_LEN: usize = 4;
    /// `rtmsg` and `ndmsg` are both twelve bytes
    const MSG_LEN: usize = 12;

    #[inline]
    fn align(len: usize) -> usize {
        (len + 3) & !3
    }

    fn ip_bytes(ip: IpAddr) -> Vec<u8> {
        match ip {
            IpAddr::V4(v4) => v4.octets().to_vec(),
            IpAddr::V6(v6) => v6.octets().to_vec(),
        }
    }

    fn ip_from(family: u8, bytes: &[u8]) -> Option<IpAddr> {
        match (family as i32, bytes.len()) {
            (libc::AF_INET, 4) => Some(IpAddr::from(<[u8; 4]>::try_from(bytes).ok()?)),
            (libc::AF_INET6, 16) => Some(IpAddr::from(<[u8; 16]>::try_from(bytes).ok()?)),
            _ => None,
        }
    }

    fn family(ip: IpAddr) -> u8 {
        if ip.is_ipv4() {
            libc::AF_INET as u8
        } else {
            libc::AF_INET6 as u8
        }
    }

    fn push_attr(buf: &mut Vec<u8>, kind: u16, data: &[u8]) {
        let len = ATTR_LEN + data.len();
        buf.extend_from_slice(&(len as u16).to_ne_bytes());
        buf.extend_from_slice(&kind.to_ne_bytes());
        buf.extend_from_slice(data);
        buf.resize(buf.len() + align(len) - len, 0);
    }

    fn attrs(mut data: &[u8]) -> impl Iterator<Item = (u16, &[u8])> {
        std::iter::from_fn(move || {
            if data.len() < ATTR_LEN {
                return None;
            }
            let len = u16::from_ne_bytes([data[0], data[1]]) as usize;
            let kind = u16::from_ne_bytes([data[2], data[3]]);
            if len < ATTR_LEN || len > data.len() {
                return None;
            }
            let value = &data[ATTR_LEN..len];
            data = &data[align(len).min(data.len())..];
            Some((kind, value))
        })
    }

    fn rtmsg(dst: IpAddr) -> [u8; MSG_LEN] {
        let mut msg = [0u8; MSG_LEN];
        msg[0] = family(dst);
        msg[1] = if dst.is_ipv4() { 32 } else { 128 };
        msg
    }

    fn ndmsg(ip: IpAddr, ifindex: u32, state: u16, flags: u8) -> [u8; MSG_LEN] {
        let mut msg = [0u8; MSG_LEN];
        msg[0] = family(ip);
        msg[4..8].copy_from_slice(&(ifindex as i32).to_ne_bytes());
        msg[8..10].copy_from_slice(&state.to_ne_bytes());
        msg[10] = flags;
        msg
    }

    /// The next hop and interface in an `RTM_NEWROUTE` reply for `dst`
    pub(super) fn parse_route(msg: &[u8], dst: IpAddr) -> Option<Route> {
        let family = *msg.first()?;
        let mut next_hop = dst;
        let mut ifindex = None;
        for (kind, value) in attrs(msg.get(MSG_LEN..)?) {
            match kind {
                libc::RTA_GATEWAY => next_hop = ip_from(family, value)?,
                libc::RTA_OIF => {
                    ifindex = Some(i32::from_ne_bytes(<[u8; 4]>::try_from(value).ok()?) as u32);
                }
                _ => {}
            }
        }
        Some(Route {
            next_hop,
            ifindex: ifindex?,
        })
    }

    /// The entry an `RTM_NEWNEIGH` message describes if it is `ip` on `ifindex`
    pub(super) fn parse_neighbor(msg: &[u8], ifindex: u32, ip: IpAddr) -> Option<Neighbor> {
        const CONFIRMED: u16 = libc::NUD_PERMANENT | libc::NUD_NOARP | libc::NUD_REACHABLE;
        const RECONFIRMING: u16 = libc::NUD_STALE | libc::NUD_DELAY | libc::NUD_PROBE;

        let family = *msg.first()?;
        let entry_ifindex = i32::from_ne_bytes(<[u8; 4]>::try_from(msg.get(4..8)?).ok()?) as u32;
        let state = u16::from_ne_bytes([*msg.get(8)?, *msg.get(9)?]);
        if entry_ifindex != ifindex {
            return None;
        }

        let mut mac = None;
        let mut matches = false;
        for (kind, value) in attrs(msg.get(MSG_LEN..)?) {
            match kind {
                libc::NDA_DST => matches = ip_from(family, value) == Some(ip),
                libc::NDA_LLADDR => mac = <[u8; 6]>::try_from(value).ok().map(MacAddress),
                _ => {}
            }
        }

        if !matches {
            return None;
        }
        Some(match mac {
            Some(mac) if state & CONFIRMED != 0 => Neighbor::Resolved(mac),
            Some(mac) if state & RECONFIRMING != 0 => Neighbor::Stale(mac),
            _ => Neighbor::Unresolved,
        })
    }

    /// An rtnetlink socket, one per worker as requests are serialized on it
    pub struct Netlink {
        fd: OwnedFd,
        seq: u32,
        buf: Vec<u8>,
    }

    impl Netlink {
        pub fn new() -> io::Result<Self> {
            // SAFETY: syscalls with valid arguments
            let fd = unsafe {
                let fd = libc::socket(
                    libc::AF_NETLINK,
                    libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                    libc::NETLINK_ROUTE,
                );
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                OwnedFd::from_raw_fd(fd)
            };

            Ok(Self {
                fd,
                seq: 0,
                buf: vec![0; 64 * 1024],
            })
        }

        /// Sends one request and returns the payload of every reply to it
        fn request(&mut self, kind: u16, flags: u16, body: &[u8]) -> io::Result<Vec<Vec<u8>>> {
            self.seq = self.seq.wrapping_add(1);
            let msg = message(kind, flags, self.seq, body);

            // SAFETY: syscall with a valid fd and buffer
            let sent =
                unsafe { libc::send(self.fd.as_raw_fd(), msg.as_ptr().cast(), msg.len(), 0) };
            if sent != msg.len() as isize {
                return Err(io::Error::last_os_error());
            }

            let mut replies = Vec::new();
            loop {
                // SAFETY: syscall with a valid fd and buffer
                let received = unsafe {
                    libc::recv(
                        self.fd.as_raw_fd(),
                        self.buf.as_mut_ptr().cast(),
                        self.buf.len(),
                        libc::MSG_DONTWAIT,
                    )
                };
                if received < 0 {
                    return Err(io::Error::last_os_error());
                }

                if collect(&self.buf[..received as usize], self.seq, &mut replies)? {
                    return Ok(replies);
                }
            }
        }
    }

    fn message(kind: u16, flags: u16, seq: u32, body: &[u8]) -> Vec<u8> {
        let mut msg = Vec::with_capacity(HEADER_LEN + body.len());
        msg.extend_from_slice(&((HEADER_LEN + body.len()) as u32).to_ne_bytes());
        msg.extend_from_slice(&kind.to_ne_bytes());
        msg.extend_from_slice(&flags.to_ne_bytes());
        msg.extend_from_slice(&seq.to_ne_bytes());
        msg.extend_from_slice(&0u32.to_ne_bytes());
        msg.extend_from_slice(body);
        msg
    }

    /// Adds the payloads in one received buffer that answer `seq` to `replies`,
    /// returning whether the reply is complete. Messages left over from an
    /// earlier request are skipped, so they can't be taken for this reply
    fn collect(mut data: &[u8], seq: u32, replies: &mut Vec<Vec<u8>>) -> io::Result<bool> {
        let mut complete = false;
        while data.len() >= HEADER_LEN {
            let len = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let kind = u16::from_ne_bytes([data[4], data[5]]);
            let flags = u16::from_ne_bytes([data[6], data[7]]);
            let msg_seq = u32::from_ne_bytes([data[8], data[9], data[10], data[11]]);
            if len < HEADER_LEN || len > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated netlink message",
                ));
            }
            let payload = &data[HEADER_LEN..len];
            data = &data[align(len).min(data.len())..];

            if msg_seq != seq {
                continue;
            }
            match kind as i32 {
                libc::NLMSG_DONE => return Ok(true),
                libc::NLMSG_ERROR => {
                    let errno = payload.get(..4).map_or(-libc::EINVAL, |b| {
                        i32::from_ne_bytes([b[0], b[1], b[2], b[3]])
                    });
                    return if errno == 0 {
                        Ok(true)
                    } else {
                        Err(io::Error::from_raw_os_error(-errno))
                    };
                }
                _ => {
                    replies.push(payload.to_vec());
                    complete |= flags & libc::NLM_F_MULTI as u16 == 0;
                }
            }
        }
        Ok(complete)
    }

    impl Tables for Netlink {
        fn route(&mut self, dst: IpAddr) -> io::Result<Option<Route>> {
            let mut body = rtmsg(dst).to_vec();
            push_attr(&mut body, libc::RTA_DST, &ip_bytes(dst));

            match self.request(libc::RTM_GETROUTE, libc::NLM_F_REQUEST as u16, &body) {
                Ok(replies) => Ok(replies.first().and_then(|msg| parse_route(msg, dst))),
                Err(error) if error.raw_os_error() == Some(libc::ENETUNREACH) => Ok(None),
                Err(error) if error.raw_os_error() == Some(libc::EHOSTUNREACH) => Ok(None),
                Err(error) => Err(error),
            }
        }

        fn neighbor(&mut self, ifindex: u32, ip: IpAddr) -> io::Result<Neighbor> {
            let mut body = ndmsg(ip, ifindex, 0, 0).to_vec();
            push_attr(&mut body, libc::NDA_DST, &ip_bytes(ip));

            match self.request(libc::RTM_GETNEIGH, libc::NLM_F_REQUEST as u16, &body) {
                Ok(replies) => Ok(replies
                    .iter()
                    .find_map(|msg| parse_neighbor(msg, ifindex, ip))
                    .unwrap_or(Neighbor::Missing)),
                Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(Neighbor::Missing),
                Err(error) => Err(error),
            }
        }

        fn resolve(&mut self, ifindex: u32, ip: IpAddr) -> io::Result<()> {
            let mut body = ndmsg(ip, ifindex, libc::NUD_NONE, libc::NTF_USE).to_vec();
            push_attr(&mut body, libc::NDA_DST, &ip_bytes(ip));
            self.request(
                libc::RTM_NEWNEIGH,
                (libc::NLM_F_REQUEST | libc::NLM_F_CREATE | libc::NLM_F_ACK) as u16,
                &body,
            )
            .map(drop)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn msg(head: [u8; MSG_LEN], attrs: &[(u16, &[u8])]) -> Vec<u8> {
            let mut msg = head.to_vec();
            for (kind, value) in attrs {
                push_attr(&mut msg, *kind, value);
            }
            msg
        }

        #[test]
        fn attrs_are_four_byte_aligned() {
            let mut buf = Vec::new();
            push_attr(&mut buf, 1, &[0xaa; 6]);
            push_attr(&mut buf, 2, &[0xbb; 4]);
            assert_eq!(buf.len(), 12 + 8);

            let parsed: Vec<_> = attrs(&buf).collect();
            assert_eq!(parsed, [(1, &[0xaa; 6][..]), (2, &[0xbb; 4][..])]);
        }

        #[test]
        fn route_via_gateway() {
            let dst = IpAddr::from([10, 10, 167, 136]);
            let reply = msg(
                rtmsg(dst),
                &[
                    (libc::RTA_DST, &[10, 10, 167, 136]),
                    (libc::RTA_GATEWAY, &[10, 10, 96, 1]),
                    (libc::RTA_OIF, &2i32.to_ne_bytes()),
                ],
            );
            assert_eq!(
                parse_route(&reply, dst),
                Some(Route {
                    next_hop: IpAddr::from([10, 10, 96, 1]),
                    ifindex: 2,
                })
            );

            let dst = IpAddr::from([0x2a05, 0xd012, 0, 0, 0, 0, 0, 0x10]);
            let gateway = IpAddr::from([0xfe80, 0, 0, 0, 0, 0, 0, 1]);
            let reply = msg(
                rtmsg(dst),
                &[
                    (libc::RTA_GATEWAY, &ip_bytes(gateway)),
                    (libc::RTA_OIF, &2i32.to_ne_bytes()),
                ],
            );
            assert_eq!(
                parse_route(&reply, dst),
                Some(Route {
                    next_hop: gateway,
                    ifindex: 2,
                })
            );
        }

        #[test]
        fn route_on_link_uses_the_destination() {
            let dst = IpAddr::from([10, 10, 115, 170]);
            let reply = msg(rtmsg(dst), &[(libc::RTA_OIF, &2i32.to_ne_bytes())]);
            assert_eq!(
                parse_route(&reply, dst),
                Some(Route {
                    next_hop: dst,
                    ifindex: 2,
                })
            );
            assert_eq!(parse_route(&msg(rtmsg(dst), &[]), dst), None);
        }

        #[test]
        fn neighbor_states() {
            let ip = IpAddr::from([10, 10, 96, 1]);
            let mac = [0x0e, 0x14, 0x14, 0x09, 0x70, 0xc9];
            let entry = |ifindex, state, lladdr: Option<&[u8]>| {
                let mut attrs = vec![(libc::NDA_DST, &[10, 10, 96, 1][..])];
                if let Some(lladdr) = lladdr {
                    attrs.push((libc::NDA_LLADDR, lladdr));
                }
                msg(ndmsg(ip, ifindex, state, 0), &attrs)
            };

            assert_eq!(
                parse_neighbor(&entry(2, libc::NUD_REACHABLE, Some(&mac)), 2, ip),
                Some(Neighbor::Resolved(MacAddress(mac)))
            );
            assert_eq!(
                parse_neighbor(&entry(2, libc::NUD_STALE, Some(&mac)), 2, ip),
                Some(Neighbor::Stale(MacAddress(mac)))
            );
            assert_eq!(
                parse_neighbor(&entry(2, libc::NUD_INCOMPLETE, None), 2, ip),
                Some(Neighbor::Unresolved)
            );
            assert_eq!(
                parse_neighbor(&entry(2, libc::NUD_FAILED, Some(&mac)), 2, ip),
                Some(Neighbor::Unresolved)
            );
            // Other interfaces and addresses are skipped
            assert_eq!(
                parse_neighbor(&entry(3, libc::NUD_REACHABLE, Some(&mac)), 2, ip),
                None
            );
            assert_eq!(
                parse_neighbor(
                    &entry(2, libc::NUD_REACHABLE, Some(&mac)),
                    2,
                    IpAddr::from([10, 10, 96, 2])
                ),
                None
            );
        }

        fn error(seq: u32, errno: i32) -> Vec<u8> {
            message(libc::NLMSG_ERROR as u16, 0, seq, &errno.to_ne_bytes())
        }

        #[test]
        fn single_reply() {
            let mut replies = Vec::new();
            let buf = message(libc::RTM_NEWROUTE, 0, 7, &[1, 2, 3, 4]);
            assert!(collect(&buf, 7, &mut replies).unwrap());
            assert_eq!(replies, [vec![1, 2, 3, 4]]);
        }

        #[test]
        fn multipart_reply_ends_with_done() {
            let multi = libc::NLM_F_MULTI as u16;
            let mut replies = Vec::new();

            let mut buf = message(libc::RTM_NEWNEIGH, multi, 7, &[1; 4]);
            buf.extend(message(libc::RTM_NEWNEIGH, multi, 7, &[2; 4]));
            assert!(!collect(&buf, 7, &mut replies).unwrap());

            let mut buf = message(libc::RTM_NEWNEIGH, multi, 7, &[3; 4]);
            buf.extend(message(libc::NLMSG_DONE as u16, multi, 7, &[0; 4]));
            assert!(collect(&buf, 7, &mut replies).unwrap());
            assert_eq!(replies, [[1; 4], [2; 4], [3; 4]]);
        }

        #[test]
        fn acks_and_errors() {
            let mut replies = Vec::new();
            assert!(collect(&error(7, 0), 7, &mut replies).unwrap());
            assert!(replies.is_empty());

            let err = collect(&error(7, -libc::ENOENT), 7, &mut replies).unwrap_err();
            assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
        }

        #[test]
        fn leftovers_from_an_earlier_request_are_skipped() {
            let mut replies = Vec::new();

            // A buffer holding only an earlier reply leaves this one incomplete
            let stale = message(libc::RTM_NEWROUTE, 0, 6, &[6; 4]);
            assert!(!collect(&stale, 7, &mut replies).unwrap());
            assert!(!collect(&error(6, -libc::ENOENT), 7, &mut replies).unwrap());
            assert!(replies.is_empty());

            let mut buf = stale;
            buf.extend(message(libc::RTM_NEWROUTE, 0, 7, &[7; 4]));
            assert!(collect(&buf, 7, &mut replies).unwrap());
            assert_eq!(replies, [[7; 4]]);
        }

        #[test]
        fn truncated_messages_are_rejected() {
            let mut buf = message(libc::RTM_NEWROUTE, 0, 7, &[1; 8]);
            buf.truncate(HEADER_LEN + 4);
            assert!(collect(&buf, 7, &mut Vec::new()).is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    const IFINDEX: u32 = 2;
    const GATEWAY: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 10, 96, 1));
    const GATEWAY_MAC: MacAddress = MacAddress([0x0e, 0x14, 0x14, 0x09, 0x70, 0xc9]);
    const PEER: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 10, 115, 170));
    const PEER_MAC: MacAddress = MacAddress([0x0e, 0xff, 0xd5, 0x20, 0x16, 0xcf]);
    const REMOTE: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 10, 167, 136));
    const OTHER_REMOTE: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 10, 167, 137));
    /// Routed through another interface
    const ELSEWHERE: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 10, 200, 1));

    #[derive(Default)]
    struct Fake {
        neighbors: HashMap<IpAddr, Neighbor>,
        failing: bool,
        route_calls: usize,
        neighbor_calls: usize,
        resolution_requests: Vec<IpAddr>,
    }

    type Shared = Rc<RefCell<Fake>>;

    impl Tables for Shared {
        fn route(&mut self, dst: IpAddr) -> io::Result<Option<Route>> {
            let mut fake = self.borrow_mut();
            fake.route_calls += 1;
            if fake.failing {
                return Err(io::ErrorKind::TimedOut.into());
            }
            Ok(match dst {
                PEER => Some(Route {
                    next_hop: PEER,
                    ifindex: IFINDEX,
                }),
                REMOTE | OTHER_REMOTE => Some(Route {
                    next_hop: GATEWAY,
                    ifindex: IFINDEX,
                }),
                ELSEWHERE => Some(Route {
                    next_hop: ELSEWHERE,
                    ifindex: IFINDEX + 1,
                }),
                _ => None,
            })
        }

        fn neighbor(&mut self, _ifindex: u32, ip: IpAddr) -> io::Result<Neighbor> {
            let mut fake = self.borrow_mut();
            fake.neighbor_calls += 1;
            if fake.failing {
                return Err(io::ErrorKind::TimedOut.into());
            }
            Ok(fake
                .neighbors
                .get(&ip)
                .copied()
                .unwrap_or(Neighbor::Missing))
        }

        fn resolve(&mut self, _ifindex: u32, ip: IpAddr) -> io::Result<()> {
            self.borrow_mut().resolution_requests.push(ip);
            Ok(())
        }
    }

    fn cache(neighbors: &[(IpAddr, Neighbor)]) -> (NextHopCache<Shared>, Shared) {
        let fake = Rc::new(RefCell::new(Fake {
            neighbors: neighbors.iter().copied().collect(),
            ..Default::default()
        }));
        (NextHopCache::new(fake.clone(), IFINDEX), fake)
    }

    #[test]
    fn remotes_share_the_gateway_an_on_link_peer_is_its_own_next_hop() {
        let (mut cache, fake) = cache(&[
            (GATEWAY, Neighbor::Resolved(GATEWAY_MAC)),
            (PEER, Neighbor::Resolved(PEER_MAC)),
        ]);
        assert_eq!(cache.lookup(REMOTE, 0), Some(GATEWAY_MAC));
        assert_eq!(cache.lookup(PEER, 0), Some(PEER_MAC));
        // A second remote reuses the gateway's one neighbor entry
        assert_eq!(cache.lookup(OTHER_REMOTE, 0), Some(GATEWAY_MAC));
        assert_eq!(fake.borrow().route_calls, 3);
        assert_eq!(fake.borrow().neighbor_calls, 2);
        assert_eq!(fake.borrow().resolution_requests, [GATEWAY, PEER]);

        // Both tables are read again after the refresh interval
        cache.lookup(REMOTE, REFRESH);
        assert_eq!(fake.borrow().route_calls, 4);
        assert_eq!(fake.borrow().neighbor_calls, 3);
    }

    #[test]
    fn unresolved_and_stale_entries_are_reread_soon() {
        // Unresolved: handed to the kernel, retried after the retry interval
        let (mut unresolved, fake) = cache(&[]);
        assert_eq!(unresolved.lookup(PEER, 0), None);
        assert_eq!(unresolved.lookup(PEER, RETRY / 2), None);
        assert_eq!(fake.borrow().neighbor_calls, 1);
        assert_eq!(fake.borrow().resolution_requests, [PEER]);
        fake.borrow_mut()
            .neighbors
            .insert(PEER, Neighbor::Resolved(PEER_MAC));
        assert_eq!(unresolved.lookup(PEER, RETRY), Some(PEER_MAC));

        // Stale: usable now, re-read after a second to pick up a changed MAC
        let (mut stale, fake) = cache(&[(GATEWAY, Neighbor::Stale(GATEWAY_MAC))]);
        assert_eq!(stale.lookup(REMOTE, 0), Some(GATEWAY_MAC));
        let changed = MacAddress([0xaa; 6]);
        fake.borrow_mut()
            .neighbors
            .insert(GATEWAY, Neighbor::Resolved(changed));
        assert_eq!(stale.lookup(REMOTE, RETRY / 2), Some(GATEWAY_MAC));
        assert_eq!(stale.lookup(REMOTE, RETRY), Some(changed));
        // Resolved now, so back to the longer interval
        stale.lookup(REMOTE, RETRY * 2);
        assert_eq!(fake.borrow().neighbor_calls, 2);
    }

    #[test]
    fn unusable_routes_are_cached() {
        let (mut cache, fake) = cache(&[]);
        for dst in [IpAddr::from([192, 0, 2, 1]), ELSEWHERE] {
            assert_eq!(cache.lookup(dst, 0), None);
            assert_eq!(cache.lookup(dst, 0), None);
        }
        assert_eq!(fake.borrow().route_calls, 2);
        assert!(fake.borrow().resolution_requests.is_empty());
    }

    #[test]
    fn known_entries_survive_a_failed_read() {
        let (mut cache, fake) = cache(&[(GATEWAY, Neighbor::Resolved(GATEWAY_MAC))]);
        assert_eq!(cache.lookup(REMOTE, 0), Some(GATEWAY_MAC));

        fake.borrow_mut().failing = true;
        assert_eq!(cache.lookup(REMOTE, REFRESH), Some(GATEWAY_MAC));
        assert_eq!(cache.lookup(REMOTE, REFRESH + RETRY), Some(GATEWAY_MAC));
        assert_eq!(fake.borrow().neighbor_calls, 3);
        assert_eq!(cache.lookup(PEER, REFRESH), None);
    }

    #[test]
    fn idle_entries_are_swept() {
        let (mut cache, _) = cache(&[
            (GATEWAY, Neighbor::Resolved(GATEWAY_MAC)),
            (PEER, Neighbor::Resolved(PEER_MAC)),
        ]);
        cache.lookup(REMOTE, 0);
        cache.lookup(PEER, REFRESH * 2 - RETRY);

        cache.lookup(IpAddr::from([192, 0, 2, 1]), REFRESH * 3);
        assert!(!cache.routes.contains_key(&REMOTE));
        assert!(!cache.neighbors.contains_key(&GATEWAY));
        assert!(cache.routes.contains_key(&PEER));
        assert!(cache.neighbors.contains_key(&PEER));
    }

    #[test]
    fn mac_address_sysfs_format() {
        assert_eq!(
            parse_mac("0e:ff:f4:8b:7d:73\n").unwrap(),
            MacAddress([0x0e, 0xff, 0xf4, 0x8b, 0x7d, 0x73])
        );
        assert!(parse_mac("0e:ff:f4:8b:7d").is_err());
        assert!(parse_mac("0e:ff:f4:8b:7d:73:00").is_err());
        assert!(parse_mac("zz:ff:f4:8b:7d:73").is_err());
    }

    #[test]
    fn kernel_frames_are_sent_from_the_ip_header() {
        let frame = |ip: &[u8]| [&[0u8; EthHdr::LEN][..], ip].concat();

        let mut v4 = [0u8; 28];
        v4[0] = 0x45;
        v4[16..20].copy_from_slice(&[10, 0, 2, 10]);
        let v4 = frame(&v4);
        let (packet, dst) = ip_packet(&v4).unwrap();
        assert_eq!(packet.len(), 28);
        assert_eq!(dst, IpAddr::from([10, 0, 2, 10]));

        let client = std::net::Ipv6Addr::new(0xfd00, 2, 0, 0, 0, 0, 0, 0x10);
        let mut v6 = [0u8; 48];
        v6[0] = 0x60;
        v6[24..40].copy_from_slice(&client.octets());
        let v6 = frame(&v6);
        assert_eq!(ip_packet(&v6).unwrap().1, IpAddr::V6(client));

        assert!(ip_packet(&[0; EthHdr::LEN]).is_err());
        assert!(ip_packet(&frame(&[0x45; 12])).is_err());
        assert!(ip_packet(&frame(&[0x10; 40])).is_err());
    }
}
