use super::{CacheSpawnError, icmp, io};
use crate::net::io::completion::eventfd;
use io_uring::{
    opcode, squeue,
    types::{Fd, Timespec},
};
use quilkin_xdp::aya;
use std::{
    fmt,
    net::{IpAddr, Ipv6Addr},
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, Instant},
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

impl From<IpAddr> for Ip {
    #[inline]
    fn from(value: IpAddr) -> Self {
        match value {
            IpAddr::V6(v6) => Self(v6),
            IpAddr::V4(v4) => Self(v4.to_ipv6_mapped()),
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
#[repr(transparent)]
pub struct MacAddr(pub [u8; 6]);

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5]
        )
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum LinkLayerAddr {
    Known(MacAddr),
    Unreachable,
}

impl fmt::Display for LinkLayerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Known(mac) => write!(f, "{mac}"),
            Self::Unreachable => f.write_str("<unreachable>"),
        }
    }
}

pub(super) struct RequestSender {
    pub(super) sender: crossbeam_channel::Sender<Ip>,
    pub(super) writer: eventfd::EventFdWriter,
}

impl RequestSender {
    #[inline]
    pub(super) fn send(&self, ip: Ip) {
        if self.sender.send(ip).is_err() {
            // TODO: metrics
        } else {
            self.writer.write(1);
        }
    }
}

pub(super) struct RequestReceiver {
    pub(super) rx: crossbeam_channel::Receiver<Ip>,
    /// File descriptor we use to signal requests have been queued
    pub(super) event: eventfd::EventFd,
}

pub(super) struct EbpfRing {
    inner: aya::maps::RingBuf<aya::maps::MapData>,
    epoll: std::os::fd::OwnedFd,
    events: libc::epoll_event,
}

#[repr(C)]
struct RingEntry {
    ip: Ip,
    mac: MacAddr,
    kind: u16,
}

impl EbpfRing {
    pub(super) fn new(rb: aya::maps::RingBuf<aya::maps::MapData>) -> Result<Self, CacheSpawnError> {
        // SAFETY: syscalls. We setup an epoll instance as (I don't believe) we can use a regular `read` on an eBPF ring
        // buffer because it's actually a mmap with atomic head and tail
        let mut events = libc::epoll_event {
            events: libc::EPOLLIN as _,
            u64: 0,
        };

        let epoll = unsafe {
            let fd = libc::epoll_create1(libc::EPOLL_CLOEXEC);
            if fd < 0 {
                return Err(CacheSpawnError::Epoll(std::io::Error::last_os_error()));
            }

            if libc::epoll_ctl(fd, libc::EPOLL_CTL_ADD, rb.as_raw_fd(), &mut events) < 0 {
                return Err(CacheSpawnError::EpollRingBuf(
                    std::io::Error::last_os_error(),
                ));
            }

            std::os::fd::OwnedFd::from_raw_fd(fd)
        };

        Ok(Self {
            inner: rb,
            epoll,
            events,
        })
    }

    #[inline]
    pub(super) fn read(&mut self) -> Option<RingEntry> {
        if let Some(item) = self.inner.next() {
            // We're the ones inserting from eBPF, there should never be anything else in here
            if item.len() != std::mem::size_of::<RingEntry>() {
                return None;
            }

            // SAFETY: we've verified the size, the caller is responsible for validating the actual contents
            Some(unsafe { std::ptr::read_unaligned(item.as_ptr().cast()) })
        } else {
            None
        }
    }

    #[inline]
    pub(super) fn entry(&mut self) -> squeue::Entry {
        opcode::EpollWait::new(
            Fd(self.epoll.as_raw_fd()),
            (&mut self.events as *mut libc::epoll_event).cast(),
            1,
        )
        .build()
    }
}

enum Source {
    Arp,
    Icmp,
    Icmpv6,
    Neighbor,
    Unknown,
}

impl From<u16> for Source {
    #[inline]
    fn from(value: u16) -> Self {
        match value {
            0x0608 => Self::Arp,
            1 => Self::Icmp,
            136 => Self::Neighbor,
            58 => Self::Icmpv6,
            _ => Self::Unknown,
        }
    }
}

enum PingState {
    Sending,
    Sent,
    Mac { mac: MacAddr, src: Source },
}

const PING_ATTEMPTS: usize = 3;

pub(super) struct InflightPing {
    seq: corrosion::SmallVec<[u16; PING_ATTEMPTS]>,
    dst: Ip,
    state: PingState,
    time: Instant,
}

struct Ping {
    addr: [u8; std::mem::size_of::<libc::sockaddr_in6>()],
    payload: [u8; 8],
}

impl Ping {
    #[inline]
    fn new() -> Self {
        Self {
            addr: [0u8; _],
            payload: [0u8; _],
        }
    }

    #[inline]
    fn build(&mut self, ip: Ip, seq: u16) -> u32 {
        icmp::make_echo_request(seq, &mut self.payload);
        let ip = ip.0.octets();

        unsafe {
            if &ip[..12] == &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff] {
                let addr = &mut *self.addr.as_mut_ptr().cast::<libc::sockaddr_in>();
                addr.sin_family = libc::AF_INET as _;
                addr.sin_addr.s_addr = u32::from_ne_bytes([ip[12], ip[13], ip[14], ip[15]]);
                addr.sin_port = 7u16.to_be();

                std::mem::size_of::<libc::sockaddr_in>() as _
            } else {
                let addr = &mut *self.addr.as_mut_ptr().cast::<libc::sockaddr_in6>();
                addr.sin6_family = libc::AF_INET6 as _;
                addr.sin6_addr.s6_addr = ip;
                addr.sin6_port = 7u16.to_be();
                addr.sin6_flowinfo = 0;
                addr.sin6_scope_id = 0;

                std::mem::size_of::<libc::sockaddr_in6>() as _
            }
        }
    }

    #[inline]
    fn ip(&self) -> Ip {
        // SAFETY: byte reinterpretation
        unsafe {
            let sas = &*self.addr.as_ptr().cast::<libc::sockaddr_storage>();

            match sas.ss_family as i32 {
                libc::AF_INET => {
                    let v4 = &*self.addr.as_ptr().cast::<libc::sockaddr_in>();
                    let [a, b, c, d] = v4.sin_addr.s_addr.to_ne_bytes();
                    Ip(Ipv6Addr::from_octets([
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d,
                    ]))
                }
                libc::AF_INET6 => {
                    let v6 = &*self.addr.as_ptr().cast::<libc::sockaddr_in6>();
                    Ip(Ipv6Addr::from_octets(v6.sin6_addr.s6_addr))
                }
                _ => unreachable!(),
            }
        }
    }
}

pub(super) struct InflightPings {
    v: Vec<InflightPing>,
    s: slab::Slab<Ping>,
    cache: std::sync::Arc<super::L2Cache>,
    socket: Fd,
    seq: u16,
}

const SEND_TIMEOUT: Timespec = Timespec::new().sec(2);

impl InflightPings {
    pub(super) fn new(socket: Fd, cache: std::sync::Arc<super::L2Cache>) -> Self {
        Self {
            v: Vec::with_capacity(32),
            s: slab::Slab::with_capacity(16),
            cache,
            socket,
            seq: 0,
        }
    }

    #[inline]
    pub(super) fn ping(&mut self, ip: Ip, sq: &mut squeue::SubmissionQueue<'_>) -> bool {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);

        let entry = self.s.vacant_entry();
        let key = entry.key();
        // Something is very wrong if we have more than u32 outstanding requests
        assert!(key < 0xffffffff);
        let key = key as u32;

        let ping = entry.insert(Ping::new());
        let addr_len = ping.build(ip, seq);

        let time = Instant::now();

        if let Some(ping) = self.v.iter_mut().find(|p| p.dst == ip) {
            ping.seq.push(seq);
            ping.time = time;
        } else {
            self.v.push(InflightPing {
                seq: corrosion::SmallVec::from_buf_and_len([seq, 0, 0], 1),
                dst: ip,
                state: PingState::Sending,
                time,
            });
        }

        // SAFETY:
        unsafe {
            if sq
                .push_multiple(&[
                    opcode::Send::new(self.socket, ping.payload.as_ptr(), ping.payload.len() as _)
                        .dest_addr(ping.addr.as_ptr().cast())
                        .dest_addr_len(addr_len)
                        .build()
                        .user_data((key as u64) << 32 | (seq as u64) << 16 | io::code::SEND)
                        .flags(squeue::Flags::IO_LINK),
                    opcode::LinkTimeout::new(&SEND_TIMEOUT)
                        .build()
                        .user_data((key as u64) << 32 | (seq as u64) << 16 | io::code::TIMEOUT),
                ])
                .is_err()
            {
                // Cleanup the memory if our squeue is full, we will try to enqueue it again later
                if self.s.try_remove(key as usize).is_none() {
                    // This should not happen since we literally just allocated it, but want to avoid a panic
                    tracing::error!(%ip, "failed to cleanup entry after failing to enqueue it to submission queue");
                }
                return false;
            }
        }

        true
    }

    #[inline]
    pub(super) fn process_send(&mut self, cqe: io_uring::cqueue::Entry) {
        let ud = cqe.user_data();
        let key = (ud >> 32) as usize;
        let seq = (ud >> 16) as u16;

        let Some(entry) = self.s.try_remove(key) else {
            tracing::error!(key, seq, "completion entry had no associated send data");
            return;
        };

        let ip = entry.ip();

        let Some(i) = self.v.iter().position(|i| i.dst == ip) else {
            tracing::error!(%ip, seq, "completion entry had no associated ping data");
            return;
        };

        if cqe.result() < 0 {
            let res = cqe.result().abs();
            // Don't print on timeouts, if the destination is unreachable/slow then depend on the less spammy failure
            // after retry printing
            if res != libc::ECANCELED && res != libc::EINTR {
                tracing::error!(error = %std::io::Error::from_raw_os_error(res), %ip, seq, "failed to send ping");
            }
            return;
        }

        let ping = &mut self.v[i];

        ping.time = Instant::now();

        match &ping.state {
            PingState::Sending => {
                ping.state = PingState::Sent;
            }
            PingState::Mac { .. } => {
                // This could easily happen if the kernel doesn't have the layer 2 address of the destination and thus sends
                // its own ARP or ICMPv6 request, which we'll probably get and process before the kernel then emits our actual
                // ICMP packet and completes the send
            }
            PingState::Sent => {
                tracing::warn!(%ip, seq, "received completion for same ip");
            }
        }
    }

    #[inline]
    pub(super) fn process_ebpf(&mut self, entry: RingEntry) {
        let Some(i) = self.v.iter().position(|i| i.dst == entry.ip) else {
            // TODO: other processes on this node could be causing ARP/ICMPv6 address resolution to nodes that we might
            // be interested in in the future, but since we're not interested now we don't care, but we could cache them
            // for some amount of time
            return;
        };

        let src = Source::from(entry.kind);

        if matches!(src, Source::Arp | Source::Neighbor)
            || matches!(self.v[i].state, PingState::Sent)
        {
            let _ping = self.v.swap_remove(i);
            self.cache
                .update_mac(entry.ip, LinkLayerAddr::Known(entry.mac));
        } else {
            let item = &mut self.v[i];
            item.time = Instant::now();
            item.state = PingState::Mac {
                mac: entry.mac,
                src,
            };
        }
    }

    #[inline]
    pub(super) fn process_reply(&mut self, ip: Ip, rb: &[u8]) {
        let Some(i) = self.v.iter().position(|i| i.dst == ip) else {
            // We've (probably) already gotten the L2 information from eBPF
            return;
        };

        match icmp::read_echo_reply(&rb) {
            Ok(seq) => {
                // This should realistically never happen
                if !self.v[i].seq.contains(&seq) {
                    return;
                }
            }
            Err(error) => {
                tracing::error!(%error, "failed to read ICMP echo reply");
                return;
            }
        }

        if matches!(&self.v[i].state, PingState::Mac { .. }) {
            let PingState::Mac { mac, .. } = self.v.swap_remove(i).state else {
                unreachable!("expected a mac address, we literally just checked we had one");
            };
            self.cache.update_mac(ip, LinkLayerAddr::Known(mac));
        } else {
            let item = &mut self.v[i];
            item.time = Instant::now();
            item.state = PingState::Sent;
        }
    }

    #[inline]
    pub(super) fn process_timeouts(&mut self, sq: &mut squeue::SubmissionQueue<'_>) {
        const TIMEOUT: Duration = Duration::from_millis(500);

        let mut i = 0;

        let now = Instant::now();

        while i < self.v.len() {
            if now.duration_since(self.v[i].time) < TIMEOUT {
                continue;
            }

            // If we've already sent 3 pings then consider the host unreachable, falling back to the default behavior of
            // using a gateway mac
            if self.v[i].seq.len() < PING_ATTEMPTS {
                if !self.ping(self.v[i].dst, sq) {
                    tracing::error!("failed to enqueue ping retry");
                }

                i += 1;
            } else {
                let failed = self.v.swap_remove(i);
                tracing::error!(ip = %failed.dst, attempts = PING_ATTEMPTS, "failed to ping");

                let lladdr = if let PingState::Mac { mac, .. } = failed.state {
                    LinkLayerAddr::Known(mac)
                } else {
                    LinkLayerAddr::Unreachable
                };

                self.cache.update_mac(failed.dst, lladdr);
            }
        }
    }
}
