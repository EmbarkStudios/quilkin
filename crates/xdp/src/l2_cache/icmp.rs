//! Simple [ICMPv4](https://en.wikipedia.org/wiki/Internet_Control_Message_Protocol)
//! ping socket

use std::{io, net::UdpSocket};

pub const ECHO_REPLYV4: u8 = 0;
pub const ECHO_REQUESTV4: u8 = 8;

pub const ECHO_REQUESTV6: u8 = 128;
pub const ECHO_REPLYV6: u8 = 129;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct IcmpHeader {
    pub kind: u8,
    pub code: u8,
    pub checksum: [u8; 2],
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct IcmpEcho {
    pub identifier: u16,
    pub sequence_number: u16,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct IcmpEchoMsg {
    pub header: IcmpHeader,
    pub echo: IcmpEcho,
}

pub struct IcmpSocket {
    pub(crate) nic: xdp::nic::NicIndex,
    pub(crate) in6: UdpSocket,
    pub(crate) in4: UdpSocket,
}

impl IcmpSocket {
    #[inline]
    pub fn new(nic: xdp::nic::NicIndex) -> io::Result<Self> {
        let in4 = Self::create_socket(None)?;
        let in6 = Self::create_socket(Some(nic.0))?;

        // We're using this in io-uring so make sure it's nonblocking
        in6.set_nonblocking(true)?;
        in4.set_nonblocking(true)?;

        Ok(Self { nic, in6, in4 })
    }

    /// Ideally we would utilize the non-privileged ICMP sockets available in Linux
    /// <https://lwn.net/Articles/443051/> (see <https://ekman.cx/articles/icmp_sockets/>
    /// for more in-depth information) it's basically the only good source of information
    /// on this aspect of userland ICMP on Linux that I can find...
    ///
    /// However! When using veth (and probably most/all other virtual network utilities) the special unprivileged
    /// path is not taken/unavailable and socket creation fails, so rather than maintain a separate path just for
    /// testing purposes we fall back to raw sockets, which is only slightly more tedious and doesn't actually impact
    /// deployment as we already need the same privileges for XDP anyways
    pub fn create_socket(scope_id: Option<u32>) -> io::Result<UdpSocket> {
        use std::os::fd::FromRawFd;

        // SAFETY: syscalls, the arguments are valid, barring kernel bugs this is safe
        unsafe {
            let (domain, proto, addr_len) = if scope_id.is_none() {
                (
                    libc::AF_INET,
                    libc::IPPROTO_ICMP,
                    std::mem::size_of::<libc::sockaddr_in>(),
                )
            } else {
                (
                    libc::AF_INET6,
                    libc::IPPROTO_ICMPV6,
                    std::mem::size_of::<libc::sockaddr_in6>(),
                )
            };

            let socket = libc::socket(domain, libc::SOCK_RAW, proto);
            if socket < 0 {
                return Err(io::Error::last_os_error());
            }

            // We need to call bind explicitly
            let mut addr: libc::sockaddr_storage = std::mem::zeroed();
            addr.ss_family = domain as _;

            if let Some(sid) = scope_id {
                let in6 =
                    &mut *(&mut addr as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in6>();
                in6.sin6_scope_id = sid;
            }

            if libc::bind(
                socket,
                (&addr as *const libc::sockaddr_storage).cast(),
                addr_len as _,
            ) < 0
            {
                return Err(io::Error::last_os_error());
            }

            // We've verified the socket
            Ok(UdpSocket::from_raw_fd(socket))
        }
    }
}

/// Makes an ICMP echo request
///
/// Note that on Linux we can only set the `sequence number` in the echo request, the kernel uses the `identifier`
/// field to map echo responses back to this particular socket, so we have no control over it
#[inline]
pub fn make_echo_request(sequence: u16, req: &mut [u8; 8], is_v4: bool) {
    use xdp::packet::csum;

    // SAFETY: the array is valid and it's fine to be 0 initialized
    unsafe {
        std::ptr::write_bytes(req.as_mut_ptr(), 0, req.len());
    }

    req[0] = if is_v4 {
        ECHO_REQUESTV4
    } else {
        ECHO_REQUESTV6
    };
    req[6..8].copy_from_slice(&sequence.to_be_bytes());

    let sum = csum::partial(req, 0);
    req[2..4].copy_from_slice(&csum::fold_checksum(sum).to_ne_bytes());
}

/// Parses an ICMP echo reply, returning the sequence number of it is a valid reply
#[inline]
pub fn read_echo_reply(mut buf: &[u8], is_v4: bool) -> io::Result<u16> {
    // We need to skip the IPv4 header, but don't need to care for IPv6
    if is_v4 {
        if buf.len() <= xdp::packet::net_types::Ipv4Hdr::LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "invalid length for IPv4 headers",
            ));
        }

        // SAFETY: we've validated we have at least enough space for the expected IPv4 header but we haven't allocated
        // enough space for options, so ensure there are none
        unsafe {
            let ipv4 = buf
                .as_ptr()
                .cast::<xdp::packet::net_types::Ipv4Hdr>()
                .read_unaligned();
            if ipv4.internet_header_length() as usize > xdp::packet::net_types::Ipv4Hdr::LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "IPv4 header contained options",
                ));
            }

            buf = &buf[xdp::packet::net_types::Ipv4Hdr::LEN..];
        }
    }

    if buf.len() != std::mem::size_of::<IcmpEchoMsg>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "invalid length for ICMP echo response",
        ));
    }

    // SAFETY: We ensure the size above
    let echo_response: IcmpEchoMsg = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast()) };

    if echo_response.header.kind != if is_v4 { ECHO_REPLYV4 } else { ECHO_REPLYV6 } {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "got unexpected ICMP payload",
        ));
    }

    Ok(u16::from_be(echo_response.echo.sequence_number))
}
