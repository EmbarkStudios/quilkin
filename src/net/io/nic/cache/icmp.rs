//! Simple [ICMPv4](https://en.wikipedia.org/wiki/Internet_Control_Message_Protocol)
//! ping socket
//!
//! Utilizes the non-privileged ICMP sockets available in Linux
//! <https://lwn.net/Articles/443051/>, see <https://ekman.cx/articles/icmp_sockets/>
//! for more in-depth information, it's basically the only good source of information
//! on this aspect of userland ICMP on Linux that I can find

use std::{
    io,
    net::{self, UdpSocket},
    os::fd::{AsFd, AsRawFd},
};

const ECHO_REPLY: u8 = 0;
const ECHO_REQUEST: u8 = 8;

#[repr(C)]
struct IcmpHeader {
    kind: u8,
    code: u8,
    checksum: [u8; 2],
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct IcmpEcho {
    pub identifier: u16,
    pub sequence_number: u16,
}

#[repr(C)]
struct IcmpEchoMsg {
    header: IcmpHeader,
    echo: IcmpEcho,
}

pub struct IcmpSocket {
    /// We can take advantage of the fact that we create the socket ourselves,
    /// but the same syscalls are used for sending and receiving, so we don't
    /// need to reimplement parts of the socket API
    inner: UdpSocket,
}

impl IcmpSocket {
    #[inline]
    pub fn new() -> io::Result<Self> {
        // SAFETY: syscall, the arguments are valid, barring kernel bugs this is safe
        let inner = unsafe {
            let socket = libc::socket(libc::AF_INET6, libc::SOCK_DGRAM, libc::IPPROTO_ICMP);
            if socket < 0 {
                return Err(io::Error::last_os_error());
            }

            // We need to call bind explicitly
            let addr = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as _,
                sin6_addr: libc::in6_addr {
                    s6_addr: net::Ipv6Addr::UNSPECIFIED.octets(),
                },
                sin6_port: 0,
                sin6_flowinfo: 0,
                sin6_scope_id: 0,
            };
            if libc::bind(
                socket,
                (&addr as *const libc::sockaddr_in6).cast(),
                std::mem::size_of::<libc::sockaddr_in6>() as _,
            ) < 0
            {
                return Err(io::Error::last_os_error());
            }

            use std::os::fd::FromRawFd;
            // We've verified the socket
            UdpSocket::from_raw_fd(socket)
        };

        // We're using this in io-uring so make sure it's nonblocking
        inner.set_nonblocking(true)?;

        Ok(Self { inner })
    }
}

impl AsRawFd for IcmpSocket {
    fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsFd for IcmpSocket {
    fn as_fd(&self) -> std::os::unix::prelude::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

/// Makes an ICMP echo request
///
/// Note that on Linux we can only set the `sequence number` in the echo request, the kernel uses the `identifier`
/// field to map echo responses back to this particular socket, so we have no control over it
#[inline]
pub fn make_echo_request(sequence: u16, req: &mut [u8; 8]) {
    use quilkin_xdp::xdp::packet::csum;

    // SAFETY: the array is valid and it's fine to be 0 initialized2
    unsafe {
        std::ptr::write_bytes(req.as_mut_ptr(), 0, req.len());
    }

    req[0] = ECHO_REQUEST;
    req[6..8].copy_from_slice(&sequence.to_be_bytes());

    let sum = csum::partial(req, 0);
    req[2..4].copy_from_slice(&csum::fold_checksum(sum).to_ne_bytes());
}

/// Parses an ICMP echo reply, returning the sequence number of it is a valid reply
#[inline]
pub fn read_echo_reply(buf: &[u8]) -> io::Result<u16> {
    if buf.len() != std::mem::size_of::<IcmpEchoMsg>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "invalid length for ICMP echo response",
        ));
    }

    let echo_response: IcmpEchoMsg = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast()) };

    if echo_response.header.kind != ECHO_REPLY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "got unexpected ICMP payload",
        ));
    }

    Ok(u16::from_be(echo_response.echo.sequence_number))
}
