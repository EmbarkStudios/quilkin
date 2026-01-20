/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub mod cluster;
pub mod endpoint;
pub mod error;
pub mod http;
pub mod io;
pub(crate) mod maxmind_db;
pub mod packet;
pub mod phoenix;
pub mod sessions;

use std::{
    io::Result as IoResult,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use socket2::{Protocol, Socket, Type};

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        use std::net::UdpSocket;
    } else {
        use tokio::net::UdpSocket;
    }
}

pub use {
    self::{
        cluster::ClusterMap,
        endpoint::{Endpoint, EndpointAddress},
        error::PipelineError,
        packet::{Packet, PacketMut, PacketQueue, PacketQueueSender, queue},
        sessions::SessionPool,
    },
    quilkin_xds as xds,
    xds::net::TcpListener,
};

fn socket_with_reuse_and_address(addr: SocketAddr) -> IoResult<UdpSocket> {
    cfg_if::cfg_if! {
        if #[cfg(target_os = "linux")] {
            raw_socket_with_reuse_and_address(addr)
                .map(From::from)
        } else {
            epoll_socket_with_reuse_and_address(addr)
        }
    }
}

fn epoll_socket_with_reuse(port: u16) -> IoResult<tokio::net::UdpSocket> {
    raw_socket_with_reuse_and_address((Ipv6Addr::UNSPECIFIED, port).into())
        .map(From::from)
        .and_then(tokio::net::UdpSocket::from_std)
}

fn epoll_socket_with_reuse_and_address(addr: SocketAddr) -> IoResult<tokio::net::UdpSocket> {
    raw_socket_with_reuse_and_address(addr)
        .map(From::from)
        .and_then(tokio::net::UdpSocket::from_std)
}

#[inline]
pub fn raw_socket_with_reuse(port: u16) -> IoResult<Socket> {
    raw_socket_with_reuse_and_address((Ipv6Addr::UNSPECIFIED, port).into())
}

pub fn raw_socket_with_reuse_and_address(addr: SocketAddr) -> IoResult<Socket> {
    let domain = match addr {
        SocketAddr::V4(_) => socket2::Domain::IPV4,
        SocketAddr::V6(_) => socket2::Domain::IPV6,
    };

    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    enable_reuse(&sock)?;
    sock.set_nonblocking(true)?;
    if domain == socket2::Domain::IPV6 {
        // be explicit so we can have dual stack sockets.
        sock.set_only_v6(false)?;
    }
    sock.bind(&addr.into())?;

    Ok(sock)
}

#[inline]
pub fn socket_port(socket: &socket2::Socket) -> u16 {
    match socket.local_addr().unwrap().as_socket().unwrap() {
        SocketAddr::V4(addr) => addr.port(),
        SocketAddr::V6(addr) => addr.port(),
    }
}

#[cfg(not(target_family = "windows"))]
fn enable_reuse(sock: &Socket) -> IoResult<()> {
    sock.set_reuse_port(true)?;
    Ok(())
}

#[cfg(target_family = "windows")]
fn enable_reuse(sock: &Socket) -> IoResult<()> {
    sock.set_reuse_address(true)?;
    Ok(())
}

/// An ipv6 socket that can accept and send data from either a local ipv4 address or ipv6 address
/// with port reuse enabled and `only_v6` set to false.
pub struct DualStackLocalSocket {
    socket: UdpSocket,
    local_addr: SocketAddr,
}

impl DualStackLocalSocket {
    pub fn from_raw(socket: Socket) -> Self {
        let socket: std::net::UdpSocket = socket.into();
        let local_addr = socket.local_addr().unwrap();
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                let socket = socket;
            } else {
                // This is only for macOS and Windows (non-production platforms),
                // and should never happen anyway, so unwrap here is fine.
                let socket = UdpSocket::from_std(socket).unwrap();
            }
        }
        Self { socket, local_addr }
    }

    pub fn new(port: u16) -> IoResult<Self> {
        raw_socket_with_reuse(port).map(Self::from_raw)
    }

    pub fn bind_local(port: u16) -> IoResult<Self> {
        let local_addr = (Ipv6Addr::LOCALHOST, port).into();
        let socket = socket_with_reuse_and_address(local_addr)?;
        Ok(Self { socket, local_addr })
    }

    pub fn local_ipv4_addr(&self) -> IoResult<SocketAddr> {
        Ok(match self.local_addr {
            SocketAddr::V4(_) => self.local_addr,
            SocketAddr::V6(_) => (Ipv4Addr::UNSPECIFIED, self.local_addr.port()).into(),
        })
    }

    pub fn local_ipv6_addr(&self) -> IoResult<SocketAddr> {
        Ok(match self.local_addr {
            SocketAddr::V4(v4addr) => SocketAddr::new(
                IpAddr::V6(v4addr.ip().to_ipv6_mapped()),
                self.local_addr.port(),
            ),
            SocketAddr::V6(_) => self.local_addr,
        })
    }

    cfg_if::cfg_if! {
        if #[cfg(not(target_os = "linux"))] {
            pub async fn recv_from<B: std::ops::DerefMut<Target = [u8]>>(&self, mut buf: B) -> (IoResult<(usize, SocketAddr)>, B) {
                let result = self.socket.recv_from(&mut buf).await;
                (result, buf)
            }

            pub async fn send_to<B: std::ops::Deref<Target = [u8]>>(&self, buf: B, target: SocketAddr) -> (IoResult<usize>, B) {
                let result = self.socket.send_to(&buf, target).await;
                (result, buf)
            }
        } else {
            #[inline]
            pub fn raw_fd(&self) -> ::io_uring::types::Fd {
                use std::os::fd::AsRawFd;
                ::io_uring::types::Fd(self.socket.as_raw_fd())
            }
        }
    }

    pub fn make_refcnt(self) -> DualStackLocalSocketRc {
        DualStackLocalSocketRc::new(self)
    }
}

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        pub type DualStackLocalSocketRc = std::rc::Rc<DualStackLocalSocket>;
    } else {
        pub type DualStackLocalSocketRc = std::sync::Arc<DualStackLocalSocket>;
    }
}

/// The same as [`DualStackSocket`] but uses epoll instead of uring.
#[derive(Debug)]
pub struct DualStackEpollSocket {
    socket: tokio::net::UdpSocket,
}

impl DualStackEpollSocket {
    pub fn new(port: u16) -> IoResult<Self> {
        Ok(Self {
            socket: epoll_socket_with_reuse(port)?,
        })
    }

    pub fn bind_local(port: u16) -> IoResult<Self> {
        Ok(Self {
            socket: epoll_socket_with_reuse_and_address((Ipv6Addr::LOCALHOST, port).into())?,
        })
    }

    /// Primarily used for testing of ipv4 vs ipv6 addresses.
    pub(crate) fn new_with_address(addr: SocketAddr) -> IoResult<Self> {
        Ok(Self {
            socket: epoll_socket_with_reuse_and_address(addr)?,
        })
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> IoResult<(usize, SocketAddr)> {
        self.socket.recv_from(buf).await
    }

    pub fn local_addr(&self) -> IoResult<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn local_ipv4_addr(&self) -> IoResult<SocketAddr> {
        let addr = self.socket.local_addr()?;
        match addr {
            SocketAddr::V4(_) => Ok(addr),
            SocketAddr::V6(_) => Ok((Ipv4Addr::UNSPECIFIED, addr.port()).into()),
        }
    }

    pub fn local_ipv6_addr(&self) -> IoResult<SocketAddr> {
        let addr = self.socket.local_addr()?;
        match addr {
            SocketAddr::V4(v4addr) => Ok(SocketAddr::new(
                IpAddr::V6(v4addr.ip().to_ipv6_mapped()),
                addr.port(),
            )),
            SocketAddr::V6(_) => Ok(addr),
        }
    }

    pub async fn send_to<A: tokio::net::ToSocketAddrs>(
        &self,
        buf: &[u8],
        target: A,
    ) -> IoResult<usize> {
        self.socket.send_to(buf, target).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        time::Duration,
    };

    use tokio::time::timeout;

    use crate::net::endpoint::address::AddressKind;
    use crate::test::{AddressType, TestHelper, available_addr};

    #[tokio::test]
    async fn dual_stack_socket_reusable() {
        let expected = available_addr(AddressType::Random).await;
        let socket = super::DualStackEpollSocket::new(expected.port()).unwrap();
        let addr = socket.local_ipv4_addr().unwrap();

        match expected {
            SocketAddr::V4(_) => assert_eq!(expected, socket.local_ipv4_addr().unwrap()),
            SocketAddr::V6(_) => assert_eq!(expected, socket.local_ipv6_addr().unwrap()),
        }

        assert_eq!(expected.port(), socket.local_ipv4_addr().unwrap().port());
        assert_eq!(expected.port(), socket.local_ipv6_addr().unwrap().port());

        // should be able to do it a second time, since we are reusing the address.
        let socket = super::DualStackEpollSocket::new(expected.port()).unwrap();

        match expected {
            SocketAddr::V4(_) => assert_eq!(expected, socket.local_ipv4_addr().unwrap()),
            SocketAddr::V6(_) => assert_eq!(expected, socket.local_ipv6_addr().unwrap()),
        }
        assert_eq!(addr.port(), socket.local_ipv4_addr().unwrap().port());
        assert_eq!(addr.port(), socket.local_ipv6_addr().unwrap().port());
    }

    #[tokio::test]
    #[cfg_attr(target_os = "macos", ignore)]
    async fn dual_stack_socket() {
        // Since the TestHelper uses the DualStackSocket, we can use it to test ourselves.
        let mut t = TestHelper::default();

        let echo_addr = t.run_echo_server(AddressType::Random).await;
        let (mut rx, socket) = t.open_socket_and_recv_multiple_packets().await;

        let msg = "hello";
        let addr = echo_addr.to_socket_addr().unwrap();

        socket.send_to(msg.as_bytes(), &addr).await.unwrap();
        assert_eq!(
            msg,
            timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("should not timeout")
                .unwrap()
        );

        // try again, but from the opposite type of IP Address
        // Proof that a dual stack ipv6 socket can send to both ipv6 and ipv4.
        let ipv4_echo_addr = (Ipv4Addr::UNSPECIFIED, echo_addr.port).into();
        let opp_addr: SocketAddr = match echo_addr.host {
            AddressKind::Ip(ip) => match ip {
                IpAddr::V4(_) => (Ipv6Addr::UNSPECIFIED, echo_addr.port).into(),
                IpAddr::V6(_) => ipv4_echo_addr,
            },
            // we're not testing this, since DNS resolves to IP.
            AddressKind::Name(_) => unreachable!(),
        };

        socket.send_to(msg.as_bytes(), &opp_addr).await.unwrap();
        assert_eq!(
            msg,
            timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("should not timeout")
                .unwrap()
        );

        // Since all other sockets are actual ipv6 sockets, let's force a test with a real ipv4 socket sending to our dual
        // stack socket.
        let (mut rx, socket) = t.open_ipv4_socket_and_recv_multiple_packets().await;
        socket
            .send_to(msg.as_bytes(), &ipv4_echo_addr)
            .await
            .unwrap();
        assert_eq!(
            msg,
            timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("should not timeout")
                .unwrap()
        );
    }
}
