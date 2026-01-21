/*
 * Copyright 2024 Google LLC
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

use std::{io, net::SocketAddr};

/// TCP listener for a GRPC service, always binds to the local IPv6 address
pub struct TcpListener {
    inner: std::net::TcpListener,
}

impl TcpListener {
    /// Binds a TCP listener, if `None` is passed, binds to an ephemeral port
    #[inline]
    pub fn bind(port: Option<u16>) -> io::Result<Self> {
        std::net::TcpListener::bind((std::net::Ipv6Addr::UNSPECIFIED, port.unwrap_or_default()))
            .map(|inner| Self { inner })
    }

    /// Retrieves the port the listener is bound to
    #[inline]
    pub fn port(&self) -> u16 {
        self.inner.local_addr().expect("failed to bind").port()
    }

    /// Retrieves the local address the listener is bound to
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr().expect("failed to bind")
    }

    /// Returns the underlying std library TCP listener.
    #[inline]
    pub fn into_std(self) -> std::net::TcpListener {
        self.inner
    }

    /// Returns the listener as a tokio listener.
    #[inline]
    pub fn into_tokio(self) -> io::Result<tokio::net::TcpListener> {
        self.inner.set_nonblocking(true)?;
        tokio::net::TcpListener::from_std(self.inner)
    }

    #[inline]
    pub fn into_stream(self) -> io::Result<tokio_stream::wrappers::TcpListenerStream> {
        self.into_tokio()
            .map(tokio_stream::wrappers::TcpListenerStream::new)
    }
}

impl From<TcpListener> for std::net::TcpListener {
    #[inline]
    fn from(value: TcpListener) -> Self {
        value.inner
    }
}
