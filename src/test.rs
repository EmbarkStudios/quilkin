/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::net::Ipv4Addr;
/// Common utilities for testing
use std::{net::SocketAddr, str::from_utf8, sync::Arc, sync::Once};

use tokio::sync::{mpsc, oneshot};
use tracing_subscriber::EnvFilter;

use crate::{
    collections::BufferPool,
    config::Config,
    filters::{FilterRegistry, prelude::*},
    net::DualStackEpollSocket as DualStackLocalSocket,
    net::endpoint::metadata::Value,
    net::endpoint::{Endpoint, EndpointAddress},
    signal::{ShutdownRx, ShutdownTx},
};

static LOG_ONCE: Once = Once::new();

/// Call to safely enable logging calls with a given tracing env filter, e.g. "quilkin=debug"
/// This can be very useful when attempting to debug unit and integration tests.
pub fn enable_log(filter: impl Into<EnvFilter>) {
    LOG_ONCE.call_once(|| {
        tracing_subscriber::fmt()
            .pretty()
            .with_ansi(false)
            .with_env_filter(filter)
            .init();
    });
}

/// Which type of Address do you want? Random may give ipv4 or ipv6
#[derive(Copy, Clone)]
pub enum AddressType {
    Random,
    Ipv4,
    Ipv6,
}

impl From<AddressType> for SocketAddr {
    fn from(value: AddressType) -> Self {
        match value {
            AddressType::Random => {
                if rand::random() {
                    (std::net::Ipv6Addr::LOCALHOST, 0).into()
                } else {
                    (std::net::Ipv4Addr::LOCALHOST, 0).into()
                }
            }
            AddressType::Ipv4 => (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            AddressType::Ipv6 => (std::net::Ipv6Addr::LOCALHOST, 0).into(),
        }
    }
}

/// Returns a local address on a port that is not assigned to another test.
/// If Random address tye is used, it might be v4, Might be v6. It's random.
pub async fn available_addr(address_type: AddressType) -> SocketAddr {
    let socket = create_socket().await;
    let addr = get_address(address_type, &socket);

    tracing::debug!(addr = ?addr, "test_util::available_addr");
    addr
}

fn get_address(address_type: AddressType, socket: &DualStackLocalSocket) -> SocketAddr {
    let addr = match address_type {
        AddressType::Random => {
            // sometimes give ipv6, sometimes ipv4.
            match rand::random() {
                true => socket.local_ipv6_addr().unwrap(),
                false => socket.local_ipv4_addr().unwrap(),
            }
        }
        AddressType::Ipv4 => socket.local_ipv4_addr().unwrap(),
        AddressType::Ipv6 => socket.local_ipv6_addr().unwrap(),
    };
    tracing::debug!(addr = ?addr, "test_util::get_address");
    addr
}

// TestFilter is useful for testing that commands are executing filters appropriately.
pub struct TestFilter;

impl TestFilter {
    pub fn testing(_config: Option<()>) -> Self {
        Self
    }
}

impl Filter for TestFilter {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        // append values on each run
        ctx.metadata
            .entry("downstream".into())
            .and_modify(|e| e.as_mut_string().unwrap().push_str(":receive"))
            .or_insert_with(|| Value::String("receive".into()));

        ctx.contents
            .extend_tail(format!(":odr:{}", ctx.source).as_bytes());
        Ok(())
    }

    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        // append values on each run
        ctx.metadata
            .entry("upstream".into())
            .and_modify(|e| e.as_mut_string().unwrap().push_str(":receive"))
            .or_insert_with(|| Value::String("receive".to_string()));

        ctx.contents
            .extend_tail(format!(":our:{}:{}", ctx.source, ctx.dest).as_bytes());
        Ok(())
    }
}

impl StaticFilter for TestFilter {
    const NAME: &'static str = "TestFilter";
    type Configuration = ();
    type BinaryConfiguration = ();

    fn try_from_config(_: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Self)
    }
}

#[derive(Default)]
pub struct TestHelper {
    /// Channel to subscribe to, and trigger the shutdown of created resources.
    shutdown_ch: Option<(ShutdownTx, ShutdownRx)>,
    server_shutdown_tx: Vec<Option<ShutdownTx>>,
}

/// Returned from [creating a socket](TestHelper::open_socket_and_recv_single_packet)
pub struct OpenSocketRecvPacket {
    /// The opened socket
    pub socket: Arc<DualStackLocalSocket>,
    /// A channel on which the received packet will be forwarded.
    pub packet_rx: oneshot::Receiver<String>,
}

impl Drop for TestHelper {
    fn drop(&mut self) {
        for shutdown_tx in self
            .server_shutdown_tx
            .iter_mut()
            .filter_map(|tx| tx.take())
        {
            shutdown_tx
                .send(())
                .map_err(|error| {
                    tracing::warn!(
                        %error,
                        "Failed to send server shutdown over channel"
                    );
                })
                .ok();
        }

        if let Some((shutdown_tx, _)) = self.shutdown_ch.take() {
            shutdown_tx.send(()).unwrap();
        }
    }
}

impl TestHelper {
    /// Opens a socket, listening for a packet. Once a packet is received, it
    /// is forwarded over the returned channel.
    pub async fn open_socket_and_recv_single_packet(&self) -> OpenSocketRecvPacket {
        let socket = Arc::new(create_socket().await);
        let (packet_tx, packet_rx) = oneshot::channel::<String>();
        let socket_recv = socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 1024];
            let (size, _) = socket_recv.recv_from(&mut buf).await.unwrap();
            packet_tx
                .send(from_utf8(&buf[..size]).unwrap().to_string())
                .unwrap();
        });
        OpenSocketRecvPacket { socket, packet_rx }
    }

    /// Opens a socket, listening for packets. Received packets are forwarded over the
    /// returned channel.
    pub async fn open_socket_and_recv_multiple_packets(
        &mut self,
    ) -> (mpsc::Receiver<String>, Arc<DualStackLocalSocket>) {
        let socket = Arc::new(create_socket().await);
        let packet_rx = self.recv_multiple_packets(&socket).await;
        (packet_rx, socket)
    }

    // Same as above, but sometimes you just need an ipv4 socket
    pub async fn open_ipv4_socket_and_recv_multiple_packets(
        &mut self,
    ) -> (mpsc::Receiver<String>, Arc<DualStackLocalSocket>) {
        let socket = Arc::new(
            DualStackLocalSocket::new_with_address((Ipv4Addr::LOCALHOST, 0).into()).unwrap(),
        );
        let packet_rx = self.recv_multiple_packets(&socket).await;
        (packet_rx, socket)
    }

    async fn recv_multiple_packets(
        &mut self,
        socket: &Arc<DualStackLocalSocket>,
    ) -> mpsc::Receiver<String> {
        let (packet_tx, packet_rx) = mpsc::channel::<String>(10);
        let mut shutdown_rx = self.get_shutdown_subscriber().await;
        let socket_recv = socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 1024];
            loop {
                tokio::select! {
                    received = socket_recv.recv_from(&mut buf) => {
                        let (size, _) = received.unwrap();
                        let str = from_utf8(&buf[..size]).unwrap().to_string();
                        match packet_tx.send(str).await {
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(target: "recv_multiple_packets", %error, "recv_chan dropped");
                                return;
                            }
                        };
                    },
                    _ = shutdown_rx.changed() => {
                        return;
                    }
                }
            }
        });
        packet_rx
    }

    /// Runs a simple UDP server that echos back payloads.
    /// Returns the server's address.
    pub async fn run_echo_server(&mut self, address_type: AddressType) -> EndpointAddress {
        self.run_echo_server_with_tap(address_type, |_, _, _| {})
            .await
    }

    /// Runs a simple UDP server that echos back payloads.
    /// The provided function is invoked for each received payload.
    /// Returns the server's address.
    pub async fn run_echo_server_with_tap<F>(
        &mut self,
        address_type: AddressType,
        tap: F,
    ) -> EndpointAddress
    where
        F: Fn(SocketAddr, &[u8], SocketAddr) + Send + 'static,
    {
        let socket = create_socket().await;
        // sometimes give ipv6, sometimes ipv4.
        let mut addr = get_address(address_type, &socket);
        crate::test::map_addr_to_localhost(&mut addr);
        let mut shutdown = self.get_shutdown_subscriber().await;
        let local_addr = addr;
        tokio::spawn(async move {
            loop {
                let mut buf = vec![0; 1024];
                tokio::select! {
                    recvd = socket.recv_from(&mut buf) => {
                        let (size, sender) = recvd.unwrap();
                        let packet = &buf[..size];
                        tracing::trace!(%sender, %size, "echo server received and returning packet");
                        tap(sender, packet, local_addr);
                        socket.send_to(packet, sender).await.unwrap();
                    },
                    _ = shutdown.changed() => {
                        return;
                    }
                }
            }
        });
        addr.into()
    }

    pub fn new_config() -> Arc<Config> {
        let providers = crate::Providers::default();
        let service = crate::Service::builder().udp().qcmp();
        crate::Config::new_rc(
            Some("test-server".into()),
            Default::default(),
            &providers,
            &service,
            tokio_util::sync::CancellationToken::new(),
        )
    }

    pub async fn run_server(
        &mut self,
        config: Arc<Config>,
        server: Option<crate::components::proxy::Proxy>,
        with_admin: Option<Option<SocketAddr>>,
    ) -> u16 {
        let (shutdown_tx, shutdown_rx) = crate::signal::channel();
        self.server_shutdown_tx.push(Some(shutdown_tx.clone()));
        let ready = <_>::default();

        if let Some(address) = with_admin {
            crate::components::admin::server(config.clone(), ready, shutdown_tx.clone(), address);
        }

        let server = server.unwrap_or_else(|| {
            let qcmp = crate::net::raw_socket_with_reuse(0).unwrap();
            let phoenix = crate::net::TcpListener::bind(None).unwrap();

            crate::components::proxy::Proxy {
                num_workers: std::num::NonZeroUsize::new(1).unwrap(),
                socket: Some(crate::net::raw_socket_with_reuse(0).unwrap()),
                qcmp,
                phoenix,
                ..Default::default()
            }
        });

        let (prox_tx, prox_rx) = tokio::sync::oneshot::channel();
        let shutdown = crate::signal::ShutdownHandler::new(shutdown_tx, shutdown_rx);

        let port = crate::net::socket_port(server.socket.as_ref().unwrap());

        tokio::spawn(async move {
            server
                .run(
                    crate::components::RunArgs {
                        config,
                        ready: Default::default(),
                        shutdown,
                    },
                    Some(prox_tx),
                )
                .unwrap();
        });

        prox_rx.await.unwrap();
        port
    }

    /// Returns a receiver subscribed to the helper's shutdown event.
    async fn get_shutdown_subscriber(&mut self) -> ShutdownRx {
        // If this is the first call, then we set up the channel first.
        if let Some((_, rx)) = &self.shutdown_ch {
            rx.clone()
        } else {
            let ch = crate::signal::channel();
            let recv = ch.1.clone();
            self.shutdown_ch = Some(ch);
            recv
        }
    }
}

pub static BUFFER_POOL: once_cell::sync::Lazy<Arc<BufferPool>> =
    once_cell::sync::Lazy::new(|| Arc::new(BufferPool::default()));

#[inline]
pub fn alloc_buffer(data: impl AsRef<[u8]>) -> crate::collections::PoolBuffer {
    BUFFER_POOL.clone().alloc_slice(data.as_ref())
}

/// assert that read makes no changes
#[cfg(test)]
pub fn assert_filter_read_no_change<F>(filter: &F)
where
    F: Filter,
{
    let endpoints = std::sync::Arc::new(crate::net::cluster::ClusterMap::default());
    endpoints.insert_default(std::collections::BTreeSet::from(["127.0.0.1:80"
        .parse::<Endpoint>()
        .unwrap()]));
    let source = "127.0.0.1:90".parse().unwrap();
    let contents = b"hello";
    let mut dest = Vec::new();
    let mut context = ReadContext::new(&endpoints, source, alloc_buffer(contents), &mut dest);

    filter.read(&mut context).unwrap();
    assert!(context.destinations.is_empty());
    assert_eq!(endpoints.as_ref(), context.endpoints);
    assert_eq!(contents, &*context.contents);
}

/// assert that write makes no changes
pub fn assert_write_no_change<F>(filter: &F)
where
    F: Filter,
{
    let endpoint = "127.0.0.1:90".parse::<Endpoint>().unwrap();
    let contents = b"hello";
    let mut context = WriteContext::new(
        endpoint.address,
        "127.0.0.1:70".parse().unwrap(),
        alloc_buffer(contents),
    );

    filter.write(&mut context).unwrap();
    assert_eq!(contents, &*context.contents);
}

pub fn map_to_localhost(address: &mut EndpointAddress) {
    let mut socket_addr = address.to_socket_addr().unwrap();
    map_addr_to_localhost(&mut socket_addr);
    *address = socket_addr.into();
}

pub fn map_addr_to_localhost(address: &mut SocketAddr) {
    match address {
        SocketAddr::V4(addr) => addr.set_ip(std::net::Ipv4Addr::LOCALHOST),
        SocketAddr::V6(addr) => addr.set_ip(std::net::Ipv6Addr::LOCALHOST),
    }
}

/// Opens a new socket bound to an ephemeral port
pub async fn create_socket() -> DualStackLocalSocket {
    DualStackLocalSocket::new(0).unwrap()
}

fn test_proxy_id() -> String {
    "test-proxy-id".to_owned()
}

/// Copy of [`crate::config::Config`] without all of the watcher things making
/// debugging tests confusing
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct TestConfig {
    #[serde(default)]
    pub clusters: crate::net::ClusterMap,
    #[serde(default)]
    pub filters: crate::filters::FilterChain,
    #[serde(default = "test_proxy_id")]
    pub id: String,
    #[serde(default)]
    pub version: crate::config::Version,
}

impl TestConfig {
    pub fn with_dummy_endpoint() -> Self {
        let config = Self::default();
        config.clusters.insert(
            None,
            None,
            [Endpoint::new((std::net::Ipv4Addr::LOCALHOST, 8080).into())].into(),
        );
        config
    }

    #[track_caller]
    pub fn new() -> Self {
        load_test_filters();

        Self {
            filters: crate::filters::FilterChain::try_create(std::iter::once(
                crate::config::filter::Filter {
                    name: "TestFilter".into(),
                    label: None,
                    config: None,
                },
            ))
            .unwrap(),
            ..Default::default()
        }
    }

    pub fn write_to_file(&self, path: impl AsRef<std::path::Path>) {
        std::fs::write(
            path,
            serde_yaml::to_string(self).expect("failed to serialize TestConfig"),
        )
        .expect("failed to write TestConfig to path");
    }
}

/// Creates a dummy endpoint with `id` as a suffix.
pub fn ep(id: u8) -> Endpoint {
    Endpoint {
        address: ([127, 0, 0, id], 8080).into(),
        ..<_>::default()
    }
}

pub fn load_test_filters() {
    FilterRegistry::register([TestFilter::factory()]);
}

/// Macro that can get the function name of the function the macro is invoked
/// within
#[macro_export]
macro_rules! __func_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

/// Creates a temporary file with the specified prefix in a directory named
/// after the calling function, ie using it within a test will place it in a
/// temporary directory named after the test
#[macro_export]
macro_rules! temp_file {
    ($prefix:expr_2021) => {{
        let name = $crate::__func_name!();
        let name = name.strip_suffix("::{{closure}}").unwrap_or(name);
        let mut name = name.replace("::", ".");
        name.push('-');
        name.push_str($prefix);
        name.push('-');
        tempfile::NamedTempFile::with_prefix(name).unwrap()
    }};
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::test::{AddressType, TestHelper};

    #[tokio::test]
    #[cfg_attr(target_os = "macos", ignore)]
    async fn test_echo_server() {
        let mut t = TestHelper::default();
        let echo_addr = t.run_echo_server(AddressType::Random).await;
        let endpoint = t.open_socket_and_recv_single_packet().await;
        let msg = "hello";
        endpoint
            .socket
            .send_to(msg.as_bytes(), &echo_addr.to_socket_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(
            msg,
            timeout(Duration::from_secs(5), endpoint.packet_rx)
                .await
                .expect("should not timeout")
                .unwrap()
        );
    }
}
