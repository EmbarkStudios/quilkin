/*
 * Copyright 2023 Google LLC
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

//! Logic for parsing and generating Quilkin Control Message Protocol (QCMP) messages.

use eyre::Context;

use crate::{
    metrics,
    net::{
        DualStackEpollSocket,
        phoenix::{DistanceMeasure, Measurement},
    },
    time::{DurationNanos, UtcTimestamp},
};
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

// Magic number to distinguish control packets from regular traffic.
const MAGIC_NUMBER: &[u8] = b"QLKN";
const VERSION: u8 = 0;
/// The minimum length of a QCMP packet
pub const MIN_QCMP_PACKET_LEN: usize = 4 /* MAGIC_NUMBER */ + 1 /* VERSION */ + 1 /* DISCRIMINANT */ + 1 /* NONCE */ + 2 /* LENGTH */ + std::mem::size_of::<u64>();
/// The maximum length of a QCMP packet, including 2 additional i64 timestamps
pub const MAX_QCMP_PACKET_LEN: usize = MIN_QCMP_PACKET_LEN + std::mem::size_of::<u64>() * 2;
const PING: u8 = 0;
const PONG: u8 = 1;

pub struct QcmpPacket {
    buf: [u8; MAX_QCMP_PACKET_LEN],
    len: usize,
}

impl Default for QcmpPacket {
    fn default() -> Self {
        Self {
            buf: [0; MAX_QCMP_PACKET_LEN],
            len: 0,
        }
    }
}

impl std::ops::Deref for QcmpPacket {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[..self.len]
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

struct PacketBuilder<'buf> {
    packet: &'buf mut QcmpPacket,
    offset: usize,
}

impl<'buf> PacketBuilder<'buf> {
    #[inline]
    fn wrap(packet: &'buf mut QcmpPacket) -> Self {
        packet.len = 0;
        Self { packet, offset: 0 }
    }

    #[inline]
    fn push(&mut self, val: u8) {
        self.packet.buf[self.offset] = val;
        self.offset += 1;
    }

    #[inline]
    fn push_slice(&mut self, slice: &[u8]) {
        self.packet.buf[self.offset..self.offset + slice.len()].copy_from_slice(slice);
        self.offset += slice.len();
    }

    #[inline]
    fn finalize(self) -> &'buf [u8] {
        self.packet.buf[self.offset..].fill(0);
        self.packet.len = self.offset;
        &self.packet.buf[..self.offset]
    }
}

struct PacketParser<'buf> {
    packet: &'buf [u8],
    offset: usize,
}

impl<'buf> PacketParser<'buf> {
    fn wrap(packet: &'buf [u8]) -> Result<Self> {
        if packet.len() < MIN_QCMP_PACKET_LEN {
            return Err(Error::LengthMismatch(
                MIN_QCMP_PACKET_LEN as _,
                packet.len(),
            ));
        }

        Ok(Self { packet, offset: 0 })
    }

    #[inline]
    fn read(&mut self) -> u8 {
        let index = self.offset;
        self.offset += 1;
        // SAFETY: We manually check the packet size before any reads
        unsafe { *self.packet.get_unchecked(index) }
    }

    #[inline]
    fn read_slice<const N: usize>(&mut self) -> [u8; N] {
        let mut s = [0u8; N];
        // SAFETY: We manually check the packet size before any reads
        s.copy_from_slice(unsafe { self.packet.get_unchecked(self.offset..self.offset + N) });
        self.offset += N;
        s
    }
}

/// The maximum capacity of concurrent in-flight qcmp pings are bound by the number of unique
/// nonces we can hold, since that is how we pair a response to a request
const MAX_WAITER_CAPACITY: usize = u8::MAX as usize + 1;

/// `NoncePool` keeps a pool of nonces that can be leased and ensures that a given nonce lease can
/// only be held by one lessee at a time
#[derive(Debug, Clone)]
struct NoncePool {
    nonces: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl NoncePool {
    pub fn new() -> Self {
        let mut nonces = Vec::with_capacity(MAX_WAITER_CAPACITY);
        for i in 0..u8::MAX {
            nonces.push(i);
        }

        Self {
            nonces: Arc::new(std::sync::Mutex::new(nonces)),
        }
    }

    /// Return a `NonceLease` with a randomly selected nonce, or None if there are no more nonces to
    /// lease. The `NonceLease` will return the lease when dropped.
    pub fn lease(&self) -> Option<NonceLease> {
        let mut guard = match self.nonces.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                let guard = poisoned.into_inner();
                tracing::warn!("recovered from poisoned mutex");
                guard
            }
        };

        let length = guard.len();

        if length == 0 {
            None
        } else {
            let nonce = guard.swap_remove(rand::random_range(..length));
            Some(NonceLease {
                pool: self.nonces.clone(),
                nonce,
            })
        }
    }
}

/// A lease of a nonce that will return the lease when dropped
struct NonceLease {
    pool: Arc<std::sync::Mutex<Vec<u8>>>,
    nonce: u8,
}

impl NonceLease {
    pub fn nonce(&self) -> u8 {
        self.nonce
    }
}

impl Drop for NonceLease {
    fn drop(&mut self) {
        let mut guard = match self.pool.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                let guard = poisoned.into_inner();
                tracing::warn!("recovered from poisoned mutex");
                guard
            }
        };
        guard.push(self.nonce);
    }
}

/// A transciever that can handle multiple simultaneous QCMP pings over the same socket and ensure
/// that responses are forwarded to the correct receiver via the QCMP nonce
#[derive(Debug, Clone)]
pub struct QcmpTransceiver {
    socket: Arc<DualStackEpollSocket>,
    #[cfg(test)]
    delay: Option<Duration>,
    nonces: NoncePool,
    waiters: Arc<dashmap::DashMap<u8, tokio::sync::oneshot::Sender<(UtcTimestamp, Protocol)>>>,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl Drop for QcmpTransceiver {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

/// Asynchronous receive task that listens on the socket and receives all responses as soon as
/// possible, recording the receive time, and forwarding to the matching request channel if it is
/// registered.
async fn receive_task(
    socket: Arc<DualStackEpollSocket>,
    waiters: Arc<dashmap::DashMap<u8, tokio::sync::oneshot::Sender<(UtcTimestamp, Protocol)>>>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    loop {
        let mut recv = [0u8; 512];
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::debug!("task cancelled, stopping receiving on socket");
                return;
            }
            result = socket.recv_from(&mut recv) => {
                match result {
                    Ok((size, addr)) => {
                        let recv_timestamp = UtcTimestamp::now();
                        let Ok(Some(reply)) = Protocol::parse(&recv[..size]) else {
                            tracing::warn!("received non qcmp packet {:?}", &recv[..size]);
                            continue;
                        };

                        let key = reply.nonce();
                        if let Some((_, waiter)) = waiters.remove(&key) {
                            if let Err(error) = waiter.send((recv_timestamp, reply)) {
                                tracing::error!(?error, "failed to inform waiter");
                            }
                        } else {
                            tracing::debug!(
                                ?addr,
                                nonce = reply.nonce(),
                                "received packet without a waiter"
                            );
                        }
                    }
                    Err(error) => tracing::error!(?error, "recv error"),
                }
            }
        }
    }
}

impl QcmpTransceiver {
    pub fn new() -> crate::Result<Self> {
        let socket = Arc::new(DualStackEpollSocket::new(0)?);
        let nonces = NoncePool::new();
        let waiters = Arc::new(dashmap::DashMap::with_capacity(MAX_WAITER_CAPACITY));
        let cancellation_token = tokio_util::sync::CancellationToken::new();

        let task_socket = socket.clone();
        let task_waiters = waiters.clone();
        let task_cancellation_token = cancellation_token.clone();

        // Spawn receiver task that will receive and route packets to the registered waiters
        tokio::spawn(async move {
            receive_task(task_socket, task_waiters, task_cancellation_token).await;
        });

        Ok(Self {
            socket,
            #[cfg(test)]
            delay: None,
            nonces,
            waiters,
            cancellation_token,
        })
    }

    #[cfg(test)]
    pub fn with_artificial_delay(delay: Duration) -> crate::Result<Self> {
        QcmpTransceiver::new().map(|mut q| {
            q.delay = Some(delay);
            q
        })
    }

    /// Attempt to ping the address with the given timeout
    pub async fn ping(
        &self,
        address: std::net::SocketAddr,
        timeout: std::time::Duration,
    ) -> eyre::Result<(UtcTimestamp, Protocol)> {
        let (tx, rx) = tokio::sync::oneshot::channel::<(UtcTimestamp, Protocol)>();

        let nonce_lease = self
            .nonces
            .lease()
            .ok_or(eyre::eyre!("maximum bandwidth reached"))?;

        let nonce = nonce_lease.nonce();

        // Register our sender channel so the receiver task knows where to forward the packet
        drop(
            self.waiters
                .insert(nonce, tx)
                .inspect(|_| tracing::warn!(nonce, "waiter channel collision")),
        );

        self.socket
            .send_to(
                Protocol::ping_with_nonce(nonce).encode(&mut QcmpPacket::default()),
                address,
            )
            .await?;

        // Wait until timeout for the receiver task to forward the packet to us
        let result = tokio::time::timeout(timeout, rx).await;

        // Unregister our sender channel as we are no longer interested in a response
        drop(self.waiters.remove(&nonce));

        match result {
            Ok(result) => match result {
                #[cfg(test)]
                Ok(mut pong) => {
                    if let Some(ad) = self.delay {
                        pong.0 =
                            UtcTimestamp::from_nanos(pong.0.unix_nanos() + ad.as_nanos() as i64);
                    }

                    Ok(pong)
                }
                #[cfg(not(test))]
                Ok(pong) => Ok(pong),
                Err(error) => Err(error.into()),
            },
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait::async_trait]
impl Measurement for QcmpTransceiver {
    async fn measure_distance(
        &self,
        address: std::net::SocketAddr,
    ) -> eyre::Result<DistanceMeasure> {
        let (recv_timestamp, reply) = self
            .ping(address, std::time::Duration::from_secs(5))
            .await?;

        reply
            .distance(recv_timestamp)
            .ok_or_else(|| eyre::eyre!("received non ping reply"))
    }
}

#[inline]
pub fn port_channel() -> tokio::sync::broadcast::Sender<u16> {
    tokio::sync::broadcast::channel(1).0
}

pub fn spawn(
    socket: socket2::Socket,
    port_rx: tokio::sync::broadcast::Receiver<u16>,
    shutdown: &mut crate::signal::ShutdownHandler,
) -> crate::Result<()> {
    let finished = shutdown.push("qcmp");
    let shutdown_rx = shutdown.shutdown_rx();

    let _qcmp_thread = std::thread::Builder::new()
        .name("qcmp".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("qcmp-worker")
                .build()
                .expect("couldn't create tokio runtime in thread");

            let res = runtime.block_on(async move {
                let task = spawn_task(socket, port_rx, shutdown_rx)?;
                drop(finished.send(task.await.wrap_err("qcmp task error")));

                Ok::<_, eyre::Report>(())
            });

            if let Err(error) = res {
                tracing::error!(%error, "qcmp thread failed with an error");
            }
        })
        .expect("failed to spawn qcmp thread");

    Ok(())
}

pub(crate) fn spawn_task(
    socket: socket2::Socket,
    mut port_rx: tokio::sync::broadcast::Receiver<u16>,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
) -> crate::Result<tokio::task::JoinHandle<()>> {
    use tracing::{Instrument as _, instrument::WithSubscriber as _};

    let mut port = crate::net::socket_port(&socket);
    let mut socket = DualStackEpollSocket::new(port)?;

    Ok(tokio::task::spawn(
        async move {
            let mut input_buf = [0u8; MAX_QCMP_PACKET_LEN];
            let mut output_buf = QcmpPacket::default();
            metrics::qcmp::active(true);

            loop {
                let result = tokio::select! {
                    result = socket.recv_from(&mut input_buf) => result,
                    _ = shutdown_rx.changed() => {
                        metrics::qcmp::active(false);
                        return;
                    }
                    new_port = port_rx.recv() => {
                        tracing::info!(change=?new_port, "received qcmp port change");
                        match new_port {
                            Ok(new_port) => {
                                // Attempt to bind the new port
                                match DualStackEpollSocket::new(new_port) {
                                    Ok(new_socket) => {
                                        tracing::debug!(old_port = port, new_port, "bound QCMP server to new port");
                                        port = new_port;
                                        socket = new_socket;
                                    }
                                    Err(error) => {
                                        tracing::error!(%error, old_port = port, new_port, "failed to bind QCMP to new port, continuing to use old port to respond to QCMP pings");
                                        metrics::qcmp::errors_total("failed_port_change").inc();
                                    }
                                }
                            }
                            Err(error) => {
                                match error {
                                    tokio::sync::broadcast::error::RecvError::Closed => {
                                    }
                                    tokio::sync::broadcast::error::RecvError::Lagged(missed) => {
                                        tracing::error!(missed, "the port changed many times and we missed changes");
                                    }
                                }
                            }
                        }

                        continue;
                    }
                };

                match track_error(result.map_err(Error::from)) {
                    Ok((size, source)) => {
                        tracing::debug!(
                            %source,
                            "received QCMP ping",
                        );
                        let received_at = UtcTimestamp::now();
                        let command = match track_error(Protocol::parse(&input_buf[..size])) {
                            Ok(Some(command)) => command,
                            Ok(None) => {
                                tracing::debug!("rejected non-qcmp packet");
                                metrics::qcmp::packets_total_invalid(size);
                                continue;
                            }
                            Err(error) => {
                                tracing::debug!(%error, %source, "rejected malformed packet");
                                continue;
                            }
                        };

                        let Protocol::Ping {
                            client_timestamp,
                            nonce,
                        } = command
                        else {
                            tracing::warn!(%source, "rejected unsupported QCMP packet");
                            metrics::qcmp::packets_total_unsupported(size);
                            continue;
                        };
                        tracing::debug!(
                            %source,
                            %nonce,
                            "received QCMP ping",
                        );

                        metrics::qcmp::packets_total_valid(size);
                        Protocol::ping_reply(nonce, client_timestamp, received_at)
                            .encode(&mut output_buf);

                        tracing::debug!(
                            %source,
                            %nonce,
                            "sending QCMP pong",
                        );

                        match track_error(socket.send_to(&output_buf, source).await.map_err(Error::from)) {
                            Ok(len) => {
                                if len != output_buf.len() {
                                    tracing::error!(%source, "failed to send entire QCMP pong response, expected {} but only sent {len}", output_buf.len());
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, %source, "error responding to ping");
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "error receiving packet");
                    }
                };
            }
        }
        .instrument(tracing::debug_span!("qcmp"))
        .with_current_subscriber(),
    ))
}

fn track_error<T>(result: Result<T>) -> Result<T> {
    result.inspect_err(|error| {
        let reason = match error {
            Error::UnknownVersion(version) => format!("unknown_version: {}", version),
            Error::LengthMismatch(_, _) => "length_mismatch".into(),
            Error::InvalidCommand(command) => format!("invalid_command: {}", command),
            Error::Io(e) => format!("io: {}", e),
        };
        metrics::qcmp::errors_total(&reason).inc();
    })
}

/// The set of possible QCMP commands.
#[derive(Clone, Copy, Debug)]
pub enum Protocol {
    /// The initation of a ping command to send to a Quilkin proxy to measure
    /// latency.
    Ping {
        /// The timestamp from when the client sent the packet.
        client_timestamp: UtcTimestamp,
        /// The client's nonce.
        nonce: u8,
    },

    /// The reply from a Quilkin proxy from a [`Self::Ping`] command. Can be
    /// used with [`Protocol::round_trip_delay`] to measure latency between
    /// two machines.
    PingReply {
        /// The timestamp from when the client sent the ping packet.
        client_timestamp: UtcTimestamp,
        /// The client's nonce.
        nonce: u8,
        /// The timestamp from when the server received the ping packet.
        server_start_timestamp: UtcTimestamp,
        /// The timestamp from when the server sent the reply.
        server_transmit_timestamp: UtcTimestamp,
    },
}

impl Protocol {
    /// Creates a [`Self::Ping`] with a random nonce, should be sent
    /// as soon as possible from creation to maintain accuracy.
    pub fn ping() -> Self {
        Self::ping_with_nonce(rand::random())
    }

    /// Creates a [`Self::Ping`] with a user-specified nonce, should be sent
    /// as soon as possible from creation to maintain accuracy.
    pub fn ping_with_nonce(nonce: u8) -> Self {
        Self::Ping {
            nonce,
            client_timestamp: UtcTimestamp::now(),
        }
    }

    /// Creates a [`Self::PingReply`] from the client and server start timestamp.
    /// It's recommended to transmit as as soon as possible from creation to
    /// keep the start and transmit times as accurate as possible.
    pub fn ping_reply(
        nonce: u8,
        client_timestamp: UtcTimestamp,
        server_start_timestamp: UtcTimestamp,
    ) -> Self {
        Self::PingReply {
            nonce,
            client_timestamp,
            server_start_timestamp,
            server_transmit_timestamp: UtcTimestamp::now(),
        }
    }

    /// Encodes the protocol command into a buffer of bytes for network transmission.
    pub fn encode<'buf>(&self, buffer: &'buf mut QcmpPacket) -> &'buf [u8] {
        let mut pb = PacketBuilder::wrap(buffer);
        pb.push_slice(MAGIC_NUMBER);
        pb.push(VERSION);
        pb.push(self.discriminant());
        pb.push_slice(&self.discriminant_length().to_be_bytes());

        #[cfg(debug_assertions)]
        {
            let length = pb.offset;
            self.encode_payload(&mut pb);

            assert_eq!(pb.offset, length + usize::from(self.discriminant_length()));
        }

        #[cfg(not(debug_assertions))]
        {
            self.encode_payload(&mut pb);
        }

        pb.finalize()
    }

    /// Returns the packet's nonce.
    pub fn nonce(&self) -> u8 {
        match self {
            Protocol::Ping { nonce, .. } | Protocol::PingReply { nonce, .. } => *nonce,
        }
    }

    fn encode_payload(&self, pb: &mut PacketBuilder<'_>) {
        pb.push(self.nonce());

        let mut ets = |ts: &UtcTimestamp| {
            pb.push_slice(&ts.unix_nanos().to_be_bytes());
        };

        match self {
            Protocol::Ping {
                client_timestamp, ..
            } => {
                ets(client_timestamp);
            }
            Protocol::PingReply {
                client_timestamp,
                server_start_timestamp,
                server_transmit_timestamp,
                ..
            } => {
                ets(client_timestamp);
                ets(server_start_timestamp);
                ets(server_transmit_timestamp);
            }
        }
    }

    /// If the command is [`Protocol::PingReply`], with `client_response_timestamp`
    /// measures the roundtrip delay between the client machine, and the Quilkin
    /// proxy, using the same algorithm as [Network Time Protocol (NTP)][ntp].
    ///
    /// [ntp]: https://en.wikipedia.org/wiki/Network_Time_Protocol#Clock_synchronization_algorithm
    pub fn round_trip_delay(
        &self,
        client_response_timestamp: UtcTimestamp,
    ) -> Option<DurationNanos> {
        let Protocol::PingReply {
            client_timestamp,
            server_start_timestamp,
            server_transmit_timestamp,
            ..
        } = self
        else {
            return None;
        };

        Some(DurationNanos::from_nanos(
            (client_response_timestamp.unix_nanos() - client_timestamp.unix_nanos())
                - (server_transmit_timestamp.unix_nanos() - server_start_timestamp.unix_nanos()),
        ))
    }

    /// If the command is [`Protocol::PingReply`], with `client_response_timestamp`
    /// returns the time between the client -> server, and the server -> client.
    pub fn distance(&self, client_response_timestamp: UtcTimestamp) -> Option<DistanceMeasure> {
        let Protocol::PingReply {
            client_timestamp,
            server_start_timestamp,
            server_transmit_timestamp,
            ..
        } = self
        else {
            return None;
        };

        Some(DistanceMeasure {
            incoming: *server_start_timestamp - *client_timestamp,
            outgoing: client_response_timestamp - *server_transmit_timestamp,
        })
    }

    /// Returns the discriminant code, identifying the payload.
    const fn discriminant(&self) -> u8 {
        match self {
            Self::Ping { .. } => PING,
            Self::PingReply { .. } => PONG,
        }
    }

    /// Same as [`payload_length`] except for when the payload is already known.
    fn discriminant_length(&self) -> u16 {
        Self::payload_length(self.discriminant()).unwrap()
    }

    /// The expected length of payload based on its discriminant.
    const fn payload_length(discriminant: u8) -> Result<u16> {
        let num = match discriminant {
            PING => 1,
            PONG => 3,
            code => return Err(Error::InvalidCommand(code)),
        };

        Ok(1 + std::mem::size_of::<i64>() as u16 * num)
    }

    /// Parses the provided input, and attempts to parse it as a `Protocol`
    /// packet.
    ///
    /// Returns `None` if the magic number is not present, and thus is not a
    /// QCMP packet, and returning `Err` when it was detected as a QCMP packet,
    /// but there was an error in parsing the payload.
    pub fn parse(input: &[u8]) -> Result<Option<Self>> {
        let mut pp = PacketParser::wrap(input)?;

        let magic = pp.read_slice::<4>();
        if magic != MAGIC_NUMBER {
            return Ok(None);
        }

        let version = pp.read();
        if version != VERSION {
            return Err(Error::UnknownVersion(version));
        }

        let discriminant = pp.read();
        // Now that we know the packet kind we can ensure the rest of the expected
        // packet length is available to avoid panics
        let size = match discriminant {
            PING => MIN_QCMP_PACKET_LEN,
            PONG => MAX_QCMP_PACKET_LEN,
            unknown => return Err(Error::InvalidCommand(unknown)),
        };

        if pp.packet.len() < size {
            return Err(Error::LengthMismatch(size as _, pp.packet.len()));
        }

        let length = u16::from_be_bytes(pp.read_slice::<2>());
        let payload_length = Self::payload_length(discriminant)?;

        if length != payload_length {
            return Err(Error::LengthMismatch(length, payload_length.into()));
        }

        let remainder = pp.packet.len() - pp.offset;

        if usize::from(length) != remainder {
            return Err(Error::LengthMismatch(length, remainder));
        }

        let nonce = pp.read();

        fn parse_timestamp(pp: &mut PacketParser<'_>) -> UtcTimestamp {
            UtcTimestamp::from_nanos(i64::from_be_bytes(pp.read_slice::<8>()))
        }

        let payload = match discriminant {
            PING => Self::Ping {
                nonce,
                client_timestamp: parse_timestamp(&mut pp),
            },
            PONG => {
                let client_timestamp = parse_timestamp(&mut pp);
                let server_start_timestamp = parse_timestamp(&mut pp);
                let server_transmit_timestamp = parse_timestamp(&mut pp);
                Self::PingReply {
                    nonce,
                    client_timestamp,
                    server_start_timestamp,
                    server_transmit_timestamp,
                }
            }
            _ => unreachable!("we should have already verified the discriminant"),
        };

        Ok(Some(payload))
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("unknown version: {0}")]
    UnknownVersion(u8),
    #[error(
        "available input does not match specified packet length. specified: {0}, available: {1}"
    )]
    LengthMismatch(u16, usize),
    #[error("unknown command code: {0}")]
    InvalidCommand(u8),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use crate::net::raw_socket_with_reuse;

    use super::*;

    #[test]
    fn ping() {
        #[rustfmt::skip]
        const INPUT: &[u8] = &[
            // Magic
            b'Q', b'L', b'K', b'N',
            // Version
            0,
            // Code
            PING,
            // Length
            0, 9,
            // Nonce
            0xBF,
            // Payload
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
        ];

        let ping = Protocol::parse(INPUT).unwrap().unwrap();

        assert!(matches!(ping, Protocol::Ping { nonce: 0xBF, .. }));

        let mut packet = QcmpPacket::default();
        assert_eq!(ping.encode(&mut packet), INPUT);
    }

    #[test]
    fn ping_reply() {
        #[rustfmt::skip]
        const INPUT: &[u8] = &[
            // Magic
            b'Q', b'L', b'K', b'N',
            // Version
            0,
            // Code
            PONG,
            // Length
            0, 25,
            // Nonce
            0xBF,
            // Payload
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
        ];

        let ping_reply = Protocol::parse(INPUT).unwrap().unwrap();

        assert!(matches!(
            ping_reply,
            Protocol::PingReply { nonce: 0xBF, .. }
        ));
        let mut packet = QcmpPacket::default();
        assert_eq!(ping_reply.encode(&mut packet), INPUT);
    }

    #[test]
    fn reject_malformed_packet() {
        #[rustfmt::skip]
        const INPUT: &[u8] = &[
            // Magic
            b'Q', b'L', b'K', b'N',
            // Version
            0,
            // Code (intentionally Ping)
            PING,
            // Length
            0, 25,
            // Nonce
            0xBF,
            // Payload
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
        ];

        Protocol::parse(INPUT).unwrap_err();
    }

    #[test]
    fn reject_unknown_packet() {
        #[rustfmt::skip]
        const INPUT: &[u8] = &[
            // Magic
            b'Q', b'L', b'K', b'N',
            // Version
            0,
            // Code
            0xff,
        ];

        Protocol::parse(INPUT).unwrap_err();
    }

    #[test]
    fn reject_unknown_version() {
        #[rustfmt::skip]
        const INPUT: &[u8] = &[
            // Magic
            b'Q', b'L', b'K', b'N',
            // Version
            0xff,
        ];

        Protocol::parse(INPUT).unwrap_err();
    }

    #[test]
    fn reject_no_magic_header() {
        #[rustfmt::skip]
        const INPUT: &[u8] = &[
            // Magic
            b'Q', 0xff, b'K', b'N',
            // Version
            0,
            // Code
            PING,
            // Length
            0, 9,
            // Nonce
            0xBF,
            // Payload
            0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
        ];

        assert!(Protocol::parse(INPUT).unwrap().is_none());
    }

    #[tokio::test]
    #[cfg_attr(target_os = "macos", ignore)]
    async fn qcmp_measurement() {
        let socket = raw_socket_with_reuse(0).unwrap();
        let addr = socket.local_addr().unwrap().as_socket().unwrap();

        let (_tx, rx) = crate::signal::channel();
        let pc = super::port_channel();
        spawn_task(socket, pc.subscribe(), rx).unwrap();

        let delay = Duration::from_millis(50);
        let node = QcmpTransceiver::with_artificial_delay(delay).unwrap();

        // fire messages until we get one back, so we know the socket is ready.
        let mut check = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if node.measure_distance(addr).await.is_ok() {
                check = true;
                break;
            }
        }
        assert!(check, "timed out on initial qcmp spawn");

        for _ in 0..3 {
            let dm = node.measure_distance(addr).await.unwrap();
            let total = dm.total();

            assert!(
                total > delay && total < delay * 2,
                "Node1's distance is {total:?}, expected > {delay:?} and less than {:?}",
                delay * 2
            );
        }
    }

    #[tokio::test]
    async fn nonce_pool() {
        // we want to lease _all_ of the available nonces
        let num_leasers = u8::MAX as usize;
        // and then wait for one extra lease attempt that will fail
        let barrier_limit = num_leasers + 1;

        let nonce_pool = NoncePool::new();
        let barrier_one = Arc::new(tokio::sync::Barrier::new(barrier_limit));
        let barrier_two = Arc::new(tokio::sync::Barrier::new(barrier_limit));
        let mut handles = Vec::with_capacity(num_leasers);

        for _ in 0..num_leasers {
            let nb = nonce_pool.clone();
            let b1 = barrier_one.clone();
            let b2 = barrier_two.clone();

            handles.push(tokio::spawn(async move {
                let nonce_lease = nb.lease();
                assert!(nonce_lease.is_some());
                let nonce_lease = nonce_lease.unwrap();
                let nonce = nonce_lease.nonce();

                b1.wait().await;
                b2.wait().await;

                nonce
            }));
        }

        // Make sure all tasks have reached the first barrier
        println!("waiting at barrier one");
        barrier_one.wait().await;

        // Ensure all nonces have been leased out
        assert!(nonce_pool.lease().is_none());

        // Release all waiting tasks
        println!("waiting at barrier two");
        barrier_two.wait().await;

        let mut results = Vec::with_capacity(num_leasers);
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // Ensure that all leased nonces were unique
        let mut set = std::collections::HashSet::with_capacity(num_leasers);
        assert!(results.iter().all(|x| set.insert(x)));
    }
}
