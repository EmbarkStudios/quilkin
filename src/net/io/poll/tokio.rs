/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

//! Tokio epoll-based UDP I/O backend — available on all platforms.

/// On linux spawns a io-uring runtime + thread, everywhere else spawns a regular tokio task.
macro_rules! uring_spawn {
    ($span:expr_2021, $future:expr_2021) => {{
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        use tracing::Instrument as _;

        use tracing::instrument::WithSubscriber as _;

        let fut = async move {
            let _ = tx.send(());
            $future.await
        };

        if let Some(span) = $span {
            tokio::spawn(fut.instrument(span).with_current_subscriber());
        } else {
            tokio::spawn(fut.with_current_subscriber());
        }
        rx
    }};
}

macro_rules! uring_inner_spawn {
    ($future:expr_2021) => {
        tokio::spawn($future);
    };
}

// On Linux, DualStackEpollSocket wraps a tokio::net::UdpSocket for async I/O.
// On non-Linux, DualStackLocalSocket already wraps a tokio socket.
cfg_select! {
    target_os = "linux" => {
        type PollSocket = crate::net::DualStackEpollSocket;
        type PollSocketRc = std::sync::Arc<PollSocket>;

        fn new_poll_socket(port: u16) -> std::io::Result<PollSocketRc> {
            PollSocket::new(port).map(std::sync::Arc::new)
        }

        fn poll_socket_from_raw(socket: socket2::Socket) -> std::io::Result<PollSocketRc> {
            PollSocket::from_raw(socket).map(std::sync::Arc::new)
        }

        async fn ps_recv_from(
            socket: &PollSocket,
            mut buf: bytes::BytesMut,
        ) -> (std::io::Result<(usize, std::net::SocketAddr)>, bytes::BytesMut) {
            let result = socket.recv_from(&mut buf).await;
            (result, buf)
        }

        async fn ps_send_to(
            socket: &PollSocket,
            data: bytes::Bytes,
            dest: std::net::SocketAddr,
        ) -> (std::io::Result<usize>, bytes::Bytes) {
            let result = socket.send_to(&data, dest).await;
            (result, data)
        }
    }
    _ => {
        type PollSocket = crate::net::DualStackLocalSocket;
        type PollSocketRc = crate::net::DualStackLocalSocketRc;

        fn new_poll_socket(port: u16) -> std::io::Result<PollSocketRc> {
            PollSocket::new(port).map(|s| s.make_refcnt())
        }

        fn poll_socket_from_raw(socket: socket2::Socket) -> std::io::Result<PollSocketRc> {
            Ok(std::sync::Arc::new(PollSocket::from_raw(socket)))
        }

        async fn ps_recv_from(
            socket: &PollSocket,
            buf: bytes::BytesMut,
        ) -> (std::io::Result<(usize, std::net::SocketAddr)>, bytes::BytesMut) {
            socket.recv_from(buf).await
        }

        async fn ps_send_to(
            socket: &PollSocket,
            data: bytes::Bytes,
            dest: std::net::SocketAddr,
        ) -> (std::io::Result<usize>, bytes::Bytes) {
            socket.send_to(data, dest).await
        }
    }
}

/// Extract the watch receiver from a `PacketQueueReceiver`.
fn extract_watch_receiver(
    pqr: crate::net::PacketQueueReceiver,
) -> eyre::Result<tokio::sync::watch::Receiver<bool>> {
    cfg_select! {
        target_os = "linux" => {
            match pqr {
                crate::net::PacketQueueReceiver::Watch(rx) => Ok(rx),
                crate::net::PacketQueueReceiver::EventFd(_) => eyre::bail!("poll backend requires a Watch packet queue receiver"),
            }
        }
        _ => {
            Ok(pqr)
        }
    }
}

pub fn spawn_listener(
    listener: crate::net::io::Listener,
    packet_queue: crate::net::PacketQueue,
    fc: crate::config::filter::CachedFilterChain,
) -> eyre::Result<()> {
    spawn_poll_listener_impl(listener, packet_queue, fc)
}

fn spawn_poll_listener_impl(
    listener: crate::net::io::Listener,
    packet_queue: crate::net::PacketQueue,
    mut fc: crate::config::filter::CachedFilterChain,
) -> eyre::Result<()> {
    let crate::net::io::Listener {
        worker_id,
        port,
        config,
        sessions,
        ..
    } = listener;

    let thread_span = uring_span!(tracing::debug_span!("receiver", id = worker_id).or_current());
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    let (pqs, pqr) = packet_queue;
    let mut sends_rx = extract_watch_receiver(pqr)?;

    let worker = uring_spawn!(thread_span, async move {
        crate::metrics::game_traffic_tasks().inc();
        let mut last_received_at = None;
        let socket = new_poll_socket(port).unwrap();

        tracing::trace!(port, "bound worker");
        let send_socket = socket.clone();

        let inner_task = async move {
            let mut sends_double_buffer = Vec::with_capacity(pqs.capacity());

            while sends_rx.changed().await.is_ok() {
                if !*sends_rx.borrow() {
                    tracing::trace!("io loop shutdown requested");
                    break;
                }

                sends_double_buffer = pqs.swap(sends_double_buffer);

                for packet in sends_double_buffer.drain(..sends_double_buffer.len()) {
                    let destination = packet.destination;
                    let (result, _) = ps_send_to(&send_socket, packet.data, destination).await;
                    let asn_info = packet.asn_info.as_ref().into();
                    match result {
                        Ok(size) => {
                            crate::metrics::packets_total(crate::metrics::WRITE, &asn_info).inc();
                            crate::metrics::bytes_total(crate::metrics::WRITE, &asn_info)
                                .inc_by(size as u64);
                        }
                        Err(error) => {
                            let source = error.to_string();
                            crate::metrics::errors_total(crate::metrics::WRITE, &source, &asn_info)
                                .inc();
                            crate::metrics::packets_dropped_total(
                                crate::metrics::WRITE,
                                &source,
                                &asn_info,
                            )
                            .inc();
                        }
                    }
                }
            }

            let _ = tx.send(());
        };

        cfg_select! {
            debug_assertions => {
                uring_inner_spawn!(inner_task.instrument(tracing::debug_span!("upstream").or_current()));
            }
            _ => {
                uring_inner_spawn!(inner_task);
            }
        }

        let mut destinations = Vec::with_capacity(1);

        loop {
            let buffer = bytes::BytesMut::zeroed(2048);

            tokio::select! {
                received = ps_recv_from(&socket, buffer) => {
                    let received_at = crate::time::UtcTimestamp::now();
                    let (result, buffer) = received;

                    match result {
                        Ok((size, source)) => {
                            let mut buffer = buffer;
                            buffer.truncate(size);
                            let filters = fc.load();
                            let packet = crate::net::packet::DownstreamPacket { contents: buffer, source, filters };

                            if let Some(last_received_at) = last_received_at {
                                crate::metrics::packet_jitter(
                                    crate::metrics::READ,
                                    &crate::metrics::EMPTY,
                                )
                                .set((received_at - last_received_at).nanos());
                            }
                            last_received_at = Some(received_at);

                            packet.process(
                                worker_id,
                                &config,
                                &sessions,
                                &mut destinations,
                            );
                        }
                        Err(error) => {
                            crate::metrics::errors_total(
                                crate::metrics::READ,
                                &error.to_string(),
                                &crate::metrics::EMPTY,
                            )
                            .inc();
                            tracing::error!(%error, "error receiving packet");
                            return;
                        }
                    }
                }
                _ = &mut rx => {
                    crate::metrics::game_traffic_task_closed().inc();
                    tracing::debug!("Closing downstream socket loop, shutdown requested");
                    return;
                }
            }
        }
    });

    use eyre::WrapErr as _;
    worker.recv().context("failed to spawn receiver task")?;
    Ok(())
}

pub fn spawn_session(
    pool: std::sync::Arc<crate::net::sessions::SessionPool>,
    raw_socket: socket2::Socket,
    port: u16,
    pending_sends: crate::net::PacketQueue,
    mut filters: crate::config::filter::CachedFilterChain,
) -> Result<(), crate::net::error::PipelineError> {
    let (pqs, pqr) = pending_sends;
    let mut sends_rx = match extract_watch_receiver(pqr) {
        Ok(rx) => rx,
        Err(_) => {
            return Err(crate::net::error::PipelineError::Session(
                crate::net::sessions::SessionError::SocketAddressUnavailable,
            ));
        }
    };

    let socket = match poll_socket_from_raw(raw_socket) {
        Ok(s) => s,
        Err(e) => return Err(e.into()),
    };

    uring_spawn!(
        uring_span!(tracing::debug_span!("session pool")),
        async move {
            let mut last_received_at = None;

            let socket2 = socket.clone();
            let (tx, mut rx) = tokio::sync::oneshot::channel();

            uring_inner_spawn!(async move {
                let mut sends_double_buffer = Vec::with_capacity(pqs.capacity());

                while sends_rx.changed().await.is_ok() {
                    if !*sends_rx.borrow() {
                        tracing::trace!("io loop shutdown requested");
                        break;
                    }

                    sends_double_buffer = pqs.swap(sends_double_buffer);

                    for packet in sends_double_buffer.drain(..sends_double_buffer.len()) {
                        let destination = packet.destination;
                        tracing::trace!(
                            %destination,
                            length = packet.data.len(),
                            "sending packet upstream"
                        );
                        let (result, _) = ps_send_to(&socket2, packet.data, destination).await;
                        let asn_info = packet.asn_info.as_ref().into();
                        match result {
                            Ok(size) => {
                                crate::metrics::packets_total(crate::metrics::READ, &asn_info)
                                    .inc();
                                crate::metrics::bytes_total(crate::metrics::READ, &asn_info)
                                    .inc_by(size as u64);
                            }
                            Err(error) => {
                                tracing::trace!(%error, "sending packet upstream failed");
                                let source = error.to_string();
                                crate::metrics::errors_total(
                                    crate::metrics::READ,
                                    &source,
                                    &asn_info,
                                )
                                .inc();
                                crate::metrics::packets_dropped_total(
                                    crate::metrics::READ,
                                    &source,
                                    &asn_info,
                                )
                                .inc();
                            }
                        }
                    }
                }

                let _ = tx.send(());
            });

            loop {
                let buf = bytes::BytesMut::zeroed(2048);
                tokio::select! {
                    received = ps_recv_from(&socket, buf) => {
                        let (result, buf) = received;
                        match result {
                            Err(error) => {
                                tracing::trace!(%error, "error receiving packet");
                                crate::metrics::errors_total(crate::metrics::WRITE, &error.to_string(), &crate::metrics::EMPTY).inc();
                            },
                            Ok((size, recv_addr)) => {
                                let mut buf = buf;
                                buf.truncate(size);
                                let filters = filters.load();
                                pool.process_received_upstream_packet(buf, recv_addr, port, &mut last_received_at, filters);
                            },
                        }
                    }
                    _ = &mut rx => {
                        tracing::debug!("Closing upstream socket loop, downstream closed");
                        return;
                    }
                }
            }
        }
    );

    Ok(())
}
