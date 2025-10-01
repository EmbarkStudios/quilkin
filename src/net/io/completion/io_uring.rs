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

//! We have two cases in the proxy where io-uring is used that are _almost_ identical
//! so this just has a shared implementation of utilities
//!
//! Note there is also the QCMP loop, but that one is simpler and is different
//! enough that it doesn't make sense to share the same code

use std::{
    os::fd::{AsRawFd, FromRawFd},
    sync::Arc,
};

use eyre::Context as _;
use io_uring::{squeue::Entry, types::Fd};
use socket2::SockAddr;

use crate::{
    collections::PoolBuffer,
    config::filter::CachedFilterChain,
    metrics,
    net::{PacketQueue, error::PipelineError, packet::queue::SendPacket, sessions::SessionPool},
    time::UtcTimestamp,
};

static SESSION_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl crate::net::io::Listener {
    pub fn spawn_io_loop(
        self,
        pending_sends: crate::net::PacketQueue,
        filter_chain: CachedFilterChain,
    ) -> eyre::Result<()> {
        let Self {
            worker_id,
            port,
            config,
            sessions,
            buffer_pool,
        } = self;

        let socket =
            crate::net::DualStackLocalSocket::new(port).context("failed to bind socket")?;

        let io_loop = IoUringLoop::new(2000, socket)?;
        io_loop
            .spawn_io_loop(
                format!("packet-router-{worker_id}"),
                PacketProcessorCtx::Router {
                    config,
                    sessions,
                    worker_id,
                    destinations: Vec::with_capacity(1),
                },
                pending_sends,
                buffer_pool,
                filter_chain,
            )
            .context("failed to spawn io-uring loop")
    }
}

/// A simple wrapper around [eventfd](https://man7.org/linux/man-pages/man2/eventfd.2.html)
///
/// We use eventfd to signal to io uring loops from async tasks, it is essentially
/// the equivalent of a signalling 64 bit cross-process atomic
pub struct EventFd {
    fd: std::os::fd::OwnedFd,
    val: u64,
}

impl EventFd {
    #[inline]
    pub(crate) fn new() -> std::io::Result<Self> {
        // SAFETY: We have no invariants to uphold, but we do need to check the
        // return value
        let fd = unsafe { libc::eventfd(0, 0) };

        // This can fail for various reasons mostly around resource limits, if
        // this is hit there is either something really wrong (OOM, too many file
        // descriptors), or resource limits were externally placed that were too strict
        if fd == -1 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            // SAFETY: we've validated the file descriptor
            fd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
            val: 0,
        })
    }

    #[inline]
    pub(crate) fn writer(&self) -> EventFdWriter {
        EventFdWriter {
            fd: self.fd.as_raw_fd(),
        }
    }

    /// Constructs an io-uring entry to read (ie wait) on this eventfd
    #[inline]
    pub(crate) fn io_uring_entry(&mut self) -> Entry {
        io_uring::opcode::Read::new(
            Fd(self.fd.as_raw_fd()),
            (&mut self.val as *mut u64).cast(),
            8,
        )
        .build()
    }
}

#[derive(Clone)]
pub(crate) struct EventFdWriter {
    fd: i32,
}

impl EventFdWriter {
    #[inline]
    pub(crate) fn write(&self, val: u64) {
        // SAFETY: we have a valid descriptor, and most of the errors that apply
        // to the general write call that eventfd_write wraps are not applicable
        //
        // Note that while the docs state eventfd_write is glibc, it is implemented
        // on musl as well, but really is just a write with 8 bytes
        unsafe {
            libc::eventfd_write(self.fd, val);
        }
    }
}

struct RecvPacket {
    /// The buffer filled with data during `recv_from`
    buffer: PoolBuffer,
    /// The IP of the sender
    source: std::net::SocketAddr,
}

enum LoopPacketInner {
    Recv(RecvPacket),
    Send(SendPacket),
}

/// A packet that is currently on the io-uring loop, either being received or sent
///
/// The struct is expected to be pinned at a location in memory in a slab, as we
/// give pointers to the internal data in the struct, which also contains
/// referential pointers that need to stay pinned until the I/O is complete
#[repr(C)]
struct LoopPacket {
    msghdr: libc::msghdr,
    addr: socket2::SockAddrStorage,
    packet: Option<LoopPacketInner>,
    io_vec: libc::iovec,
}

impl LoopPacket {
    #[inline]
    fn new() -> Self {
        Self {
            // SAFETY: msghdr is POD
            msghdr: unsafe { std::mem::zeroed() },
            packet: None,
            io_vec: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            addr: socket2::SockAddrStorage::zeroed(),
        }
    }

    #[inline]
    fn set_packet(&mut self, mut packet: LoopPacketInner) {
        match &mut packet {
            LoopPacketInner::Recv(recv) => {
                // For receives, the length of the buffer is the total capacity
                self.io_vec.iov_base = recv.buffer.as_mut_ptr().cast();
                self.io_vec.iov_len = recv.buffer.capacity();
            }
            LoopPacketInner::Send(send) => {
                // For sends, the length of the buffer is the actual number of initialized bytes,
                // and note that iov_base is a *mut even though for sends the buffer is not actually
                // mutated
                self.io_vec.iov_base = (send.data.as_ptr() as *mut u8).cast();
                self.io_vec.iov_len = send.data.len();

                // SAFETY: both pointers are valid at this point, with the same size
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        send.destination.as_ptr().cast(),
                        &mut self.addr,
                        1,
                    );
                }
            }
        }

        // Increment the refcount of the buffer to ensure it stays alive for the
        // duration of the I/O
        self.packet = Some(packet);

        self.msghdr.msg_iov = std::ptr::addr_of_mut!(self.io_vec);
        self.msghdr.msg_iovlen = 1;
        self.msghdr.msg_name = std::ptr::addr_of_mut!(self.addr).cast();
        self.msghdr.msg_namelen = std::mem::size_of::<socket2::SockAddrStorage>() as _;
    }

    #[inline]
    fn finalize_recv(mut self, ret: usize) -> RecvPacket {
        let LoopPacketInner::Recv(mut recv) = self.packet.take().unwrap() else {
            unreachable!("finalized a send packet")
        };

        // SAFETY: we're initialising it with correctly sized data
        let mut source = unsafe {
            SockAddr::new(
                self.addr,
                std::mem::size_of::<libc::sockaddr_storage>() as _,
            )
        }
        .as_socket()
        .unwrap();
        source.set_ip(source.ip().to_canonical());

        recv.source = source;
        recv.buffer.set_len(ret);
        recv
    }

    #[inline]
    fn finalize_send(mut self) -> SendPacket {
        let LoopPacketInner::Send(send) = self.packet.take().unwrap() else {
            unreachable!("finalized a recv packet")
        };

        send
    }
}

pub enum PacketProcessorCtx {
    Router {
        config: Arc<crate::config::Config>,
        sessions: Arc<SessionPool>,
        worker_id: usize,
        destinations: Vec<crate::net::EndpointAddress>,
    },
    SessionPool {
        pool: Arc<SessionPool>,
        port: u16,
    },
}

fn process_packet(
    ctx: &mut PacketProcessorCtx,
    filters: &crate::filters::FilterChain,
    packet: RecvPacket,
    last_received_at: &mut Option<UtcTimestamp>,
) {
    match ctx {
        PacketProcessorCtx::Router {
            config,
            sessions,
            worker_id,
            destinations,
        } => {
            let received_at = UtcTimestamp::now();
            if let Some(last_received_at) = last_received_at {
                metrics::packet_jitter(metrics::READ, &metrics::EMPTY)
                    .set((received_at - *last_received_at).nanos());
            }
            *last_received_at = Some(received_at);

            let ds_packet = crate::net::packet::DownstreamPacket {
                contents: packet.buffer,
                source: packet.source,
                filters,
            };

            ds_packet.process(*worker_id, config, sessions, destinations);
        }
        PacketProcessorCtx::SessionPool { pool, port, .. } => {
            let mut last_received_at = None;

            pool.process_received_upstream_packet(
                packet.buffer,
                packet.source,
                *port,
                &mut last_received_at,
                filters,
            );
        }
    }
}

#[inline]
fn empty_net_addr() -> std::net::SocketAddr {
    std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
}

enum Token {
    /// Packet received
    Recv { key: usize },
    /// Packet sent
    Send { key: usize },
    /// One or more packets are ready to be sent OR shutdown of the loop is requested
    PendingsSends,
}

struct LoopCtx<'uring> {
    sq: io_uring::squeue::SubmissionQueue<'uring, Entry>,
    backlog: std::collections::VecDeque<Entry>,
    socket_fd: Fd,
    tokens: slab::Slab<Token>,
    /// Packets currently being received or sent in the io-uring loop
    loop_packets: slab::Slab<LoopPacket>,
}

impl<'uring> LoopCtx<'uring> {
    #[inline]
    fn sync(&mut self) {
        self.sq.sync();
    }

    /// Enqueues a `recv_from` on the socket
    #[inline]
    fn enqueue_recv(&mut self, buffer: crate::collections::PoolBuffer) {
        let packet = LoopPacketInner::Recv(RecvPacket {
            buffer,
            source: empty_net_addr(),
        });

        let (key, msghdr) = {
            let entry = self.loop_packets.vacant_entry();
            let key = entry.key();
            let pp = entry.insert(LoopPacket::new());
            pp.set_packet(packet);
            (key, std::ptr::addr_of_mut!(pp.msghdr))
        };

        let token = self.tokens.insert(Token::Recv { key });
        self.push(
            io_uring::opcode::RecvMsg::new(self.socket_fd, msghdr)
                .build()
                .user_data(token as _),
        );
    }

    /// Enqueues a `send_to` on the socket
    #[inline]
    fn enqueue_send(&mut self, packet: SendPacket) {
        // We rely on sends using state with stable addresses, but realistically we should
        // never be at capacity
        if self.loop_packets.capacity() - self.loop_packets.len() == 0 {
            metrics::errors_total(
                metrics::WRITE,
                "io-uring packet send slab is at capacity",
                &packet.asn_info.as_ref().into(),
            );
            return;
        }

        let (key, msghdr) = {
            let entry = self.loop_packets.vacant_entry();
            let key = entry.key();
            let pp = entry.insert(LoopPacket::new());
            pp.set_packet(LoopPacketInner::Send(packet));
            (key, std::ptr::addr_of!(pp.msghdr))
        };

        let token = self.tokens.insert(Token::Send { key });
        self.push(
            io_uring::opcode::SendMsg::new(self.socket_fd, msghdr)
                .build()
                .user_data(token as _),
        );
    }

    #[inline]
    fn pop_packet(&mut self, key: usize) -> LoopPacket {
        self.loop_packets.remove(key)
    }

    /// For now we have a backlog, but this would basically mean that we are receiving
    /// more upstream packets than we can send downstream, which should? never happen
    #[inline]
    fn process_backlog(&mut self, submitter: &io_uring::Submitter<'uring>) -> std::io::Result<()> {
        loop {
            if self.sq.is_full() {
                match submitter.submit() {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
                    Err(err) => return Err(err),
                }
            }
            self.sq.sync();

            match self.backlog.pop_front() {
                // SAFETY: Same as Self::push, all memory pointed to in our ops are pinned at
                // stable locations in memory
                Some(sqe) => unsafe {
                    let _ = self.sq.push(&sqe);
                },
                None => break,
            }
        }

        Ok(())
    }

    #[inline]
    fn push_with_token(&mut self, entry: Entry, token: Token) {
        let token = self.tokens.insert(token);
        self.push(entry.user_data(token as _));
    }

    #[inline]
    fn push(&mut self, entry: Entry) {
        // SAFETY: we keep all memory/file descriptors alive and in a stable locations
        // for the duration of the I/O requests
        unsafe {
            if self.sq.push(&entry).is_err() {
                self.backlog.push_back(entry);
            }
        }
    }

    #[inline]
    fn remove(&mut self, token: usize) -> Token {
        self.tokens.remove(token)
    }
}

pub struct IoUringLoop {
    socket: crate::net::DualStackLocalSocket,
    concurrent_sends: usize,
}

impl IoUringLoop {
    pub fn new(
        concurrent_sends: u16,
        socket: crate::net::DualStackLocalSocket,
    ) -> Result<Self, PipelineError> {
        Ok(Self {
            concurrent_sends: concurrent_sends as _,
            socket,
        })
    }

    pub fn spawn_io_loop(
        self,
        thread_name: String,
        mut ctx: PacketProcessorCtx,
        pending_sends: PacketQueue,
        buffer_pool: Arc<crate::collections::BufferPool>,
        mut filter_chain: CachedFilterChain,
    ) -> Result<(), PipelineError> {
        let dispatcher = tracing::dispatcher::get_default(|d| d.clone());

        let socket = self.socket;
        let concurrent_sends = self.concurrent_sends;

        let mut ring = io_uring::IoUring::new((concurrent_sends + 3) as _)?;

        let mut pending_sends_event = pending_sends.1;
        let pending_sends = pending_sends.0;

        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                crate::metrics::game_traffic_tasks().inc();
                let _guard = tracing::dispatcher::set_default(&dispatcher);

                let tokens = slab::Slab::with_capacity(concurrent_sends + 1 + 1);
                let loop_packets = slab::Slab::with_capacity(concurrent_sends + 1);

                // Just double buffer the pending writes for simplicity
                let mut double_pending_sends = Vec::with_capacity(pending_sends.capacity());

                // When sending packets, this is the direction used when updating metrics
                let send_dir = if matches!(ctx, PacketProcessorCtx::Router { .. }) {
                    metrics::WRITE
                } else {
                    metrics::READ
                };

                let (submitter, sq, mut cq) = ring.split();

                let mut loop_ctx = LoopCtx {
                    sq,
                    socket_fd: socket.raw_fd(),
                    backlog: Default::default(),
                    loop_packets,
                    tokens,
                };

                loop_ctx.enqueue_recv(buffer_pool.clone().alloc());
                loop_ctx
                    .push_with_token(pending_sends_event.io_uring_entry(), Token::PendingsSends);

                // Sync always needs to be called when entries have been pushed
                // onto the submission queue for the loop to actually function (ie, similar to await on futures)
                loop_ctx.sync();

                let mut last_received_at = None;

                // The core io uring loop
                'io: loop {
                    match submitter.submit_and_wait(1) {
                        Ok(_) => {}
                        Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {}
                        Err(ref err) if err.raw_os_error() == Some(libc::EINTR) => {
                            continue;
                        }
                        Err(error) => {
                            tracing::error!(%error, "io-uring submit_and_wait failed");
                            break 'io;
                        }
                    }
                    cq.sync();

                    if let Err(error) = loop_ctx.process_backlog(&submitter) {
                        tracing::error!(%error, "failed to process io-uring backlog");
                        break 'io;
                    }

                    // Now actually process all of the completed io requests
                    for cqe in &mut cq {
                        let ret = cqe.result();
                        let token_index = cqe.user_data() as usize;

                        let token = loop_ctx.remove(token_index);
                        match token {
                            Token::Recv { key } => {
                                // Pop the packet regardless of whether we failed or not so that
                                // we don't consume a buffer slot forever
                                let packet = loop_ctx.pop_packet(key);

                                if ret < 0 {
                                    let error = std::io::Error::from_raw_os_error(-ret);
                                    tracing::error!(%error, "error receiving packet");
                                    loop_ctx.enqueue_recv(buffer_pool.clone().alloc());
                                    continue;
                                }

                                let packet = packet.finalize_recv(ret as usize);
                                let filters = filter_chain.load();
                                process_packet(&mut ctx, filters, packet, &mut last_received_at);

                                loop_ctx.enqueue_recv(buffer_pool.clone().alloc());
                            }
                            Token::PendingsSends => {
                                double_pending_sends = pending_sends.swap(double_pending_sends);
                                loop_ctx.push_with_token(
                                    pending_sends_event.io_uring_entry(),
                                    Token::PendingsSends,
                                );

                                for pending in
                                    double_pending_sends.drain(0..double_pending_sends.len())
                                {
                                    loop_ctx.enqueue_send(pending);
                                }
                            }
                            Token::Send { key } => {
                                let packet = loop_ctx.pop_packet(key).finalize_send();
                                let asn_info = packet.asn_info.as_ref().into();

                                if ret < 0 {
                                    let source =
                                        std::io::Error::from_raw_os_error(-ret).to_string();
                                    metrics::errors_total(send_dir, &source, &asn_info).inc();
                                    metrics::packets_dropped_total(send_dir, &source, &asn_info)
                                        .inc();
                                } else if ret as usize != packet.data.len() {
                                    metrics::packets_total(send_dir, &asn_info).inc();
                                    metrics::errors_total(
                                        send_dir,
                                        "sent bytes != packet length",
                                        &asn_info,
                                    )
                                    .inc();
                                } else {
                                    metrics::packets_total(send_dir, &asn_info).inc();
                                    metrics::bytes_total(send_dir, &asn_info).inc_by(ret as u64);
                                }
                            }
                        }
                    }

                    loop_ctx.sync();
                }

                crate::metrics::game_traffic_task_closed().inc();
            })?;

        Ok(())
    }
}

impl SessionPool {
    pub(crate) fn spawn_session(
        self: Arc<Self>,
        raw_socket: socket2::Socket,
        port: u16,
        pending_sends: crate::net::PacketQueue,
        filter_chain: CachedFilterChain,
    ) -> Result<(), PipelineError> {
        let pool = self;
        let id = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _thread_span = uring_span!(tracing::debug_span!("session", id).or_current());

        let io_loop =
            IoUringLoop::new(2000, crate::net::DualStackLocalSocket::from_raw(raw_socket))?;
        let buffer_pool = pool.buffer_pool.clone();

        io_loop.spawn_io_loop(
            format!("session-{id}"),
            PacketProcessorCtx::SessionPool { pool, port },
            pending_sends,
            buffer_pool,
            filter_chain,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// This is just a sanity check that eventfd, which we use to notify the io-uring
    /// loop of events from async tasks, functions as we need to, namely that
    /// an event posted before the I/O request is submitted to the I/O loop still
    /// triggers the completion of the I/O request
    #[test]
    #[cfg(target_os = "linux")]
    #[allow(clippy::undocumented_unsafe_blocks)]
    fn eventfd_works_as_expected() {
        let mut event = EventFd::new().unwrap();
        let event_writer = event.writer();

        // Write even before we create the loop
        event_writer.write(1);

        let mut ring = io_uring::IoUring::new(2).unwrap();
        let (submitter, mut sq, mut cq) = ring.split();

        unsafe {
            sq.push(&event.io_uring_entry().user_data(1)).unwrap();
        }

        sq.sync();

        loop {
            match submitter.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {}
                Err(error) => {
                    panic!("oh no {error}");
                }
            }
            cq.sync();

            for cqe in &mut cq {
                assert_eq!(cqe.result(), 8);

                match cqe.user_data() {
                    // This was written before the loop started, but now write to the event
                    // before queuing up the next read
                    1 => {
                        assert_eq!(event.val, 1);
                        event_writer.write(9999);

                        unsafe {
                            sq.push(&event.io_uring_entry().user_data(2)).unwrap();
                        }
                    }
                    2 => {
                        assert_eq!(event.val, 9999);
                        return;
                    }
                    _ => unreachable!(),
                }
            }

            sq.sync();
        }
    }
}
