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

struct RecvPacket<'rb> {
    /// The packet contents
    buffer: super::ring::RingBuffer<'rb>,
    /// The address of the source
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
    packet: RecvPacket<'_>,
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

// io-uring keeps many things private which is incredibly tedious
const IORING_OP_RECVMSG: u8 = 10;

mod flags {
    pub type Enum = u32;

    /// If set, the upper 16 bits of the flags field carries the buffer ID that was chosen for this request.
    ///
    /// The request must have been issued with `IOSQE_BUFFER_SELECT` set, and used with a request type that supports
    /// buffer selection. Additionally, buffers must have been provided upfront either via the IORING_OP_PROVIDE_BUFFERS
    /// or the IORING_REGISTER_PBUF_RING methods.
    pub const IORING_CQE_F_BUFFER: Enum = 1 << 0;
    /// If set, the application should expect more completions from the request.
    ///
    /// This is used for requests that can generate multiple completions, such as multi-shot requests, receive, or accept.
    pub const IORING_CQE_F_MORE: Enum = 1 << 1;
}

enum Token {
    /// Packet received
    Recv { key: usize },
    /// Packet sent
    Send { key: usize },
    /// One or more packets are ready to be sent OR shutdown of the loop is requested
    PendingSends,
}

struct LoopCtx<'uring> {
    sq: io_uring::squeue::SubmissionQueue<'uring, Entry>,
    backlog: std::collections::VecDeque<Entry>,
    socket_fd: Fd,
    /// Packets currently being received or sent in the io-uring loop
    loop_packets: slab::Slab<LoopPacket>,
    /// The buffer ring for receives, it's our responsibility to return buffers
    /// to this when we are done, otherwise we'll get an `-ENOBUFS` error
    recv_ring: super::ring::BufferRing,
    /// The msghdr for receives, for multishot this is only used for the `namelen`
    /// and `controllen` fields, as the kernel will prepend the details for the
    /// individual recvmsg into the buffer itself, followed by the actual payload
    recv_hdr: libc::msghdr,
}

impl<'uring> LoopCtx<'uring> {
    #[inline]
    fn sync(&mut self) {
        self.sq.sync();
    }

    /// Enqueues a multishot [`IORING_OP_RECVMSG`](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_RECVMSG)
    ///
    /// For every receive, we need to check the flags of the CQE to potentially requeue the recv
    #[inline]
    fn enqueue_recvmsg(&mut self) {
        self.push(
            io_uring::opcode::RecvMsgMulti::new(self.socket_fd, &self.recv_hdr, BUFFER_RING)
                .build()
                .user_data((IORING_OP_RECVMSG as u64) << 56),
        );
    }

    #[inline]
    fn pop_recv(&mut self, cqe: io_uring::cqueue::Entry) -> Option {
        let ret = cqe.result();

        if ret < 0 {
            let error = std::io::Error::from_raw_os_error(-ret);
            tracing::error!(%error, "error receiving packet");
            return None;
        }

        let flags = cqe.flags();

        // This _should_ theoretically never happen
        if flags & flags::IORING_CQE_F_BUFFER == 0 {
            tracing::error!("failed to receive packet, a buffer was not selected");
            return None;
        }

        let buffer_id = (flags >> 16) as u16;

        let mut rb = self.recv_ring.dequeue(buffer_id);
        let addr = match rb.extract(ret as _, &self.recv_hdr) {
            Ok(addr) => addr,
            Err(error) => {
                tracing::error!(%error, "failed to extract source address");
                return None;
            }
        };

        // Requeue the recv if needed
        if flags & flags::IORING_CQE_F_MORE == 0 {
            self.enqueue_recvmsg();
        }
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
}

const BUFFER_RING: u16 = 0xfeed;

pub struct IoUringLoop {
    socket: crate::net::DualStackLocalSocket,
    concurrent_sends: u32,
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

        let mut ring = io_uring::IoUring::builder()
            .setup_cqsize(self.concurrent_sends)
            .build(self.concurrent_sends >> 1)?;

        let mut pending_sends_event = pending_sends.1;
        let pending_sends = pending_sends.0;

        let mut rb = super::ring::BufferRing::new(
            concurrent_sends as u16,
            // we only deal with non-fragmented UDP with a presumed MTU of 1500, though we also need to account for the extra metadata for multishot recvmsg, so just round up to the next power of 2
            2048,
            BUFFER_RING,
        )
        .map_err(|err| std::io::Error::other(err))?;

        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                crate::metrics::game_traffic_tasks().inc();
                let _guard = tracing::dispatcher::set_default(&dispatcher);

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
                    recv_ring: rb,
                    recv_hdr: libc::msghdr {
                        // Reserve space for up to an IPv6 socket address, if we get
                        // an IPv4 address it will 0 fill the remaining 8 bytes
                        msg_namelen: std::mem::size_of::<libc::sockaddr_in6>() as _,
                        // We don't use nor care about control length, but just being
                        // explicit here for clarity as it is the only other relevant field
                        // for multishot recvmsg
                        msg_controllen: 0,
                        ..unsafe { std::mem::zeroed() }
                    },
                };

                // SAFETY: we ensure the buffer ring lives as long as the io ring itself
                unsafe {
                    submitter.register_buf_ring_with_flags(
                        loop_ctx.recv_ring.mmap.buf as u64,
                        loop_ctx.recv_ring.count,
                        BUFFER_RING,
                        0 /* https://man.archlinux.org/man/extra/liburing/io_uring_register_buf_ring.3.en#IOU_PBUF_RING_INC is the only suppported flag */,
                    )
                };

                loop_ctx.enqueue_recvmsg();
                loop_ctx.push_with_token(pending_sends_event.io_uring_entry(), Token::PendingSends);

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

                    let filters = filter_chain.load();

                    // Now actually process all of the completed io requests
                    for cqe in &mut cq {
                        let ud = cqe.user_data();

                        let op = (ud >> 56) as u8;
                        match token {
                            IORING_OP_RECVMSG => {
                                if let Some(packet) = loop_ctx.pop_recv(cqe) {
                                    process_packet(&mut ctx, filters, packet, &mut last_received_at);
                                    loop_ctx.enqueue_recv(buffer_pool.clone().alloc());
                                }
                            }
                            Token::PendingSends => {
                                double_pending_sends = pending_sends.swap(double_pending_sends);
                                loop_ctx.push_with_token(
                                    pending_sends_event.io_uring_entry(),
                                    Token::PendingSends,
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
            IoUringLoop::new(2048, crate::net::DualStackLocalSocket::from_raw(raw_socket))?;
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
