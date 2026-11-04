use super::{icmp::IcmpSocket, types::*};
use eyre::WrapErr;
use quilkin_uring::{eventfd, flags, io_uring, ring};
use std::os::fd::AsRawFd;

pub mod code {
    pub const RECV4: u64 = 0;
    pub const RECV6: u64 = 1;
    pub const SEND: u64 = 2;
    pub const REQUEST: u64 = 3;
    pub const EBPF: u64 = 4;
    pub const INTERVAL: u64 = 5;
    pub const TIMEOUT: u64 = 6;
    pub const SHUTDOWN: u64 = 7;
}

pub(super) fn cache_io_loop(
    icmp: IcmpSocket,
    mut req: RequestReceiver,
    mut io_ring: io_uring::IoUring,
    mut ebpf_ring: EbpfRing,
    br: ring::BufferRing,
    cache: std::sync::Arc<super::L2Cache>,
    mut shutdown: eventfd::EventFd,
) -> eyre::Result<()> {
    use quilkin_uring::io_uring::{opcode, squeue::SubmissionQueue, types::Fd};

    const BUFFER_RING: u16 = 0xbeef;

    let (submitter, mut sq, mut cq) = io_ring.split();

    // SAFETY: The ring buffer is valid at this point and lives at least as long as the io ring itself
    unsafe {
        submitter.register_buf_ring_with_flags(
            br.mmap.buf as u64,
            br.count,
            BUFFER_RING,
            0 /* https://man.archlinux.org/man/extra/liburing/io_uring_register_buf_ring.3.en#IOU_PBUF_RING_INC is the only suppported flag */,
        ).context("failed to register buffer ring")?;
    };

    let recv4_hdr = libc::msghdr {
        msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as _,
        ..unsafe { std::mem::zeroed() }
    };
    let recv6_hdr = libc::msghdr {
        msg_namelen: std::mem::size_of::<libc::sockaddr_storage>() as _,
        ..unsafe { std::mem::zeroed() }
    };

    let recv4 = opcode::RecvMsgMulti::new(Fd(icmp.in4.as_raw_fd()), &recv4_hdr, BUFFER_RING)
        .build()
        .user_data(code::RECV4);
    let recv6 = opcode::RecvMsgMulti::new(Fd(icmp.in6.as_raw_fd()), &recv6_hdr, BUFFER_RING)
        .build()
        .user_data(code::RECV6);

    let enqueue_recv = |sq: &mut SubmissionQueue<'_>, v4: bool| -> eyre::Result<()> {
        // SAFETY: the socket and buffer ring live as long as the io ring
        unsafe {
            sq.push(if v4 { &recv4 } else { &recv6 })
                .context("failed to enqueue multishot IORING_OP_RECVMSG")
        }
    };

    enqueue_recv(&mut sq, true)?;
    enqueue_recv(&mut sq, false)?;

    let ping_request = req.event.io_uring_entry().user_data(code::REQUEST);

    let enqueue_req = |sq: &mut SubmissionQueue<'_>| -> eyre::Result<()> {
        // SAFETY: the eventfd lives as long as the io ring
        unsafe {
            sq.push(&ping_request)
                .context("failed to enqueue eventfd IORING_OP_READ")
        }
    };

    enqueue_req(&mut sq)?;

    let ebpf = ebpf_ring.entry().user_data(code::EBPF);

    let enqueue_ebpf = |sq: &mut SubmissionQueue<'_>| -> eyre::Result<()> {
        // SAFETY: the eBPF ring buffer lives as long as the io ring
        unsafe {
            sq.push(&ebpf)
                .context("failed to enqueue IORING_OP_EPOLL_WAIT")
        }
    };

    enqueue_ebpf(&mut sq)?;

    let ts = io_uring::types::Timespec::from(std::time::Duration::from_millis(200));
    let interval = opcode::Timeout::new(&ts)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .build()
        .user_data(code::INTERVAL);

    let enqueue_interval = |sq: &mut SubmissionQueue<'_>| -> eyre::Result<()> {
        // SAFETY: the timespec lives as long as the io ring
        unsafe {
            sq.push(&interval)
                .context("failed to enqueue IORING_OP_TIMEOUT")
        }
    };

    enqueue_interval(&mut sq)?;

    let shutdown = shutdown.io_uring_entry().user_data(code::SHUTDOWN);

    // SAFETY: The shutdown lives as long as the uring
    unsafe {
        sq.push(&shutdown)
            .context("failed to enqueue shutdown event")?;
    }

    let mut pings = InflightPings::new(&icmp, cache);

    loop {
        sq.sync();

        match submitter.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref err) if matches!(err.raw_os_error(), Some(libc::EBUSY | libc::EINTR)) => {
                tracing::warn!(%err, "BOO");
                continue;
            }
            Err(error) => {
                return Err(error).context("io_uring_submit_and_wait failed");
            }
        }

        cq.sync();

        {
            let mut bre = br.enqueue();

            for cqe in &mut cq {
                let ud = cqe.user_data();

                match ud & 0xff {
                    // We've received a response to an ICMP echo request
                    code::RECV4 | code::RECV6 => {
                        let ret = cqe.result();
                        let flags = cqe.flags();
                        let is_v4 = ud & 0xff == code::RECV4;

                        // Requeue the recv if needed
                        if flags & flags::IORING_CQE_F_MORE == 0 {
                            enqueue_recv(&mut sq, is_v4)?;
                        }

                        if ret < 0 {
                            let error = std::io::Error::from_raw_os_error(-ret);
                            tracing::error!(%error, "error receiving ICMP packet");
                            continue;
                        }

                        // This _should_ theoretically never happen
                        if flags & flags::IORING_CQE_F_BUFFER == 0 {
                            tracing::error!("failed to receive packet, a buffer was not selected");
                            continue;
                        }

                        let buffer_id = (flags >> 16) as u16;

                        let mut rb = br.dequeue(buffer_id);
                        let id = rb.id();
                        let addr = match rb
                            .extract(ret as _, if is_v4 { &recv4_hdr } else { &recv6_hdr })
                        {
                            Ok(addr) => addr,
                            Err(error) => {
                                tracing::error!(%error, "failed to extract source address");
                                bre.enqueue_by_id(id);
                                continue;
                            }
                        };

                        pings.process_reply(addr.ip(), &rb);

                        bre.enqueue_by_id(id);
                    }
                    // An XDP worker needs to send a packet to an IP address it doesn't know the MAC address of
                    code::REQUEST => {
                        for req_ip in req.rx.try_iter() {
                            if !pings.ping(req_ip, &mut sq) {}
                        }

                        enqueue_req(&mut sq)?;
                    }
                    // We've finished sending an ICMP echo request
                    code::SEND => {
                        pings.process_send(cqe);
                    }
                    // We've intercepted either an ARP (IPv4) response or an ICMPv6 (IPv6) neighbor advertisement
                    code::EBPF => {
                        while let Some(entry) = ebpf_ring.read() {
                            pings.process_ebpf(entry);
                        }

                        enqueue_ebpf(&mut sq)?;
                    }
                    // Since we're sending ICMP packets we need to handle the timeouts ourselves
                    code::INTERVAL => {
                        let flags = cqe.flags();

                        // Requeue the interval if needed
                        if flags & flags::IORING_CQE_F_MORE == 0 {
                            enqueue_interval(&mut sq)?;
                        }

                        pings.process_timeouts(&mut sq);
                    }
                    // The linked timeout completed, we don't do anything special here, the send handling will happen
                    // the same regardless of success or failure, and our interval handles retries
                    code::TIMEOUT => {
                        // TODO: keep metrics of number of send timeouts?
                    }
                    // Shutdown was requested
                    code::SHUTDOWN => {
                        tracing::info!("shutdown requested for L2 cache thread");
                        return Ok(());
                    }
                    _ => unreachable!(),
                }
            }
        }

        sq.sync();
    }
}
