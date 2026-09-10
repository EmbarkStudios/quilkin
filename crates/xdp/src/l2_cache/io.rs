use super::{icmp::IcmpSocket, types::*};
use eyre::WrapErr;
use quilkin_uring::{eventfd, flags, io_uring, ring};
use std::os::fd::AsRawFd;

pub mod code {
    pub const RECV4: u64 = 0;
    pub const RECV6: u64 = 1;
    pub const SEND: u64 = 2;
    pub const REQUEST: u64 = 4;
    pub const EBPF: u64 = 5;
    pub const INTERVAL: u64 = 6;
    pub const TIMEOUT: u64 = 7;
    pub const SHUTDOWN: u64 = 8;
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

    const BUFFER_RING: u16 = 0xfeed;

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

    let recv_hdr = libc::msghdr {
        msg_namelen: std::mem::size_of::<libc::sockaddr_in6>() as _,
        // This is the only other relevant field for multishot recvmsg, we _could_ use this to get extended error information
        // via `libc::sock_extended_err` in the future if needed https://man.archlinux.org/man/core/man-pages/IP_RECVERR.2const.en
        msg_controllen: 0,
        msg_name: std::ptr::null_mut(),
        msg_control: std::ptr::null_mut(),
        msg_iov: std::ptr::null_mut(),
        msg_iovlen: 0,
        msg_flags: 0,
    };

    let enqueue_recv = |sq: &mut SubmissionQueue<'_>, v4: bool| -> eyre::Result<()> {
        let (sock, code) = (
            Fd(if v4 { &icmp.in4 } else { &icmp.in6 }.as_raw_fd()),
            if v4 { code::RECV4 } else { code::RECV6 },
        );
        // SAFETY: the socket and buffer ring live as long as the io ring
        unsafe {
            sq.push(
                &opcode::RecvMsgMulti::new(sock, &recv_hdr, BUFFER_RING)
                    .build()
                    .user_data(code),
            )
            .context("failed to enqueue multishot IORING_OP_RECVMSG")
        }
    };

    enqueue_recv(&mut sq, true)?;
    enqueue_recv(&mut sq, false)?;

    // SAFETY: the eventfd lives as long as the io ring
    unsafe {
        sq.push(&req.event.io_uring_entry().user_data(code::REQUEST))
            .context("failed to enqueue eventfd IORING_OP_READ")?;
    }

    let mut enqueue_ebpf = |sq: &mut SubmissionQueue<'_>| -> eyre::Result<()> {
        // SAFETY: the eBPF ring buffer lives as long as the io ring
        unsafe {
            sq.push(&ebpf_ring.entry().user_data(code::EBPF))
                .context("failed to enqueue IORING_OP_EPOLL_WAIT")
        }
    };

    enqueue_ebpf(&mut sq)?;

    let ts = io_uring::types::Timespec::from(std::time::Duration::from_millis(200));

    let enqueue_interval = |sq: &mut SubmissionQueue<'_>| -> eyre::Result<()> {
        // SAFETY: the timespec lives as long as the io ring
        unsafe {
            sq.push(
                &opcode::Timeout::new(&ts)
                    .flags(io_uring::types::TimeoutFlags::MULTISHOT)
                    .build()
                    .user_data(code::INTERVAL),
            )
            .context("failed to enqueue IORING_OP_TIMEOUT")
        }
    };

    enqueue_interval(&mut sq)?;

    // SAFETY: The shutdown lives as long as the uring
    unsafe {
        sq.push(&shutdown.io_uring_entry().user_data(code::SHUTDOWN))
            .context("failed to enqueue shutdown event")?;
    }

    let mut pings = InflightPings::new(&icmp, cache);

    loop {
        match submitter.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref err) if matches!(err.raw_os_error(), Some(libc::EBUSY | libc::EINTR)) => {
                continue;
            }
            Err(error) => {
                return Err(error).context("io_uring_submit_and_wait failed");
            }
        }

        tracing::info!("got here");
        cq.sync();

        {
            let mut bre = br.enqueue();

            for cqe in &mut cq {
                let ud = cqe.user_data();

                match dbg!(ud & 0xff) {
                    // We've received a response to an ICMP echo request
                    code::RECV4 | code::RECV6 => {
                        let ret = cqe.result();
                        let flags = cqe.flags();

                        // Requeue the recv if needed
                        if flags & flags::IORING_CQE_F_MORE == 0 {
                            enqueue_recv(&mut sq, ud & 0xff == code::RECV4)?;
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
                        let addr = match rb.extract(ret as _, &recv_hdr) {
                            Ok(addr) => addr,
                            Err(error) => {
                                tracing::error!(%error, "failed to extract source address");
                                bre.enqueue_by_id(id);
                                continue;
                            }
                        };

                        pings.process_reply(addr.ip().into(), &rb, addr.is_ipv4());

                        bre.enqueue_by_id(id);
                    }
                    // An XDP worker needs to send a packet to an IP address it doesn't know the MAC address of
                    code::REQUEST => {
                        for req_ip in req.rx.try_iter() {
                            if !pings.ping(req_ip, &mut sq) {}
                        }
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
