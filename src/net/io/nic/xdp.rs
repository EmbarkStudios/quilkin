#![allow(dead_code)]

use super::ThreadPolicy;
use quilkin_xdp::xdp::{
    self,
    nic::{NicIndex, NicName},
};
use std::sync::Arc;
pub mod diagnostics;
pub mod process;

pub enum NicConfig<'n> {
    /// Specifies a NIC by name, setup will fail if a NIC with that name doesn't exist
    Name(&'n str),
    /// Specifies a NIC by index, setup will fail if the index isn't valid
    Index(u32),
    /// The NIC will be determined from the set of available NICs, setup will fail
    /// if more than one NIC is found that could be used for handling traffic
    Default,
}

#[derive(Copy, Clone)]
pub struct WorkerThreadScheduling {
    /// The thread priority
    ///
    /// This value is automatically clamped within the range of the scheduling policy
    pub thread_priority: i32,
    /// The scheduling policy, see [sched](https://man.archlinux.org/man/sched.7.en)
    pub thread_policy: ThreadPolicy,
    /// If true, each worker is pinned to its own core
    pub pin_threads: bool,
}

impl Default for WorkerThreadScheduling {
    fn default() -> Self {
        Self {
            thread_priority: 99,
            thread_policy: ThreadPolicy::Default,
            pin_threads: false,
        }
    }
}

/// User supplied configuration
pub struct XdpConfig<'n> {
    /// The NIC to attach to
    pub nic: NicConfig<'n>,
    /// The external port that downstream clients use to communicate with Quilkin
    pub external_port: u16,
    /// The port QCMP packets can be sent to
    pub qcmp_port: u16,
    /// Requires that the chosen NIC supports [`XDP_ZEROCOPY`](https://www.kernel.org/doc/html/latest/networking/af_xdp.html#xdp-copy-and-xdp-zerocopy-bind-flags)
    ///
    /// If this is false, zero copy will be used if the NIC supports it, but will
    /// fallback to copy mode if not, which will mean lower performance
    pub require_zero_copy: bool,
    /// Requires that the chosen NIC supports [`XDP_TXMD_FLAGS_TIMESTAMP`](https://docs.kernel.org/6.8/networking/xsk-tx-metadata.html)
    /// which allows [internet checksum]() calculation to be offloaded to the NIC
    pub require_tx_checksum: bool,
    /// Total amount of packets per UMEM, which is used to determine the size of each ring
    pub packets_per_queue: u32,
    /// Worker thread scheduling configuration
    pub worker_thread_scheduling: WorkerThreadScheduling,
}

impl Default for XdpConfig<'_> {
    fn default() -> Self {
        Self {
            nic: NicConfig::Default,
            external_port: 7777,
            qcmp_port: 7600,
            require_zero_copy: false,
            require_tx_checksum: false,
            packets_per_queue: 8 * 1024,
            worker_thread_scheduling: WorkerThreadScheduling::default(),
        }
    }
}

pub struct XdpWorkers {
    ebpf_prog: quilkin_xdp::EbpfProgram,
    workers: Vec<quilkin_xdp::XdpWorker>,
    nic: NicIndex,
    xdp_link: quilkin_xdp::aya::programs::xdp::XdpLinkId,
    external_port: NetworkU16,
    qcmp_port: NetworkU16,
    ipv6: std::net::Ipv6Addr,
    ipv4: std::net::Ipv4Addr,
    worker_thread_scheduling: WorkerThreadScheduling,
}

#[derive(thiserror::Error, Debug)]
pub enum NicUnavailable {
    #[error("failed to query NIC: {0}")]
    Query(#[source] std::io::Error),
    #[error("no NICs available that could be considered a default")]
    NoAvailableDefault,
    #[error("no NIC named '{0}'")]
    UnknownName(String),
    #[error("no NIC with index '{0}'")]
    UnknownIndex(u32),
}

#[derive(thiserror::Error, Debug)]
pub enum XdpSetupError {
    #[error("NIC is unavailable: {0}")]
    NicUnavailable(#[from] NicUnavailable),
    #[error("failed to query device capabilities for {0}: {1}")]
    NicQuery(NicName, #[source] std::io::Error),
    #[error("failed to query ip addresses for {0}: {1}")]
    AddressQuery(NicName, #[source] std::io::Error),
    #[error("`XDP_ZEROCOPY` is unavailable for {0}")]
    ZeroCopyUnavailable(NicName),
    #[error("`XDP_TXMD_FLAGS_TIMESTAMP` is unavailable for {0}")]
    TxChecksumUnavailable(NicName),
    #[error(
        "the requested maximum packet memory {max:.2}{xunit} must be at least {min:.2}{nunit} as {nic} has a queue count of {queue_count}"
    )]
    MinimumMemoryRequirementsExceeded {
        max: f64,
        xunit: &'static str,
        min: f64,
        nunit: &'static str,
        nic: NicName,
        queue_count: u32,
    },
    #[error("XDP error: {0}")]
    Xdp(#[from] xdp::error::Error),
    #[error("XDP load error: {0}")]
    XdpLoad(#[from] quilkin_xdp::LoadError),
    #[error("failed to attach XDP program: {0}")]
    XdpAttach(#[from] quilkin_xdp::aya::programs::ProgramError),
    #[error("bind error: {0}")]
    BindError(#[from] quilkin_xdp::BindError),
    #[error("failed to clamp thread priority for policy `{0}`: {1}")]
    ThreadPriorityClamp(ThreadPolicy, #[source] std::io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum XdpSpawnError {
    #[error("Failed to spawn worker thread: {0}")]
    Thread(#[source] std::io::Error),
}

/// Attempts to setup XDP by querying NIC support and allocating ring buffers
/// based on user configuration, failing if requirements cannot be met
///
/// This function currently only supports one mode of operation, which is that
/// Returns `true` if a unique default NIC suitable for XDP is present.
///
/// This mirrors the `NicConfig::Default` selection in [`setup_xdp_io`] but
/// stops before allocating sockets or loading the eBPF program.
pub fn is_available() -> bool {
    let Ok(iter) = xdp::nic::InterfaceIter::new() else {
        return false;
    };
    let mut chosen = None;
    for iface in iter {
        if let Some(c) = chosen {
            if iface != c {
                return false;
            }
        } else {
            chosen = Some(iface);
        }
    }
    chosen.is_some()
}

/// Some drivers (eg `gve`) reserve queues for XDP, rejecting attach at the
/// full count. Returns the next smaller count to retry, or `None` if there's
/// no room left to shrink.
fn next_queue_count_to_try(current: u32) -> Option<u32> {
    let half = current / 2;
    (half >= 1).then_some(half)
}

/// Loads and attaches `ebpf_prog` to `nic`, starting at `initial_channels`'
/// current queue count and only shrinking it reactively on an attach failure
/// — NICs that don't need it are never touched. Returns the link and the
/// queue count XDP ended up using.
fn attach_xdp_program(
    ebpf_prog: &mut quilkin_xdp::EbpfProgram,
    nic: NicIndex,
    queues: &mut quilkin_xdp::xdp::nic::Queues,
) -> Result<quilkin_xdp::aya::programs::xdp::XdpLinkId, quilkin_xdp::aya::programs::ProgramError> {
    ebpf_prog.load_into_kernel()?;

    let mode = quilkin_xdp::aya::programs::xdp::XdpMode::default();
    // We've already synchronized the tx and rx counts if needed before this function
    let mut queue_count = queues.rx_count();

    loop {
        match ebpf_prog.attach(nic, mode) {
            Ok(link) => return Ok(link),
            Err(error) => {
                let reduced = queue_count / 2;
                if reduced < 1 {
                    return Err(error);
                }

                queues.set_rx_and_tx(reduced);

                if let Err(shrink_error) = nic.set_queue_counts(queues) {
                    tracing::warn!(
                        %shrink_error,
                        ?nic,
                        "failed to reduce NIC queue count for XDP, giving up"
                    );
                    return Err(error);
                }

                tracing::warn!(
                    %error,
                    ?nic,
                    from = queue_count,
                    to = reduced,
                    "XDP attach rejected at current queue count, retrying with fewer queues"
                );
                queue_count = reduced;
            }
        }
    }
}

/// Binds an `AF_XDP` socket to every available queue on the NIC, and when [`spawn`]
/// is invoked, each socket is processed in its own thread
pub fn setup_xdp_io(config: XdpConfig<'_>) -> Result<XdpWorkers, XdpSetupError> {
    // This is validated when parsing arguments, but sanity check just in case eg. tests
    assert!(
        config.packets_per_queue.is_power_of_two(),
        "packets per queue was not a power of 2"
    );

    let nic = match config.nic {
        NicConfig::Default => {
            let mut chosen = None;

            for iface in xdp::nic::InterfaceIter::new().map_err(NicUnavailable::Query)? {
                if let Some(chosen) = chosen {
                    if iface != chosen {
                        return Err(NicUnavailable::NoAvailableDefault.into());
                    }
                } else {
                    chosen = Some(iface);
                }
            }

            chosen.ok_or(NicUnavailable::NoAvailableDefault)?
        }
        NicConfig::Name(name) => {
            let cname = std::ffi::CString::new(name)
                .map_err(|_nul| NicUnavailable::UnknownName(name.to_owned()))?;
            xdp::nic::NicIndex::lookup_by_name(&cname)
                .map_err(NicUnavailable::Query)?
                .ok_or_else(|| NicUnavailable::UnknownName(name.to_owned()))?
        }
        NicConfig::Index(index) => xdp::nic::NicIndex::new(index),
    };

    let name = nic
        .name()
        .map_err(|_err| NicUnavailable::UnknownIndex(nic.into()))?;

    tracing::info!(nic = ?nic, "selected NIC");

    let mut device_caps = nic
        .query_capabilities()
        .map_err(|err| XdpSetupError::NicQuery(name, err))?;

    tracing::info!(?device_caps, nic = ?nic, "XDP features for device");

    if config.require_zero_copy
        && matches!(device_caps.zero_copy, xdp::nic::XdpZeroCopy::Unavailable)
    {
        tracing::error!(?device_caps, nic = ?nic, "XDP features for device");
        return Err(XdpSetupError::ZeroCopyUnavailable(name));
    }

    if config.require_tx_checksum && !device_caps.tx_metadata.checksum() {
        tracing::error!(?device_caps, nic = ?nic, "XDP features for device");
        return Err(XdpSetupError::TxChecksumUnavailable(name));
    }

    let (ipv4, ipv6) = nic
        .addresses()
        .and_then(|(ipv4, ipv6)| {
            if ipv4.is_none() && ipv6.is_none() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "neither an ipv4 nor ipv6 address could be determined for the device",
                ))
            } else {
                Ok((
                    ipv4.unwrap_or(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                    ipv6.unwrap_or(std::net::Ipv6Addr::from_bits(0)),
                ))
            }
        })
        .map_err(|err| XdpSetupError::AddressQuery(name, err))?;

    // We don't support unaligned chunks, so this size can only be 2k or 4k,
    // and we only need 2k since we only care about non-fragmented UDP packets
    const PACKET_SIZE: u64 = 2 * 1024;

    // It's extremely unlikely these number will ever differ, but if they do we take the minimum
    let rx_count = device_caps.queues.rx_count();
    let tx_count = device_caps.queues.tx_count();

    if rx_count != tx_count {
        tracing::warn!(
            ?nic,
            rx_count,
            tx_count,
            "the selected NIC has a different number of RX and TX queues"
        );
        let qc = rx_count.min(tx_count);

        // If the tx count is higher it doesn't really matter since they just wouldn't be used, but if we have more rx than tx
        // we need to reduce as otherwise we might not be able to send on some of the i/o loops
        if rx_count > tx_count {
            device_caps.queues.set_rx_and_tx(qc);
            if let Err(error) = nic.set_queue_counts(&device_caps.queues) {
                tracing::error!(%error, ?nic, rx_count, tx_count, "failed to synchronize queue counts before attach");
                return Err(XdpSetupError::NicQuery(name, error));
            }
        }
    }

    let mut ebpf_prog = quilkin_xdp::EbpfProgram::load(config.external_port, config.qcmp_port)?;

    // Attach before binding: some drivers (eg gve) need XDP already enabled
    // before a socket can bind a queue with `XDP_ZEROCOPY`.
    let xdp_link = attach_xdp_program(&mut ebpf_prog, nic, &mut device_caps.queues)?;

    // In GCP, and possibly (probably?) other virtualized environments, the NIC will report the full theoretical speed
    // even if in reality the amount of bandwidth will be shared among multiple tenants, maybe even other instances of
    // this proxy, so rather than try to account for the amount of variation between clouds/virtualized/physical capabilities,
    // we just require the operator to specify the number of packets they want to support

    let umem_cfg = xdp::umem::UmemCfgBuilder {
        frame_size: xdp::umem::FrameSize::TwoK,
        // Provide enough headroom so that we can convert an ipv4 header to ipv6
        // header without needing to copy any bytes. note this doesn't take into
        // account if a filter adds or removes bytes from the beginning of the
        // data payload
        head_room: (xdp::packet::net_types::Ipv6Hdr::LEN - xdp::packet::net_types::Ipv4Hdr::LEN)
            as u32,
        frame_count: config.packets_per_queue,
        // TODO: This should be done in the type system so we can avoid logic
        // that doesn't change during the course of operation, but for now just
        // do it at runtime
        tx_checksum: device_caps.tx_metadata.checksum(),
        ..Default::default()
    }
    .build()?;

    let ring_size = config.packets_per_queue >> 2;

    let ring_cfg = xdp::RingConfigBuilder {
        rx_count: ring_size,
        tx_count: ring_size,
        fill_count: ring_size,
        completion_count: ring_size,
    }
    .build()?;
    let ring_heap = ring_cfg.heap_size();
    let workers =
        ebpf_prog.create_and_bind_sockets(nic, umem_cfg, &device_caps, ring_cfg, ring_size)?;

    // Report the amount of heap memory we're using specifically for XDP
    fn byte_units(b: u64) -> String {
        let mut units = b as f64;
        let mut unit = 0;
        const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB"];

        while units > 1024.0 && unit + 1 < UNITS.len() {
            units /= 1024.0;
            unit += 1;
        }

        format!("{units:.02}{}", UNITS[unit])
    }

    let total_heap_memory = byte_units(
        (
            // The amount of memory mapped for each of the rings shared between the kernel and userspace
            ring_heap as u64
            // The amount of memory mapped in each Umem
            + config.packets_per_queue as u64 * PACKET_SIZE
            // The amount of memory allocated for the RX and TX slabs
            + ring_size as u64 * std::mem::size_of::<xdp::Packet>() as u64
        ) * workers.len() as u64,
    );
    tracing::info!(
        queue_count = workers.len(),
        total_heap_memory,
        "total heap memory used by XDP workers"
    );

    let mut worker_thread_scheduling = config.worker_thread_scheduling;

    // SAFETY: syscalls
    unsafe {
        let min = libc::sched_get_priority_min(worker_thread_scheduling.thread_policy as _);
        let max = libc::sched_get_priority_max(worker_thread_scheduling.thread_policy as _);

        if min < 0 || max < 0 {
            return Err(XdpSetupError::ThreadPriorityClamp(
                worker_thread_scheduling.thread_policy,
                std::io::Error::last_os_error(),
            ));
        }

        worker_thread_scheduling.thread_priority =
            worker_thread_scheduling.thread_priority.clamp(min, max);
    }

    Ok(XdpWorkers {
        ebpf_prog,
        workers,
        nic,
        xdp_link,
        external_port: config.external_port.into(),
        qcmp_port: config.qcmp_port.into(),
        ipv4,
        ipv6,
        worker_thread_scheduling,
    })
}

pub struct XdpLoop {
    threads: Vec<std::thread::JoinHandle<()>>,
    ebpf_prog: quilkin_xdp::EbpfProgram,
    xdp_link: quilkin_xdp::aya::programs::xdp::XdpLinkId,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl XdpLoop {
    /// Detaches the eBPF program from the attacked NIC and cancels all I/O
    /// threads, waiting for them to exit
    pub fn shutdown(mut self, wait: bool) {
        if let Err(error) = self.ebpf_prog.detach(self.xdp_link) {
            tracing::error!(%error, "failed to detach eBPF program");
        }

        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);

        if !wait {
            return;
        }

        tracing::info!("waiting on XDP workers");
        let start = std::time::Instant::now();

        for jh in self.threads {
            if let Err(error) = jh.join() {
                if let Some(error) = error.downcast_ref::<&'static str>() {
                    tracing::error!(error, "XDP I/O thread encountered error");
                } else if let Some(error) = error.downcast_ref::<String>() {
                    tracing::error!(error, "XDP I/O thread encountered error");
                } else {
                    tracing::error!(?error, "XDP I/O thread encountered error");
                };
            }
        }

        tracing::info!(elapsed = ?start.elapsed(), "finished shutting down XDP workers");
    }
}

fn spawn_worker<F, T>(
    i: usize,
    scheduling: WorkerThreadScheduling,
    f: F,
) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("xdp-io-{i}"))
        .spawn(move || {
            'ts: {
                // SAFETY: syscalls
                unsafe {
                    if libc::sched_setscheduler(0, scheduling.thread_policy as _, &libc::sched_param {
                        sched_priority: scheduling.thread_priority,
                    }) != 0 {
                        tracing::warn!(thread = std::thread::current().name(), error = %std::io::Error::last_os_error(), "failed to set XDP thread scheduler");
                    }

                    if !scheduling.pin_threads {
                        break 'ts;
                    }

                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    libc::CPU_SET(i, &mut set);
                    if libc::pthread_setaffinity_np(libc::pthread_self(), std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
                        tracing::warn!(thread = std::thread::current().name(), error = %std::io::Error::last_os_error(), "failed to pin XDP worker to core");
                    }
                }
            }

            f()
        })
}

/// The entrypoint into the XDP I/O loop.
///
/// This spawns a named thread for each configured XDP socket to run the packet
/// receiving + processing + sending. The eBPF program used to route packets to
/// the XDP sockets is already attached to the NIC by [`setup_xdp_io`] by this
/// point.
///
/// # Errors
///
/// This can fail if threads can not be spawned for some reason (unlikely)
pub fn spawn(workers: XdpWorkers, config: process::ConfigState) -> Result<XdpLoop, XdpSpawnError> {
    let nic = workers.nic;
    let ebpf_prog = workers.ebpf_prog;
    let xdp_link = workers.xdp_link;
    let external_port = workers.external_port;
    let qcmp_port = workers.qcmp_port;
    let ipv4 = workers.ipv4;
    let ipv6 = workers.ipv6;
    let session_state = Arc::new(process::SessionState::default());
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let queue_count = workers.workers.len();
    let mut threads = Vec::with_capacity(queue_count);
    for (i, mut worker) in workers.workers.into_iter().enumerate() {
        let cfg = config.clone();
        let ss = session_state.clone();
        let shutdown = shutdown.clone();

        let jh = spawn_worker(i, workers.worker_thread_scheduling, move || {
            // Enqueue buffers to the fill ring to ensure that we don't miss any packets
            // SAFETY: we keep the umem alive for as long as the socket is alive
            unsafe {
                if let Err(error) =
                    worker
                        .fill
                        .enqueue(&mut worker.umem, worker.ring_len >> 2, true)
                {
                    tracing::error!(%error, "failed to kick fill ring during initial spinup");
                }
            };

            io_loop(
                worker,
                external_port,
                qcmp_port,
                cfg,
                ss,
                ipv4,
                ipv6,
                shutdown.clone(),
            );
        })
        .map_err(XdpSpawnError::Thread)?;

        threads.push(jh);
    }

    tracing::info!(?nic, queue_count, "XDP is running");

    Ok(XdpLoop {
        threads,
        ebpf_prog,
        xdp_link,
        shutdown,
    })
}

use xdp::packet::net_types::NetworkU16;

use crate::time::UtcTimestamp;

/// The core I/O loop
///
/// All of the ring operations are done in this loop so that the actual
/// [`process::process_packets`] code can be cleanly tested without relying on
/// a fully setup XDP socket/rings, relying only on a `Umem` (memory map)
#[allow(clippy::too_many_arguments)]
fn io_loop(
    worker: quilkin_xdp::XdpWorker,
    external_port: NetworkU16,
    qcmp_port: NetworkU16,
    mut config: process::ConfigState,
    sessions: Arc<process::SessionState>,
    local_ipv4: std::net::Ipv4Addr,
    local_ipv6: std::net::Ipv6Addr,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    let quilkin_xdp::XdpWorker {
        mut umem,
        socket,
        mut fill,
        mut rx,
        mut tx,
        mut completion,
        ring_len,
    } = worker;

    const POLL_TIMEOUT: xdp::socket::PollTimeout =
        xdp::socket::PollTimeout::new(Some(std::time::Duration::from_millis(100)));

    let mut state = process::State {
        external_port,
        qcmp_port,
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions,
        local_ipv4,
        local_ipv6,
        last_receive: UtcTimestamp::now(),
    };

    use xdp::slab::{HeapSlab, Slab};

    let mut rx_slab = HeapSlab::with_capacity(ring_len);
    let mut tx_slab = HeapSlab::with_capacity(ring_len);
    let mut pending_sends = 0;
    let mut outstanding = umem.outstanding() as i64;

    crate::metrics::allocated_xdp_packets().add(outstanding);

    // SAFETY: the cases of unsafe in this code block all concern the relationship
    // between frames and the Umem, the frames cannot outlive the Umem which is
    // the owner of the actual memory map
    unsafe {
        while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            // Wait for packets to be received, note that
            // [poll](https://www.man7.org/linux/man-pages/man2/poll.2.html) also acts
            // as a [cancellation point](https://www.man7.org/linux/man-pages/man7/pthreads.7.html),
            // so shutdown will cause the thread to exit here
            let Ok(true) = socket.poll_read(POLL_TIMEOUT) else {
                continue;
            };

            let recvd = rx.recv(&umem, &mut rx_slab);

            // Ensure the fill ring doesn't get starved, which could drop packets
            if let Err(error) = fill.enqueue(&mut umem, ring_len - recvd, true) {
                // EAGAIN means the wakeup wasn't delivered, but the buffers are
                // already enqueued in the ring, so the kernel will pick them up
                // on the next successful wakeup or its own polling; not an error.
                if error.raw_os_error() != Some(libc::EAGAIN) {
                    // This is shoehorning an error that isn't attributable to a particular packet
                    crate::metrics::errors_total(
                        crate::metrics::Direction::Read,
                        &io_error_to_discriminant(error),
                        &crate::metrics::EMPTY,
                    )
                    .inc();
                }
            }

            // Process each of the packets that we received, potentially queuing
            // packets to be sent
            process::process_packets(
                &mut rx_slab,
                &mut umem,
                &mut tx_slab,
                &mut config,
                &mut state,
            );

            let before = tx_slab.len();
            let enqueued_sends = match tx.send(&mut tx_slab, true) {
                Ok(es) => es,
                Err(error) => {
                    // EAGAIN means the wakeup wasn't delivered, but the packets are
                    // already enqueued in the ring, so the kernel will send them on
                    // the next successful wakeup or its own polling; not an error.
                    if error.raw_os_error() != Some(libc::EAGAIN) {
                        // This is shoehorning an error that isn't attributable to a particular packet
                        crate::metrics::errors_total(
                            crate::metrics::Direction::Read,
                            &io_error_to_discriminant(error),
                            &crate::metrics::EMPTY,
                        )
                        .inc();
                    }

                    before - tx_slab.len()
                }
            };

            // Return frames that have completed sending
            pending_sends += enqueued_sends;
            pending_sends -= completion.dequeue(&mut umem, pending_sends);

            let new = umem.outstanding() as i64;
            crate::metrics::allocated_xdp_packets().add(new - outstanding);
            outstanding = new;
        }
    }
}

#[inline]
fn io_error_to_discriminant(error: std::io::Error) -> std::borrow::Cow<'static, str> {
    let Some(code) = error.raw_os_error() else {
        return error.to_string().into();
    };

    match code {
        libc::EBUSY => "EBUSY".into(),
        libc::ENOBUFS => "ENOBUFS".into(),
        libc::EAGAIN => "EAGAIN".into(),
        libc::ENETDOWN => "ENETDOWN".into(),
        other => format!("{other}").into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_queue_count_halves() {
        assert_eq!(next_queue_count_to_try(8), Some(4));
        assert_eq!(next_queue_count_to_try(4), Some(2));
        assert_eq!(next_queue_count_to_try(3), Some(1));
    }

    #[test]
    fn next_queue_count_stops_at_one() {
        assert_eq!(next_queue_count_to_try(1), None);
        assert_eq!(next_queue_count_to_try(0), None);
    }
}
