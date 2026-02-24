#![allow(dead_code)]

use quilkin_xdp::xdp::{
    self,
    nic::{NicIndex, NicName},
};
use std::sync::Arc;
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

/// User supplied configuration
pub struct XdpConfig<'n> {
    /// The NIC to attach to
    pub nic: NicConfig<'n>,
    /// The external port that downstream clients use to communicate with Quilkin
    pub external_port: u16,
    /// The port QCMP packets can be sent to
    pub qcmp_port: u16,
    /// The maximum amount of memory, in bytes, that the memory mappings used for
    /// packet buffers will be allowed to take.
    ///
    /// Quilkin currently uses one [`UMEM`](https://www.kernel.org/doc/html/latest/networking/af_xdp.html#umem)
    /// for each socket, and there is one socket per NIC queue. Setup will fail
    /// if this option is set at a too low value.
    ///
    /// By default we use 4MiB per queue, eg. 128 MiB on a 32 queue NIC
    ///
    /// Note that there are other ring buffers allocated that aren't counted
    /// under this allocation, but they are much smaller
    pub maximum_packet_memory: Option<u64>,
    /// Requires that the chosen NIC supports [`XDP_ZEROCOPY`](https://www.kernel.org/doc/html/latest/networking/af_xdp.html#xdp-copy-and-xdp-zerocopy-bind-flags)
    ///
    /// If this is false, zero copy will be used if the NIC supports it, but will
    /// fallback to copy mode if not, which will mean lower performance
    pub require_zero_copy: bool,
    /// Requires that the chosen NIC supports [`XDP_TXMD_FLAGS_TIMESTAMP`](https://docs.kernel.org/6.8/networking/xsk-tx-metadata.html)
    /// which allows [internet checksum]() calculation to be offloaded to the NIC
    pub require_tx_checksum: bool,
}

impl Default for XdpConfig<'_> {
    fn default() -> Self {
        Self {
            nic: NicConfig::Default,
            external_port: 7777,
            qcmp_port: 7600,
            maximum_packet_memory: None,
            require_zero_copy: false,
            require_tx_checksum: false,
        }
    }
}

pub struct XdpWorkers {
    ebpf_prog: quilkin_xdp::EbpfProgram,
    workers: Vec<quilkin_xdp::XdpWorker>,
    nic: NicIndex,
    external_port: NetworkU16,
    qcmp_port: NetworkU16,
    ipv6: std::net::Ipv6Addr,
    ipv4: std::net::Ipv4Addr,
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
    #[error("bind error: {0}")]
    BindError(#[from] quilkin_xdp::BindError),
}

#[derive(thiserror::Error, Debug)]
pub enum XdpSpawnError {
    #[error("Failed to spawn worker thread: {0}")]
    Thread(#[source] std::io::Error),
    #[error("Failed to attach XDP program: {0}")]
    XdpAttach(#[from] quilkin_xdp::aya::programs::ProgramError),
}

/// Attempts to setup XDP by querying NIC support and allocating ring buffers
/// based on user configuration, failing if requirements cannot be met
///
/// This function currently only supports one mode of operation, which is that
/// a socket is bound to every available queue on the NIC, and when [`spawn`]
/// is invoked, each socket is processed in its own thread
///
/// Binding to fewer queues is possible in the future but requires additional
/// work in the `xdp` crate
pub fn setup_xdp_io(config: XdpConfig<'_>) -> Result<XdpWorkers, XdpSetupError> {
    let nic_index = match config.nic {
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
            let cname = std::ffi::CString::new(name).unwrap();
            xdp::nic::NicIndex::lookup_by_name(&cname)
                .map_err(NicUnavailable::Query)?
                .ok_or_else(|| NicUnavailable::UnknownName(name.to_owned()))?
        }
        NicConfig::Index(index) => xdp::nic::NicIndex::new(index),
    };

    let name = nic_index
        .name()
        .map_err(|_err| NicUnavailable::UnknownIndex(nic_index.into()))?;

    tracing::info!(nic = ?nic_index, "selected NIC");

    let device_caps = nic_index
        .query_capabilities()
        .map_err(|err| XdpSetupError::NicQuery(name, err))?;

    tracing::debug!(?device_caps, nic = ?nic_index, "XDP features for device");

    if config.require_zero_copy
        && matches!(device_caps.zero_copy, xdp::nic::XdpZeroCopy::Unavailable)
    {
        tracing::error!(?device_caps, nic = ?nic_index, "XDP features for device");
        return Err(XdpSetupError::ZeroCopyUnavailable(name));
    }

    if config.require_tx_checksum && !device_caps.tx_metadata.checksum() {
        tracing::error!(?device_caps, nic = ?nic_index, "XDP features for device");
        return Err(XdpSetupError::TxChecksumUnavailable(name));
    }

    let (ipv4, ipv6) = nic_index
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

    // Bit arbitrary, but set the floor at 128 packets per umem
    const MINIMUM_UMEM_COUNT: u64 = 128;
    // We don't support unaligned chunks, so this size can only be 2k or 4k,
    // and we only need 2k since we only care about non-fragmented UDP packets
    const PACKET_SIZE: u64 = 2 * 1024;

    let packet_count = if let Some(max) = config.maximum_packet_memory {
        let bytes_per_socket = max / device_caps.queue_count as u64;
        let packet_count = (bytes_per_socket / PACKET_SIZE).next_power_of_two();
        if MINIMUM_UMEM_COUNT > packet_count {
            fn byte_units(b: u64) -> (f64, &'static str) {
                let mut units = b as f64;
                let mut unit = 0;
                const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB"];

                while units > 1024.0 {
                    units /= 1024.0;
                    unit += 1;
                }

                (units, UNITS[unit])
            }

            let (max, xunit) = byte_units(max);
            let (min, nunit) =
                byte_units(MINIMUM_UMEM_COUNT * PACKET_SIZE * device_caps.queue_count as u64);

            return Err(XdpSetupError::MinimumMemoryRequirementsExceeded {
                max,
                xunit,
                min,
                nunit,
                nic: name,
                queue_count: device_caps.queue_count,
            });
        }

        packet_count as u32
    } else {
        2 * 1024
    };

    let mut ebpf_prog = quilkin_xdp::EbpfProgram::load(config.external_port, config.qcmp_port)?;

    let umem_cfg = xdp::umem::UmemCfgBuilder {
        frame_size: xdp::umem::FrameSize::TwoK,
        // Provide enough headroom so that we can convert an ipv4 header to ipv6
        // header without needing to copy any bytes. note this doesn't take into
        // account if a filter adds or removes bytes from the beginning of the
        // data payload
        head_room: (xdp::packet::net_types::Ipv6Hdr::LEN - xdp::packet::net_types::Ipv4Hdr::LEN)
            as u32,
        frame_count: packet_count,
        // TODO: This should be done in the type system so we can avoid logic
        // that doesn't change during the course of operation, but for now just
        // do it at runtime
        tx_checksum: device_caps.tx_metadata.checksum(),
        ..Default::default()
    }
    .build()?;

    let ring_cfg = xdp::RingConfigBuilder::default().build()?;
    let workers = ebpf_prog.create_and_bind_sockets(nic_index, umem_cfg, &device_caps, ring_cfg)?;

    Ok(XdpWorkers {
        ebpf_prog,
        workers,
        nic: nic_index,
        external_port: config.external_port.into(),
        qcmp_port: config.qcmp_port.into(),
        ipv4,
        ipv6,
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

        for jh in self.threads {
            if let Err(error) = jh.join() {
                if let Some(error) = error.downcast_ref::<&'static str>() {
                    tracing::error!(error, "XDP I/O thread enountered error");
                } else if let Some(error) = error.downcast_ref::<String>() {
                    tracing::error!(error, "XDP I/O thread enountered error");
                } else {
                    tracing::error!(?error, "XDP I/O thread enountered error");
                };
            }
        }
    }
}

/// The entrypoint into the XDP I/O loop.
///
/// This spawns a named thread for each configured XDP socket to run the packet
/// receiving + processing + sending, after which the eBPF program used to route
/// packets to the XDP sockets is attached to the NIC.
///
/// # Errors
///
/// This can fail if threads can not be spawned for some reason (unlikely), the
/// more likely reason for failure is the inability to attach the eBPF program
pub fn spawn(workers: XdpWorkers, config: process::ConfigState) -> Result<XdpLoop, XdpSpawnError> {
    let external_port = workers.external_port;
    let qcmp_port = workers.qcmp_port;
    let ipv4 = workers.ipv4;
    let ipv6 = workers.ipv6;
    let session_state = Arc::new(process::SessionState::default());
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut threads = Vec::with_capacity(workers.workers.len());
    for (i, mut worker) in workers.workers.into_iter().enumerate() {
        let cfg = config.clone();
        let ss = session_state.clone();
        let shutdown = shutdown.clone();

        let jh = std::thread::Builder::new()
            .name(format!("xdp-io-{i}"))
            .spawn(move || {
                // Enqueue buffers to the fill ring to ensure that we don't miss any packets
                // SAFETY: we keep the umem alive for as long as the socket is alive
                unsafe {
                    if let Err(error) = worker.fill.enqueue(&mut worker.umem, BATCH_SIZE * 2, true)
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

    // Now that all the io loops are running, attach the eBPF program to route
    // packets to the bound sockets
    let mut ebpf_prog = workers.ebpf_prog;

    // We use the default flags here, which means that the program will be attached
    // in driver mode if the NIC + driver is capable of it, otherwise it will fallback
    // to SKB mode. This allows maximum compatibility, and we already provide
    // flags to force zerocopy, which relies on driver mode, so the user can use
    // that if they don't want the fallback behavior
    let xdp_link =
        ebpf_prog.attach(workers.nic, quilkin_xdp::aya::programs::XdpFlags::default())?;

    Ok(XdpLoop {
        threads,
        ebpf_prog,
        xdp_link,
        shutdown,
    })
}

const BATCH_SIZE: usize = 64;
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

    use xdp::slab::Slab;

    let mut rx_slab = xdp::slab::StackSlab::<BATCH_SIZE>::new();
    let mut tx_slab = xdp::slab::StackSlab::<{ BATCH_SIZE << 2 }>::new();
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
            if let Err(error) = fill.enqueue(&mut umem, BATCH_SIZE * 2 - recvd, true) {
                // This is shoehorning an error that isn't attributable to a particular packet
                crate::metrics::errors_total(
                    crate::metrics::Direction::Read,
                    &io_error_to_discriminant(error),
                    &crate::metrics::EMPTY,
                )
                .inc();
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
                    // These are all temporary errors that can occur during normal
                    // operation
                    // if !matches!(
                    //     error.raw_os_error(),
                    //     Some(libc::EBUSY | libc::ENOBUFS | libc::EAGAIN | libc::ENETDOWN)
                    // ) {
                    // This is shoehorning an error that isn't attributable to a particular packet
                    crate::metrics::errors_total(
                        crate::metrics::Direction::Read,
                        &io_error_to_discriminant(error),
                        &crate::metrics::EMPTY,
                    )
                    .inc();
                    //}

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
