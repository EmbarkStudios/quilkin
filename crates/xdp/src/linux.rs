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

pub use aya;
pub use xdp::{self, nic::NicIndex};

// object unfortunately has alignment requirements, so we need to make sure
// the raw bytes are aligned for a 64-bit ELF (8 bytes)

// https://users.rust-lang.org/t/can-i-conveniently-compile-bytes-into-a-rust-program-with-a-specific-alignment/24049/2
// This struct is generic in Bytes to admit unsizing coercions.
#[repr(C)] // guarantee 'bytes' comes after '_align'
struct AlignedTo<Align, Bytes: ?Sized> {
    _align: [Align; 0],
    bytes: Bytes,
}

// dummy static used to create aligned data
static ALIGNED_MAIN: &AlignedTo<u64, [u8]> = &AlignedTo {
    _align: [],
    bytes: *include_bytes!("../bin/main.bin"),
};

/// The normal eBPF program used when proxying to a different physical network
pub static PROGRAM_MAIN: &[u8] = &ALIGNED_MAIN.bytes;

static ALIGNED_L2: &AlignedTo<u64, [u8]> = &AlignedTo {
    _align: [],
    bytes: *include_bytes!("../bin/layer2.bin"),
};

/// The eBPF program used when proxying to the same physical network, requiring resolution of layer 2 (ethernet) addresses
pub static PROGRAM_L2: &[u8] = &ALIGNED_L2.bytes;

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("'{0}' map not found in eBPF program")]
    MissingMap(&'static str),
    #[error("failed to insert socket: {0}")]
    Map(#[from] aya::maps::MapError),
    #[error("failed to bind socket: {0}")]
    Socket(#[from] xdp::socket::SocketError),
    #[error("XDP error: {0}")]
    Xdp(#[from] xdp::error::Error),
    #[error("mmap error: {0}")]
    Mmap(#[from] std::io::Error),
    #[error("pin error: {0}")]
    Pin(#[from] aya::pin::PinError),
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("eBPF load error")]
    Ebpf(#[from] aya::EbpfError),
    #[error("failed to read ephemeral port range from /proc/sys/net/ipv4/ip_local_port_range")]
    Io(#[from] std::io::Error),
    #[error("the default Linux ephemeral port range 32768..=60999 has been modified to {0}..={1}")]
    DefaultPortRangeModified(u16, u16),
}

/// An individual XDP worker.
///
/// For now there is always one worker per NIC queue, and doesn't use shared
/// memory allowing them to work on the queue in complete isolation
pub struct XdpWorker {
    /// The actual socket bound to the queue, used for polling operations
    pub socket: xdp::socket::XdpSocket,
    /// The memory map shared with the kernel where buffers used to receive
    /// and send packets are stored
    pub umem: xdp::Umem,
    /// The ring used to indicate to the kernel we wish to receive packets
    pub fill: xdp::WakableFillRing,
    /// The ring the kernel pushes received packets to
    pub rx: xdp::RxRing,
    /// The ring we push packets we wish to send
    pub tx: xdp::WakableTxRing,
    /// The ring the kernel pushes packets that have finished sending
    pub completion: xdp::CompletionRing,
}

pub struct EbpfProgram {
    pub bpf: aya::Ebpf,
    /// The external port is a variable that we modify at load time so the eBPF
    /// program can filter out which packets it is interested in. This needs to
    /// be the same port used in the I/O loop to determine if the packet is sent
    /// from a client or a server
    pub external_port: xdp::packet::net_types::NetworkU16,
    /// The port QCMP packets are sent to
    pub qcmp_port: xdp::packet::net_types::NetworkU16,
}

impl EbpfProgram {
    /// Loads the XDP program.
    ///
    /// The external port, the port used by clients, must be passed in so that
    /// the global constants can be patched in the object file before load
    ///
    /// If `cache_layer2` is set, we load a program that keeps updates a mapping of IP -> MAC addresses so that we can set
    /// the proper destination MAC address for the outgoing packet
    pub fn load(external_port: u16, qcmp_port: u16, cache_layer2: bool) -> Result<Self, LoadError> {
        Self::validate_port_range()?;

        Self::load_program(
            external_port,
            qcmp_port,
            if cache_layer2 {
                PROGRAM_L2
            } else {
                PROGRAM_MAIN
            },
        )
    }

    /// The eBPF load itself, [`Self::load`] additionally validates the assumption
    /// the port mapping relies on
    ///
    /// This is public, but really only for integration tests
    #[doc(hidden)]
    pub fn load_program(
        external_port: u16,
        qcmp_port: u16,
        program: &'static [u8],
    ) -> Result<Self, LoadError> {
        let mut loader = aya::EbpfLoader::new();
        let external_port_no = external_port.to_be_bytes();
        loader.override_global("EXTERNAL_PORT_NO", &external_port_no, true);

        let qcmp_port_no = qcmp_port.to_be_bytes();
        loader.override_global("QCMP_PORT_NO", &qcmp_port_no, true);

        let bpf = loader.load(program)?;

        Ok(Self {
            bpf,
            external_port: xdp::packet::net_types::NetworkU16(u16::from_ne_bytes(external_port_no)),
            qcmp_port: xdp::packet::net_types::NetworkU16(u16::from_ne_bytes(qcmp_port_no)),
        })
    }

    // Validate the ephemeral port range has not been modified
    //
    // We exploit the fact that Linux by default does not assign ephemeral ports in the full range allowed by IANA, but
    // we want to sanity check it here, as otherwise something else could have been assigned an ephemeral port that we
    // think we can use, which would lead to both quilkin and whatever program was assigned that port misbehaving
    fn validate_port_range() -> Result<(), LoadError> {
        let port_range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")?;
        let (start, end) =
            port_range
                .trim()
                .split_once(char::is_whitespace)
                .ok_or(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "expected 2 u16 integers",
                ))?;
        let start: u16 = start.parse().map_err(|_e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse range start '{start}'"),
            )
        })?;
        let end: u16 = end.parse().map_err(|_e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse range end '{end}'"),
            )
        })?;

        if end != 60999 {
            return Err(LoadError::DefaultPortRangeModified(start, end));
        }

        Ok(())
    }

    /// Creates and binds sockets
    pub fn create_and_bind_sockets(
        &mut self,
        nic: NicIndex,
        umem_cfg: xdp::umem::UmemCfg,
        device_caps: &xdp::nic::NetdevCapabilities,
        ring_cfg: xdp::RingConfig,
    ) -> Result<Vec<XdpWorker>, BindError> {
        use std::os::fd::AsRawFd as _;

        let mut xsk_map = aya::maps::XskMap::try_from(
            self.bpf
                .map_mut("XSK")
                .ok_or(BindError::MissingMap("XSK"))?,
        )?;

        let queue_count = device_caps.queues.rx_count();

        let mut entries = Vec::with_capacity(queue_count as _);
        for i in 0..queue_count {
            let umem = xdp::Umem::map(umem_cfg)?;
            let mut sb = xdp::socket::XdpSocketBuilder::new()?;
            let (rings, mut bind_flags) = sb.build_wakable_rings(&umem, ring_cfg)?;

            if device_caps.zero_copy.is_available() {
                bind_flags.force_zerocopy();
            }

            let socket = sb.bind(nic, i, bind_flags)?;
            xsk_map.set(i, socket.as_raw_fd(), 0)?;

            entries.push(XdpWorker {
                socket,
                umem,
                fill: rings.fill_ring,
                rx: rings.rx_ring.unwrap(),
                tx: rings.tx_ring.unwrap(),
                completion: rings.completion_ring,
            });
        }

        Ok(entries)
    }

    /// We use this entrypoint for now, but in the future we could also use
    /// a round robin mode when the xdp lib supports shared Umem
    #[doc(hidden)]
    pub fn program_mut(&mut self) -> &mut aya::programs::Xdp {
        self.bpf
            .program_mut("all_queues")
            .expect("failed to locate 'all_queues' program")
            .try_into()
            .expect("'all_queues' is not an xdp program")
    }

    /// Returns the ring buffer of IP + MAC entries that is written to by eBPF when receiving an ICMP or ARP packet
    pub fn layer2_ring(&mut self) -> Result<aya::maps::RingBuf<aya::maps::MapData>, BindError> {
        let map = self
            .bpf
            // We exclusively use this in another thread so we need to take ownership of the map to avoid
            // annoying lifetime issues
            .take_map("IP_TO_MAC")
            .ok_or(BindError::MissingMap("IP_TO_MAC"))?;

        Ok(aya::maps::RingBuf::try_from(map)?)
    }

    /// Verifies and loads the program into the kernel; call once, before [`Self::attach`].
    pub fn load_into_kernel(&mut self) -> Result<(), aya::programs::ProgramError> {
        if let Err(_error) = aya_log::EbpfLogger::init(&mut self.bpf) {
            // Would be good to enable this if we do end up adding log messages to
            // the eBPF program, right now we don't so this will error as the ring
            // buffer used to transfer log messages is not created if there are none
            //tracing::warn!(%error, "failed to initialize eBPF logging");
        }

        self.program_mut().load()
    }

    /// Attaches the eBPF program to the specified interface
    ///
    /// Once attached, the program controls the RX queues of the interface and forwards packets to our userspace processing,
    /// or passes them to the kernel
    pub fn attach(
        &mut self,
        nic: NicIndex,
        mode: aya::programs::xdp::XdpMode,
    ) -> Result<aya::programs::xdp::XdpLinkId, aya::programs::ProgramError> {
        // We use this entrypoint for now, but in the future we could also use
        // a round robin mode when the xdp lib supports shared Umem
        let program: &mut aya::programs::Xdp = self
            .bpf
            .program_mut("all_queues")
            .expect("failed to locate 'all_queues' program")
            .try_into()
            .expect("'all_queues' is not an xdp program");
        if let Err(err) = program.load()
            && !matches!(err, aya::programs::ProgramError::AlreadyLoaded)
        {
            return Err(err);
        }

        program.attach_to_if_index(nic.into(), mode)
    }

    pub fn detach(
        &mut self,
        link_id: aya::programs::xdp::XdpLinkId,
    ) -> Result<(), aya::programs::ProgramError> {
        self.program_mut().detach(link_id)
    }
}
