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
static ALIGNED: &AlignedTo<u64, [u8]> = &AlignedTo {
    _align: [],
    bytes: *include_bytes!("../bin/packet-router.bin"),
};

static PROGRAM: &[u8] = &ALIGNED.bytes;

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("'XSK' map not found in eBPF program")]
    MissingXskMap,
    #[error("failed to insert socket: {0}")]
    Map(#[from] aya::maps::MapError),
    #[error("failed to bind socket: {0}")]
    Socket(#[from] xdp::socket::SocketError),
    #[error("XDP error: {0}")]
    Xdp(#[from] xdp::error::Error),
    #[error("mmap error: {0}")]
    Mmap(#[from] std::io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("eBPF load error")]
    Ebpf(#[from] aya::EbpfError),
    #[error("failed to read ephemeral port range")]
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
    bpf: aya::Ebpf,
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
    /// The external port, the port used by clients, must be passed in due to
    /// how globals work in eBPF.
    pub fn load(external_port: u16, qcmp_port: u16) -> Result<Self, LoadError> {
        // We exploit the fact that Linux by default does not assign ephemeral
        // ports in the full range allowed by IANA, but we want to sanity check
        // it here, as otherwise something else could have been assigned an
        // ephemeral port that we think we can use, which would lead to both
        // quilkin and whatever program was assigned that port misbehaving
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

        Ok(Self::load_program(external_port, qcmp_port)?)
    }

    /// The eBPF load itself, [`Self::load`] additionally validates the assumption
    /// the port mapping relies on
    fn load_program(external_port: u16, qcmp_port: u16) -> Result<Self, aya::EbpfError> {
        let mut loader = aya::EbpfLoader::new();
        let external_port_no = external_port.to_be();
        loader.override_global("EXTERNAL_PORT_NO", &external_port_no, true);

        let qcmp_port_no = qcmp_port.to_be();
        loader.override_global("QCMP_PORT_NO", &qcmp_port_no, true);

        Ok(Self {
            bpf: loader.load(PROGRAM)?,
            external_port: xdp::packet::net_types::NetworkU16(external_port_no),
            qcmp_port: xdp::packet::net_types::NetworkU16(qcmp_port_no),
        })
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
            self.bpf.map_mut("XSK").expect("failed to retrieve XSK map"),
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

    // We use this entrypoint for now, but in the future we could also use
    // a round robin mode when the xdp lib supports shared Umem
    fn program_mut(&mut self) -> &mut aya::programs::Xdp {
        self.bpf
            .program_mut("all_queues")
            .expect("failed to locate 'all_queues' program")
            .try_into()
            .expect("'all_queues' is not an xdp program")
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

    /// Attaches the loaded program to `nic`; safe to retry, eg after reconfiguring the NIC.
    pub fn attach(
        &mut self,
        nic: NicIndex,
        mode: aya::programs::xdp::XdpMode,
    ) -> Result<aya::programs::xdp::XdpLinkId, aya::programs::ProgramError> {
        self.program_mut().attach_to_if_index(nic.into(), mode)
    }

    pub fn detach(
        &mut self,
        link_id: aya::programs::xdp::XdpLinkId,
    ) -> Result<(), aya::programs::ProgramError> {
        self.program_mut().detach(link_id)
    }
}

/// The eBPF object is committed rather than built, so these validate it still
/// has what [`EbpfProgram::load`] expects.
///
/// Loading a program requires `CAP_BPF` + `CAP_NET_ADMIN`, so the tests that
/// need the kernel are `#[ignore]`d. Run the built test binary under sudo rather
/// than cargo, which would leave root owned artifacts in `target`:
///
/// ```sh
/// BIN=$(cargo test -p quilkin-xdp --no-run 2>&1 | grep -oE '\(target/[^)]+\)' | tr -d '()')
/// sudo "$BIN" --ignored --test-threads 1
/// ```
#[cfg(test)]
mod tests {
    use super::{EbpfProgram, PROGRAM};
    use aya::programs::{TestRun as _, TestRunOptions};

    const EXTERNAL_PORT: u16 = 7777;
    const QCMP_PORT: u16 = 7600;

    /// The action the kernel reports the program returned
    const XDP_PASS: u32 = 2;
    const XDP_REDIRECT: u32 = 4;

    fn parse() -> aya_obj::Object {
        aya_obj::Object::parse(PROGRAM).expect("failed to parse eBPF program")
    }

    /// Options for the ipv4 frames [`frame`] builds
    struct Ipv4 {
        proto: u8,
        /// The low nibble of the first header byte, 5 means no options
        ihl: u8,
        /// The fragment offset and flags, non-zero means fragmented
        fragment: u16,
        destination_port: u16,
    }

    impl Default for Ipv4 {
        fn default() -> Self {
            Self {
                proto: 17,
                ihl: 5,
                fragment: 0,
                destination_port: EXTERNAL_PORT,
            }
        }
    }

    /// Builds a minimal ethernet + ipv4 + UDP frame, the payload is irrelevant as
    /// the program only ever reads headers
    fn frame(ip: Ipv4) -> Vec<u8> {
        const LEN: usize = 14 + 20 + 8 + 4;
        let mut frame = vec![0u8; LEN];

        // Ethernet, destination and source are never read
        frame[12..14].copy_from_slice(&0x0800u16.to_be_bytes());

        // ipv4
        frame[14] = 0x40 | ip.ihl;
        frame[16..18].copy_from_slice(&((LEN - 14) as u16).to_be_bytes());
        frame[20..22].copy_from_slice(&ip.fragment.to_be_bytes());
        frame[22] = 64;
        frame[23] = ip.proto;
        frame[26..30].copy_from_slice(&[5, 5, 5, 5]);
        frame[30..34].copy_from_slice(&[2, 2, 2, 2]);

        // UDP
        frame[34..36].copy_from_slice(&8888u16.to_be_bytes());
        frame[36..38].copy_from_slice(&ip.destination_port.to_be_bytes());
        frame[38..40].copy_from_slice(&12u16.to_be_bytes());

        frame
    }

    /// A loaded program with a socket in its `XSK` map, everything has to be kept
    /// alive for the map entry to stay valid
    struct Loaded {
        program: EbpfProgram,
        _socket: xdp::socket::XdpSocket,
        _rings: xdp::WakableRings,
        _umem: xdp::Umem,
    }

    /// Loads the program into the kernel, which is where the verifier runs, and
    /// binds a socket to the first NIC queue so that redirect decisions are
    /// observable, without one the fallback for an empty `XSK` map is the same
    /// [`XDP_PASS`] as a packet the program isn't interested in
    fn load() -> Loaded {
        let mut prog =
            EbpfProgram::load_program(EXTERNAL_PORT, QCMP_PORT).expect("failed to load program");
        prog.load_into_kernel()
            .expect("the kernel rejected the program");

        let nic = xdp::nic::InterfaceIter::new()
            .expect("failed to enumerate NICs")
            .next()
            .expect("no NIC available to bind an AF_XDP socket to");

        let umem = xdp::Umem::map(
            xdp::umem::UmemCfgBuilder {
                frame_size: xdp::umem::FrameSize::TwoK,
                frame_count: 64,
                ..Default::default()
            }
            .build()
            .expect("invalid umem config"),
        )
        .expect("failed to map umem");

        let mut sb = xdp::socket::XdpSocketBuilder::new().expect("failed to create socket");
        let (rings, mut bind_flags) = sb
            .build_wakable_rings(
                &umem,
                xdp::RingConfigBuilder::default()
                    .build()
                    .expect("invalid ring config"),
            )
            .expect("failed to build rings");
        bind_flags.force_copy();

        let socket = sb
            .bind(nic, 0, bind_flags)
            .expect("failed to bind socket to queue 0");

        {
            use std::os::fd::AsRawFd as _;
            let mut xsk = aya::maps::XskMap::try_from(prog.bpf.map_mut("XSK").expect("no XSK map"))
                .expect("XSK is not an xskmap");
            xsk.set(0, socket.as_raw_fd(), 0)
                .expect("failed to insert socket into the XSK map");
        }

        Loaded {
            program: prog,
            _socket: socket,
            _rings: rings,
            _umem: umem,
        }
    }

    /// Note the kernel rejects a `data_in` below the size of an ethernet header
    fn run(prog: &mut EbpfProgram, frame: &[u8]) -> u32 {
        prog.program_mut()
            .test_run(TestRunOptions {
                data_in: Some(frame),
                ..Default::default()
            })
            .expect("failed to run program")
            .return_value
    }

    /// The verifier is the only authority on whether the committed object is
    /// loadable, everything else here is downstream of this passing
    #[test]
    #[ignore = "requires CAP_BPF"]
    fn loads_into_kernel() {
        let mut prog =
            EbpfProgram::load_program(EXTERNAL_PORT, QCMP_PORT).expect("failed to load program");
        prog.load_into_kernel()
            .expect("the kernel rejected the program");
    }

    #[test]
    #[ignore = "requires CAP_BPF"]
    fn routes_udp_quilkin_owns() {
        let mut loaded = load();
        let prog = &mut loaded.program;

        for port in [EXTERNAL_PORT, QCMP_PORT, 61000, u16::MAX] {
            assert_eq!(
                run(
                    prog,
                    &frame(Ipv4 {
                        destination_port: port,
                        ..Default::default()
                    })
                ),
                XDP_REDIRECT,
                "port {port} should be routed to a socket"
            );
        }
    }

    /// The I/O loop parses at fixed header offsets, so anything the offsets don't
    /// hold for has to be left to the kernel
    #[test]
    #[ignore = "requires CAP_BPF"]
    fn passes_frames_the_io_loop_cant_parse() {
        let mut loaded = load();
        let prog = &mut loaded.program;

        let cases = [
            (
                "not UDP",
                Ipv4 {
                    proto: 6,
                    ..Default::default()
                },
            ),
            (
                "ipv4 options",
                Ipv4 {
                    ihl: 6,
                    ..Default::default()
                },
            ),
            // A later fragment has no UDP header at all
            (
                "fragment offset",
                Ipv4 {
                    fragment: 0x0001,
                    ..Default::default()
                },
            ),
            // The first fragment of a fragmented datagram
            (
                "more fragments",
                Ipv4 {
                    fragment: 0x2000,
                    ..Default::default()
                },
            ),
            // Don't fight the kernel for ports we don't own
            (
                "unrelated port",
                Ipv4 {
                    destination_port: 53,
                    ..Default::default()
                },
            ),
        ];

        for (case, ip) in cases {
            assert_eq!(
                run(prog, &frame(ip)),
                XDP_PASS,
                "{case} should be passed to the kernel"
            );
        }

        // Frames too short to hold the headers the program reads
        let full = frame(Ipv4::default());
        for length in [14, 33, 41] {
            assert_eq!(
                run(prog, &full[..length]),
                XDP_PASS,
                "a {length} byte frame should be passed to the kernel"
            );
        }

        // The don't fragment flag is not a fragment
        assert_eq!(
            run(
                prog,
                &frame(Ipv4 {
                    fragment: 0x4000,
                    ..Default::default()
                })
            ),
            XDP_REDIRECT,
        );
    }

    #[test]
    fn has_expected_programs_and_maps() {
        let object = parse();

        for program in ["all_queues", "round_robin"] {
            let program = object
                .programs
                .get(program)
                .unwrap_or_else(|| panic!("'{program}' program not found"));
            assert!(
                matches!(program.section, aya_obj::ProgramSection::Xdp { .. }),
                "'{:?}' is not an xdp program",
                program.section
            );
        }

        assert!(object.maps.contains_key("XSK"), "'XSK' map not found");
    }

    #[test]
    fn port_globals_can_be_overridden() {
        let mut object = parse();

        let port = 7777u16.to_be_bytes();
        object
            .patch_map_data(
                [
                    ("EXTERNAL_PORT_NO", (&port[..], true)),
                    ("QCMP_PORT_NO", (&port[..], true)),
                ]
                .into(),
            )
            .expect("failed to override port globals");
    }
}
