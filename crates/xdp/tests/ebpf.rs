use aya::programs::{TestRun as _, TestRunOptions};
use etherparse as ep;
use std::net::{Ipv4Addr, Ipv6Addr};

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
use quilkin_xdp::{EbpfProgram, PROGRAM_L2, PROGRAM_MAIN};

const EXTERNAL_PORT: u16 = 7777;
const QCMP_PORT: u16 = 7600;

/// The action the kernel reports the program returned
#[derive(PartialEq, Debug)]
enum XdpAction {
    Aborted,
    Drop,
    Pass,
    Tx,
    Redirect,
}

/// A loaded program with a socket in its `XSK` map, everything has to be kept
/// alive for the map entry to stay valid
struct Loaded {
    program: EbpfProgram,
    ip_to_mac: Option<aya::maps::RingBuf<aya::maps::MapData>>,
    _socket: xdp::socket::XdpSocket,
    _rings: xdp::WakableRings,
    _umem: xdp::Umem,
}

/// Loads the program into the kernel, which is where the verifier runs, and
/// binds a socket to the first NIC queue so that redirect decisions are
/// observable, without one the fallback for an empty `XSK` map is the same
/// [`XDP_PASS`] as a packet the program isn't interested in
fn load(program: &'static [u8]) -> Loaded {
    let mut prog = EbpfProgram::load_program(EXTERNAL_PORT, QCMP_PORT, program)
        .expect("failed to load program");
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

    let ip_to_mac = prog
        .bpf
        .take_map("IP_TO_MAC")
        .and_then(|i2m| aya::maps::RingBuf::try_from(i2m).ok());

    Loaded {
        program: prog,
        ip_to_mac,
        _socket: socket,
        _rings: rings,
        _umem: umem,
    }
}

/// Note the kernel rejects a `data_in` below the size of an ethernet header
#[inline]
fn run(prog: &mut EbpfProgram, frame: &[u8]) -> XdpAction {
    let res = prog
        .program_mut()
        .test_run(TestRunOptions {
            data_in: Some(frame),
            ..Default::default()
        })
        .expect("failed to run program")
        .return_value;

    match res {
        0 => XdpAction::Aborted,
        1 => XdpAction::Drop,
        2 => XdpAction::Pass,
        3 => XdpAction::Tx,
        4 => XdpAction::Redirect,
        _ => unreachable!("a new xdp action is available in this kernel version"),
    }
}

#[test]
#[ignore = "requires CAP_BPF"]
fn validate_bpf_programs() {
    let run = |prog: &mut Loaded| {
        routes_udp_quilkin_owns(&mut prog.program);
        passes_frames_the_io_loop_cant_parse(&mut prog.program);
    };

    {
        let mut main = load(PROGRAM_MAIN);
        run(&mut main);
    }

    {
        let mut l2 = load(PROGRAM_L2);
        run(&mut l2);

        handles_ping_responses(&mut l2);
    }
}

macro_rules! ether {
    () => {
        ep::PacketBuilder::ethernet2([1; 6], [2; 6])
    };
}

macro_rules! to_vec {
    ($pb:expr) => {{
        let mut v = Vec::with_capacity($pb.size(32));
        $pb.write_to_vec(&mut v, &[0x83; 32]).unwrap();
        v
    }};
    ($pb:expr, $proto:expr) => {{
        let mut v = Vec::with_capacity($pb.size(32));
        $pb.write_to_vec(&mut v, $proto, &[0x83; 32]).unwrap();
        v
    }};
}

/// Validates that receiving UDP packets destined for ports that Quilkin is supposed to handle are correctly redirected
fn routes_udp_quilkin_owns(prog: &mut EbpfProgram) {
    for port in [
        EXTERNAL_PORT, /* the port that clients send packets to to be routed to game servers */
        QCMP_PORT,     /* port QCMP messages are sent to for determining latency between nodes */
        61000,         /* the beginning of the ephemeral port range that linux doesn't use */
        u16::MAX,      /* the end of the ephemeral port range that linux doesn't use */
    ] {
        assert_eq!(
            run(
                prog,
                &to_vec!(ether!().ipv4([8; 4], [9; 4], 20).udp(4545, port))
            ),
            XdpAction::Redirect,
            "ipv4 port {port} should be routed to a socket"
        );

        assert_eq!(
            run(
                prog,
                &to_vec!(ether!().ipv6([8; 16], [9; 16], 20).udp(4545, port))
            ),
            XdpAction::Redirect,
            "ipv6 port {port} should be routed to a socket"
        );
    }
}

/// The I/O loop parses at fixed header offsets, so anything the offsets don't
/// hold for has to be left to the kernel
fn passes_frames_the_io_loop_cant_parse(prog: &mut EbpfProgram) {
    let cases = [
        (
            "not UDP",
            to_vec!(ether!().ipv4([1; 4], [2; 4], 1).tcp(1, 2, 78, 1024)),
        ),
        (
            "ipv4 options",
            to_vec!(
                ether!()
                    .ip(ep::IpHeaders::Ipv4(
                        Default::default(),
                        ep::Ipv4Extensions {
                            auth: Some(
                                ep::IpAuthHeader::new(ep::IpNumber::UDP, 1, 2, &[7; 4]).unwrap()
                            ),
                        }
                    ))
                    .udp(234, 567)
            ),
        ),
        // A later fragment has no UDP header at all
        (
            "ipv4 fragment offset",
            to_vec!(
                ether!().ip(ep::IpHeaders::Ipv4(
                    ep::Ipv4Header {
                        fragment_offset: ep::IpFragOffset::try_new(0x1).unwrap(),
                        ..Default::default()
                    },
                    Default::default()
                )),
                ep::IpNumber::UDP
            ),
        ),
        // The first fragment of a fragmented datagram
        (
            "ipv4 more fragments",
            to_vec!(
                ether!()
                    .ip(ep::IpHeaders::Ipv4(
                        ep::Ipv4Header {
                            more_fragments: true,
                            ..Default::default()
                        },
                        Default::default()
                    ))
                    .udp(234, 567)
            ),
        ),
        // Don't fight the kernel for ports we don't own
        (
            "unrelated UDP port",
            to_vec!(ether!().ipv4([1; 4], [2; 4], 1).udp(1, 53)),
        ),
        (
            "ipv6 TCP",
            to_vec!(ether!().ipv6([1; 16], [2; 16], 1).tcp(1, 2, 78, 1024)),
        ),
        (
            "ipv6 fragmented",
            to_vec!(
                ether!()
                    .ip(ep::IpHeaders::Ipv6(
                        Default::default(),
                        ep::Ipv6Extensions {
                            fragment: Some(ep::Ipv6FragmentHeader::new(
                                ep::IpNumber::UDP,
                                ep::IpFragOffset::try_new(0x1).unwrap(),
                                false,
                                2
                            )),
                            ..Default::default()
                        }
                    ))
                    .udp(45, 7777)
            ),
        ),
        (
            "ipv6 more fragments",
            to_vec!(
                ether!()
                    .ip(ep::IpHeaders::Ipv6(
                        Default::default(),
                        ep::Ipv6Extensions {
                            fragment: Some(ep::Ipv6FragmentHeader::new(
                                ep::IpNumber::UDP,
                                ep::IpFragOffset::try_new(0x1).unwrap(),
                                true,
                                2
                            )),
                            ..Default::default()
                        }
                    ))
                    .udp(45, 7777)
            ),
        ),
    ];

    for (case, frame) in cases {
        assert_eq!(
            run(prog, &frame),
            XdpAction::Pass,
            "{case} should be passed to the kernel"
        );
    }

    // Frames too short to hold the headers the program reads
    {
        let v4 = to_vec!(ether!().ipv4([6; 4], [4; 4], 34).udp(234, 7777));
        for length in [14, 33, 41] {
            assert_eq!(
                run(prog, &v4[..length]),
                XdpAction::Pass,
                "a {length} byte frame should be passed to the kernel"
            );
        }

        let v6 = to_vec!(ether!().ipv6([6; 16], [4; 16], 34).udp(234, 7777));
        for length in [14, 33, 41, 45] {
            assert_eq!(
                run(prog, &v6[..length]),
                XdpAction::Pass,
                "a {length} byte frame should be passed to the kernel"
            );
        }
    }

    // Ignore the Don't Fragment flag
    assert_eq!(
        run(
            prog,
            &to_vec!(
                ether!()
                    .ip(ep::IpHeaders::Ipv4(
                        ep::Ipv4Header {
                            dont_fragment: true,
                            ..Default::default()
                        },
                        Default::default()
                    ))
                    .udp(456, 7777)
            )
        ),
        XdpAction::Redirect
    );
}

#[test]
fn has_expected_programs_and_maps() {
    for program in [PROGRAM_MAIN, PROGRAM_L2] {
        let object = aya_obj::Object::parse(program).expect("failed to parse eBPF program");
        let program = object
            .programs
            .get("all_queues")
            .expect("all_queues not found");
        assert!(
            matches!(program.section, aya_obj::ProgramSection::Xdp { .. }),
            "'{:?}' is not an xdp program",
            program.section
        );

        assert!(object.maps.contains_key("XSK"), "'XSK' map not found");
    }
}

#[test]
fn port_globals_can_be_overridden() {
    for program in [PROGRAM_MAIN, PROGRAM_L2] {
        let mut object = aya_obj::Object::parse(program).expect("failed to parse eBPF program");

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

fn handles_ping_responses(loaded: &mut Loaded) {
    let mut rb = loaded.ip_to_mac.take().expect("expected ring buffer");

    macro_rules! assert_entry {
        ($actual:expr, $ip:expr, $mac:expr) => {
            assert_eq!($actual.ip, $ip);
            assert_eq!($actual.mac, $mac);
        };
    }

    // An ARP reply, sent in response to the kernel request when the IP's ethernet address is not cached by the kernel
    {
        assert_eq!(
            run(&mut loaded.program, &{
                let pb = ep::PacketBuilder::ethernet2([1, 2, 3, 4, 5, 6], [77; 6]).arp(
                    ep::ArpPacket::new(
                        ep::ArpHardwareId::ETHERNET,
                        ep::EtherType::IPV4,
                        ep::ArpOperation::REPLY,
                        &[0, 2, 4, 8, 16, 32],
                        &[192, 168, 1, 1],
                        &[1, 3, 5, 7, 11, 13],
                        &[192, 168, 1, 2],
                    )
                    .unwrap(),
                );

                let mut v = Vec::new();
                pb.write_to_vec(&mut v).unwrap();
                v
            }),
            XdpAction::Pass
        );

        let entry =
            quilkin_xdp::l2_cache::types::read_entry(&mut rb).expect("expected entry via ARP");

        assert_entry!(entry, Ipv4Addr::new(192, 168, 1, 1), [0, 2, 4, 8, 16, 32]);
        assert!(
            matches!(entry.source, quilkin_xdp::l2_cache::types::Source::Arp),
            "expected source as an ARP packet"
        );
    }

    // An ICMP echo reply, sent in response to an ICMP request we sent, which may or may not send an ARP request before
    // if the kernel hasn't cached the ethernet address for the IP we are pinging
    {
        assert_eq!(
            run(
                &mut loaded.program,
                &to_vec!(
                    ep::PacketBuilder::ethernet2([1, 2, 3, 4, 5, 6], [77; 6])
                        .ipv4([192, 168, 1, 100], [192, 168, 1, 2], 3)
                        .icmpv4_echo_reply(23, 1)
                )
            ),
            XdpAction::Pass
        );

        let entry =
            quilkin_xdp::l2_cache::types::read_entry(&mut rb).expect("expected entry via ICMPv4");

        assert_entry!(entry, Ipv4Addr::new(192, 168, 1, 100), [1, 2, 3, 4, 5, 6]);
        assert!(
            matches!(entry.source, quilkin_xdp::l2_cache::types::Source::Icmp),
            "expected source as an ICMPv4 packet"
        );
    }

    // An ICMPv6 echo reply, sent in response to an ICMPv6 request we sent, which may or may not send an ICMP neighbour
    // solicitation before if the kernel hasn't cached the ethernet address
    {
        let src_ip = Ipv6Addr::from_bits(0xfefe1234);
        assert_eq!(
            run(
                &mut loaded.program,
                &to_vec!(
                    ep::PacketBuilder::ethernet2([7; 6], [77; 6])
                        .ipv6(src_ip.octets(), [0x99; 16], 3)
                        .icmpv6_echo_reply(32, 3)
                )
            ),
            XdpAction::Pass
        );

        let entry =
            quilkin_xdp::l2_cache::types::read_entry(&mut rb).expect("expected entry via ICMPv6");

        assert_entry!(entry, src_ip, [7; 6]);
        assert!(
            matches!(entry.source, quilkin_xdp::l2_cache::types::Source::Icmpv6),
            "expected source as an ICMPv6 packet"
        );
    }

    // Reply to a neighbour solicitation made by the kernel when it doesn't know the ethernet address of an IPv6 IP
    {
        const NEIGHBOR_ADVERT: u8 = 136;

        let src_ip = Ipv6Addr::from_bits(0xfe800000000000005cd7e09c1b4ddaaf);
        let src_mac = [0x30, 0xc5, 0x99, 0xd0, 0x0e, 0x7a];

        let pb = ep::PacketBuilder::ethernet2(src_mac, [0x04, 0xf4, 0x1c, 0xea, 0x7f, 0x17])
            .ipv6(
                src_ip.octets(),
                [
                    0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5c, 0xd7, 0xe0, 0x9c, 0x1b,
                    0x4d, 0xda, 0xaf,
                ],
                255,
            )
            .icmpv6_raw(
                NEIGHBOR_ADVERT,
                0,
                ep::icmpv6::NeighborAdvertisementHeader {
                    solicited: true,
                    r#override: false,
                    router: false,
                }
                .to_bytes(),
            );

        let mut v = Vec::new();

        // This is a solicited advertisement with no options, so just the target IP
        pb.write_to_vec(&mut v, &src_ip.octets()).unwrap();

        assert_eq!(run(&mut loaded.program, &v,), XdpAction::Pass);

        let entry = quilkin_xdp::l2_cache::types::read_entry(&mut rb)
            .expect("expected entry via neighbor advertisement");

        assert_entry!(entry, src_ip, src_mac);
        assert!(
            matches!(
                entry.source,
                quilkin_xdp::l2_cache::types::Source::NeighbourAdvertisement
            ),
            "expected source as an ICMP packet"
        );

        let pb = ep::PacketBuilder::ethernet2(
            // this purposely mismatches, as we set the target link layer address, in the neighbor discovery header,
            // which we want the eBPF to use as the MAC address if it exists
            [0xbb; 6],
            [0x04, 0xf4, 0x1c, 0xea, 0x7f, 0x17],
        )
        .ipv6(
            src_ip.octets(),
            [
                0xfe, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5c, 0xd7, 0xe0, 0x9c, 0x1b, 0x4d,
                0xda, 0xaf,
            ],
            255,
        )
        .icmpv6_raw(
            NEIGHBOR_ADVERT,
            0,
            ep::icmpv6::NeighborAdvertisementHeader {
                solicited: true,
                r#override: false,
                router: false,
            }
            .to_bytes(),
        );

        let mut payload = Vec::new();

        payload.extend_from_slice(&src_ip.octets());
        // https://datatracker.ietf.org/doc/html/rfc2461#section-4.6.1
        // 0                   1                   2                   3
        // 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        // |     Type      |    Length     |    Link-Layer Address ...
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        payload.extend_from_slice(
            &ep::icmpv6::NdpOptionHeader {
                option_type: ep::icmpv6::NdpOptionType::TARGET_LINK_LAYER_ADDRESS,
                // This is in bytes, so 1 since the length includes this 2-byte header and 6-byte MAC address
                length_units: 1,
            }
            .to_bytes(),
        );
        payload.extend_from_slice(&src_mac);

        let mut v = Vec::new();
        pb.write_to_vec(&mut v, &payload).unwrap();

        assert_eq!(run(&mut loaded.program, &v,), XdpAction::Pass);

        let entry = quilkin_xdp::l2_cache::types::read_entry(&mut rb)
            .expect("expected entry via neighbor advertisement");

        assert_entry!(entry, src_ip, src_mac);
        assert!(
            matches!(
                entry.source,
                quilkin_xdp::l2_cache::types::Source::NeighbourAdvertisement
            ),
            "expected source as an ICMP packet"
        );
    }
}
