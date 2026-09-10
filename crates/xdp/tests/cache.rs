use quilkin_xdp::{
    l2_cache::types::LinkLayerAddr,
    xdp::{self, packet::net_types as nt},
};
use std::{
    net::{self, Ipv4Addr, Ipv6Addr, UdpSocket},
    os::fd::AsRawFd,
};

#[inline]
fn cmd(args: &[&str]) {
    cmd_inner(args, true)
}

#[inline]
fn cmd_inner(args: &[&str], care: bool) {
    let out = std::process::Command::new("ip")
        .args(args)
        .output()
        .expect("failed to run `ip`");

    if care && !out.status.success() {
        eprintln!(
            "failed to execute `ip {args:?}\n\n{}`",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

const NAMESPACES: &[(&str, Option<Ipv4Addr>, Option<Ipv6Addr>)] = &[
    ("client", Some(Ipv4Addr::new(192, 168, 100, 11)), None),
    (
        "proxy",
        Some(Ipv4Addr::new(192, 168, 100, 77)),
        Some(Ipv6Addr::from_bits(0x20010db8000100000000000000000077)),
    ),
    ("ipv4", Some(Ipv4Addr::new(192, 168, 100, 44)), None),
    (
        "ipv6",
        None,
        Some(Ipv6Addr::from_bits(0x20010db8000100000000000000000066)),
    ),
];

fn create_namespaces() {
    // Nuke the namespaces if they exist, to avoid state from previous runs
    let mut args = ["netns", "del", "_"];

    for (ns, _, _) in NAMESPACES {
        args[2] = ns;
        cmd_inner(&args, false);
    }

    let mut args = ["netns", "add", "_"];

    for (ns, _, _) in NAMESPACES {
        args[2] = ns;
        cmd(&args);
    }
}

fn create_vlans(nic: &xdp::nic::NicIndex) {
    let nic_name = nic.name().expect("failed to get nic name");
    let nic_name = nic_name.as_str().expect("invalid nic name");

    let mut vlan = String::new();

    for (ns, ipv4, ipv6) in NAMESPACES {
        vlan.clear();
        vlan.push_str(ns);
        vlan.push_str("vlan");

        cmd(&[
            "link", "add", &vlan, "link", nic_name, "type", "macvlan", "mode", "bridge",
        ]);
        cmd(&["link", "set", &vlan, "netns", ns]);

        cmd(&["netns", "exec", ns, "ip", "link", "set", "dev", &vlan]);

        if let Some(ipv4) = ipv4 {
            let ip = format!("{ipv4}/24");
            cmd(&["netns", "exec", ns, "ip", "addr", "add", &ip, "dev", &vlan]);
        }

        if let Some(ipv6) = ipv6 {
            let ip = format!("{ipv6}/64");
            cmd(&[
                "netns", "exec", ns, "ip", "-6", "addr", "add", &ip, "dev", &vlan,
            ]);
        }

        cmd(&["netns", "exec", ns, "ip", "link", "set", "dev", &vlan, "up"]);
    }
}

const EXTERNAL_PORT: u16 = 7777;
const QCMP_PORT: u16 = 7600;

#[test]
#[ignore = "requires sudo privileges"]
fn layer2_caching() {
    use std::fs::File;

    let nic = xdp::nic::InterfaceIter::new()
        .expect("failed to create nic iterator")
        .next()
        .expect("failed to find default interface");

    create_namespaces();
    create_vlans(&nic);

    let (spinup_tx, spinup_rx) = crossbeam_channel::bounded(4);

    let shutdown = std::sync::Arc::<std::sync::atomic::AtomicBool>::default();

    const PACKET_SIZE: usize = 128;

    let client = std::thread::Builder::new()
        .name("client".into())
        .spawn(move || {
            let ns = File::open("/var/run/netns/client").expect("failed to open client namespace");
            unsafe {
                libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET);
            }

            let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 1111))
                .expect("failed to bind client socket");

            // Set an aggressive timeout, this is all local
            sock.set_read_timeout(Some(std::time::Duration::from_millis(10)))
                .expect("failed to set read timeout");

            // Wait for the other threads to finish spinning up
            while spinup_rx.recv().is_ok() {}

            let proxy = net::SocketAddr::new(NAMESPACES[1].1.unwrap().into(), EXTERNAL_PORT);

            for i in 0..100u8 {
                let mut data = [i; PACKET_SIZE];

                assert_eq!(
                    sock.send_to(&data, proxy).expect("failed to send packet"),
                    PACKET_SIZE
                );
                let (len, from) = sock.recv_from(&mut data).expect("failed to recv packet");
                assert_eq!(len, PACKET_SIZE);
                assert_eq!(from, proxy);

                // We expect even packets to be mutated by ipv4 and odd to be ipv6
                if i % 2 == 0 {
                    assert_eq!(data, [i.wrapping_add(44); PACKET_SIZE]);
                } else {
                    assert_eq!(data, [i.wrapping_add(66); PACKET_SIZE]);
                }
            }
        })
        .expect("failed to spawn client thread");

    let ipv4 = std::thread::Builder::new()
        .name("proxy".into())
        .spawn(move || {
            let ns = File::open("/var/run/netns/proxy").expect("failed to open proxy namespace");
            unsafe {
                libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET);
            }

            let mut prog = quilkin_xdp::EbpfProgram::load_program(
                EXTERNAL_PORT,
                QCMP_PORT,
                quilkin_xdp::PROGRAM_L2,
            )
            .expect("failed to load program");
            prog.load_into_kernel()
                .expect("the kernel rejected the program");

            let nic = xdp::nic::NicIndex::lookup_by_name(c"proxyvlan")
                .expect("failed to lookup NIC")
                .expect("failed to find expected nic");

            let umem = xdp::Umem::map(
                xdp::umem::UmemCfgBuilder {
                    frame_size: xdp::umem::FrameSize::TwoK,
                    frame_count: 64,
                    head_room: 20, // ipv4 -> ipv6
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
                let mut xsk =
                    aya::maps::XskMap::try_from(prog.bpf.map_mut("XSK").expect("no XSK map"))
                        .expect("XSK is not an xskmap");
                xsk.set(0, socket.as_raw_fd(), 0)
                    .expect("failed to insert socket into the XSK map");
            }

            let ip_to_mac = prog
                .bpf
                .take_map("IP_TO_MAC")
                .and_then(|i2m| aya::maps::RingBuf::try_from(i2m).ok())
                .expect("failed to get eBPF ring buffer");

            // Signal we are done
            spinup_tx.send(()).expect("failed to signal");

            const POLL_TIMEOUT: xdp::socket::PollTimeout =
                xdp::socket::PollTimeout::new(Some(std::time::Duration::from_millis(100)));

            use xdp::slab::Slab;
            const BATCH_SIZE: usize = 64;

            let mut rx_slab = xdp::slab::StackSlab::<BATCH_SIZE>::new();
            let mut tx_slab = xdp::slab::StackSlab::<{ BATCH_SIZE << 2 }>::new();
            let mut pending_sends = 0;
            let mut outstanding = umem.outstanding() as i64;

            let mut fill = rings.fill_ring;
            let mut rx = rings.rx_ring.unwrap();
            let mut tx = rings.tx_ring.unwrap();
            let mut completion = rings.completion_ring;

            let ipv4_proxy = NAMESPACES[1].1.unwrap();
            let ipv6_proxy = NAMESPACES[1].2.unwrap();

            let ipv4_server = NAMESPACES[2].1.unwrap();
            let ipv6_server = NAMESPACES[3].2.unwrap();

            // We aren't using a gateway otherwise this would be the gateway mac address
            let mut client_mac = nt::MacAddress([0; 6]);

            let (ctx, crx) = crossbeam_channel::bounded(64);
            let (cache, _jh) = quilkin_xdp::l2_cache::L2Cache::with_channels(vec![ctx], ip_to_mac)
                .expect("failed to initialize cache");

            let mut queued = Vec::new();

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
                        if error.raw_os_error() != Some(libc::EAGAIN) {}
                    }

                    while let Some(mut packet) = rx_slab.pop_back() {
                        let mut udp = nt::UdpHeaders::parse_packet(&mut packet)
                            .expect("failed to parse packet")
                            .expect("expected UDP packet");

                        match udp.udp.destination.host() {
                            EXTERNAL_PORT => {
                                client_mac = udp.eth.source;

                                let ips = if packet[udp.data.start] % 2 == 0 {
                                    udp.udp.source = 4444.into();
                                    udp.udp.destination = 4444.into();

                                    nt::IpAddresses::V4 {
                                        source: ipv4_proxy,
                                        destination: ipv4_server,
                                    }
                                } else {
                                    packet.adjust_head(20).unwrap();

                                    udp.udp.source = 6666.into();
                                    udp.udp.destination = 6666.into();

                                    nt::IpAddresses::V6 {
                                        source: ipv6_proxy,
                                        destination: ipv6_server,
                                    }
                                };

                                udp.ip = ips.with_header(&udp.ip);
                            }
                            4444 => {}
                            6666 => {
                                packet.adjust_head(-20).unwrap();
                            }
                            _ => unreachable!("unexpected UDP destination port"),
                        }

                        if udp.udp.destination.host() == 1111 {
                            udp.eth.source = udp.eth.destination;
                            udp.eth.destination = client_mac;
                        }

                        udp.set_packet_headers(&mut packet)
                            .expect("failed to set headers");
                        packet
                            .calc_udp_checksum()
                            .expect("failed to calculate UDP checksum");

                        if udp.udp.destination.host() == 1111 {
                            assert!(tx_slab.push_front(packet).is_none(), "tx slab was full");
                        } else if let Some(ll) = cache.mac_for_ip(udp.ip.destination_addr(), 0) {
                            let LinkLayerAddr::Known(mac) = ll else {
                                panic!(
                                    "we failed to find a mac address for {}",
                                    udp.ip.destination_addr()
                                );
                            };

                            udp.eth.source = udp.eth.destination;
                            udp.eth.destination = mac;

                            packet.write(0, udp.eth).expect("failed to write ethernet");
                            assert!(tx_slab.push_front(packet).is_none(), "tx slab was full");
                        } else {
                            queued.push((packet, udp));
                        }
                    }

                    while let Ok((ip, ll)) = crx.try_recv() {}

                    let before = tx_slab.len();
                    let enqueued_sends = match tx.send(&mut tx_slab, true) {
                        Ok(es) => es,
                        Err(error) => {
                            // EAGAIN means the wakeup wasn't delivered, but the packets are
                            // already enqueued in the ring, so the kernel will send them on
                            // the next successful wakeup or its own polling; not an error.
                            if error.raw_os_error() != Some(libc::EAGAIN) {}

                            before - tx_slab.len()
                        }
                    };

                    // Return frames that have completed sending
                    pending_sends += enqueued_sends;
                    pending_sends -= completion.dequeue(&mut umem, pending_sends);

                    let new = umem.outstanding() as i64;
                    outstanding = new;
                }
            }
        })
        .expect("failed to spawn client thread");

    let ipv4 = std::thread::Builder::new()
        .name("ipv4".into())
        .spawn(move || {
            let ns = File::open("/var/run/netns/ipv4").expect("failed to open ipv4 namespace");
            unsafe {
                libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET);
            }

            let sock =
                UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 4444)).expect("failed to bind ipv4 socket");

            // Signal we are done
            spinup_tx.send(()).expect("failed to signal");

            let mut packet = [0u8; PACKET_SIZE];

            loop {
                let (len, from) = sock.recv_from(&mut packet).expect("ipv4 failed to recv");

                let np = [packet[0].wrapping_add(44); PACKET_SIZE];

                assert_eq!(
                    sock.send_to(&np, from).expect("ipv4 failed to send"),
                    PACKET_SIZE
                );
            }
        })
        .expect("failed to spawn client thread");

    let ipv6 = std::thread::Builder::new()
        .name("ipv6".into())
        .spawn(move || {
            let ns = File::open("/var/run/netns/ipv6").expect("failed to open ipv6 namespace");
            unsafe {
                libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET);
            }

            let sock =
                UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 6666)).expect("failed to bind ipv6 socket");

            // Signal we are done
            spinup_tx.send(()).expect("failed to signal");
        })
        .expect("failed to spawn client thread");

    // Drop this so we don't spin forever waiting
    drop(spinup_tx);
}
