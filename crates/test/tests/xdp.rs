#![cfg(target_os = "linux")]
#![allow(clippy::undocumented_unsafe_blocks)]

use qt::xdp_util::{endpoints, make_config};
use quilkin::{
    filters,
    net::io::nic::xdp::process::{
        self,
        xdp::{
            self,
            packet::net_types::{self as nt, UdpHeaders},
            slab::Slab,
        },
    },
    time::UtcTimestamp,
};
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::Arc,
};

type LittleSlab = xdp::slab::StackSlab<1>;

/// Validates we can do basic processing and forwarding of packets
#[tokio::test]
async fn simple_forwarding() {
    const SERVER: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 1111);
    const PROXY: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 7777);
    const CLIENT: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(5, 5, 5, 5), 8888);

    let mut cfg_state = make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                size: 1,
                remove: false,
            }),
            TokenRouter => None,
        ]),
        endpoints(&[(SERVER.into(), &[&[0xf0]])]),
    );

    let mut state = process::State {
        external_port: PROXY.port().into(),
        qcmp_port: 0.into(),
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions: Arc::new(Default::default()),
        local_ipv4: *PROXY.ip(),
        local_ipv6: Ipv6Addr::from_bits(0),
        last_receive: UtcTimestamp::now(),
    };

    let data = [0xf0u8; 11];

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 0,
            frame_count: 1,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    let mut client_packet = unsafe { umem.alloc().unwrap() };

    etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
        .ipv4(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
        .udp(CLIENT.port(), PROXY.port())
        .write(&mut client_packet, &data)
        .unwrap();

    let mut rx_slab = LittleSlab::new();
    rx_slab.push_front(client_packet);
    let mut tx_slab = LittleSlab::new();
    process::process_packets(
        &mut rx_slab,
        &mut umem,
        &mut tx_slab,
        &mut cfg_state,
        &mut state,
    );

    assert!(rx_slab.is_empty());
    let server_packet = tx_slab.pop_back().unwrap();

    let mut packet_headers = etherparse::PacketHeaders::from_ethernet_slice(&server_packet)
        .expect("failed to parse packet");

    let th = packet_headers.transport.as_mut().unwrap();
    let udp = th.mut_udp().unwrap();
    assert_eq!(
        udp.checksum,
        udp.calc_checksum_ipv4_raw(PROXY.ip().octets(), SERVER.ip().octets(), &data)
            .unwrap()
    );

    insta::assert_debug_snapshot!(&packet_headers);
}

/// Validates that we can change between ipv4 <-> ipv6 packets
#[tokio::test]
async fn changes_ip_version() {
    const SERVER: SocketAddrV6 =
        SocketAddrV6::new(Ipv6Addr::new(1, 1, 1, 1, 1, 1, 1, 1), 1111, 0, 0);
    const PROXY4: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 7777);
    const PROXY6: SocketAddrV6 =
        SocketAddrV6::new(Ipv6Addr::new(2, 2, 2, 2, 2, 2, 2, 2), 7777, 0, 0);
    const CLIENT: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(5, 5, 5, 5), 8888);

    let mut cfg_state = make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                size: 1,
                remove: false,
            }),
            TokenRouter => None,
        ]),
        endpoints(&[(SERVER.into(), &[&[0xf1]])]),
    );

    let mut state = process::State {
        external_port: PROXY4.port().into(),
        qcmp_port: 0.into(),
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions: Arc::new(Default::default()),
        local_ipv4: *PROXY4.ip(),
        local_ipv6: *PROXY6.ip(),
        last_receive: UtcTimestamp::now(),
    };

    let data = [0xf1u8; 11];

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 20,
            frame_count: 1,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    let mut rx_slab = LittleSlab::new();
    let mut tx_slab = LittleSlab::new();

    let port = {
        let mut client_packet = unsafe { umem.alloc().unwrap() };

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(CLIENT.ip().octets(), PROXY4.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY4.port())
            .write(&mut client_packet, &data)
            .unwrap();

        rx_slab.push_front(client_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        assert!(rx_slab.is_empty());
        let server_packet = tx_slab.pop_back().unwrap();

        let mut packet_headers = etherparse::PacketHeaders::from_ethernet_slice(&server_packet)
            .expect("failed to parse packet");

        let port = {
            let th = packet_headers.transport.as_mut().unwrap();
            let udp = th.mut_udp().unwrap();
            assert_eq!(
                udp.checksum,
                udp.calc_checksum_ipv6_raw(PROXY6.ip().octets(), SERVER.ip().octets(), &data)
                    .unwrap()
            );
            udp.source_port
        };

        insta::assert_debug_snapshot!(&packet_headers);
        umem.free_packet(server_packet);
        port
    };

    let mut server_packet = unsafe { umem.alloc().unwrap() };

    etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
        .ipv6(SERVER.ip().octets(), PROXY6.ip().octets(), 64)
        .udp(SERVER.port(), port)
        .write(&mut server_packet, &data)
        .unwrap();

    rx_slab.push_front(server_packet);
    process::process_packets(
        &mut rx_slab,
        &mut umem,
        &mut tx_slab,
        &mut cfg_state,
        &mut state,
    );

    assert!(rx_slab.is_empty());
    let client_packet = tx_slab.pop_back().unwrap();

    let mut packet_headers = etherparse::PacketHeaders::from_ethernet_slice(&client_packet)
        .expect("failed to parse packet");

    let th = packet_headers.transport.as_mut().unwrap();
    let udp = th.mut_udp().unwrap();
    assert_eq!(
        udp.checksum,
        udp.calc_checksum_ipv4_raw(PROXY4.ip().octets(), CLIENT.ip().octets(), &data)
            .unwrap()
    );

    insta::assert_debug_snapshot!(&packet_headers);
}

/// Validates we can do both removal and concatenation to the packet buffer
#[tokio::test]
async fn packet_manipulation() {
    const SERVER: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 1111);
    const PROXY: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 7777);
    const CLIENT: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(5, 5, 5, 5), 8888);

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 20,
            frame_count: 1,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    let mut rx_slab = LittleSlab::new();
    let mut tx_slab = LittleSlab::new();

    // Test suffix removal
    {
        let mut cfg_state = make_config(
            qt::filter_chain!([
                Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                    size: 1,
                    remove: true,
                }),
                TokenRouter => None,
            ]),
            endpoints(&[(SERVER.into(), &[&[0xf1]])]),
        );

        let mut state = process::State {
            external_port: PROXY.port().into(),
            qcmp_port: 0.into(),
            destinations: Vec::with_capacity(1),
            addr_to_asn: Default::default(),
            sessions: Arc::new(Default::default()),
            local_ipv4: *PROXY.ip(),
            local_ipv6: Ipv6Addr::from_bits(0),
            last_receive: UtcTimestamp::now(),
        };

        let data = [0xf1u8; 11];
        let mut len = data.len();

        while len > 0 {
            let mut client_packet = unsafe { umem.alloc().unwrap() };

            etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
                .ipv4(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
                .udp(CLIENT.port(), PROXY.port())
                .write(&mut client_packet, &data[..len])
                .unwrap();

            rx_slab.push_front(client_packet);
            process::process_packets(
                &mut rx_slab,
                &mut umem,
                &mut tx_slab,
                &mut cfg_state,
                &mut state,
            );

            assert!(rx_slab.is_empty());
            let server_packet = tx_slab.pop_back().unwrap();

            let udp = UdpHeaders::parse_packet(&server_packet).unwrap().unwrap();
            len -= 1;
            assert_eq!(&server_packet[udp.data], &data[..len]);

            umem.free_packet(server_packet);
        }
    }

    // Test prefix removal
    {
        let mut cfg_state = make_config(
            qt::filter_chain!([
                Capture => filters::capture::Config::with_strategy(filters::capture::Prefix {
                    size: 1,
                    remove: true,
                }),
                TokenRouter => None,
            ]),
            endpoints(&[(SERVER.into(), &[&[0xf1]])]),
        );

        let mut state = process::State {
            external_port: PROXY.port().into(),
            qcmp_port: 0.into(),
            destinations: Vec::with_capacity(1),
            addr_to_asn: Default::default(),
            sessions: Arc::new(Default::default()),
            local_ipv4: *PROXY.ip(),
            local_ipv6: Ipv6Addr::from_bits(0),
            last_receive: UtcTimestamp::now(),
        };

        let data = [0xf1u8; 11];
        let mut len = data.len();

        while len > 0 {
            let mut client_packet = unsafe { umem.alloc().unwrap() };

            etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
                .ipv4(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
                .udp(CLIENT.port(), PROXY.port())
                .write(&mut client_packet, &data[..len])
                .unwrap();

            rx_slab.push_front(client_packet);
            process::process_packets(
                &mut rx_slab,
                &mut umem,
                &mut tx_slab,
                &mut cfg_state,
                &mut state,
            );

            assert!(rx_slab.is_empty());
            let server_packet = tx_slab.pop_back().unwrap();

            let udp = UdpHeaders::parse_packet(&server_packet).unwrap().unwrap();
            len -= 1;
            assert_eq!(&server_packet[udp.data], &data[..len]);

            umem.free_packet(server_packet);
        }
    }

    // Test suffix removal, combined with read append and write prepend
    {
        let concat_data = [0xff; 11];
        let data = [0xf1u8; 20];
        let mut cfg_state = make_config(
            qt::filter_chain!([
                Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                    size: 18,
                    remove: true,
                }),
                TokenRouter => None,
                Concatenate => filters::concatenate::Config {
                    on_read: filters::concatenate::Strategy::Append,
                    on_write: filters::concatenate::Strategy::Prepend,
                    bytes: concat_data.to_vec(),
                },
            ]),
            endpoints(&[(SERVER.into(), &[&data[..data.len() - 2]])]),
        );

        let mut state = process::State {
            external_port: PROXY.port().into(),
            qcmp_port: 0.into(),
            destinations: Vec::with_capacity(1),
            addr_to_asn: Default::default(),
            sessions: Arc::new(Default::default()),
            local_ipv4: *PROXY.ip(),
            local_ipv6: Ipv6Addr::from_bits(0),
            last_receive: UtcTimestamp::now(),
        };

        let mut client_packet = unsafe { umem.alloc().unwrap() };

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY.port())
            .write(&mut client_packet, &data)
            .unwrap();

        rx_slab.push_front(client_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        let server_packet = tx_slab.pop_back().unwrap();

        let udp = UdpHeaders::parse_packet(&server_packet).unwrap().unwrap();
        let pdata = server_packet[udp.data].to_vec();
        assert_eq!(&pdata[..2], &data[..2]);
        assert_eq!(&pdata[2..], &concat_data,);

        umem.free_packet(server_packet);
        let mut server_packet = unsafe { umem.alloc().unwrap() };
        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(SERVER.ip().octets(), PROXY.ip().octets(), 64)
            .udp(SERVER.port(), udp.udp.source.host())
            .write(&mut server_packet, &pdata)
            .unwrap();

        rx_slab.push_front(server_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );
        let server_packet = tx_slab.pop_back().unwrap();

        let udp = UdpHeaders::parse_packet(&server_packet).unwrap().unwrap();
        let pdata = &server_packet[udp.data];
        assert_eq!(&pdata[..concat_data.len()], &concat_data,);
        assert_eq!(&pdata[concat_data.len()..concat_data.len() + 2], &data[..2]);
        assert_eq!(&pdata[concat_data.len() + 2..], &concat_data,);
    }
}

/// Ensures that client packets can get routed to multiple servers
#[tokio::test]
async fn multiple_servers() {
    const PROXY: SocketAddrV6 =
        SocketAddrV6::new(Ipv6Addr::new(2, 2, 2, 2, 2, 2, 2, 2), 7777, 0, 0);
    const CLIENT: SocketAddrV6 =
        SocketAddrV6::new(Ipv6Addr::new(5, 5, 5, 5, 5, 5, 5, 5), 8888, 0, 0);

    const COUNT: usize = 32;

    let mut servers: Vec<SocketAddr> = (0..COUNT as u16)
        .map(|i| {
            if i % 2 == 0 {
                (Ipv6Addr::new(i, i, i, i, i, i, i, i), 1000 + i).into()
            } else {
                (Ipv4Addr::from_bits(i as u32), 2000 + i).into()
            }
        })
        .collect();
    let tok = [0xf1u8];
    let tok = [&tok[..]];

    let mut cfg_state = make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Prefix {
                size: 1,
                remove: false,
            }),
            TokenRouter => None,
        ]),
        endpoints(
            servers
                .iter()
                .map(|a| (*a, &tok[..]))
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    );

    let mut state = process::State {
        external_port: PROXY.port().into(),
        qcmp_port: 0.into(),
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions: Arc::new(Default::default()),
        local_ipv4: Ipv4Addr::from_bits(0),
        local_ipv6: *PROXY.ip(),
        last_receive: UtcTimestamp::now(),
    };

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 20,
            frame_count: COUNT as u32,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    let mut rx_slab = LittleSlab::new();
    let mut tx_slab = xdp::slab::StackSlab::<COUNT>::new();

    let mut client_packet = unsafe { umem.alloc().unwrap() };

    etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
        .ipv6(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
        .udp(CLIENT.port(), PROXY.port())
        .write(&mut client_packet, tok[0])
        .unwrap();

    rx_slab.push_front(client_packet);
    process::process_packets(
        &mut rx_slab,
        &mut umem,
        &mut tx_slab,
        &mut cfg_state,
        &mut state,
    );

    while let Some(sp) = tx_slab.pop_back() {
        let udp = UdpHeaders::parse_packet(&sp).unwrap().unwrap();

        let dip = udp.destination_address().ip();

        servers.remove(servers.iter().position(|s| s.ip() == dip).unwrap());
    }

    assert!(servers.is_empty());
}

/// Ensures that surpassing the session limits doesn't completely break
#[tokio::test]
async fn many_sessions() {
    const SERVER: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 1111);
    const PROXY: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 7777);

    let mut cfg_state = make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                size: 1,
                remove: false,
            }),
            TokenRouter => None,
        ]),
        endpoints(&[(SERVER.into(), &[&[0xf0]])]),
    );

    let mut state = process::State {
        external_port: PROXY.port().into(),
        qcmp_port: 0.into(),
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions: Arc::new(Default::default()),
        local_ipv4: *PROXY.ip(),
        local_ipv6: Ipv6Addr::from_bits(0),
        last_receive: UtcTimestamp::now(),
    };

    let data = [0xf0u8; 11];

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 0,
            frame_count: 1,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    fn swap(packet: &mut xdp::Packet) {
        let udp = UdpHeaders::parse_packet(packet).unwrap().unwrap();

        let mut new = UdpHeaders {
            eth: udp.eth.swapped(),
            ip: udp.ip.swapped(),
            udp: udp.udp.swapped(),
            data: udp.data,
        };

        new.set_packet_headers(packet).unwrap();
        packet.calc_udp_checksum().unwrap();
    }

    let mut rx_slab = LittleSlab::new();
    let mut tx_slab = LittleSlab::new();
    for i in 1..10000u32 {
        let mut client_packet = unsafe { umem.alloc().unwrap() };

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(i.to_ne_bytes(), PROXY.ip().octets(), 64)
            .udp(i as u16, PROXY.port())
            .write(&mut client_packet, &data)
            .unwrap();

        rx_slab.push_front(client_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        let mut server_packet = tx_slab.pop_back().unwrap();

        swap(&mut server_packet);

        rx_slab.push_front(server_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        let client_packet = tx_slab.pop_back().unwrap();

        let udp = UdpHeaders::parse_packet(&client_packet).unwrap().unwrap();

        assert_eq!(&client_packet[udp.data], &data);
        assert_eq!(udp.eth.destination.0, [3; 6]);
        assert_eq!(udp.udp.destination.host(), i as u16);
        assert_eq!(udp.eth.source.0, [4; 6]);
        assert_eq!(udp.udp.source.host(), PROXY.port());
        assert_eq!(
            udp.ip,
            nt::IpAddresses::V4 {
                source: *PROXY.ip(),
                destination: Ipv4Addr::from_bits(i.to_be())
            }
        );

        umem.free_packet(client_packet);
    }
}

/// Ensures that we free packets back to the umem when we drop packets instead of forward them
#[tokio::test]
async fn frees_dropped_packets() {
    const SERVER: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 1111);
    const PROXY4: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 7777);
    const PROXY6: SocketAddrV6 =
        SocketAddrV6::new(Ipv6Addr::new(2, 2, 2, 2, 2, 2, 2, 2), 7777, 0, 0);
    const CLIENT: SocketAddrV6 =
        SocketAddrV6::new(Ipv6Addr::new(1, 2, 3, 4, 5, 6, 7, 8), 9999, 0, 0);

    let mut cfg_state = make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                size: 1,
                remove: false,
            }),
            TokenRouter => None,
        ]),
        endpoints(&[(SERVER.into(), &[&[0xf0]])]),
    );

    let mut state = process::State {
        external_port: PROXY4.port().into(),
        qcmp_port: 0.into(),
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions: Arc::new(Default::default()),
        local_ipv4: *PROXY4.ip(),
        local_ipv6: *PROXY6.ip(),
        last_receive: UtcTimestamp::now(),
    };

    let data = [0xf0u8; 11];

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 0,
            frame_count: 1,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    let mut rx_slab = LittleSlab::new();
    let mut tx_slab = LittleSlab::new();

    // sanity check the umem won't allow more than 1 packet at a time
    unsafe {
        let first = umem.alloc().unwrap();
        assert!(umem.alloc().is_none());
        umem.free_packet(first);
    };

    // Client packet that doesn't have a token
    {
        let mut client_packet = unsafe { umem.alloc().unwrap() };

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv6(CLIENT.ip().octets(), PROXY6.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY6.port())
            .write(&mut client_packet, &[1])
            .unwrap();

        rx_slab.push_front(client_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        assert!(tx_slab.is_empty());
    }

    // Valid client packet
    {
        // If this fails, the dropped packet wasn't freed
        let mut client_packet = unsafe { umem.alloc().expect("umem has no available packets") };

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv6(CLIENT.ip().octets(), PROXY6.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY6.port())
            .write(&mut client_packet, &data)
            .unwrap();

        rx_slab.push_front(client_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        let server_packet = tx_slab.pop_back().unwrap();
        umem.free_packet(server_packet);
    }

    // Server packet that doesn't have a session
    {
        let mut server_packet = unsafe { umem.alloc().expect("umem has no available packets") };

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(SERVER.ip().octets(), PROXY4.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY4.port())
            .write(&mut server_packet, &[1, 2, 3])
            .unwrap();

        rx_slab.push_front(server_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        assert!(tx_slab.is_empty());
        unsafe { umem.alloc().expect("umem should have available memory") };
    }
}

/// Validates we can process QCMP packets with the same loop as regular packets
#[tokio::test]
async fn qcmp() {
    use quilkin::{codec::qcmp, time::UtcTimestamp};

    const PROXY: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), 2020);
    const CLIENT: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 9999);

    let mut cfg_state = make_config(filters::FilterChain::default(), endpoints(&[]));

    let mut state = process::State {
        external_port: 7777.into(),
        qcmp_port: PROXY.port().into(),
        destinations: Vec::with_capacity(1),
        addr_to_asn: Default::default(),
        sessions: Arc::new(Default::default()),
        local_ipv4: *PROXY.ip(),
        local_ipv6: Ipv6Addr::from_bits(0),
        last_receive: UtcTimestamp::now(),
    };

    let mut umem = xdp::Umem::map(
        xdp::umem::UmemCfgBuilder {
            frame_size: xdp::umem::FrameSize::TwoK,
            head_room: 0,
            frame_count: 1,
            ..Default::default()
        }
        .build()
        .unwrap(),
    )
    .unwrap();

    let mut rx_slab = LittleSlab::new();
    let mut tx_slab = LittleSlab::new();

    // sanity check the umem won't allow more than 1 packet at a time
    unsafe {
        let first = umem.alloc().unwrap();
        assert!(umem.alloc().is_none());
        umem.free_packet(first);
    };

    let mut qp = qcmp::QcmpPacket::default();

    let ping_time = UtcTimestamp::from_nanos(100000);

    // Valid ping packet
    {
        // If this fails, the dropped packet wasn't freed
        let mut ping_packet = unsafe { umem.alloc().expect("umem has no available packets") };

        let ping = qcmp::Protocol::Ping {
            client_timestamp: ping_time,
            nonce: 99,
        };

        ping.encode(&mut qp);

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY.port())
            .write(&mut ping_packet, &qp)
            .unwrap();

        rx_slab.push_front(ping_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        let pong_packet = tx_slab.pop_back().unwrap();
        let udp = UdpHeaders::parse_packet(&pong_packet).unwrap().unwrap();
        let pong = qcmp::Protocol::parse(&pong_packet[udp.data])
            .unwrap()
            .unwrap();

        match pong {
            qcmp::Protocol::PingReply {
                client_timestamp,
                nonce,
                ..
            } => {
                assert_eq!(ping_time, client_timestamp);
                assert_eq!(nonce, 99);
            }
            qcmp::Protocol::Ping { .. } => unreachable!(),
        }

        umem.free_packet(pong_packet);
    }

    // A pong packet, should be rejected
    {
        let mut bad_packet = unsafe { umem.alloc().expect("umem has no available packets") };

        let pong = qcmp::Protocol::PingReply {
            client_timestamp: ping_time,
            nonce: 200,
            server_start_timestamp: UtcTimestamp::from_nanos(100001),
            server_transmit_timestamp: UtcTimestamp::from_nanos(100002),
        };
        pong.encode(&mut qp);

        etherparse::PacketBuilder::ethernet2([3, 3, 3, 3, 3, 3], [4, 4, 4, 4, 4, 4])
            .ipv4(CLIENT.ip().octets(), PROXY.ip().octets(), 64)
            .udp(CLIENT.port(), PROXY.port())
            .write(&mut bad_packet, &qp)
            .unwrap();

        rx_slab.push_front(bad_packet);
        process::process_packets(
            &mut rx_slab,
            &mut umem,
            &mut tx_slab,
            &mut cfg_state,
            &mut state,
        );

        assert!(tx_slab.is_empty());
        unsafe { umem.alloc().expect("umem should have available memory") };
    }
}
