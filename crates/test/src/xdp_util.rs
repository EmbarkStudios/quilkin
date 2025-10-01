use quilkin::net::io::nic::xdp::process;
use xdp::{Packet, packet::net_types::UdpHeaders};

#[inline]
pub fn make_config(
    filters: quilkin::filters::FilterChain,
    endpoints: std::collections::BTreeSet<quilkin::net::Endpoint>,
) -> process::ConfigState {
    let cm = quilkin::net::ClusterMap::new();
    cm.insert(None, None, endpoints);

    let fc = quilkin::config::filter::FilterChainConfig::new(filters);
    process::ConfigState {
        filters: fc.cached(),
        clusters: quilkin::config::Watch::new(cm),
    }
}

pub fn default_xdp_state(
    cfg_state: process::ConfigState,
) -> (process::State, process::ConfigState) {
    (
        process::State {
            external_port: 7777.into(),
            qcmp_port: 0.into(),
            destinations: Vec::with_capacity(1),
            addr_to_asn: Default::default(),
            sessions: std::sync::Arc::new(Default::default()),
            local_ipv4: std::net::Ipv4Addr::new(1, 1, 1, 1),
            local_ipv6: std::net::Ipv6Addr::from_bits(u128::from_ne_bytes([1; 16])),
            last_receive: quilkin::time::UtcTimestamp::now(),
        },
        cfg_state,
    )
}

#[inline]
pub fn endpoints(
    eps: &[(std::net::SocketAddr, &[&[u8]])],
) -> std::collections::BTreeSet<quilkin::net::Endpoint> {
    eps.iter()
        .map(|(addr, tokens)| {
            quilkin::net::Endpoint::with_metadata(
                (*addr).into(),
                quilkin::net::endpoint::Metadata {
                    tokens: tokens.iter().map(|tok| tok.to_vec()).collect(),
                },
            )
        })
        .collect()
}

pub struct TestPacket {
    pub inner: Option<Packet>,
    pub udp_headers: xdp::packet::net_types::UdpHeaders,
    umem: *mut xdp::Umem,
}

impl TestPacket {
    pub fn payload(&self) -> &[u8] {
        let packet = self.inner.as_ref().unwrap();
        &packet[self.udp_headers.data]
    }
}

impl Drop for TestPacket {
    fn drop(&mut self) {
        if let Some(packet) = self.inner.take() {
            // SAFETY: test helper, the umem must outlive the packet
            (unsafe { &mut *self.umem }).free_packet(packet);
        }
    }
}

pub struct SimpleLoop {
    pub umem: xdp::Umem,
    pub state: process::State,
    pub cfg: process::ConfigState,
}

impl SimpleLoop {
    pub fn new(count: u32, state: process::State, cfg: process::ConfigState) -> Self {
        let umem = xdp::Umem::map(
            xdp::umem::UmemCfgBuilder {
                frame_size: xdp::umem::FrameSize::TwoK,
                head_room: 20,
                frame_count: count,
                ..Default::default()
            }
            .build()
            .unwrap(),
        )
        .unwrap();

        Self { umem, state, cfg }
    }

    pub fn make_client_packet(
        &mut self,
        source_ip: std::net::IpAddr,
        src_port: u16,
        payload: &[u8],
    ) -> Option<TestPacket> {
        // SAFETY: the packet we return should not outlive this loop
        let mut packet = unsafe { self.umem.alloc()? };

        let pb = etherparse::PacketBuilder::ethernet2([1; 6], [2; 6]);
        let pb = match source_ip {
            std::net::IpAddr::V4(v4) => pb.ipv4(v4.octets(), self.state.local_ipv4.octets(), 63),
            std::net::IpAddr::V6(v6) => pb.ipv6(v6.octets(), self.state.local_ipv6.octets(), 63),
        };

        pb.udp(src_port, self.state.external_port.host())
            .write(&mut packet, payload)
            .unwrap();
        let udp_headers = UdpHeaders::parse_packet(&packet).unwrap().unwrap();
        Some(TestPacket {
            inner: Some(packet),
            udp_headers,
            umem: &mut self.umem,
        })
    }

    pub fn make_server_packet(
        &mut self,
        source_ip: std::net::IpAddr,
        src: u16,
        dest: u16,
        payload: &[u8],
    ) -> Option<TestPacket> {
        // SAFETY: the packet we return should not outlive this loop
        let mut packet = unsafe { self.umem.alloc()? };

        let pb = etherparse::PacketBuilder::ethernet2([1; 6], [2; 6]);
        let pb = match source_ip {
            std::net::IpAddr::V4(v4) => pb.ipv4(v4.octets(), self.state.local_ipv4.octets(), 63),
            std::net::IpAddr::V6(v6) => pb.ipv6(v6.octets(), self.state.local_ipv6.octets(), 63),
        };

        pb.udp(src, dest).write(&mut packet, payload).unwrap();
        let udp_headers = UdpHeaders::parse_packet(&packet).unwrap().unwrap();
        Some(TestPacket {
            inner: Some(packet),
            udp_headers,
            umem: &mut self.umem,
        })
    }

    /// Roundtrips a packet
    ///
    /// Processes the specified inputs as a client (read) packet, and if a server
    /// is chosen, processes that one, returning the packet that would be sent
    /// back to the client, if any
    pub fn echo(
        &mut self,
        source_ip: std::net::IpAddr,
        src_port: u16,
        payload: &[u8],
    ) -> Option<TestPacket> {
        let client = self
            .make_client_packet(source_ip, src_port, payload)
            .unwrap();
        let (source, dest, payload) = {
            let to_server = self.process(client)?;
            (
                to_server.udp_headers.destination_address(),
                to_server.udp_headers.udp.source.host(),
                to_server.payload().to_vec(),
            )
        };

        let server = self
            .make_server_packet(source.ip(), source.port(), dest, &payload)
            .unwrap();
        self.process(server)
    }

    /// Runs the specified packet through
    pub fn process(&mut self, mut packet: TestPacket) -> Option<TestPacket> {
        use xdp::slab::Slab;

        let mut rx = xdp::slab::StackSlab::<1>::new();
        let mut tx = xdp::slab::StackSlab::<1>::new();

        rx.push_front(packet.inner.take().unwrap());

        process::process_packets(
            &mut rx,
            &mut self.umem,
            &mut tx,
            &mut self.cfg,
            &mut self.state,
        );

        let packet = tx.pop_back()?;
        let udp_headers = UdpHeaders::parse_packet(&packet)
            .expect("failed to parse packet")
            .expect("not a UDP packet");
        Some(TestPacket {
            inner: Some(packet),
            udp_headers,
            umem: &mut self.umem,
        })
    }

    pub fn process_multi<const N: usize>(
        &mut self,
        mut packet: TestPacket,
    ) -> [Option<TestPacket>; N] {
        use xdp::slab::Slab;

        let mut rx = xdp::slab::StackSlab::<1>::new();
        let mut tx = xdp::slab::StackSlab::<N>::new();

        rx.push_front(packet.inner.take().unwrap());

        process::process_packets(
            &mut rx,
            &mut self.umem,
            &mut tx,
            &mut self.cfg,
            &mut self.state,
        );

        let mut send = [const { None }; N];
        let mut i = 0;
        while let Some(packet) = tx.pop_back() {
            let udp_headers = xdp::packet::net_types::UdpHeaders::parse_packet(&packet)
                .expect("failed to parse packet")
                .expect("not a UDP packet");
            send[i] = Some(TestPacket {
                inner: Some(packet),
                udp_headers,
                umem: &mut self.umem,
            });
            i += 1;
        }

        send
    }
}
