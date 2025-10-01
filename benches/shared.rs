#![allow(dead_code)]

pub use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{Arc, mpsc},
};

pub const READ_QUILKIN_PORT: u16 = 9001;
pub const WRITE_QUILKIN_PORT: u16 = 9002;

pub const NUMBER_OF_PACKETS: u16 = 10_000;

pub const PACKET_SIZES: &[usize] = &[
    254,  // Half IPv4 MTU.
    508,  // IPv4 MTU.
    1500, // Ethernet MTU.
];

pub fn make_socket(addr: SocketAddr) -> UdpSocket {
    let socket = UdpSocket::bind(addr).expect("failed to bind");
    // socket
    //     .set_read_timeout(Some(std::time::Duration::from_millis(1)))
    //     .expect("failed to set read timeout");
    socket
        .set_nonblocking(true)
        .expect("failed to set non-blocking");
    socket
}

#[inline]
pub fn spawn<F>(name: impl Into<String>, func: F) -> std::thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    std::thread::Builder::new()
        .name(name.into())
        .spawn(func)
        .unwrap()
}

#[derive(Debug)]
pub enum ReadLoopMsg {
    #[allow(dead_code)]
    Blocked(PacketStats),
    Acked(PacketStats),
    Finished(PacketStats),
}

#[derive(Debug)]
pub struct PacketStats {
    /// Number of individual receives that were completed
    pub num_packets: u16,
    /// Total number of bytes received
    pub size_packets: usize,
}

#[inline]
pub fn channel() -> (mpsc::Sender<ReadLoopMsg>, mpsc::Receiver<ReadLoopMsg>) {
    mpsc::channel()
}

#[inline]
pub fn socket_pair(write: Option<u16>, read: Option<u16>) -> (UdpSocket, UdpSocket) {
    let w = make_socket((Ipv4Addr::LOCALHOST, write.unwrap_or_default()).into());
    let r = make_socket((Ipv4Addr::LOCALHOST, read.unwrap_or_default()).into());

    (w, r)
}

/// Writes never block even if the kernel's ring buffer is full, so we occasionally
/// ack chunks so the writer isn't waiting until the reader is blocked due to
/// ring buffer exhaustion in case
const CHUNK_SIZE: usize = 8 * 1024;

const fn batch_size(packet_size: usize) -> u16 {
    (CHUNK_SIZE / packet_size) as u16
}

/// Runs a loop, reading from the socket until all the expected number of bytes (based on packet count and size)
/// have been successfully received.
///
/// If the recv would block, a message is sent to request more bytes be sent to the socket,
/// we do this because while recv will fail if the timeout is surpassed and there is no
/// data to read, send (at least on linux) will never block on loopback even if there
/// not enough room in the ring buffer to hold the specified bytes
pub fn read_to_end<const N: usize>(
    socket: &UdpSocket,
    tx: &mpsc::Sender<ReadLoopMsg>,
    packet_count: u16,
) {
    let mut packet = [0; N];

    let mut num_packets = 0;
    let mut size_packets = 0;

    let expected = packet_count as usize * N;

    let batch_size = batch_size(N);

    struct Batch {
        received: usize,
        range: std::ops::Range<u16>,
    }

    let mut batch_i = 0u16;
    let mut batch_range = || -> std::ops::Range<u16> {
        let start = batch_size * batch_i;

        if start > packet_count {
            return 0..0;
        }

        batch_i += 1;
        start..(start + batch_size).min(packet_count)
    };

    // We can have a max of 2 batches in flight at a time
    let mut batches = [
        Batch {
            received: 0,
            range: batch_range(),
        },
        Batch {
            received: 0,
            range: batch_range(),
        },
    ];

    while size_packets < expected {
        let length = match socket.recv_from(&mut packet) {
            Ok(t) => t.0,
            Err(ref err) if matches!(err.kind(), std::io::ErrorKind::WouldBlock) => {
                continue;
            }
            Err(err) => panic!("failed waiting for packet: {err}"),
        };

        assert_eq!(length, N);

        {
            let seq = ((packet[1] as u16) << 8) | packet[0] as u16;

            let batch = batches.iter_mut().find(|b| b.range.contains(&seq)).unwrap();

            batch.received += 1;
            if batch.received == batch.range.len() {
                batch.received = 0;
                batch.range = batch_range();

                if tx
                    .send(ReadLoopMsg::Acked(PacketStats {
                        num_packets,
                        size_packets,
                    }))
                    .is_err()
                {
                    return;
                }
            }
        }

        num_packets += 1;
        size_packets += length;
    }

    match socket.recv_from(&mut packet) {
        Ok(t) => panic!("writer sent more data than was intended: {t:?}"),
        Err(ref err) if matches!(err.kind(), std::io::ErrorKind::WouldBlock) => {
            let _ = tx.send(ReadLoopMsg::Finished(PacketStats {
                num_packets,
                size_packets,
            }));
        }
        Err(err) => panic!("failed waiting for packet: {err}"),
    }
}

pub struct Writer<const N: usize> {
    socket: UdpSocket,
    destination: SocketAddr,
    rx: mpsc::Receiver<ReadLoopMsg>,
}

impl<const N: usize> Writer<N> {
    pub fn new(
        socket: UdpSocket,
        destination: SocketAddr,
        rx: mpsc::Receiver<ReadLoopMsg>,
    ) -> Self {
        Self {
            socket,
            destination,
            rx,
        }
    }

    /// Waits until a write is received by the specified socket
    pub fn wait_ready(&self, quilkin: QuilkinLoop, reader: &UdpSocket) -> QuilkinLoop {
        const MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

        let start = std::time::Instant::now();

        let send_packet = [0xaa; 1];
        let mut recv_packet = [0x00; 1];

        // Temporarily make the socket blocking
        reader.set_nonblocking(false).unwrap();
        reader
            .set_read_timeout(Some(std::time::Duration::from_millis(10)))
            .unwrap();

        while start.elapsed() < MAX_WAIT {
            self.socket.send_to(&send_packet, self.destination).unwrap();

            match reader.recv_from(&mut recv_packet) {
                Ok(_) => {
                    assert_eq!(send_packet, recv_packet);

                    // Drain until block just in case
                    loop {
                        match reader.recv_from(&mut recv_packet) {
                            Ok(_) => {}
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                reader.set_nonblocking(true).unwrap();
                                reader.set_read_timeout(None).unwrap();
                                return quilkin;
                            }
                            Err(err) => {
                                panic!("failed to drain read socket: {err:?}");
                            }
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    println!("debugger might have attached");
                }
                Err(err) => {
                    panic!("failed to wait on read socket: {err:?}");
                }
            }
        }

        panic!("waited for {MAX_WAIT:?} for quilkin");
    }

    pub fn write_all(&self, packet_count: u16) -> bool {
        let batch_size = batch_size(N);

        let mut packet_buf = [0xffu8; N];

        let mut send_batch = |sent: u16| -> u16 {
            let to_send = (packet_count - sent).min(batch_size);

            for seq in sent..sent + to_send {
                let b = seq.to_ne_bytes();
                packet_buf[0] = b[0];
                packet_buf[1] = b[1];

                self.socket.send_to(&packet_buf, self.destination).unwrap();
            }

            to_send
        };

        let mut sent_packets = 0;

        // Queue 2 batches at the beginning, giving the reader enough work to do
        // after the initial batch has been read
        sent_packets += send_batch(sent_packets);
        sent_packets += send_batch(sent_packets);

        let mut finished = false;
        while let Ok(msg) = self.rx.recv() {
            match msg {
                ReadLoopMsg::Blocked(ps) => {
                    panic!("reader blocked {ps:?}");
                }
                ReadLoopMsg::Acked(ps) => {
                    if sent_packets < packet_count {
                        assert!(sent_packets > ps.num_packets);
                        sent_packets += send_batch(sent_packets);
                    }
                }
                ReadLoopMsg::Finished(ps) => {
                    assert_eq!(sent_packets, ps.num_packets);
                    assert_eq!(ps.size_packets, N * packet_count as usize);
                    finished = true;
                    break;
                }
            }
        }

        finished
    }
}

pub struct QuilkinLoop {
    shutdown: Option<quilkin::signal::ShutdownTx>,
    thread: Option<std::thread::JoinHandle<()>>,
    port: u16,
    endpoint: SocketAddr,
}

impl QuilkinLoop {
    /// Run and instance of quilkin that sends and receives data from the given address.
    pub fn spinup(port: u16, endpoint: SocketAddr) -> Self {
        Self::spinup_inner(port, endpoint)
    }

    #[allow(dead_code)]
    fn reinit(self) -> Self {
        let port = self.port;
        let endpoint = self.endpoint;

        drop(self);

        Self::spinup_inner(port, endpoint)
    }

    fn spinup_inner(port: u16, endpoint: SocketAddr) -> Self {
        let (t, r) = quilkin::signal::channel();
        let shutdown = quilkin::signal::ShutdownHandler::new(t, r);
        let shutdown_tx = shutdown.shutdown_tx();

        let thread = spawn("quilkin", move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let providers = quilkin::Providers::default();
            let service = quilkin::Service::default();
            let config = Arc::new(quilkin::Config::new(
                None,
                Default::default(),
                &providers,
                &service,
            ));
            config.dyn_cfg.clusters().unwrap().modify(|clusters| {
                clusters.insert_default(
                    [quilkin::net::endpoint::Endpoint::new(endpoint.into())].into(),
                );
            });

            let proxy = quilkin::Service::builder()
                .udp()
                .udp_port(port)
                .qcmp()
                .qcmp_port(
                    runtime
                        .block_on(quilkin::test::available_addr(
                            quilkin::test::AddressType::Random,
                        ))
                        .port(),
                );

            runtime.block_on(async move {
                proxy
                    .spawn_services(&config, shutdown)
                    .unwrap()
                    .await
                    .unwrap()
                    .1
                    .unwrap();
            });
        });

        Self {
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
            port,
            endpoint,
        }
    }
}

impl Drop for QuilkinLoop {
    fn drop(&mut self) {
        let Some(stx) = self.shutdown.take() else {
            return;
        };
        stx.send(()).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}

use quilkin::net::{Endpoint, EndpointAddress, cluster::ClusterMap, endpoint::Locality};
use rand::Rng;
use std::{
    collections::BTreeSet,
    hash::{Hash as _, Hasher as _},
    net::Ipv6Addr,
};
use xxhash_rust::xxh3::Xxh3 as Hasher;

pub const LOCALITIES: &[&str] = &[
    "us:east1:b",
    "us:east1:c",
    "us:east1:d",
    "us:east4:c",
    "us:east4:b",
    "us:east4:a",
    "us:central1:c",
    "us:central1:a",
    "us:central1:f",
    "us:central1:b",
    "us:west1:b",
    "us:west1:c",
    "us:west1:a",
    "europe:west4:a",
    "europe:west4:b",
    "europe:west4:c",
    "europe:west1:b",
    "europe:west1:d",
    "europe:west1:c",
    "europe:west3:c",
    "europe:west3:a",
    "europe:west3:b",
    "europe:west2:c",
    "europe:west2:b",
    "europe:west2:a",
    "asia:east1:b",
    "asia:east1:a",
    "asia:east1:c",
    "asia:southeast1:b",
    "asia:southeast1:a",
    "asia:southeast1:c",
    "asia:northeast1:b",
    "asia:northeast1:c",
    "asia:northeast1:a",
    "asia:south1:c",
    "asia:south1:b",
    "asia:south1:a",
    "australia:southeast1:b",
    "australia:southeast1:c",
    "australia:southeast1:a",
    "southamerica:east1:b",
    "southamerica:east1:c",
    "southamerica:east1:a",
    "asia:east2:a",
    "asia:east2:b",
    "asia:east2:c",
    "asia:northeast2:a",
    "asia:northeast2:b",
    "asia:northeast2:c",
    "asia:northeast3:a",
    "asia:northeast3:b",
    "asia:northeast3:c",
    "asia:south2:a",
    "asia:south2:b",
    "asia:south2:c",
    "asia:southeast2:a",
    "asia:southeast2:b",
    "asia:southeast2:c",
    "australia:southeast2:a",
    "australia:southeast2:b",
    "australia:southeast2:c",
    "europe:central2:a",
    "europe:central2:b",
    "europe:central2:c",
    "europe:north1:a",
    "europe:north1:b",
    "europe:north1:c",
    "europe:southwest1:a",
    "europe:southwest1:b",
    "europe:southwest1:c",
    "europe:west10:a",
    "europe:west10:b",
    "europe:west10:c",
    "europe:west12:a",
    "europe:west12:b",
    "europe:west12:c",
    "europe:west6:a",
    "europe:west6:b",
    "europe:west6:c",
    "europe:west8:a",
    "europe:west8:b",
    "europe:west8:c",
    "europe:west9:a",
    "europe:west9:b",
    "europe:west9:c",
    "me:central1:a",
    "me:central1:b",
    "me:central1:c",
    "me:central2:a",
    "me:central2:b",
    "me:central2:c",
    "me:west1:a",
    "me:west1:b",
    "me:west1:c",
    "northamerica:northeast1:a",
    "northamerica:northeast1:b",
    "northamerica:northeast1:c",
    "northamerica:northeast2:a",
    "northamerica:northeast2:b",
    "northamerica:northeast2:c",
    "southamerica:west1:a",
    "southamerica:west1:b",
    "southamerica:west1:c",
    "us:east5:a",
    "us:east5:b",
    "us:east5:c",
    "us:south1:a",
    "us:south1:b",
    "us:south1:c",
    "us:west2:a",
    "us:west2:b",
    "us:west2:c",
    "us:west3:a",
    "us:west3:b",
    "us:west3:c",
    "us:west4:a",
    "us:west4:b",
    "us:west4:c",
];

pub fn gen_endpoints(
    rng: &mut rand::rngs::SmallRng,
    hasher: &mut Hasher,
    mut tg: Option<&mut TokenGenerator>,
) -> BTreeSet<Endpoint> {
    let num_endpoints = rng.random_range(100..10_000);
    hasher.write_u16(num_endpoints);

    let mut endpoints = BTreeSet::new();
    if let Some(tg) = &mut tg {
        if let Some(prev) = &mut tg.previous {
            prev.clear();
        }
    }

    for i in 0..num_endpoints {
        let ep_addr = match i % 3 {
            0 => (Ipv4Addr::new(100, 200, (i >> 8) as _, (i & 0xff) as _), i).into(),
            1 => EndpointAddress {
                host: quilkin::net::endpoint::AddressKind::Name(format!("benchmark-{i}")),
                port: i,
            },
            2 => (Ipv6Addr::new(100, 200, i, 0, 0, 1, 2, 3), i).into(),
            _ => unreachable!(),
        };

        let ep = if let Some(tg) = &mut tg {
            let set = tg.next().unwrap();

            Endpoint::with_metadata(
                ep_addr,
                quilkin::net::endpoint::EndpointMetadata::new(quilkin::net::endpoint::Metadata {
                    tokens: set,
                }),
            )
        } else {
            Endpoint::new(ep_addr)
        };

        endpoints.insert(ep);
    }

    for ep in &endpoints {
        ep.address.hash(hasher);
    }

    endpoints
}

#[allow(dead_code)]
pub struct GenCluster {
    pub cm: ClusterMap,
    hash: u64,
    pub total_endpoints: usize,
    sets: std::collections::BTreeMap<Option<Locality>, BTreeSet<Endpoint>>,
}

#[inline]
fn write_locality(hasher: &mut Hasher, loc: &Option<Locality>) {
    if let Some(key) = loc {
        key.hash(hasher);
    } else {
        hasher.write(b"None");
    }
}

pub enum TokenKind {
    None,
    Single {
        duplicates: bool,
    },
    Multi {
        range: std::ops::Range<usize>,
        duplicates: bool,
    },
}

impl std::str::FromStr for TokenKind {
    type Err = eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let dupes = |s: &str| match s {
            "duplicates" => Ok(true),
            "unique" => Ok(false),
            _ => eyre::bail!("must be `duplicates` or `unique`"),
        };

        if let Some(rest) = s.strip_prefix("single:") {
            Ok(Self::Single {
                duplicates: dupes(rest)?,
            })
        } else if let Some(rest) = s.strip_prefix("multi:") {
            let (r, rest) = rest
                .split_once(':')
                .ok_or_else(|| eyre::format_err!("multi must specify 'range:duplicates'"))?;

            let (start, end) = r
                .split_once("..")
                .ok_or_else(|| eyre::format_err!("range must be specified as '<start>..<end>'"))?;

            let range = start.parse()?..end.parse()?;

            Ok(Self::Multi {
                range,
                duplicates: dupes(rest)?,
            })
        } else {
            eyre::bail!("unknown token kind");
        }
    }
}

pub struct TokenGenerator {
    rng: rand::rngs::SmallRng,
    previous: Option<Vec<Vec<u8>>>,
    range: Option<std::ops::Range<usize>>,
}

impl Iterator for TokenGenerator {
    type Item = quilkin_types::TokenSet;

    fn next(&mut self) -> Option<Self::Item> {
        use rand::RngCore;
        let mut set = std::collections::BTreeSet::new();

        let count = if let Some(range) = self.range.clone() {
            self.rng.random_range(range)
        } else {
            1
        };

        if let Some(prev) = &mut self.previous {
            for _ in 0..count {
                if !prev.is_empty() && self.rng.random_ratio(1, 10) {
                    let prev = &prev[self.rng.random_range(0..prev.len())];
                    set.insert(prev.clone());
                } else {
                    let count = self.rng.random_range(4..20);
                    let mut v = vec![0; count];
                    self.rng.fill_bytes(&mut v);
                    prev.push(v.clone());
                    set.insert(v);
                }
            }
        } else {
            for _ in 0..count {
                let count = self.rng.random_range(4..20);
                let mut v = vec![0; count];
                self.rng.fill_bytes(&mut v);
                set.insert(v);
            }
        }

        Some(quilkin_types::TokenSet(set))
    }
}

pub fn gen_cluster_map<const S: u64>(token_kind: TokenKind) -> GenCluster {
    use rand::prelude::*;

    let mut rng = rand::rngs::SmallRng::seed_from_u64(S);

    let mut hasher = Hasher::with_seed(S);
    let mut total_endpoints = 0;

    let num_locals = rng.random_range(10..LOCALITIES.len());

    // Select how many localities we want, note we add 1 since we always have a default cluster
    hasher.write_usize(num_locals + 1);

    let cm = ClusterMap::default();

    for locality in LOCALITIES.choose_multiple(&mut rng, num_locals) {
        let locality = locality.parse().unwrap();
        cm.insert(None, Some(locality), Default::default());
    }

    // Now actually insert the endpoints, now that the order of keys is established,
    // annoying, but note we split out iteration versus insertion, otherwise we deadlock
    let keys: Vec<_> = cm.iter().map(|kv| kv.key().clone()).collect();
    let mut sets = std::collections::BTreeMap::new();

    let mut token_generator = match token_kind {
        TokenKind::None => None,
        TokenKind::Multi { range, duplicates } => Some(TokenGenerator {
            rng: rand::rngs::SmallRng::seed_from_u64(S),
            previous: duplicates.then_some(Vec::new()),
            range: Some(range),
        }),
        TokenKind::Single { duplicates } => Some(TokenGenerator {
            rng: rand::rngs::SmallRng::seed_from_u64(S),
            previous: duplicates.then_some(Vec::new()),
            range: None,
        }),
    };

    for key in keys {
        write_locality(&mut hasher, &key);

        let ep = gen_endpoints(&mut rng, &mut hasher, token_generator.as_mut());
        total_endpoints += ep.len();
        cm.insert(None, key.clone(), ep.clone());
        sets.insert(key, ep);
    }

    GenCluster {
        cm,
        hash: hasher.finish(),
        total_endpoints,
        sets,
    }
}
