//! Feeds arbitrary frames through the XDP I/O loop's packet processing.
//!
//! The eBPF program only routes UDP packets to the loop, but it inspects frames
//! at fixed offsets, so what actually arrives is only as trustworthy as those
//! offsets are, which is what this covers. Every run also asserts that no umem
//! frame was leaked, so a packet that goes down a path that forgets to account
//! for it fails the same as a panic.
//!
//! Run it with `mise run fuzz`, which sets the flags this needs.

#![no_main]

use libfuzzer_sys::fuzz_target;
use quilkin::{
    filters,
    net::io::nic::xdp::process::{
        self,
        xdp::{self, slab::Slab},
    },
};
use std::{cell::RefCell, net::Ipv4Addr, net::Ipv6Addr};

const EXTERNAL_PORT: u16 = 7777;
const QCMP_PORT: u16 = 7600;
const FRAME_COUNT: u32 = 64;
/// The number of frames a single input can be fanned out to, one per destination
const BATCH: usize = 8;

struct Harness {
    _runtime: tokio::runtime::EnterGuard<'static>,
    umem: xdp::Umem,
    state: process::State,
    cfg: process::ConfigState,
}

impl Harness {
    fn new() -> Self {
        // The session map spawns a cleanup task, nothing here needs it to make
        // progress, but constructing the map outside a runtime panics
        let runtime = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap(),
        ))
        .enter();

        let cm = quilkin::net::ClusterMap::new();
        cm.insert(
            None,
            None,
            [
                quilkin::net::Endpoint::new((Ipv4Addr::new(1, 1, 1, 1), 1111).into()),
                quilkin::net::Endpoint::new((Ipv6Addr::new(1, 1, 1, 1, 1, 1, 1, 1), 2222).into()),
            ]
            .into_iter()
            .collect(),
        );

        // Exercises every `PacketMut` method the filters can drive
        let chain = quilkin::filters::FilterChain::testing([
            filters::FilterInstance::testing(filters::Capture::testing(
                filters::capture::Config::with_strategy(filters::capture::Prefix {
                    size: 1,
                    remove: true,
                }),
            )),
            filters::FilterInstance::testing(filters::Capture::testing(
                filters::capture::Config::with_strategy(filters::capture::Suffix {
                    size: 1,
                    remove: true,
                }),
            )),
            filters::FilterInstance::testing(filters::Concatenate::testing(
                filters::concatenate::Config {
                    on_read: filters::concatenate::Strategy::Append,
                    on_write: filters::concatenate::Strategy::Prepend,
                    bytes: vec![0xf0; 4],
                },
            )),
        ]);

        Self {
            _runtime: runtime,
            umem: xdp::Umem::map(
                xdp::umem::UmemCfgBuilder {
                    frame_size: xdp::umem::FrameSize::TwoK,
                    head_room: 20,
                    frame_count: FRAME_COUNT,
                    ..Default::default()
                }
                .build()
                .unwrap(),
            )
            .unwrap(),
            state: process::State {
                external_port: EXTERNAL_PORT.into(),
                qcmp_port: QCMP_PORT.into(),
                destinations: Vec::with_capacity(1),
                addr_to_asn: Default::default(),
                sessions: Default::default(),
                local_ipv4: Ipv4Addr::new(2, 2, 2, 2),
                local_ipv6: Ipv6Addr::new(2, 2, 2, 2, 2, 2, 2, 2),
                last_receive: quilkin::time::UtcTimestamp::now(),
            },
            cfg: process::ConfigState {
                filters: quilkin::config::filter::FilterChainConfig::new(chain).cached(),
                clusters: quilkin::config::Watch::new(cm),
            },
        }
    }

    fn run(&mut self, data: &[u8]) {
        // SAFETY: the packet is freed back to the umem before this returns, so
        // it can't outlive it
        let mut packet = match unsafe { self.umem.alloc() } {
            Some(packet) => packet,
            // Every run frees what it allocated, so the umem can't be exhausted
            None => panic!("umem exhausted, a frame was leaked"),
        };

        let data = &data[..data.len().min(packet.capacity())];
        if packet.append(data).is_err() {
            self.umem.free_packet(packet);
            return;
        }

        let mut rx = xdp::slab::StackSlab::<1>::new();
        let mut tx = xdp::slab::StackSlab::<BATCH>::new();
        rx.push_front(packet);

        process::process_packets(
            &mut rx,
            &mut self.umem,
            &mut tx,
            &mut self.cfg,
            &mut self.state,
        );

        assert!(rx.is_empty(), "a received packet wasn't processed");

        while let Some(packet) = tx.pop_back() {
            self.umem.free_packet(packet);
        }

        assert_eq!(
            self.umem.outstanding(),
            0,
            "{} frames weren't returned to the umem",
            self.umem.outstanding()
        );
    }
}

thread_local! {
    static HARNESS: RefCell<Harness> = RefCell::new(Harness::new());
}

fuzz_target!(|data: &[u8]| {
    HARNESS.with_borrow_mut(|harness| harness.run(data));
});
