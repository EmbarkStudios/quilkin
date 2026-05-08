#![no_std]
#![no_main]
#![allow(internal_features)]
//#![feature(core_intrinsics)]

use core::sync::atomic;

/// This is the same as ./ebpf-main.rs, except it updates a map of IP -> MAC addresses
/// shared with userspace so that userspace can set the destination L2 (MAC) address
use aya_ebpf::{
    bindings::{BPF_F_NO_PREALLOC, xdp_action},
    macros::{map, xdp},
    maps::{HashMap, XskMap},
    programs::XdpContext,
};
//use aya_log_ebpf::warn;
use network_types::{
    arp::ArpHdr,
    eth::{EthHdr, EtherType},
    ip::{Ipv4Hdr, Ipv6Hdr},
    udp::UdpHdr,
};

type Action = xdp_action::Type;

/// Map of sockets that packets can be redirected to
#[map]
static XSK: XskMap = XskMap::with_max_entries(128, 0);
/// Map of L3 IP -> L2 MAC
#[map]
static IP_TO_MAC: HashMap<[u8; 16], atomic::AtomicU64> =
    HashMap::with_max_entries(16 * 1024, BPF_F_NO_PREALLOC);

// Number of sockets in the `XSK` map
#[unsafe(no_mangle)]
static SOCKET_COUNT: u32 = 0;
static mut COUNTER: u32 = 0;

/// The external port used by clients. Network order.
#[unsafe(no_mangle)]
static EXTERNAL_PORT_NO: [u8; 2] = 7777u16.to_be_bytes();
/// The port used to respond to QCMP messages. Network order.
#[unsafe(no_mangle)]
static QCMP_PORT_NO: [u8; 2] = 7600u16.to_be_bytes();

/// The beginning of the port range quilkin will use for server sessions, we
/// take advantage of the fact that, by default, the range Linux uses for
/// assigning ephemeral ports is 32768–60999, so we can easily determine in eBPF
/// if a port is intended for quilkin or not without relying on extra state
const EPHEMERAL_PORT_START: u16 = 61000;

// eBPF doesn't support 32-bit atomic operations, but AtomicU64 doesn't provide
// fetch_add when targeting eBPF for some reason, so we just roll our own
// struct Atomic(core::cell::UnsafeCell<u64>);
// unsafe impl Sync for Atomic {}

// static COUNTER: Atomic = Atomic(core::cell::UnsafeCell::new(0));

#[inline(always)]
fn ptr_at<T>(ctx: &XdpContext, offset: usize) -> Result<*mut T, ()> {
    let start = ctx.data();
    let end = ctx.data_end();
    let len = core::mem::size_of::<T>();

    if start + offset + len > end {
        return Err(());
    }

    Ok((start + offset) as *mut T)
}

pub fn packet_router(ctx: &XdpContext) -> Result<(), ()> {
    let eth_hdr = unsafe { &mut *ptr_at::<EthHdr>(&ctx, 0)? };

    // Get the destination UDP port, passing all packets we don't care about
    let dest_port = unsafe {
        match eth_hdr.ether_type() {
            Ok(EtherType::Ipv4) => {
                let ipv4hdr = ptr_at::<Ipv4Hdr>(&ctx, EthHdr::LEN)?;
                let v4hdr = &*ipv4hdr;

                match v4hdr.proto {
                    17 /* IpProto::Udp */ => {
                        let udp_hdr = &*ptr_at::<UdpHdr>(&ctx, EthHdr::LEN + Ipv4Hdr::LEN)?;
                        udp_hdr.dst
                    }
                    _ => {
                        return Err(());
                    }
                }
            }
            Ok(EtherType::Ipv6) => {
                let ipv6hdr = ptr_at::<Ipv6Hdr>(&ctx, EthHdr::LEN)?;
                let v6hdr = &*ipv6hdr;

                // Note this means that we ignore packets that have extensions
                match v6hdr.next_hdr {
                    17 /* IpProto::Udp */ => {
                        let udp_hdr = &*ptr_at::<UdpHdr>(&ctx, EthHdr::LEN + Ipv6Hdr::LEN)?;
                        udp_hdr.dst
                    }
                    58 /* IpProto::Ipv6Icmp */ => {
                        #[repr(C)]
                        struct Icmpv6Hdr {
                            kind: u8,
                            code: u8,
                            checksum: [u8; 2],
                        }

                        impl Icmpv6Hdr {
                            const LEN: usize = core::mem::size_of::<Self>();
                        }

                        let icmp_hdr = &*ptr_at::<Icmpv6Hdr>(&ctx, EthHdr::LEN + Ipv6Hdr::LEN)?;

                        const NEIGHBOR_ADVERTISEMENT: u8 = 136;

                        if icmp_hdr.kind == NEIGHBOR_ADVERTISEMENT {
                            ///  0                   1                   2                   3
                            ///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
                            /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                            /// |     Type      |     Code      |          Checksum             |
                            /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                            /// |R|S|O|                     Reserved                            |
                            /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                            /// |                                                               |
                            /// +                                                               +
                            /// |                                                               |
                            /// +                       Target Address                          +
                            /// |                                                               |
                            /// +                                                               +
                            /// |                                                               |
                            /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                            /// |   Options ...
                            /// +-+-+-+-+-+-+-+-+-+-+-+-
                            #[repr(C)]
                            struct NeighAdvert {
                                /// The top 3 bits in order are
                                ///
                                /// - R - From Router
                                /// - S - Solicited
                                /// - O - Override
                                ///
                                /// The rest is reserved
                                flags_and_reserved: u32,
                                target_addr: [u8; 16],
                                ll_addr: [u8; 6],
                            }

                            let advert = &*ptr_at::<NeighAdvert>(
                                &ctx,
                                EthHdr::LEN + Ipv6Hdr::LEN + Icmpv6Hdr::LEN,
                            )?;

                            // For now, only update on solicited advertisements, ie advertisements that we explicitly
                            // asked for (or rather, the kernel did)
                            if advert.flags_and_reserved & 1u32 << 26 != 0 {
                                // SAFETY: the likelihood of getting multiple ICMP neighbor advertisement packets from
                                // the same source simultaneously on multiple queues is highly unlikely...but for now
                                // this is just POC
                                if let Some(src_mac) = IP_TO_MAC.get_ptr_mut(&v6hdr.src_addr) {
                                    let mut addr = [0u8; 8];
                                    addr[2..].copy_from_slice(&advert.ll_addr);
                                    (*src_mac).store(u64::from_be_bytes(addr), atomic::Ordering::Relaxed);
                                }
                            }
                        }

                        // Pass the ICMPv6 packet up the stack, even if we have used it to update
                        // our own userspace map
                        return Err(());
                    }
                    _ => {
                        return Err(());
                    }
                }
            }
            Ok(EtherType::Arp) => {
                let arphdr = &*ptr_at::<ArpHdr>(&ctx, EthHdr::LEN)?;

                const HTYPE_ETHER: [u8; 2] = 1u16.to_be_bytes();
                const PTYPE_IPV4: [u8; 2] = 0x0800u16.to_be_bytes();
                const HLEN_MAC: u8 = 6;
                const PLEN_IPV4: u8 = 4;

                // Do basic sanity checking
                if arphdr.htype == HTYPE_ETHER
                    && arphdr.ptype == PTYPE_IPV4
                    && arphdr.hlen == HLEN_MAC
                    && arphdr.plen == PLEN_IPV4
                {
                    // Turn the IPv4 address into an IPv4-mapped IPv6 address
                    let mut src_ip = [0u8; 16];

                    src_ip[10] = 0xff;
                    src_ip[11] = 0xff;
                    src_ip[12] = arphdr.spa[0];
                    src_ip[13] = arphdr.spa[1];
                    src_ip[14] = arphdr.spa[2];
                    src_ip[15] = arphdr.spa[3];

                    // SAFETY: the likelihood of getting multiple ARP packets from the same source simultaneously on
                    // multiple queues is highly unlikely...but for now this is just POC
                    if let Some(src_mac) = IP_TO_MAC.get_ptr_mut(&src_ip) {
                        let mut addr = [0u8; 8];
                        addr[2..].copy_from_slice(&arphdr.sha);
                        (*src_mac).store(u64::from_be_bytes(addr), atomic::Ordering::Relaxed);
                    }
                }

                // Pass the ARP packet up the stack, even if we have used it to update
                // our own userspace map
                return Err(());
            }
            _ => {
                return Err(());
            }
        }
    };

    if dest_port == unsafe { core::ptr::read_volatile(&EXTERNAL_PORT_NO) }
        || u16::from_be_bytes(dest_port) >= EPHEMERAL_PORT_START
        || dest_port == unsafe { core::ptr::read_volatile(&QCMP_PORT_NO) }
    {
        Ok(())
    } else {
        Err(())
    }
}

/// The entrypoint used when there is a AF_XDP socket bound to every queue of
/// the NIC this program is attached to
#[xdp]
pub fn all_queues(ctx: XdpContext) -> Action {
    if packet_router(&ctx).is_ok() {
        let queue_id = unsafe { (*ctx.ctx).rx_queue_index };
        XSK.redirect(queue_id, 0).unwrap_or(xdp_action::XDP_PASS)
    } else {
        xdp_action::XDP_PASS
    }
}

/// The entrypoint used when the AF_XDP sockets bound do not match the number of
/// available NIC queues
#[xdp]
pub fn round_robin(ctx: XdpContext) -> Action {
    if packet_router(&ctx).is_ok() {
        // Due to a deficiency in Aya, we can't use an atomic here, even though they
        // are supported. I believe this is because of atomics not being relocated
        // to a writable section, which is what libbpf does, and should be fixed
        // in aya, but we just take the hit for now that we'll get packets assigned
        // to the same socket
        // unsafe {
        //     let i = core::intrinsics::atomic_xadd_relaxed(COUNTER.0.get(), 1);
        //     let index = i % core::ptr::read_volatile(&SOCKET_COUNT);
        //     XSK.redirect(index as _, 0).map_err(|_| ())
        // }
        unsafe {
            COUNTER += 1;
            let index = COUNTER % core::ptr::read_volatile(&SOCKET_COUNT);
            XSK.redirect(index, 0).unwrap_or(xdp_action::XDP_PASS)
        }
    } else {
        xdp_action::XDP_PASS
    }
}

/// We can't panic, but we still need to satisfy the linker
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo<'_>) -> ! {
    unsafe { core::hint::unreachable_unchecked() }
}
