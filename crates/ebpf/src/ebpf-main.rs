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

#![no_std]
#![no_main]
#![allow(internal_features)]
#![feature(core_intrinsics)]

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::XskMap,
    programs::XdpContext,
};
//use aya_log_ebpf::warn;
use network_types::{
    eth::{EthHdr, EtherType},
    ip::{IpProto, Ipv4Hdr, Ipv6Hdr},
    udp::UdpHdr,
};

type Action = xdp_action::Type;

/// Map of sockets that packets can be redirected to
#[map]
static XSK: XskMap = XskMap::with_max_entries(128, 0);

// Number of sockets in the `XSK` map
#[no_mangle]
static SOCKET_COUNT: u32 = 0;
static mut COUNTER: u32 = 0;

/// The external port used by clients. Network order.
#[no_mangle]
static EXTERNAL_PORT_NO: u16 = u16::to_be(7777);
/// The port used to respond to QCMP messages. Network order.
#[no_mangle]
static QCMP_PORT_NO: u16 = u16::to_be(7600);

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
        match eth_hdr.ether_type {
            EtherType::Ipv4 => {
                let ipv4hdr = ptr_at::<Ipv4Hdr>(&ctx, EthHdr::LEN)?;
                let v4hdr = &*ipv4hdr;

                match v4hdr.proto {
                    IpProto::Udp => {
                        let udp_hdr = &*ptr_at::<UdpHdr>(&ctx, EthHdr::LEN + Ipv4Hdr::LEN)?;
                        udp_hdr.dest
                    }
                    _ => {
                        return Err(());
                    }
                }
            }
            EtherType::Ipv6 => {
                let ipv6hdr = ptr_at::<Ipv6Hdr>(&ctx, EthHdr::LEN)?;
                let v6hdr = &*ipv6hdr;

                // Note this means that we ignore packets that have extensions
                match v6hdr.next_hdr {
                    IpProto::Udp => {
                        let udp_hdr = &*ptr_at::<UdpHdr>(&ctx, EthHdr::LEN + Ipv6Hdr::LEN)?;
                        udp_hdr.dest
                    }
                    _ => {
                        return Err(());
                    }
                }
            }
            _ => {
                return Err(());
            }
        }
    };

    if dest_port == unsafe { core::ptr::read_volatile(&EXTERNAL_PORT_NO) }
        || u16::from_be(dest_port) >= EPHEMERAL_PORT_START
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
fn panic(_info: &core::panic::PanicInfo) -> ! {
    unsafe { core::hint::unreachable_unchecked() }
}
