#include "shared.h"

typedef u8 IP[16];
typedef u8 MAC[6];

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    // This must be a multiple of the page size, a single page gives us enough
    // space for 170 entries (16 byte IP + 6 byte MAC + 2 byte kind) which
    // should be more than enough as long as the userspace thread dequeing the
    // entries is given sufficient CPU time
    __uint(max_entries, 4 * 1024);
} IP_TO_MAC SEC(".maps");

typedef struct {
    u16 hardware_type;
    u16 protocol_type;
    u8 hardware_len;
    u8 protocol_len;
    u16 operation;
    u8 sender_hardware_address[6];
    u8 sender_protocol_address[4];
    u8 target_hardware_address[6];
    u8 target_protocol_address[4];
} ArpHdr;

const u16 HTYPE_ETHER = ntohs(1);
const u16 PTYPE_IPV4 = ntohs(0x0800);
const u8 HLEN_MAC = 6;
const u8 PLEN_IPV4 = 4;

const u8 ECHO_REPLYV4 = 0;
const u8 ECHO_REPLYV6 = 129;
// https://datatracker.ietf.org/doc/html/rfc4861#section-4.4
const u8 NEIGHBOR_ADVERTISEMENT = 136;

// https://datatracker.ietf.org/doc/html/rfc4861#section-4.6
const u8 NA_TARGET_LL_ADDR = 2;

typedef struct {
    u32 reserved : 5, override : 1, solicited : 1, router : 1, reserved2 : 24;
    u8 target_addr[16];
} NeighAdvert;

typedef struct {
    u8 type;
    u8 length;
    u8 ll_addr[6];
} AdvertOptions;

typedef struct {
    u8 kind;
    u8 code;
    u16 checksum;
} IcmpHeader;

typedef struct {
    IP ip;
    MAC mac;
    u16 kind;
} RingBufEntry;

/// The external port used by clients. Network order.
volatile const u16 EXTERNAL_PORT_NO;
/// The port used to respond to QCMP messages. Network order.
volatile const u16 QCMP_PORT_NO;

/// The beginning of the port range quilkin will use for server sessions, we
/// take advantage of the fact that, by default, the range Linux uses for
/// assigning ephemeral ports is 32768–60999, so we can easily determine in eBPF
/// if a port is intended for quilkin or not without relying on extra state
const u16 EPHEMERAL_PORT_START = 61000;

inline XdpAction redirect_udp(const UdpHdr* udp) {
    if (udp->dst == EXTERNAL_PORT_NO || udp->dst == QCMP_PORT_NO ||
        ntohs(udp->dst) >= EPHEMERAL_PORT_START) {
        return XDP_REDIRECT;
    } else {
        return XDP_PASS;
    }
}

inline void send_mac_to_userspace(const IP ip, const MAC mac, u16 kind) {
    RingBufEntry* entry =
        bpf_ringbuf_reserve(&IP_TO_MAC, sizeof(RingBufEntry), 0);

    if (!entry) {
        // TODO: Counter? Note that we can't print using bpf_printk as that
        // function is GPL and the kernel won't load our program if it's not GPL
        // as well, but maybe that's not a big deal? dunno
        // <https://github.com/nyrahul/ebpf-guide/blob/master/docs/gpl_license_ebpf.rst>
        // bpf_printk("failed to reserve space for ringbuf entry\n");
        return;
    }

    __builtin_memcpy(entry->ip, ip, 16);
    __builtin_memcpy(entry->mac, mac, 6);
    entry->kind = kind;

    bpf_ringbuf_submit(entry, BPF_RB_ADAPTIVE);
}

inline XdpAction redirect_ipv4(struct xdp_md* ctx) {
    size_t offset = sizeof(EthHdr);
    valid_or_pass(v4, Ipv4Hdr, offset);
    offset += sizeof(Ipv4Hdr);

    switch (v4->proto) {
    case UDP:
        // Ignore IPv4 packets that have options, no packets Quilkin is meant to
        // process will have them ipv4 header without options is 20 bytes (5 *
        // WORD_SIZE)
        if ((v4->vihl & 0xf) == 5) {
            // Ignore fragmented packets, we don't support them, but ignore the
            // Don't Fragment flag
            if ((((u16)v4->frags[0] << 8 | (u16)v4->frags[1]) ^ 0x4000) == 0) {
                MUTE valid_or_pass(udp, UdpHdr, offset);
                return redirect_udp(udp);
            }
        }
        break;
    case ICMP:
        MUTE valid_or_pass(icmp, IcmpHeader, offset);
        if (icmp->kind == ECHO_REPLYV4) {
            MUTE valid_or_pass(eth, EthHdr, 0);

            IP address = {
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0xff,
                0xff,
                v4->src_addr[0],
                v4->src_addr[1],
                v4->src_addr[2],
                v4->src_addr[3],
            };

            send_mac_to_userspace(address, eth->src_addr, ICMP);
        }
        break;
    default:
        break;
    }

    return XDP_PASS;
}

XdpAction redirect_ipv6(struct xdp_md* ctx) {
    size_t offset = sizeof(EthHdr);
    valid_or_pass(v6, Ipv6Hdr, offset);
    offset += sizeof(Ipv6Hdr);

    switch (v6->next_hdr) {
    case UDP:
        MUTE valid_or_pass(udp, UdpHdr, offset);
        return redirect_udp(udp);
    case ICMPv6:
        MUTE valid_or_pass(icmp, IcmpHeader, offset);
        offset += sizeof(IcmpHeader);

        if (icmp->kind == NEIGHBOR_ADVERTISEMENT) {
            MUTE valid_or_pass(na, NeighAdvert, offset);

            // For now, only update the mapping if we (ie the kernel) explicitly
            // solicited this this neighbor, as it already has all the logic
            // around caching
            if (na->solicited) {
                AdvertOptions* opts = (AdvertOptions*)ptr_at(
                    ctx, offset + sizeof(NeighAdvert), sizeof(AdvertOptions));

                // According to
                // https://datatracker.ietf.org/doc/html/rfc2461#section-4.4,
                // the target link layer address SHOULD be included for unicast
                // (ie the kernel already has the link laye address), but at
                // least testing with my local router, it doesn't do that
                if (opts && opts->type == NA_TARGET_LL_ADDR &&
                    opts->length == 1) {
                    send_mac_to_userspace(v6->src_addr, opts->ll_addr,
                                          NEIGHBOR_ADVERTISEMENT);
                } else {
                    MUTE valid_or_pass(eth, EthHdr, 0);
                    send_mac_to_userspace(v6->src_addr, eth->src_addr,
                                          NEIGHBOR_ADVERTISEMENT);
                }
            }
        } else if (icmp->kind == ECHO_REPLYV6) {
            MUTE valid_or_pass(eth, EthHdr, 0);
            send_mac_to_userspace(v6->src_addr, eth->src_addr, ICMPv6);
        }

        break;
    default:
        break;
    }

    return XDP_PASS;
}

XdpAction handle_arp(struct xdp_md* ctx) {
    valid_or_pass(arp_hdr, ArpHdr, sizeof(EthHdr));

    // Sanity check that the hardware address is a MAC and the protocol address
    // is IPv4, will this ever not be the case? Probably not.
    if (arp_hdr->hardware_type != HTYPE_ETHER ||
        arp_hdr->protocol_type != PTYPE_IPV4 ||
        arp_hdr->hardware_len != HLEN_MAC ||
        arp_hdr->protocol_len != PLEN_IPV4) {
        return XDP_PASS;
    }

    // Turn the IPv4 address into an IPv4-mapped IPv6 address
    IP address = {0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0xff,
                  0xff,
                  arp_hdr->sender_protocol_address[0],
                  arp_hdr->sender_protocol_address[1],
                  arp_hdr->sender_protocol_address[2],
                  arp_hdr->sender_protocol_address[3]};

    send_mac_to_userspace(address, arp_hdr->sender_hardware_address, Arp);

    return XDP_PASS;
}

XdpAction redirect_packet(struct xdp_md* ctx) {
    size_t off = 0;
    valid_or_pass(eth, EthHdr, off);

    switch (eth->ether_type) {
    case IPv4:
        return redirect_ipv4(ctx);
    case IPv6:
        return redirect_ipv6(ctx);
    case Arp:
        return handle_arp(ctx);
    default:
        break;
    }

    return XDP_PASS;
}

SEC("xdp")
XdpAction all_queues(struct xdp_md* ctx) {
    XdpAction action = redirect_packet(ctx);
    if (action == XDP_REDIRECT) {
        u32 index = ctx->rx_queue_index;

        if (bpf_map_lookup_elem(&XSK, &index)) {
            return bpf_redirect_map(&XSK, index, 0);
        }
    }

    return action;
}
