#include "shared.h"

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

#define set(val)                                                               \
    u8* blah = (u8*)ptr_at(ctx, 0, 10);                                        \
    if (blah == 0) {                                                           \
        return XDP_PASS;                                                       \
    } else {                                                                   \
        *blah = val;                                                           \
    }

inline XdpAction redirect_ipv4(struct xdp_md* ctx) {
    size_t offset = sizeof(EthHdr);
    valid_or_pass(v4, Ipv4Hdr, offset);
    offset += sizeof(Ipv4Hdr);

    if (v4->proto == UDP) {
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
    }

    return XDP_PASS;
}

inline XdpAction redirect_ipv6(struct xdp_md* ctx) {
    size_t offset = sizeof(EthHdr);
    valid_or_pass(v6, Ipv6Hdr, offset);
    offset += sizeof(Ipv6Hdr);

    switch (v6->next_hdr) {
    case UDP:
        MUTE valid_or_pass(udp, UdpHdr, offset);
        return redirect_udp(udp);
    default:
        break;
    }

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
