#include <linux/bpf.h>

#include <arpa/inet.h>
#include <stddef.h>

#include <bpf/bpf_helpers.h>

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __type(key, __u32);
    __type(value, __u32);
    __uint(max_entries, 128);
} XSK SEC(".maps");

typedef __u8 IP[16];

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, IP);
    __type(value, __u64);
    __uint(max_entries, 16 * 1024);
} IP_TO_MAC SEC(".maps");

typedef struct __attribute__((__packed__)) {
    __u8 dst_addr[6];
    __u8 src_addr[6];
    __u16 ether_type;
} EthHdr;

typedef struct {
    __u8 vihl;
    __u8 tos;
    __u8 tot_len[2];
    __u8 id[2];
    __u8 frags[2];
    __u8 ttl;
    __u8 proto;
    __u8 check[2];
    __u8 src_addr[4];
    __u8 dst_addr[4];
} Ipv4Hdr;

typedef struct {
    __u8 vcf[4];
    __u8 payload_len[2];
    __u8 next_hdr;
    __u8 hop_limit;
    __u8 src_addr[16];
    __u8 dst_addr[16];
} Ipv6Hdr;

typedef struct {
    __u16 src;
    __u16 dst;
    __u16 len;
    __u8 check[2];
} UdpHdr;

typedef struct {
    __u8 kind;
    __u8 code;
    __u8 checksum[2];
} Icmpv6Hdr;

typedef struct {
    __u32 reserved : 5, override : 1, solicited : 1, router : 1, reserved2 : 24;
    __u8 target_addr[16];
    __u8 ll_addr[6];
} NeighAdvert;

/// Ethernet types that we care about
enum EtherType : __u16 {
    IPv4 = 0x0800,
    Arp = 0x0608,
    IPv6 = 0xdd86,
};

enum IpProto : __u8 {
    UDP = 17,
    ICMPv6 = 58,
};

inline const void* ptr_at(struct xdp_md* ctx, size_t offset, size_t len) {
    if (ctx->data + offset + len > ctx->data_end) {
        return 0;
    }

    return (const void*)(size_t)ctx->data + offset;
}

#define valid_or_pass(name, type, offset)                                      \
    const type* name = (const type*)ptr_at(ctx, offset, sizeof(type));         \
    if (name == 0) {                                                           \
        return XDP_PASS;                                                       \
    }

int redirect_ipv4(struct xdp_md* ctx) { return XDP_PASS; }

int redirect_ipv6(struct xdp_md* ctx) {
    size_t offset = sizeof(EthHdr);
    valid_or_pass(v6, Ipv6Hdr, offset);
    offset += sizeof(Ipv6Hdr);

    switch (v6->next_hdr) {
    case UDP:

        break;
    case ICMPv6:

        break;
    default:
        break;
    }

    return XDP_PASS;
}

void handle_arp(struct xdp_md* ctx) {}

int redirect_packet(struct xdp_md* ctx) {
    size_t off = 0;
    valid_or_pass(eth, EthHdr, off);

    switch (eth->ether_type) {
    case IPv4:
        return redirect_ipv4(ctx);
    case IPv6:
        return redirect_ipv6(ctx);
    case Arp:
        handle_arp(ctx);
        break;
    default:
        break;
    }

    return XDP_PASS;
}

SEC("xdp")
int socket_router(struct xdp_md* ctx) {
    if (redirect_packet(ctx) == XDP_REDIRECT) {
        __u32 index = ctx->rx_queue_index;

        if (bpf_map_lookup_elem(&XSK, &index)) {
            return bpf_redirect_map(&XSK, index, 0);
        }
    }

    return XDP_PASS;
}
