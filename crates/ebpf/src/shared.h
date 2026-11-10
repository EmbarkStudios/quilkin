#include "bpf.h"

typedef i32 XdpAction;

typedef struct {
    u8 dst_addr[6];
    u8 src_addr[6];
    u16 ether_type;
} EthHdr;

enum EtherType : u16 {
    IPv4 = 0x0008,
    Arp = 0x0608,
    IPv6 = 0xdd86,
};

typedef struct {
    u8 vihl;
    u8 tos;
    u8 tot_len[2];
    u8 id[2];
    u8 frags[2];
    u8 ttl;
    u8 proto;
    u8 check[2];
    u8 src_addr[4];
    u8 dst_addr[4];
} Ipv4Hdr;

typedef struct {
    u8 vcf[4];
    u8 payload_len[2];
    u8 next_hdr;
    u8 hop_limit;
    u8 src_addr[16];
    u8 dst_addr[16];
} Ipv6Hdr;

enum IpProto : u8 {
    ICMP = 1,
    UDP = 17,
    ICMPv6 = 58,
};

typedef struct {
    u16 src;
    u16 dst;
    u16 len;
    u8 check[2];
} UdpHdr;

#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define ntohs(in) __builtin_bswap16(in)
#else
#define ntohs(in) in
#endif

#define force_inline __attribute__((always_inline))

/// The external port used by clients. Network order.
volatile const u16 EXTERNAL_PORT_NO;
/// The port used to respond to QCMP messages. Network order.
volatile const u16 QCMP_PORT_NO;

/// The beginning of the port range quilkin will use for server sessions, we
/// take advantage of the fact that, by default, the range Linux uses for
/// assigning ephemeral ports is 32768–60999, so we can easily determine in eBPF
/// if a port is intended for quilkin or not without relying on extra state
const u16 EPHEMERAL_PORT_START = 61000;

force_inline XdpAction redirect_udp(const UdpHdr* udp) {
    if (udp->dst == EXTERNAL_PORT_NO || udp->dst == QCMP_PORT_NO ||
        ntohs(udp->dst) >= EPHEMERAL_PORT_START) {
        return XDP_REDIRECT;
    } else {
        return XDP_PASS;
    }
}

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

// C doesn't allow declaring variables after a label, this is a hack found on
// https://stackoverflow.com/questions/77738048/why-can-i-declare-a-variable-after-a-label-in-gcc-but-not-clang
// clang-format off
#define MUTE {}
// clang-format on

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __type(key, u32);
    __type(value, u32);
    __uint(max_entries, 128);
} XSK SEC(".maps");
