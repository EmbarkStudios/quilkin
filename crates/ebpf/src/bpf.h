/// These are from <linux/bpf.h> and <bpf/bpf_helpers.h>, but they aren't going
/// to change and it makes it easier to compile without requiring additional
/// dependencies

typedef unsigned char u8;
typedef unsigned short u16;
typedef __signed__ int i32;
typedef unsigned int u32;
typedef __SIZE_TYPE__ size_t;
typedef unsigned long long u64;

#define BPF_MAP_TYPE_XSKMAP 17
#define BPF_MAP_TYPE_RINGBUF 27

#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val)* name

/// Argument passed by the kernel when a packet is received
struct xdp_md {
    /// Pointer to the beginning of the data
    u32 data;
    /// Pointer to the end of the data
    u32 data_end;
    u32 data_meta;
    /* Below access go through struct xdp_rxq_info */
    u32 ingress_ifindex; /* rxq->dev->ifindex */
    u32 rx_queue_index;  /* rxq->queue_index  */

    u32 egress_ifindex; /* txq->dev->ifindex */
};

#define SEC(name)                                                              \
    _Pragma("GCC diagnostic push")                                             \
        _Pragma("GCC diagnostic ignored \"-Wignored-attributes\"")             \
            __attribute__((section(name), used)) _Pragma("GCC diagnostic pop")

/* User return codes for XDP prog type.
 * A valid XDP program must return one of these defined values. All other
 * return codes are reserved for future use. Unknown return codes will
 * result in packet drops and a warning via bpf_warn_invalid_xdp_action().
 */
enum xdp_action {
    XDP_ABORTED = 0,
    XDP_DROP,
    XDP_PASS,
    XDP_TX,
    XDP_REDIRECT,
};

enum bpf_wakeup {
    BPF_RB_ADAPTIVE = 0ULL,
    BPF_RB_NO_WAKEUP = (1ULL << 0),
    BPF_RB_FORCE_WAKEUP = (1ULL << 1),
};

/*
 * bpf_map_lookup_elem
 *
 * 	Perform a lookup in *map* for an entry associated to *key*.
 *
 * Returns
 * 	Map value associated to *key*, or **NULL** if no entry was
 * 	found.
 */
static void* (*const bpf_map_lookup_elem)(void* map,
                                          const void* key) = (void*)1;

/*
 * bpf_redirect_map
 *
 * 	Redirect the packet to the endpoint referenced by *map* at
 * 	index *key*. Depending on its type, this *map* can contain
 * 	references to net devices (for forwarding packets through other
 * 	ports), or to CPUs (for redirecting XDP frames to another CPU;
 * 	but this is only implemented for native XDP (with driver
 * 	support) as of this writing).
 *
 * 	The lower two bits of *flags* are used as the return code if
 * 	the map lookup fails. This is so that the return value can be
 * 	one of the XDP program return codes up to **XDP_TX**, as chosen
 * 	by the caller. The higher bits of *flags* can be set to
 * 	BPF_F_BROADCAST or BPF_F_EXCLUDE_INGRESS as defined below.
 *
 * 	With BPF_F_BROADCAST the packet will be broadcasted to all the
 * 	interfaces in the map, with BPF_F_EXCLUDE_INGRESS the ingress
 * 	interface will be excluded when do broadcasting.
 *
 * 	See also **bpf_redirect**\ (), which only supports redirecting
 * 	to an ifindex, but doesn't require a map to do so.
 *
 * Returns
 * 	**XDP_REDIRECT** on success, or the value of the two lower bits
 * 	of the *flags* argument on error.
 */
static long (*const bpf_redirect_map)(void* map, u64 key,
                                      u64 flags) = (void*)51;

/*
 * bpf_ringbuf_reserve
 *
 * 	Reserve *size* bytes of payload in a ring buffer *ringbuf*.
 * 	*flags* must be 0.
 *
 * Returns
 * 	Valid pointer with *size* bytes of memory available; NULL,
 * 	otherwise.
 */
static void* (*const bpf_ringbuf_reserve)(void* ringbuf, u64 size,
                                          u64 flags) = (void*)131;

/*
 * bpf_ringbuf_submit
 *
 * 	Submit reserved ring buffer sample, pointed to by *data*.
 * 	If **BPF_RB_NO_WAKEUP** is specified in *flags*, no notification
 * 	of new data availability is sent.
 * 	If **BPF_RB_FORCE_WAKEUP** is specified in *flags*, notification
 * 	of new data availability is sent unconditionally.
 * 	If **0** is specified in *flags*, an adaptive notification
 * 	of new data availability is sent.
 *
 * 	See 'bpf_ringbuf_output()' for the definition of adaptive notification.
 *
 * Returns
 * 	Nothing. Always succeeds.
 */
static void (*const bpf_ringbuf_submit)(void* data, u64 flags) = (void*)132;
