#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

SEC("xdp")
int socket_router(struct xdp_md *ctx)
{
    return XDP_PASS;
}
