#include "shared.h"

SEC("xdp")
XdpAction dummy(struct xdp_md* ctx) { return XDP_PASS; }
