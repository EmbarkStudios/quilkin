#!/bin/bash

# Builds the eBPF program that can be loaded by the kernel. Pass `--update` to
# update the binary embedded in quilkin

set -e

ROOT=$(git rev-parse --show-toplevel)
EBPF_ROOT="$ROOT/crates/ebpf"

cargo +nightly build -Z build-std=core --release --target bpfel-unknown-none --manifest-path "$EBPF_ROOT/Cargo.toml"
clang -target bpf -Wall -O2 -g -c "$EBPF_ROOT/src/dummy.c" -o "$EBPF_ROOT/target/bpfel-unknown-none/release/dummy"

if [[ $1 == '--update' ]]; then
    cp "$EBPF_ROOT/target/bpfel-unknown-none/release/packet-router" "$ROOT/crates/xdp/bin/packet-router.bin"
    cp "$EBPF_ROOT/target/bpfel-unknown-none/release/dummy" "$ROOT/crates/xdp/bin/dummy.bin"
fi
