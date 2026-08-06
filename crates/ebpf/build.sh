#!/bin/bash

# Builds the eBPF program that can be loaded by the kernel. Pass `--update` to
# update the binary embedded in quilkin
#
# Requires `bpf-linker` (`cargo install bpf-linker`), which links against rustc's
# LLVM, so reinstall it when that changes. `clang` is only for `dummy.bin`.

set -e

ROOT=$(git rev-parse --show-toplevel)
EBPF_ROOT="$ROOT/crates/ebpf"
OUT="$EBPF_ROOT/target/bpfel-unknown-none/release"

if ! command -v bpf-linker > /dev/null; then
    echo "bpf-linker is not installed, run 'cargo install bpf-linker'" >&2
    exit 1
fi

cargo +nightly build -Z build-std=core --release --target bpfel-unknown-none --manifest-path "$EBPF_ROOT/Cargo.toml"

if command -v clang > /dev/null; then
    clang -target bpf -Wall -O2 -g -c "$EBPF_ROOT/src/dummy.c" -o "$OUT/dummy"
else
    echo "clang is not installed, skipping dummy.bin" >&2
fi

if [[ $1 == '--update' ]]; then
    cp "$OUT/packet-router" "$ROOT/crates/xdp/bin/packet-router.bin"

    if [[ -f "$OUT/dummy" ]]; then
        cp "$OUT/dummy" "$ROOT/crates/xdp/bin/dummy.bin"
    fi
fi
