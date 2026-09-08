#!/bin/bash

# Builds the eBPF program that can be loaded by the kernel. Pass `--update` to
# update the binary embedded in quilkin
#
# Requires `bpf-linker` (`cargo install bpf-linker`), which links against rustc's
# LLVM, so reinstall it when that changes. `clang` is only for `dummy.bin`.

set -e

ROOT=$(git rev-parse --show-toplevel)
EBPF_ROOT="$ROOT/crates/ebpf"
# Layout the output the same as rust for clarity
TARGET="$EBPF_ROOT/target/bpfel-unknown-none/release"
BIN="$ROOT/crates/xdp/bin"

mkdir -p "$TARGET"

if ! command -v clang > /dev/null; then
    echo "clang is required to build the eBPF code"
    exit 1
fi

clang -target bpf -Wall -O2 -c "$EBPF_ROOT/src/dummy.c" -o "$TARGET/dummy"
clang -target bpf -Wall -O2 -c -g "$EBPF_ROOT/src/main.c" -o "$TARGET/main"
clang -target bpf -Wall -O2 -c -g "$EBPF_ROOT/src/layer2.c" -o "$TARGET/layer2"

if [[ $1 == '--update' ]]; then
    cp "$TARGET/dummy" "$BIN/dummy.bin"
    cp "$TARGET/main" "$BIN/main.bin"
    cp "$TARGET/layer2" "$BIN/layer2.bin"
fi
