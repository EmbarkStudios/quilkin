#!/bin/bash
set -e

source="${BASH_SOURCE[0]}"

# Print kernel version just for confirmation in CI
echo "$(uname -r)"

cleanup() {
    echo "Cleaning up"
    ip netns del cs || true
    ip netns del proxy || true

    pkill fortio || true
    pkill quilkin || true
}

trap cleanup EXIT

ip netns del cs || true
ip netns del proxy || true

echo "::notice file=$source,line=$LINENO::Creating network namespaces"
ip netns add cs
ip netns add proxy

echo "::notice file=$source,line=$LINENO::Adding client <-> proxy <-> server links"
ip link add veth-cs type veth peer name veth-proxy

ip link set veth-cs netns cs
ip link set veth-proxy netns proxy

PROXY_IP="10.0.0.2"
OUTSIDE_IP="10.0.0.1"

echo "::notice file=$source,line=$LINENO::Adding IPs"
ip -n cs addr add $OUTSIDE_IP/24 dev veth-cs
ip -n proxy addr add $PROXY_IP/24 dev veth-proxy

echo "::notice file=$source,line=$LINENO::Creating network namespaces"
ip -n cs link set veth-cs up
ip -n proxy link set veth-proxy up

# XDP on a veth has a bit of an annoying requirement in newer kernel versions,
# both sides need to have an XDP program attached for traffic to appear on the
# one we actually want to test
ROOT=$(git rev-parse --show-toplevel)
echo "Adding dummy program"
ip -n cs link set veth-cs xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp

ip netns exec cs fortio udp-echo&
ip netns exec proxy ./target/debug/quilkin --service.udp --service.qcmp --provider.static.endpoints=$OUTSIDE_IP:8078 --service.udp.xdp --service.udp.xdp.network-interface veth-proxy&

echo "::notice file=$source,line=$LINENO::Launching client"
ip netns exec cs fortio load -n 10 udp://$PROXY_IP:7777 2> ./target/logs.txt
logs=$(cat ./target/logs.txt)

regex="Total Bytes sent: ([0-9]+), received: ([0-9]+)"

if [[ $logs =~ $regex ]]; then
  send=${BASH_REMATCH[1]}
  recv=${BASH_REMATCH[2]}
  # We could be more strict here and require they are exactly equal, but I can't
  # even consistently get that on my local machine so I doubt CI will fair better
  if [[ $recv -ne "0" ]]; then
    echo "::notice file=$source,line=$LINENO::Successfully sent ${send}B and received ${recv}B"

    # Now test QCMP pings which was also enabled in the proxy
    ip netns exec cs ./target/debug/quilkin qcmp ping $PROXY_IP:7600

    exit 0
  fi

  echo "::error file=$source,line=$LINENO::sent ${send}B but only received ${recv}B"
  exit 1
fi

echo "::error file=$source,line=$LINENO::Failed to find expected log line from UDP client"
exit 2
