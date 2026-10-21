#!/bin/bash
set -e

source="${BASH_SOURCE[0]}"

QID=

cleanup() {
     echo "Cleaning up"
#     ip netns del cs || true
#     ip netns del proxy || true
#     ip netns del server1 || true
#     ip netns del server2 || true
#
#     if [ -z $QID ]; then
#         pkill quilkin || true
#     else
#         kill $QID
#         wait $QID
#     fi
}

trap cleanup EXIT

ip netns del cs &> /dev/nul || true
ip netns del proxy &> /dev/nul || true
ip netns del server1 &> /dev/nul || true
ip netns del server2 &> /dev/nul || true

ip netns add cs
ip netns add proxy
ip netns add server1
ip netns add server2

ip link add veth-cs type veth peer name cs-proxy
ip link add veth-one type veth peer name one-proxy
ip link add veth-two type veth peer name two-proxy

ip link set veth-cs netns cs
ip link set cs-proxy netns proxy
ip link set veth-one netns server1
ip link set one-proxy netns proxy
ip link set veth-two netns server2
ip link set two-proxy netns proxy

PROXY_IP="10.200.0.254"

ip -n proxy link add bri type bridge
ip netns exec proxy ip addr add $PROXY_IP/24 dev bri
ip netns exec proxy ip link set cs-proxy master bri
ip netns exec proxy ip link set one-proxy master bri
ip netns exec proxy ip link set two-proxy master bri

CLIENT_IP="10.200.0.1"
SERVER_ONE="10.200.0.2"
SERVER_TWO="::feed"

ip -n cs addr add $CLIENT_IP/24 dev veth-cs
ip -n server1 addr add $SERVER_ONE/24 dev veth-one
ip -6 -n server2 addr add $SERVER_TWO/24 dev veth-two

ip -n proxy link set bri up
ip -n cs link set veth-cs up
ip -n server1 link set veth-one up
ip -n server2 link set veth-two up

# XDP on a veth has a bit of an annoying requirement in newer kernel versions,
# both sides need to have an XDP program attached for traffic to appear on the
# one we actually want to test
ROOT=$(git rev-parse --show-toplevel)
echo "Adding dummy program"
ip -n cs link set veth-cs xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp
ip -n server1 link set veth-one xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp
ip -n server2 link set veth-two xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp

ip netns exec server1 fortio udp-echo&
ip -6 netns exec server2 fortio udp-echo&
ip netns exec proxy ./target/debug/quilkin \
    --service.udp --service.qcmp --provider.static.endpoints=udp://$SERVER_ONE:8078,udp://[$SERVER_TWO]:8078 \
    --service.udp.backend kernel --service.udp.xdp.network-interface bri \
    --termination-timeout 1s&

QID=$!

echo "::notice file=$source,line=$LINENO::Launching client"
ip netns exec cs fortio load -gomaxprocs $(getconf _NPROCESSORS_ONLN) -qps 0 -n $TX_COUNT udp://$PROXY_IP:7777 2> ./target/logs.txt
logs=$(cat ./target/logs.txt)

ip netns exec proxy ethtool -S bri | grep -oE 'rx_queue_[0-9]+_drops: ([0-9]+)' - | awk -v source=$source -v line=$LINENO '{total += $NF} END { printf("::notice file=%s,line=%d::dropped %d packets\n", source, line, total) }' -

regex="Total Bytes sent: ([0-9]+), received: ([0-9]+)"

if [[ $logs =~ $regex ]]; then
  send=${BASH_REMATCH[1]}
  recv=${BASH_REMATCH[2]}
  # We could be more strict here and require they are exactly equal, but I can't
  # even consistently get that on my local machine so I doubt CI will fair better
  if [[ $recv -ne "0" ]]; then
    echo "::notice file=$source,line=$LINENO::Successfully sent $(numfmt --format='%.2f' --to=iec-i $send) and received $(numfmt --format='%.2f' --to=iec-i $recv)"

    exit 0
  fi

  echo "::error file=$source,line=$LINENO::sent ${send}B but only received ${recv}B"
  exit 1
fi

echo "::error file=$source,line=$LINENO::Failed to find expected log line from UDP client"
exit 2
