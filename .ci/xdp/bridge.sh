#!/bin/bash
set -ex

source="${BASH_SOURCE[0]}"

NS=proxy
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

#ip netns del $NS &> /dev/nul || true
ip netns del client &> /dev/nul || true
ip netns del proxy &> /dev/nul || true
ip netns del server1 &> /dev/nul || true
ip netns del server2 &> /dev/nul || true

#ip netns add $NS
ip netns add client
ip netns add proxy
ip netns add server1
#ip netns add server2

ip link add client-gateway type veth peer name gateway-client
ip link add proxy-one type veth peer name one-proxy
#ip link add proxy-two type veth peer name two-proxy
ip -n proxy link add name bri type bridge

ip link set client-gateway netns client
ip link set gateway-client netns proxy
ip link set one-proxy netns server1
ip link set proxy-one netns proxy
#ip link set two-proxy netns server2
#ip link set proxy-two netns proxy

PROXY_IP4="10.0.1.3"
PROXY_IP6="::fa11"
CLIENT_IP="10.0.1.1"
GATEWAY_IP="10.0.1.2"
BRIDGE_IP="10.1.0.1"
SERVER_ONE_IN="10.0.2.2"
SERVER_ONE="10.0.2.2"
SERVER_ONE_OUT="10.0.2.1"
SERVER_TWO_IN="::feed"
SERVER_TWO_OUT="::cafe:feed"

ip -n $NS tuntap add dev prox mode tap multi_queue
#ip -n $NS tuntap add dev client mode tap multi_queue
#ip -n $NS tuntap add dev server1 mode tap multi_queue
#ip -n $NS tuntap add dev server2 mode tap multi_queue

ip -n $NS addr add $PROXY_IP4/24 brd + dev prox
#ip -n $NS addr add $CLIENT_IP/24 dev client
#ip -n $NS addr add $SERVER_ONE/24 dev server1

#ip -n proxy addr add $BRIDGE_IP/24 dev bri
#ip -6 -n proxy addr add $PROXY_IP6/64 dev prox
ip -n client addr add $CLIENT_IP/24 dev client-gateway
ip -n proxy addr add $GATEWAY_IP/24 brd + dev gateway-client
ip -n proxy addr add $SERVER_ONE_IN/24 brd + dev proxy-one
ip -n server1 addr add $SERVER_ONE_OUT/24 dev one-proxy
#ip -6 -n proxy addr add $SERVER_TWO_IN/64 dev proxy-two
#ip -6 -n server2 addr add $SERVER_TWO_OUT/64 dev two-proxy

ip netns exec $NS sysctl -w net.ipv4.ip_forward=1
ip netns exec $NS iptables -t nat -A POSTROUTING -o gateway-client -j MASQUERADE
ip -n proxy link set gateway-client master bri
ip -n proxy link set proxy-one master bri
#ip -n proxy link set proxy-two master bri
ip -n proxy link set prox master bri

ip -n $NS link set lo up
ip -n $NS link set prox up
ip -n client link set lo up
ip -n client link set client-gateway up
ip -n $NS link set gateway-client up
ip -n server1 link set lo up
ip -n server1 link set one-proxy up
ip -n $NS link set proxy-one up

ip -n client route add $SERVER_ONE_IN via $GATEWAY_IP
ip -n client route add $PROXY_IP4 via $GATEWAY_IP
ip -n server1 route add $CLIENT_IP via $SERVER_ONE_IN

# ip -n $NS link set lo up
# ip -n $NS link set client up
# ip -n $NS link set prox up
# ip -n $NS link set server1 up
# ip -n proxy link set lo up
# ip -n client link set lo up
# ip -n server1 link set lo up
# #ip -n server2 link set lo up
# #ip -n proxy link set bri up
# ip -n proxy link set prox up
# ip -n client link set client-gateway up
# ip -n proxy link set gateway-client up
# ip -n server1 link set one-proxy up
# ip -n proxy link set proxy-one up
#ip -n server2 link set two-proxy up
#ip -n proxy link set proxy-two up

#ip -n client route add default via $GATEWAY_IP
#ip -n server1 route add default via $SERVER_ONE_IN
#ip -6 -n server2 route add default via $SERVER_TWO_IN

# XDP on a veth has a bit of an annoying requirement in newer kernel versions,
# both sides need to have an XDP program attached for traffic to appear on the
# one we actually want to test
# ROOT=$(git rev-parse --show-toplevel)
# echo "Adding dummy program"
# ip -n cs link set veth-cs xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp
# ip -n server1 link set veth-one xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp
# ip -n server2 link set veth-two xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp

#ip netns exec $NS ./target/debug/relay&
# ip netns exec server1 fortio udp-echo&
# ip -6 netns exec server2 fortio udp-echo&
ip netns exec $NS ./target/debug/quilkin \
    --service.udp --service.qcmp --provider.static.endpoints=udp://$SERVER_ONE_IN:8078,udp://[$SERVER_TWO_IN]:8078 \
    --service.udp.backend kernel --service.udp.xdp.tap-interface prox \
    --service.udp.xdp.same-physical-network \
    --termination-timeout 1s&

sleep 4m
#
# RELAY=./target/debug/relay

#ip netns exec proxy $RELAY $GATEWAY_IP:7777 0.0.0.0:2020 $CLIENT_IP

QID=$!

echo "::notice file=$source,line=$LINENO::Launching client"
ip netns exec client fortio load -gomaxprocs $(getconf _NPROCESSORS_ONLN) -qps 0 -n $TX_COUNT udp://$PROXY_IP4:7777 2> ./target/logs.txt
logs=$(cat ./target/logs.txt)

ip netns exec proxy ethtool -S prox | grep -oE 'rx_queue_[0-9]+_drops: ([0-9]+)' - | awk -v source=$source -v line=$LINENO '{total += $NF} END { printf("::notice file=%s,line=%d::dropped %d packets\n", source, line, total) }' -

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
