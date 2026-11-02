#!/bin/bash
set -e

source="${BASH_SOURCE[0]}"

TX_COUNT=10000

P=proxy
C=client
N=net
S1=server1
S2=server2
NS=($P $C $N $S1 $S2)
QID=
BRIDGE=bri

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

for i in "${NS[@]}"
do
	ip netns del $i &> /dev/nul || true
	ip netns add $i
done

for i in "${NS[@]}"
do
    ip link add $i-out netns $i type veth peer name $i-in netns $N
done

#ip link add proxy-two type veth peer name two-proxy
ip -n $N link add name $BRIDGE type bridge

#ip link set client-gateway netns client
#ip link set gateway-client netns $P
#ip link set one-proxy netns server1
#ip link set proxy-one netns $P

PROXY_IP4="10.0.1.1"
PROXY_IP6="::fff1"
CLIENT_IP="10.0.1.2"
SERVER_ONE_IP="10.0.1.3"
SERVER_TWO_IP="::fff2"

#ip -n $P tuntap add dev prox mode tap multi_queue
#ip -n $NS tuntap add dev client mode tap multi_queue
#ip -n $NS tuntap add dev server1 mode tap multi_queue
#ip -n $NS tuntap add dev server2 mode tap multi_queue

ip -n $P addr add $PROXY_IP4/24 brd + dev $P-out
ip -6 -n $P addr add $PROXY_IP6/64 dev $P-out
ip -n $C addr add $CLIENT_IP/24 dev $C-out
ip -n $S1 addr add $SERVER_ONE_IP/24 dev $S1-out
#ip -n $NS addr add $CLIENT_IP/24 dev client
#ip -n $NS addr add $SERVER_ONE/24 dev server1

#ip -n proxy addr add $BRIDGE_IP/24 dev bri
#ip -6 -n proxy addr add $PROXY_IP6/64 dev prox

#ip -n proxy addr add $GATEWAY_IP/24 brd + dev gateway-client
#ip -n proxy addr add $SERVER_ONE_IN/24 brd + dev proxy-one

#ip -6 -n proxy addr add $SERVER_TWO_IN/64 dev proxy-two
#ip -6 -n server2 addr add $SERVER_TWO_OUT/64 dev two-proxy

#ip netns exec $N sysctl -w net.ipv4.ip_forward=1
#ip netns exec $NS iptables -t nat -A POSTROUTING -o gateway-client -j MASQUERADE

for i in "${NS[@]}"
do
    ip -n $N link set "$i-in" master $BRIDGE
done

function up () {
  ip -n $1 link set lo up
  ip -n $1 link set $1-out up
  ip -n $N link set $1-in up
}

for i in "${NS[@]}"
do
	up $i
done

ip -n $N link set $BRIDGE up
ip -n $N link set lo up

#ip -n proxy route del  192.168.5.0/24  dev veth1R
#nsenter -t $pid_netnsR -u -n ip route del  192.168.5.0/24  dev veth2R
#ip -n $P route add $SERVER_ONE_OUT via $SERVER_ONE_IN
#ip -n client route add $PROXY_IP4 via $GATEWAY_IP
#ip -n $P route add $CLIENT_IP via $GATEWAY_IP

#ip netns exec client ping -I $C-out -c 2 $SERVER_ONE_IP

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
ROOT=$(git rev-parse --show-toplevel)
ip -n $N link set $P-in xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp
# ip -n server1 link set veth-one xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp
# ip -n server2 link set veth-two xdpgeneric obj "$ROOT/crates/xdp/bin/dummy.bin" sec xdp

#ip netns exec $NS ./target/debug/relay&
ip netns exec $S1 fortio udp-echo&
# ip -6 netns exec server2 fortio udp-echo&
ip netns exec $P ./target/debug/quilkin \
    --service.udp --service.qcmp --provider.static.endpoints=$SERVER_ONE_IP:8078 \
    --service.udp.backend kernel --service.udp.xdp.network-interface $P-out \
    --service.udp.xdp.same-physical-network \
    --termination-timeout 1s&

QID=$!

echo "::notice file=$source,line=$LINENO::Launching client"
ip netns exec $C fortio load -gomaxprocs $(getconf _NPROCESSORS_ONLN) -qps 0 -n $TX_COUNT udp://$PROXY_IP4:7777 2> ./target/logs.txt
logs=$(cat ./target/logs.txt)

ip netns exec $P ethtool -S $P-out | grep -oE 'rx_queue_[0-9]+_drops: ([0-9]+)' - | awk -v source=$source -v line=$LINENO '{total += $NF} END { printf("::notice file=%s,line=%d::dropped %d packets\n", source, line, total) }' -

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
