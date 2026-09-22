#!/bin/bash
set -e

source="${BASH_SOURCE[0]}"

PARSED=$(getopt -o m,t:,a:,p:,r -l use-max-queues,tx-count:,allocated-packets:,thread-policy:,pin,release -- "$@")

if [ $? -ne 0 ]; then
    echo "Error: invalid option supplied" >&2
    exit 1
fi

eval set -- "$PARSED"

USE_MAX_QUEUES=false
TX_COUNT=10000
ALLOCATED_PACKETS=8192
SCHEDULE_POLICY=SCHED_OTHER
TARGET=debug

while [ $# -gt 0 ]; do
    case "$1" in
        -m | --use-max-queues)
            USE_MAX_QUEUES=true
            shift
            ;;
        -t | --tx-count)
            TX_COUNT=$2
            shift 2
            ;;
        -a | --allocated-packets)
            ALLOCATED_PACKETS=$2
            shift 2
            ;;
        -p | --thread-policy)
            SCHEDULE_POLICY=$2
            shift 2
            ;;
        --pin)
            PIN_CORES=true
            shift
            ;;
        -r | --release)
            TARGET=release
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Error: invalid option $1" >&2
            exit 1
            ;;
    esac
done

# Print kernel version just for confirmation in CI
echo "::notice file=$source,line=$LINENO::Kernel $(uname -r)"

cleanup() {
    echo "Cleaning up"
    ip netns del cs || true
    ip netns del proxy || true

    pkill fortio || true
    pkill quilkin || true
}

trap cleanup EXIT

ip netns del cs &> /dev/nul || true
ip netns del proxy &> /dev/nul || true

echo "::notice file=$source,line=$LINENO::Creating network namespaces"
ip netns add cs
ip netns add proxy

echo "::notice file=$source,line=$LINENO::Adding client <-> proxy <-> server links"
ip link add veth-cs type veth peer name veth-proxy

ip link set veth-cs netns cs
ip link set veth-proxy netns proxy

# By default, veth interfaces only get 1 queue, even though they can scale up to the core count on the host, so manually
# set it after creation if requested
if [ "$USE_MAX_QUEUES" = true ]; then
    regex="Pre-set maximums:\s+RX:\s+([0-9]+)"
    channels=$(ip netns exec cs ethtool -l veth-cs)

    if [[ $channels =~ $regex ]]; then
        max=${BASH_REMATCH[1]}

        echo "::notice file=$source,line=$LINENO::Adjusting veth pair to maximum of $max"

        ip netns exec cs ethtool -L veth-cs rx $max tx $max
        ip netns exec proxy ethtool -L veth-proxy rx $max tx $max
    else
        echo "::error file=$source,line=$LINENO::failed to acquire veth channel max"
        exit 3
    fi
fi

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
ip netns exec proxy ./target/$TARGET/quilkin \
    --service.udp --service.qcmp --provider.static.endpoints=$OUTSIDE_IP:8078 \
    --service.udp.backend kernel --service.udp.xdp.network-interface veth-proxy \
    --service.udp.xdp.schedule-policy $SCHEDULE_POLICY ${PIN_CORES:+--service.udp.xdp.pin-to-core} \
    --service.udp.xdp.packets-per-queue $ALLOCATED_PACKETS&

echo "::notice file=$source,line=$LINENO::Launching client"
ip netns exec cs fortio load -gomaxprocs $(getconf _NPROCESSORS_ONLN) -qps 0 -n $TX_COUNT udp://$PROXY_IP:7777 2> ./target/logs.txt
logs=$(cat ./target/logs.txt)

ip netns exec proxy ethtool -S veth-proxy | grep -oE 'rx_queue_[0-9]+_drops: ([0-9]+)' - | awk -v source=$source -v line=$LINENO '{total += $NF} END { printf("::notice file=%s,line=%d::dropped %d packets\n", source, line, total) }' -

regex="Total Bytes sent: ([0-9]+), received: ([0-9]+)"

if [[ $logs =~ $regex ]]; then
  send=${BASH_REMATCH[1]}
  recv=${BASH_REMATCH[2]}
  # We could be more strict here and require they are exactly equal, but I can't
  # even consistently get that on my local machine so I doubt CI will fair better
  if [[ $recv -ne "0" ]]; then
    echo "::notice file=$source,line=$LINENO::Successfully sent $(numfmt --format='%.2f' --to=iec-i $send) and received $(numfmt --format='%.2f' --to=iec-i $recv)"

    # Now test QCMP pings which was also enabled in the proxy
    ip netns exec cs ./target/$TARGET/quilkin qcmp ping $PROXY_IP:7600

    exit 0
  fi

  echo "::error file=$source,line=$LINENO::sent ${send}B but only received ${recv}B"
  exit 1
fi

echo "::error file=$source,line=$LINENO::Failed to find expected log line from UDP client"
exit 2
