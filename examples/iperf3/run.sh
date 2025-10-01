#!/usr/bin/env bash
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

set -eo pipefail
set +x

mkdir -p /quilkin

# Number of parallel streams to test simultaneously. Default: (maximum: 128)
PARALLEL="${PARALLEL:-128}"
# The bitrate for each stream.
BANDWIDTH="${BANDWIDTH:-10M}"
# The size of the packet payload. Default: The size of a UDP packet with no
# IPv4 segmentation.
MTU="${MTU:-512}"
# The port for the iperf client, useful for comparing direct connections with
# proxied connections.
PORT="${PORT:-7777}"

# This tunnel is needed because iperf3 requires a tcp handshake before starting the UDP load test
echo "Starting socat tcp tunnel..."
socat tcp-listen:$PORT,reuseaddr,fork tcp:localhost:8001 > /quilkin/socat.log &

echo "Starting iperf3 server..."
iperf3 --server --interval 10 --port 8001 > /quilkin/server.log &

echo "Starting quilkin server..."
quilkin --service.udp --provider.static.endpoints=127.0.0.1:8001 > /quilkin/quilkin.log &

echo "Waiting for startup..."
# Wait for both processes to start up.
sleep 5

set -x
iperf3 --client 127.0.0.1 --port $PORT -l $MTU --interval 10 --parallel $PARALLEL --bidir --bandwidth $BANDWIDTH --time 60 --udp | tee /quilkin/client.log
set +x

echo "Taking a snapshot of Quilkin metrics..."
wget -q -O /quilkin/metrics.json http://localhost:8000/metrics
