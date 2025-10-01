#!/bin/bash
set -eu

source="${BASH_SOURCE[0]}"

proxy_image="localhost:$REGISTRY_PORT/quilkin:ci"

echo "::notice file=$source,line=$LINENO::Building quilkin proxy"
cargo build -p quilkin --bin quilkin
# strip the binary to reduce copy times into the dockerfile
strip ./target/debug/quilkin

echo "::notice file=$source,line=$LINENO::Building quilkin image"
docker build -f .ci/xdp/proxy.dockerfile -t "${proxy_image}" .
echo "::notice file=$source,line=$LINENO::Pushing quilkin image"
docker push "${proxy_image}"

echo "::notice file=$source,line=$LINENO::Starting UDP echo server"
kubectl apply --context "$CLUSTER" -f .ci/xdp/server.yaml
kubectl wait --context "$CLUSTER" --for=condition=ready pod/echo-server

server_ip=$(kubectl get --context "$CLUSTER" pod echo-server --template '{{.status.podIP}}')

echo "::notice file=$source,line=$LINENO::Starting quilkin proxy"
kubectl apply --context "$CLUSTER" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: proxy
spec:
  containers:
  - name: proxy
    image: "$proxy_image"
    ports:
    - containerPort: 7777
      hostPort: 7777
    args: ["--service.udp", "--provider.static.endpoints=${server_ip}:8078", "--service.udp.xdp"]
    securityContext:
      capabilities:
        add:
        - CAP_BPF # We load an eBPF program
        - CAP_NET_RAW # We create SOCK_RAW (XDP) sockets
EOF

kubectl wait --context "$CLUSTER" --for=condition=ready pod/proxy

proxy_ip=$(kubectl get --context "$CLUSTER" pod proxy --template '{{.status.podIP}}')

echo "::notice file=$source,line=$LINENO::Running UDP client"
kubectl apply --context "$CLUSTER" -f - <<EOF
apiVersion: v1
kind: Job
metadata:
name: client
spec:
  containers:
  - name: client
    image: fortio/fortio
    args: ["load", "-n", "100", "udp://${proxy_ip}:7777"]
EOF

logs=$(kubectl logs -f --context "$CLUSTER" job/client)

echo "::notice file=$source,line=$LINENO::Finished sending client requests"

# Total Bytes sent: 30, received: 0
regex="Total Bytes sent: (\d+), received: (\d+)"

if [[ $logs =~ $regex ]]; then
  send=${BASH_REMATCH[1]}
  recv=${BASH_REMATCH[2]}
  if [[ $send -eq $recv ]]; then
    echo "::notice file=$source,line=$LINENO::Successfully sent and received {$recv}b"
    exit 0
  fi

  echo "::error file=$source,line=$LINENO::sent ${send}b but only received ${recv}b"
  exit 1
fi

echo "::error file=$source,line=$LINENO::Failed to find expected log line from UDP client"
exit 2
