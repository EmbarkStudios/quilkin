#!/usr/bin/env bash
# Bring up a local kind cluster with Agones and run the Quilkin Agones integration tests.
#
# Usage: setup-kind.sh [all|deps|create|agones|image|run|diagnostics|teardown] [-- <nextest args>]
#        `all` (the default) does the lot, building the image while the cluster comes up.
#        Configurable via the environment variables defined below.

set -euo pipefail

readonly CLUSTER_NAME="${CLUSTER_NAME:-quilkin}"
readonly KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.33.1}"
readonly KIND_VERSION="${KIND_VERSION:-v0.29.0}"
readonly AGONES_VERSION="${AGONES_VERSION:-1.50.0}"
readonly IMAGE="${IMAGE_TAG:-quilkin:ci}"
readonly CARGO_PROFILE="${CARGO_PROFILE:-dev}"
readonly NEXTEST_PROFILE="${NEXTEST_PROFILE:-default}"

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

# Node IPs aren't routable from the host under rootless Docker/Podman, so we publish the
# data-path ports to localhost (kind extraPortMappings) and point the tests there
# (AGONES_HOST_OVERRIDE); this also works rootful. Tests run in parallel, so each proxy uses a
# distinct port from PROXY_MIN_PORT..PROXY_MAX_PORT; this must stay below the Agones GameServer
# range GS_MIN_PORT..GS_MAX_PORT so a GameServer can't grab a proxy port.
readonly DATA_PATH_HOST="127.0.0.1"
readonly PROXY_MIN_PORT=7005
readonly PROXY_MAX_PORT=7009
readonly GS_MIN_PORT=7010
readonly GS_MAX_PORT=7039

# GameServer image used by the tests, pre-loaded so the first Fleet doesn't time out on a cold
# pull. Keep in sync with GAMESERVER_IMAGE in crates/agones/src/lib.rs.
readonly GAMESERVER_IMAGE="us-docker.pkg.dev/agones-images/examples/simple-game-server:0.16"

log() { echo "==> $*" >&2; }

# kind's docker provider mis-detects cgroup delegation when `docker` is actually podman, failing
# with a spurious Delegate=yes error; its native podman provider works, so pick it automatically.
# Detect podman from DOCKER_HOST (reliable, offline) or the server (which may be transiently
# unreachable), so a momentary `docker version` hiccup doesn't fall back to the docker provider.
detect_container_provider() {
  if [ -n "${KIND_EXPERIMENTAL_PROVIDER:-}" ]; then return 0; fi
  if [[ "${DOCKER_HOST:-}" == *podman* ]] || docker version 2>/dev/null | grep -qi podman; then
    export KIND_EXPERIMENTAL_PROVIDER=podman
    log "using kind's podman provider (the docker CLI targets podman)"
  fi
}

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: '$1' is required but not installed. $2" >&2
    exit 1
  }
}

deps() {
  require docker "See https://docs.docker.com/get-docker/"
  require kubectl "See https://kubernetes.io/docs/tasks/tools/#kubectl"
  require helm "See https://helm.sh/docs/intro/install/"
  if ! command -v kind >/dev/null 2>&1; then
    log "installing kind ${KIND_VERSION}"
    local sudo=""
    [ -w /usr/local/bin ] || sudo="sudo"
    curl -fsSLo /tmp/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
    chmod +x /tmp/kind
    $sudo mv /tmp/kind /usr/local/bin/kind
  fi
}

# Rootless kind needs the cpuset and io cgroup controllers delegated to the user session (systemd
# delegates only cpu/memory/pids by default). Detect the gap and print the one-time fix.
preflight_rootless_cgroups() {
  docker info 2>/dev/null | grep -qi rootless || return 0

  local uid base controllers
  uid="$(id -u)"
  base="/sys/fs/cgroup/user.slice/user-${uid}.slice/user@${uid}.service"
  [ -r "$base/cgroup.controllers" ] || return 0
  controllers=" $(cat "$base/cgroup.controllers") "

  local missing=()
  for c in cpuset io; do
    [[ "$controllers" == *" $c "* ]] || missing+=("$c")
  done
  [ ${#missing[@]} -eq 0 ] && return 0

  cat >&2 <<EOF
error: rootless container runtime detected, but the cgroup controller(s) '${missing[*]}'
are not delegated to your user session, which kind requires. Enable delegation once on the
host and restart the user session:

  sudo mkdir -p /etc/systemd/system/user@.service.d
  printf '[Service]\nDelegate=yes\n' | sudo tee /etc/systemd/system/user@.service.d/delegate.conf
  sudo systemctl daemon-reload
  sudo systemctl restart user@${uid}.service   # under WSL2 you may instead need 'wsl --shutdown'

See https://kind.sigs.k8s.io/docs/user/rootless/
EOF
  return 1
}

# kind config publishing the proxy and GameServer ports to localhost over UDP.
kind_config() {
  cat <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
EOF
  local port
  for port in $(seq "$PROXY_MIN_PORT" "$PROXY_MAX_PORT") $(seq "$GS_MIN_PORT" "$GS_MAX_PORT"); do
    echo "  - {containerPort: $port, hostPort: $port, listenAddress: \"$DATA_PATH_HOST\", protocol: UDP}"
  done
}

create() {
  preflight_rootless_cgroups
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    # reuse only if reachable; recreate a half-created/dead cluster from an interrupted run.
    if kubectl --context "kind-${CLUSTER_NAME}" get --raw /healthz >/dev/null 2>&1; then
      log "kind cluster '$CLUSTER_NAME' already exists and is reachable"
      return 0
    fi
    log "kind cluster '$CLUSTER_NAME' exists but is unreachable; recreating"
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  log "creating kind cluster '$CLUSTER_NAME' ($KIND_NODE_IMAGE)"
  kind_config | kind create cluster --name "$CLUSTER_NAME" --image "$KIND_NODE_IMAGE" \
    --wait 120s --config -
}

agones() {
  log "installing Agones $AGONES_VERSION"
  helm repo add agones https://agones.dev/chart/stable >/dev/null
  helm repo update agones >/dev/null
  helm upgrade --install agones agones/agones \
    --namespace agones-system --create-namespace \
    --version "$AGONES_VERSION" \
    --values "$SCRIPT_DIR/helm.yaml" \
    --set "gameservers.minPort=$GS_MIN_PORT" \
    --set "gameservers.maxPort=$GS_MAX_PORT" \
    --wait --timeout 5m
  # wait for everything, incl. the GameServer admission webhook, to be ready.
  log "waiting for Agones to be ready"
  kubectl wait --for=condition=Available --timeout=300s --all deployments -n agones-system
}

build_image() {
  log "building image $IMAGE (profile=$CARGO_PROFILE)"
  local cache_args=()
  if [ -n "${BUILDCACHE_REF:-}" ]; then
    cache_args+=(--cache-from "type=registry,ref=${BUILDCACHE_REF}")
    if [ "${BUILDCACHE_WRITE:-}" = "true" ]; then
      cache_args+=(--cache-to "type=registry,ref=${BUILDCACHE_REF},mode=max")
    fi
  fi
  docker buildx build --load \
    --tag "$IMAGE" \
    --build-arg "CARGO_PROFILE=${CARGO_PROFILE}" \
    --build-arg "BUILDKIT_CONTEXT_KEEP_GIT_DIR=1" \
    "${cache_args[@]}" \
    "$REPO_ROOT"
}

# Pre-pull the (public) GameServer image on the node via crictl, so the first Fleet doesn't time
# out on a cold pull. On the node, not `docker pull`, to dodge host cred helpers (gcloud/*.pkg.dev).
warm_gameserver_image() {
  local node
  node="$(kind get nodes --name "$CLUSTER_NAME" | head -1)"
  log "pre-pulling gameserver image $GAMESERVER_IMAGE into node $node"
  docker exec "$node" crictl pull "$GAMESERVER_IMAGE"
}

load_image() {
  log "loading $IMAGE into kind cluster '$CLUSTER_NAME'"
  kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"
  warm_gameserver_image
}

image() {
  build_image
  load_image
}

run() {
  log "running Agones integration tests"
  # tests run in parallel; DELETE_DELAY_SECONDS keeps each test's namespace-cleanup from
  # deleting another test's freshly-created namespace.
  ( cd "$REPO_ROOT" && IMAGE_TAG="$IMAGE" AGONES_HOST_OVERRIDE="$DATA_PATH_HOST" \
    DELETE_DELAY_SECONDS=600 \
    cargo nextest run --profile "$NEXTEST_PROFILE" -p agones "$@" )
}

diagnostics() {
  log "cluster diagnostics"
  kubectl get nodes -o wide || true
  kubectl get all --all-namespaces || true
  kubectl get gameservers,fleets --all-namespaces || true
  kubectl describe nodes || true
}

teardown() {
  log "deleting kind cluster '$CLUSTER_NAME'"
  kind delete cluster --name "$CLUSTER_NAME"
}

all() {
  deps
  # build the image while the cluster comes up; capture setup output so it doesn't interleave
  # with the build, and is shown if the background setup fails.
  local setup_log
  setup_log="$(mktemp)"
  ( create && agones ) >"$setup_log" 2>&1 &
  local setup_pid=$!
  build_image
  if ! wait "$setup_pid"; then
    log "cluster setup failed:"
    cat "$setup_log" >&2
    return 1
  fi
  cat "$setup_log" >&2
  load_image
  run "$@"
}

main() {
  detect_container_provider
  local cmd="${1:-all}"
  if [ $# -gt 0 ]; then shift; fi
  # Allow `... run -- --no-capture`; strip a leading `--` separator.
  if [ "${1:-}" = "--" ]; then shift; fi
  case "$cmd" in
    all | deps | create | agones | build_image | load_image | image | run | diagnostics | teardown)
      "$cmd" "$@"
      ;;
    *)
      echo "error: unknown command '$cmd'" >&2
      sed -n '2,60p' "$0" | grep -E '^#' | sed 's/^# \{0,1\}//' >&2
      exit 1
      ;;
  esac
}

main "$@"
