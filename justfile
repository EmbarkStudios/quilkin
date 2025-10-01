id := "0"
icao := "NZSP"
region := "antarctica"
[private]
_agent_config_path := join(source_directory(), 'quilkin-agent-' + id + '.yaml')
[private]
_default_agent_config := """
clusters: # grouping of clusters
  - endpoints: # array of potential endpoints to send on traffic to
    - address: 127.0.0.1:26000
      metadata: # Metadata associated with the endpoint
        quilkin.dev:
          tokens:
            - MXg3aWp5Ng== # the connection byte array to route to, encoded as base64 (string value: 1x7ijy6)
            - OGdqM3YyaQ== # (string value: 8gj3v2i)
    - address: 127.0.0.1:26001
      metadata: # Metadata associated with the endpoint
        quilkin.dev:
          tokens:
            - bmt1eTcweA== # (string value: nkuy70x)
"""
[private]
_helptext := """
Recipes for running Quilkin locally:
    To avoid port collision the different components have their ports
    namespaced, and the last digit is the id of the instance:
        relay: 1XXXY
        proxy: 2XXXY
        agent: 3XXXY

    So e.g. an agent with id = 1 will server metrics under 127.0.0.1:38001/metrics

    To start a complete quilkin system, run these recipes in separate shells:
    - just start_relay
    - just start_proxy
    - just start_agent

    To start another separate system, set a different id:
    - just id=1 start_relay
    - just id=1 start_proxy 1
    - just id=1 start_agent 1
"""

_default:
    @just --list --justfile {{ justfile() }}
    @echo "Variables:"
    @echo "    [" $(just --variables --justfile {{ justfile() }}) "]"

_check_agent_config:
    #!/usr/bin/env sh
    if ! {{ path_exists(_agent_config_path) }}; then
        echo "Generating default agent config..."
        echo -n "{{ _default_agent_config }}" > {{ _agent_config_path }}
    fi

# Print helptext on how to use the recipes
help:
    @echo -n "{{ _helptext }}"

# Start a quilkin relay
start_relay:
    cargo run -- \
        --admin.address=127.0.0.1:1800{{ id }} \
        --service.id=relay-{{ id }} \
        --service.qcmp --service.qcmp.port=1760{{ id }} \
        --service.xds --service.xds.port=1780{{ id }} \
        --service.mds --service.mds.port=1790{{ id }}

# Start a quilkin proxy connected to relay `relay_id`
start_proxy relay_id="0":
    cargo run -- \
        --admin.address=127.0.0.1:2800{{ id }} \
        --service.id=proxy-{{ id }} \
        --service.qcmp --service.qcmp.port=2760{{ id }} \
        --service.phoenix --service.phoenix.port=2760{{ id }} \
        --service.udp \
        --provider.xds.endpoints=http://127.0.0.1:1780{{ relay_id }}

# Start a quilkin agent connected to relay `relay_id`
start_agent relay_id="0": _check_agent_config
    cargo run -- \
        --admin.address 127.0.0.1:3800{{ id }} \
        --service.id=agent-{{ id }} \
        --config={{ _agent_config_path }} \
        --locality.icao={{ icao }} \
        --locality.region={{ region }} \
        --service.qcmp --service.qcmp.port=3760{{ id }} \
        --provider.mds.endpoints=http://localhost:1790{{ relay_id }}
