# Control Plane Relay

| services | ports | Protocol |
|----------|-------|-----------|
| QCMP | 7600 | UDP(IPv4 && IPv6) |

> **Note:** This service is currently in active experimentation and development
  so there may be bugs which cause it to be unusable  for production, as always
  all bug reports are welcome and appreciated. 

For multi-cluster integration, Quilkin provides a `agent` service, that can be
deployed to a cluster to act as a beacon for QCMP pings and forward cluster
configuration information to a `relay` service

To view all options for the `agent` subcommand, run:

```shell
$ quilkin agent --help
{{#include ../../../target/quilkin.agent.commands}}
```

## Quickstart
The simplest version of the `agent` service is just running `quilkin agent`,
this will setup just the QCMP service allowing the agent to be pinged for
measuring round-time-trips (RTT).

```
quilkin agent
```

To run an agent with the relay (see [`relay` quickstart](./relay.md#quickstart)
for more information), you just need to specify the relay endpoint with the
`--relay` flag **and** provide a configuration discovery provider such as a
configuration file or Agones.

```
quilkin --admin-adress http://localhost:8001 agent --relay http://localhost:7900 file quilkin.yaml
```

Now if we run cURL on both the relay and the control plane we should see that
they both contain the same set of endpoints.

```bash
# Check Agent
curl localhost:8001/config
# Check Relay
curl localhost:8000/config
```
