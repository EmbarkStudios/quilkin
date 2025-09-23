# xDS Control Plane

| services | ports | Protocol            |
|----------|-------|---------------------|
| xDS      | 7800  | gRPC (IPv4 OR IPv6) |

For single-cluster integration, Quilkin provides a `manage` service, that can be
used with a number of configuration discovery providers to provide cluster
configuration multiple [`proxy`s](./proxy.md). With each provider automating the
complexity of a full xDS management control plane via integrations with popular
projects and common architecture patterns.

To view all the providers and options for the `manage` subcommand, run:

```shell
$ quilkin manage --help
{{#include ../../../target/quilkin.manage.commands}}
```

## Overview

In addition to static configuration provided upon startup, a Quiklin proxy's configuration can also be updated at
runtime. The proxy can be configured on startup to talk to a set of management servers which provide it with updates
throughout its lifecycle.

Communication between the proxy and management server uses the [xDS gRPC protocol][xDS], similar to an [envoy proxy].
xDS is one of the standard configuration mechanisms for software proxies and as a result, Quilkin can be setup to
discover configuration resources from any API compatible server. Also, given that the protocol
is [well specified][xDS-protocol], it is similarly straight-forward to implement a custom server to suit any
deployment's needs.

As described within the [xDS-api] documentation, the xDS API comprises a set of resource discovery APIs, each serving a
specific set of configuration resource types, while the protocol itself comes in several [variants][xds-variants].
Quilkin implements the **Aggregated Discovery Service (ADS)** _[Incremental xDS][incremental-xds]_ variant with gRPC.

## xDS Configuration Resources

Heavily influenced by xDS's [Cluster Discovery Service][CDS], [Endpoint Discovery Service][EDS], and 
[Listener Discovery Service][LDS], Quilkin utilises its own custom Configuration xDS resources 
[(reference)][config.proto] to enable dynamic configuration of Datacenters, Clusters, Endpoints and Filters through 
the Aggregated Discovery Service.

This [above reference][config.proto] also provides documentation for all the ProtoBuf definitions for all 
[Filters][filters-doc] that are available in Quilkin.

## Connecting to an xDS management server

Connecting a Quilkin proxy to an xDS management server can be implemented via providing one or more URLs to
the `management_servers` [command line](../../api/quilkin/struct.Proxy.html#structfield.management_server).


[xDS]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#xds-rest-and-grpc-protocol
[incremental-xds]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#incremental-xds
[envoy proxy]: https://www.envoyproxy.io/docs/envoy/latest/
[xDS-protocol]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#the-xds-transport-protocol
[xDS-api]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/dynamic_configuration
[config.proto]: ./xds/proto/index.md#quilkin_config_v1alpha1_config-proto
[CDS]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/dynamic_configuration#cds
[EDS]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/dynamic_configuration#eds
[LDS]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/dynamic_configuration#lds
[locality]: https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/base.proto#config-core-v3-locality
[filters-doc]: ./proxy/filters.md
[xds-variants]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#variants-of-the-xds-transport-protocol
[filter-protos]: https://github.com/EmbarkStudios/quilkin/tree/{{GITHUB_REF_NAME}}/proto/quilkin/filters
[xds-endpoint-metadata]: https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/base.proto#envoy-v3-api-msg-config-core-v3-metadata

