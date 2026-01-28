# Agones Provider

The [Agones] Provider is built to simplify Quilkin integration with Agones
game server hosting on top of [Kubernetes](https://kubernetes.io).

This provider watches for changes in Agones
[`GameServer` resources](https://agones.dev/site/docs/getting-started/create-gameserver/) in a cluster, and
utilises that information to provide [Endpoint][Endpoints] information to connected Quilkin proxies.

> Currently, the Agones provider can only discover resources within the cluster it is running in.

## Endpoint Configuration

This provider watches the Kubernetes clusters for `Allocated`
[Agones GameServers](https://agones.dev/site/docs/reference/gameserver/#gameserver-state-diagram)
and exposes their IP address and Port as [Endpoints] to any connected Quilkin proxies.

> Since an Agones GameServer can have multiple ports exposed, if multiple ports are in
> use, the server will pick the first port in the port list.

By default the Agones xDS provider will look in the `default` namespace for any `GameServer` resources, but it can be
configured via the `--gameservers-namespace` argument.

### Access Tokens

The set of [access tokens](../../proxy.md#specialist-endpoint-metadata) for the associated Endpoint can be
set by adding a comma separated standard base64 encoded strings. This must be added under an annotation
`quilkin.dev/tokens` in the
[GameServer](https://agones.dev/site/docs/reference/agones_crd_api_reference/#agones.dev/v1.GameServer)'s metadata.

For example:

```yaml
annotations:
   # Sets two tokens for the corresponding endpoint with values 1x7ijy6 and 8gj3v2i respectively.
   quilkin.dev/tokens: MXg3aWp5Ng==,OGdqM3YyaQ==
```

## Filter Configuration

The Agones provider watches for a singular [`ConfigMap`](https://kubernetes.io/docs/concepts/configuration/configmap/) 
that has the label of `quilkin.dev/configmap: "true"`, and any changes that happen to it, and use its contents to 
send [Filter] configuration to any connected Quilkin proxies.

The `ConfigMap` contents should be a valid Quilkin [file configuration][configuration], but with no 
Endpoint data.

For example:

```yaml
{{#include ../../../examples/agones-xonotic-xds/xds-control-plane.yaml:config-map}}
```

By default the Agones xDS provider will look in the `default` namespace for this `ConfigMap`, but it can be
configured via the `--config-namespace` argument.

## Usage

As an example, the following runs quilkin against a cluster (using default
kubeconfig authentication) in the `default` namespace.

```sh
quilkin --provider.k8s.agones --provider.k8s.agones.namespace=default
```

For a full referenmce of deploying this provider in a Kubernetes cluster, with appropriate [Deployments], [Services],
and [RBAC] Rules, there is an [Agones, xDS and Xonotic example][example].

[Agones]: https://agones.dev
[Endpoints]: ../../proxy.md#endpoints
[Deployments]: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
[Services]: https://kubernetes.io/docs/concepts/services-networking/service/
[RBAC]: https://kubernetes.io/docs/reference/access-authn-authz/rbac/
[example]: https://github.com/EmbarkStudios/quilkin/tree/{{GITHUB_REF_NAME}}/examples/agones-xonotic-xds
[Filter]: ../../../services/proxy/filters.md
[configuration]: ../../../services/proxy/configuration.md
