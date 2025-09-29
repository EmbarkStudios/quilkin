# Configuration File

While much of Quilkin's proxy configuration can be configured via its
[command line interface](../proxy.md), if you have a larger or more complex configuration
it might be useful to use a configuration file instead.

The following is the schema and reference for Quilkin's proxy configuration
file. See the [examples] folder for example configuration files.

By default, Quilkin will look for a configuration file named `quilkin.yaml` in
its current running directory first, then if not present, in
`/etc/quilkin/quilkin.yaml` on UNIX systems. This can be overridden with the
`-c/--config` command-line argument, or the `QUILKIN_FILENAME`
environment variable.

## Static Configuration

Example of a full configuration for `quilkin proxy` that utlisies a static
endpoint configuration to specify two endpoints with `token` metadata attached to each:

```yaml
{{#include ../../../../examples/proxy.yaml:17:100}}
```

This is a great use of a static configuration file, as we only get a singular `--to` endpoint address via the
command line arguments.

We can also configure [Filters](./filters.md) via the configuration file. See that section for documentation.

## Dynamic Configuration

If you need to dynamically change either Filters and/or Endpoints at runtime, see the [Control Plane](../xds.md)
documentation on the configuration API surface, and built in dynamic management providers.

## Json Schema

The full [JSON Schema](https://json-schema.org/) for the YAML configuration file.

```yaml
type: object
properties:
  version:
    type: string
    description: |
      The configuration file version to use.
    enum:
      - v1alpha1
  id:
      type: string
      description: |
          An identifier for the proxy instance.
      default: On linux, the machine hostname is used as default. On all other platforms a UUID is generated for the proxy.
  port:
      type: integer
      description: |
          The listening port. In "proxy" mode, the port for traffic to be sent to. In "manage" mode, the port to connect to the xDS API.
      default: 7000
  maxmind_db:
    type: string
    description: |
      The remote URL or local file path to retrieve the Maxmind database (requires licence).
  filters:
    type: array
    description: |
      A filter chain.
    items:
      '$ref': {} # Refer to the Filter documentation for a filter configuration schema.
  clusters:
    type: array
    description: |
      grouping of endpoints, per cluster.
    items:
      type: object
      properties:
        endpoints:
          type: array
          description: |
            A list of upstream endpoints to forward packets to.
          items:
            type: object
            description: |
              An upstream endpoint
            properties:
              address:
                type: string
                description: |
                  Socket address of the endpoint. This must be of the ´IP:Port` form e.g `192.168.1.1:7001`
                metadata:
                  type: object
                  description: |
                    Arbitrary key value pairs that is associated with the endpoint.
                    These are visible to Filters when processing packets and can be used to provide more context about endpoints (e.g whether or not to route a packet to an endpoint).
                    Keys must be of type string otherwise the configuration is rejected.
            required:
              - address
```

[examples]: https://github.com/EmbarkStudios/quilkin/blob/{{GITHUB_REF_NAME}}/examples

