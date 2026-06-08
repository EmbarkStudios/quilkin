# HTTP Provider

The HTTP provider exposes a REST API that allows game server endpoints and the active
[filter chain][filters] to be managed over plain HTTP. It is useful when you want to
drive Quilkin's configuration from an external system — such as a matchmaker, a fleet
manager, or a test harness — without deploying Kubernetes or a shared database.

By default the server listens on port `9000`:

```sh
quilkin --provider.http
```

A custom address can be specified with `--provider.http.address`:

```sh
quilkin --provider.http --provider.http.address 0.0.0.0:9000
```

## Endpoints API

Quilkin routes incoming UDP packets to one or more [endpoints][endpoints]. The HTTP
provider lets you add, replace, and remove endpoints at runtime.

| Method   | Path                   | Description                                     |
|----------|------------------------|-------------------------------------------------|
| `GET`    | `/endpoints`           | List all current endpoints                      |
| `POST`   | `/endpoints`           | Upsert an endpoint (add or replace by address)  |
| `DELETE` | `/endpoints`           | Remove all endpoints                            |
| `DELETE` | `/endpoints/{address}` | Remove the endpoint at `{address}`              |

Endpoints are represented as JSON objects with an `address` field and an optional
`metadata` field containing [access tokens][tokens]:

```json
{
  "address": "203.0.113.1:7777",
  "metadata": {
    "quilkin.dev": {
      "tokens": ["MXg3aWp5Ng=="]
    }
  }
}
```

`POST /endpoints` performs an **upsert**: if an endpoint with the same address already
exists its metadata is replaced; otherwise a new endpoint is added.

To remove a single endpoint, include its address in the path:

```sh
curl -X DELETE http://localhost:9000/endpoints/203.0.113.1:7777
```

## Filter Chain API

The filter chain determines how packets are processed before they are forwarded to an
endpoint. See [Filters][filters] for the full list of available filters and their
configuration options.

| Method   | Path           | Description                       |
|----------|----------------|-----------------------------------|
| `GET`    | `/filterchain` | Get the current filter chain      |
| `PUT`    | `/filterchain` | Replace the filter chain          |
| `DELETE` | `/filterchain` | Reset the filter chain to empty   |

The filter chain is expressed in the same JSON/YAML format used by the
[configuration file][configuration]:

```sh
curl -X PUT http://localhost:9000/filterchain \
  -H 'Content-Type: application/json' \
  -d '[{"name":"quilkin.filters.debug.v1alpha1.Debug","config":{"id":"http-provider"}}]'
```

`DELETE /filterchain` resets the chain to empty — packets are forwarded without
any processing.

> Changes made through the HTTP provider are applied immediately and affect all
> in-flight configuration readers. They are not persisted to disk; if Quilkin
> restarts the provider starts with an empty configuration again.

[endpoints]: ../services/udp.md#endpoints
[tokens]: ../services/udp.md#specialist-endpoint-metadata
[filters]: ../filters.md
[configuration]: ../deployment/configuration.md
