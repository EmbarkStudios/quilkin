# Administration

| services       | ports | Protocol            |
|----------------|-------|---------------------|
| Administration | 8000  | HTTP (IPv4 OR IPv6) |

## Logging
By default, Quilkin will log `INFO` level events, you can change this by setting
the `RUST_LOG` environment variable. See [`log` documentation][log-docs] for
more advanced usage.

> If you are debugging Quilkin set the `RUST_LOG` environment variable to `quilkin=trace`, to filter trace level
> logging to only Quilkin components.

>  Verbose logging in Quilkin will affect performance considerably, so we recommend only 
> turning it on for debugging purposes.

## HTTP API

Quilkin exposes an HTTP interface to query different aspects of the server.

> It is assumed that the administration interface will only ever be able to be accessible on `localhost`.

By default, the administration interface is bound to `[::]:8000`, but it can be
configured with the `--admin-address` CLI flag or the `QUILKIN_ADMIN_ADDRESS`
environment.

## Endpoints

The admin interface provides the following endpoints:

### /live

This provides a liveness probe endpoint, most commonly used in
[Kubernetes based systems](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command).

Liveness is defined as "hasn't panicked", as long as the process has not
panicked quilkin is considered live.

### /ready

This provides a readiness probe endpoint, most commonly used in
[Kubernetes based systems](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes).

Readiness is service and provider specific, so based on what you're running
there will be different criteria for a service to be considered ready. Here's
a list of the criteria for each service an provider.

| Service | Readiness                                                           |
|---------|---------------------------------------------------------------------|
| Proxy   | Management server is connected (or always true if config is static)  OR if there is more than one endpoint configured|
| Manage  | Provider is ready                                                   |
| Relay   | Provider is ready                                                   |
| Agent   | Provider is ready AND connected to relay                            |
   
<br/>

| Provider | Readiness                                  |
|----------|--------------------------------------------|
| Agones   | The service is connected to kube-api       |
| File     | The service has found and watches the file |

When setting thresholds for your `proxy` probes, you generally want to set a low
check period (e.g.  `periodSeconds=1`) and a low success threshold
(e.g. `successThreshold=1`), but a high `failureThreshold`
(e.g. `failureThreshold=60`) and `terminationGracePeriodSeconds` to allow for
backoff attempts and existing player sessions to continue without disruption.

#### Proxy Mode

Will return an HTTP status of 200 when there is at least one endpoint to send data to. This is primarily to ensure
that new proxies that have yet to get configuration information from an [xDS server](../services/xds.md) aren't send data
until they are fully populated.

#### xDS Provider Mode

Will return an HTTP status of 200 when all health checks pass.

### /metrics

Outputs [Prometheus](https://prometheus.io/) formatted metrics for this instance.

See the [Proxy Metrics](../services/proxy/metrics.md) documentation for what proxy metrics are available.

See the [xDS Metrics](../services/xds/metrics.md) documentation for what xDS metrics are available.

#### Heap Allocation Metrics

Quilkin exposes the following metrics on heap allocations, which are useful for performance observability:


* `quilkin_allocation_bytes_total` (Counter)
    
    The total number of allocated bytes.

* `quilkin_allocation_total` (Counter)

    The total number of allocations.

* `quilkin_extant_allocation_size` (Gauge)

    The current total of extant allocation bytes.

 * `quilkin_extant_allocation_count` (Gauge)

    The current number of extant allocations

* `quilkin_shutdown_initiated`

    Shutdown process has been started

### /debug/pprof/profile

This provides a endpoint to profile Quilkin's performance. You can use with any
system which supports pprof output such as [Pyroscope](https://pyroscope.io).

This requires setting up a writable `/tmp` directory in the Quilkin container. E.g.

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: quilkin
        # ...Other container configuration...
          volumeMounts:
            - mountPath: /tmp
              name: tmp
              subPath: tmp
      volumes:
        - name: tmp
          emptyDir:
            medium: Memory
            sizeLimit: 64Mi
```


### /config

Returns a JSON representation of the cluster and filterchain configuration that the instance is running
with at the time of invocation.

[log-docs]: https://docs.rs/env_logger/latest/env_logger/#enabling-logging
