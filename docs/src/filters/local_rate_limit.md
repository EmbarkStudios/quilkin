# LocalRateLimit

The LocalRateLimit filter controls the frequency at which packets received downstream are forwarded upstream by the proxy.
Rate limiting is done independently per source (IP, Port) combination.

## Filter name
```text
quilkin.filters.local_rate_limit.v1alpha1.LocalRateLimit
```

## Configuration Examples
```rust
# // Wrap this example within an async main function since the
# // local_rate_limit filter spawns a task on initialization
# #[tokio::main]
# async fn main() {
#   let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.local_rate_limit.v1alpha1.LocalRateLimit
    config:
      max_packets: 1000
      period: 1
clusters:
  - endpoints:
    - address: 127.0.0.1:7001
# ";
#   let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 1);
# }
```
To configure a rate limiter, we specify the maximum rate at which the proxy is allowed to forward packets. In the example above, we configured the proxy to forward a maximum of 1000 packets per second).

> Be aware that due to some optimizations in the current rate limiter implementation, the enforced maximum number of packets is not always exact.
> It is in theory possible that the rate limiter allows a few packets through, however in practice this would be a rare occurrence
> and the maximum number of such packets that is in the worse case `N-1` where `N` is the number of threads used to process packets.
> For example, a configuration allowing 1000 packets per second could potentially allow 1004 packets during some time window if we have up to 4 threads.

> Packets that that exceeds the maximum configured rate are dropped.

## Configuration Options ([Rust Doc](../../api/quilkin/filters/local_rate_limit/struct.Config.html))

```yaml
{{#include ../../../target/quilkin.filters.local_rate_limit.v1alpha1.yaml}}
```
