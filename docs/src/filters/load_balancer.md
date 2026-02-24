# LoadBalancer

The `LoadBalancer` filter distributes packets received downstream among all upstream endpoints.

## Filter name
```text
quilkin.filters.load_balancer.v1alpha1.LoadBalancer
```

## Configuration Examples
```rust
# #[tokio::main]
# async fn main() {
#   let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.load_balancer.v1alpha1.LoadBalancer
    config:
      policy: ROUND_ROBIN
clusters:
  - endpoints:
      - address: 127.0.0.1:7001
# ";
#   let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 1);
# }
```

The load balancing policy (the strategy to use to select what endpoint to send traffic to) is configurable.
In the example above, packets will be distributed by selecting endpoints in turn, in round robin fashion.

## Configuration Options ([Rust Doc](../../api/quilkin/filters/load_balancer/struct.Config.html))

```yaml
{{#include ../../../target/quilkin.filters.load_balancer.v1alpha1.yaml}}
```
