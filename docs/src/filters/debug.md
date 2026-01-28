# Debug

The Debug filter logs all incoming and outgoing packets to standard output.

This filter is useful in debugging deployments where the packets strictly contain valid `UTF-8` encoded strings. A generic error message is instead logged if conversion from bytes to `UTF-8` fails.

### Filter name
```text
quilkin.filters.debug_filter.v1alpha1.Debug
```

## Configuration Examples
```rust
# let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.debug.v1alpha1.Debug
    config:
      id: debug-1
clusters:
  - endpoints:
      - address: 127.0.0.1:7001
# ";
# let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 1);
```

## Configuration Options ([Rust Doc](../../api/quilkin/filters/debug/struct.Config.html))

```yaml
{{#include ../../../target/quilkin.filters.debug.v1alpha1.yaml}}
```
