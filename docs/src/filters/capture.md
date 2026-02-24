# Capture

The `Capture` filter's job is to find a series of bytes within a packet, and capture it into
[Filter Dynamic Metadata][filter-dynamic-metadata], so that it can be utilised by filters further
down the chain.

This is often used as a way of retrieving authentication tokens from a packet, and used in combination with
[Concatenate](concatenate.md) and
[TokenRouter](token_router.md) filter to provide common packet routing utilities.

## Capture strategies

There are multiple strategies for capturing bytes from the packet.

### Suffix
Captures bytes from the end of the packet.

### Prefix
Captures bytes from the start of the packet.

### Regex
Captures bytes using a regular expression. Unlike other capture strategies,
the regular expression can return one or many values if there are
multiple matches.


## Filter name
```text
quilkin.filters.capture.v1alpha1.Capture
```

## Configuration Examples
```rust
# let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.capture.v1alpha1.Capture
    config:
      metadataKey: myapp.com/myownkey
      prefix:
        size: 3
        remove: false
clusters:
  - endpoints:
      - address: 127.0.0.1:7001
# ";
# let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 1);
```

## Configuration Options ([Rust Doc](../../../api/quilkin/filters/capture/struct.Config.html))

```yaml
{{#include ../../../target/quilkin.filters.capture.v1alpha1.yaml}}
```

[filter-dynamic-metadata]: ../filters.md#filter-dynamic-metadata
