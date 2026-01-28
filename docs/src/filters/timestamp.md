# Timestamp

The `Timestamp` filter accepts a UNIX timestamp from metadata and observes the
duration between that timestamp and now. Mostly useful in combination with other
filters such as `Capture` to pull timestamp data from packets.

## Filter name
```text
quilkin.filters.timestamp.v1alpha1.Timestamp
```

## Configuration Examples
```rust
# let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.capture.v1alpha1.Capture
    config:
      metadataKey: example.com/session_duration
      prefix:
        size: 3
        remove: false
  - name: quilkin.filters.timestamp.v1alpha1.Timestamp
    config:
        metadataKey: example.com/session_duration
clusters:
  - endpoints:
    - address: 127.0.0.1:26000
# ";
# let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
```

## Configuration Options ([Rust Doc](../../api/quilkin/filters/timestamp/struct.Config.html))

```yaml
{{#include ../../../target/quilkin.filters.timestamp.v1alpha1.yaml}}
```

## Metrics

* `quilkin_filter_histogram{label="duration"}`
  A histogram of durations from `metadata_key` to now in the packet `direction`.

