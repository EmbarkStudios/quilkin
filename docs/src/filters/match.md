# Match

The `Match` filter's job is to provide a mechanism to change behaviour based
on dynamic metadata. This filter behaves similarly to the `match` expression
in Rust or `switch` statements in other languages.

## Filter name
```text
quilkin.filters.match.v1alpha1.Match
```

## Configuration Examples
<!-- ANCHOR: example -->
```rust
# let yaml = "
version: v1alpha1
clusters: 
  - endpoints:
    - address: 127.0.0.1:26000
    - address: 127.0.0.1:26001
filters:
  - name: quilkin.filters.capture.v1alpha1.Capture
    config:
      metadataKey: myapp.com/token
      prefix:
        size: 3
        remove: false
  - name: quilkin.filters.match.v1alpha1.Match
    config:
      on_read:
        metadataKey: myapp.com/token
        branches:
          - value: abc
            name: quilkin.filters.pass.v1alpha1.Pass
        fallthrough:
          name: quilkin.filters.drop.v1alpha1.Drop
# ";
# let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 2);
```
<!--  ANCHOR_END: example -->

## Configuration Options ([Rust Doc](../../api/quilkin/filters/match/struct.Config.html))

```yaml
{{#include ../../../target/quilkin.filters.match.v1alpha1.yaml}}
```

View the [Match](../../api/quilkin/filters/match/struct.Config.html) filter documentation for more details.

## Metrics

* `quilkin_filter_int_counter{label="packets_matched_total"}`
  A counter of the total number of packets where the dynamic metadata matches a branch value.
* `quilkin_filter_int_counter{label="packets_fallthrough_total"}`
  A counter of the total number of packets that are processed by the fallthrough configuration.
