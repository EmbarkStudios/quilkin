# Decryptor

The `Decryptor` filter's job is to decrypt a portion of a client (downstream) packet as an IPv4 or IPv6 address and port to forward the packet to.

## Filter name

```text
quilkin.filters.decryptor.v1alpha1.Decryptor
```

## Configuration Examples

```rust
# let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.capture.v1alpha1.Capture
    config:
      suffix:
        size: 24
        remove: true
  - name: quilkin.filters.decryptor.v1alpha1.Decryptor
    config:
        # the (binary) decryption key
        key: keygoeshere
        # the decryption mode, currently only `Destination` is supported, which
        # will be interpreted as either an IPv4 or IPv6 address and a port which
        # will be used as the destination address of the packet
        mode: Destination
        # the name of the metadata key to retrieve the data being decrypted.
        # defaults to `quilkin.dev/capture` unless otherwise specified
        data_key: quilkin.dev/capture
        # the name of the metadata key to retrieve the nonce key from
        nonce_key: quilkin.dev/nonce
clusters:
  - endpoints:
      - address: 127.0.0.1:7001
# ";
# let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 2);
```

## Configuration Options ([Rust Doc](../../api/quilkin/filters/decryptor/struct.Decryptor.html))

```yaml
{{#include ../../../target/quilkin.filters.decryptor.v1alpha1.yaml}}
```
