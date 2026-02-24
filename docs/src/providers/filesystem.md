# Filesystem Provider

The filesystem provider watches a configuration file on disk and sends updates to proxies whenever that file changes.

For example:
```sh
quilkin --provider.fs.path quilkin.yaml
```

You can find the configuration file schema in [Configuration File][configuration].

Example:

```rust
# let yaml = "
version: v1alpha1
filters:
  - name: quilkin.filters.debug.v1alpha1.Debug
    config:
      id: hello
clusters:
  - endpoints:
     - address: 123.0.0.1:29
       metadata:
         'quilkin.dev':
           tokens:
             - 'MXg3aWp5Ng=='
# ";
# let config = quilkin::config::Config::from_reader(yaml.as_bytes()).unwrap();
# assert_eq!(config.filters.load().len(), 1);
```

[configuration]: ../../../services/proxy/configuration.md
