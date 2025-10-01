# nmap-service-probes
A `#[no_std]` parser and code generator for the [nmap-service-probes] file format.

```rust
let probes = nmap_service_probes::parse("Probe UDP Help q|help\r\n\r\n|")?;

assert_eq!(
    &probes[0],
    &ServiceProbe {
        protocol: Protocol::Udp,
        name: "Help".into(),
        string: "help\r\n\r\n".into(),
        no_payload: false,
        total_wait_millis: None,
        fallbacks: <_>::default(),
        ports: vec![],
        rarity: None,
        ssl_ports: <_>::default(),
        tcp_wrapped_millis: <_>::default(),
        matches: vec![]
    }
);
```

[nmap-service-probes]: https://nmap.org/book/vscan-fileformat.html
