//! The relay serving xDS and mDS over TLS, with the agent and proxy verifying
//! it.
//!
//! Until the client gained a TLS configuration, tonic refused any `https`
//! endpoint outright, so none of this was reachable.

use qt::*;
use quilkin::{net::endpoint::Endpoint, test::TestConfig};

// Configuration reaches the proxy over TLS, and a packet still gets through.
trace_test!(tls_relay_carries_configuration, {
    let mut sc = qt::sandbox_config!();

    sc.push("server", ServerPailConfig::default(), &[]);
    sc.push(
        "relay",
        RelayPailConfig {
            tls: true,
            ..Default::default()
        },
        &[],
    );
    sc.push(
        "agent",
        AgentPailConfig {
            endpoints: vec![("server", &[])],
            ..Default::default()
        },
        &["server", "relay"],
    );
    sc.push("proxy", ProxyPailConfig::default(), &["relay"]);

    let mut sandbox = sc.spinup().await;

    let (mut server_rx, server_addr) = sandbox.server("server");
    let (proxy_address, _) = sandbox.proxy("proxy");
    let mut agent_config = sandbox.config_file("agent");

    sandbox.block_until_ready("proxy").await;

    // Pushed to the relay over mDS and pulled by the proxy over xDS, both on TLS.
    agent_config.update(|config| {
        config
            .clusters
            .insert_default([Endpoint::new(server_addr.into())].into());
    });

    let client = sandbox.client();
    sandbox
        .block_until_packet_gets_through(b"hello", "hello", &client, &proxy_address, &mut server_rx)
        .await;
});

// The proxy verifying against a CA that signed nothing never becomes ready.
// Without this, a handshake that silently succeeded would be
// indistinguishable from one that was never checked.
trace_test!(tls_relay_rejects_an_unknown_ca, {
    let mut sc = qt::sandbox_config!();

    sc.push("server", ServerPailConfig::default(), &[]);
    sc.push(
        "relay",
        RelayPailConfig {
            tls: true,
            ..Default::default()
        },
        &[],
    );
    sc.push(
        "proxy",
        ProxyPailConfig {
            config: Some(TestConfig::new()),
            mismatched_tls_ca: true,
            ..Default::default()
        },
        &["relay"],
    );

    let sandbox = sc.spinup().await;
    let proxy = &sandbox.pails["proxy"];

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        assert!(
            !proxy.is_healthy(),
            "the proxy became ready against a CA that did not sign the relay certificate"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
});
