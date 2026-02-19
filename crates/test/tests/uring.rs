//! Tests specifically for the io-uring implementation

#![cfg(target_os = "linux")]

use qt::*;

// Test that a client send that fans out to multiple servers works
trace_test!(fan_out, {
    let mut sc = qt::sandbox_config!();

    sc.push("server1", ServerPailConfig::default(), &[]);
    sc.push("server2", ServerPailConfig::default(), &[]);
    sc.push("server3", ServerPailConfig::default(), &[]);
    sc.push(
        "proxy",
        ProxyPailConfig::default(),
        &["server1", "server2", "server3"],
    );

    let mut sb = sc.spinup().await;

    let mut server1_rx = sb.packet_rx("server1");
    let mut server2_rx = sb.packet_rx("server2");
    let mut server3_rx = sb.packet_rx("server3");

    let (addr, _) = sb.proxy("proxy");

    tracing::trace!(%addr, "sending packet");

    let client = sb.client();

    for i in 0..100 {
        let msg = format!("hello_{i}");
        client.send_to(msg.as_bytes(), &addr).await.unwrap();
        assert_eq!(
            msg,
            sb.timeout(100, server1_rx.recv())
                .await
                .0
                .expect("should get a packet")
        );
        assert_eq!(
            msg,
            sb.timeout(100, server2_rx.recv())
                .await
                .0
                .expect("should get a packet")
        );
        assert_eq!(
            msg,
            sb.timeout(100, server3_rx.recv())
                .await
                .0
                .expect("should get a packet")
        );
    }
});

// Test that we can go through the full ring buffer dedicated to receives
trace_test!(refreshes_recv_ring, {
    let mut sc = qt::sandbox_config!();

    sc.push(
        "server",
        ServerPailConfig {
            echo: true,
            ..Default::default()
        },
        &[],
    );
    sc.push("proxy", ProxyPailConfig::default(), &["server"]);

    let mut sb = sc.spinup().await;

    let (addr, _) = sb.proxy("proxy");
    let client = sb.client();

    let mut buf = [0u8; 4];

    for i in 0..10000u32 {
        client.send_to(&i.to_ne_bytes(), &addr).await.unwrap();

        let (len, _addr) = sb
            .timeout(100, client.recv_from(&mut buf))
            .await
            .0
            .expect("should have received packet");

        assert_eq!(len, 4);
        assert_eq!(u32::from_ne_bytes(buf), i);
    }
});
