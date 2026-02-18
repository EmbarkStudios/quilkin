// these tests rely on actual socket I/O, but since they are local only they trigger
// the disallowed IP error
#![cfg(debug_assertions)]

use qt::*;
use quilkin::{net, test::TestConfig};

trace_test!(server, {
    let mut sc = qt::sandbox_config!();

    sc.push("server1", ServerPailConfig::default(), &[]);
    sc.push("server2", ServerPailConfig::default(), &[]);
    sc.push("proxy", ProxyPailConfig::default(), &["server1", "server2"]);

    let mut sb = sc.spinup().await;

    let mut server1_rx = sb.packet_rx("server1");
    let mut server2_rx = sb.packet_rx("server2");

    let (addr, _) = sb.proxy("proxy");

    tracing::trace!(%addr, "sending packet");
    let msg = "hello";

    let client = sb.client();

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

    panic!("oh no");
});

trace_test!(client, {
    let mut sc = qt::sandbox_config!();

    sc.push("dest", ServerPailConfig::default(), &[]);
    sc.push("proxy", ProxyPailConfig::default(), &["dest"]);

    let mut sb = sc.spinup().await;

    let mut dest_rx = sb.packet_rx("dest");
    let (local_addr, _) = sb.proxy("proxy");
    let client = sb.client();

    let msg = "hello";
    tracing::debug!(%local_addr, "sending packet");
    client.send_to(msg.as_bytes(), &local_addr).await.unwrap();
    assert_eq!(msg, sb.timeout(100, dest_rx.recv()).await.0.unwrap(),);
});

trace_test!(with_filter, {
    let mut sc = qt::sandbox_config!();

    sc.push("server", ServerPailConfig::default(), &[]);
    sc.push(
        "proxy",
        ProxyPailConfig {
            config: Some(TestConfig::new()),
        },
        &["server"],
    );

    let mut sb = sc.spinup().await;
    let (local_addr, _) = sb.proxy("proxy");
    let mut rx = sb.packet_rx("server");
    let client = sb.client();

    let msg = "hello";
    client.send_to(msg.as_bytes(), &local_addr).await.unwrap();

    // search for the filter strings.
    let result = sb.timeout(100, rx.recv()).await.0.unwrap();
    assert!(result.starts_with(&format!("{msg}:odr:[::1]:")));
});

trace_test!(uring_receiver, {
    let mut sc = qt::sandbox_config!();

    sc.push("server", ServerPailConfig::default(), &[]);
    let mut sb = sc.spinup().await;

    let (mut packet_rx, endpoint) = sb.server("server");

    let service = quilkin::Service::builder().udp();
    let config = std::sync::Arc::new(quilkin::Config::new(
        None,
        Default::default(),
        &Default::default(),
        &service,
    ));
    config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| clusters.insert_default([endpoint.into()].into()));

    let socket = sb.client();
    let (ws, addr) = sb.socket();

    let pending_sends = net::queue(1).unwrap();

    // we'll test a single DownstreamReceiveWorkerConfig
    quilkin::net::io::Listener {
        worker_id: 1,
        port: addr.port(),
        config: config.clone(),
        sessions: quilkin::net::sessions::SessionPool::new(
            vec![pending_sends.0.clone()],
            config.dyn_cfg.cached_filter_chain().unwrap(),
        ),
    }
    .spawn_io_loop(pending_sends, config.dyn_cfg.cached_filter_chain().unwrap())
    .expect("failed to spawn task");

    // Drop the socket, otherwise it can
    drop(ws);

    let msg = "hello-downstream";
    tracing::debug!("sending packet");
    socket.send_to(msg.as_bytes(), addr).await.unwrap();
    assert_eq!(msg, sb.timeout(200, packet_rx.recv()).await.0.unwrap());
});

trace_test!(
    #[ignore]
    recv_from,
    {
        let mut sc = qt::sandbox_config!();

        sc.push("server", ServerPailConfig::default(), &[]);
        let mut sb = sc.spinup().await;

        let (mut packet_rx, endpoint) = sb.server("server");

        let config = std::sync::Arc::new(quilkin::Config::new(
            None,
            Default::default(),
            &Default::default(),
            &Default::default(),
        ));
        config
            .dyn_cfg
            .clusters()
            .unwrap()
            .modify(|clusters| clusters.insert_default([endpoint.into()].into()));

        let pending_sends: Vec<_> = [
            net::queue(1).unwrap(),
            net::queue(1).unwrap(),
            net::queue(1).unwrap(),
        ]
        .into_iter()
        .collect();

        let sessions = net::SessionPool::new(
            pending_sends.iter().map(|ps| ps.0.clone()).collect(),
            config.dyn_cfg.cached_filter_chain().unwrap(),
        );

        const WORKER_COUNT: usize = 3;

        let (socket, addr) = sb.socket();
        net::packet::spawn_receivers(config, socket, pending_sends, &sessions).unwrap();

        let socket = std::sync::Arc::new(sb.client());
        let msg = "recv-from";

        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..WORKER_COUNT {
            let ss = socket.clone();
            tasks.spawn(async move { ss.send_to(msg.as_bytes(), addr).await.unwrap() });
        }

        while let Some(res) = tasks.join_next().await {
            assert_eq!(res.unwrap(), msg.len());
        }

        for _ in 0..WORKER_COUNT {
            assert_eq!(
                msg,
                sb.timeout(20, packet_rx.recv())
                    .await
                    .0
                    .expect("should receive a packet")
            );
        }
    }
);
