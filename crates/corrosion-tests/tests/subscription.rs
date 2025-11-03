use corro_api_types::{QueryEvent, TypedQueryEvent};
use corro_types::pubsub::ChangeType;
use corrosion::db::{
    read::{self, FromSqlValue, ServerRow},
    write,
};
use corrosion_tests::TestSubsDb;
use quilkin_types::{Endpoint, IcaoCode, TokenSet};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};
use tokio_stream::StreamExt;

#[derive(PartialEq, Debug, Clone)]
struct Server {
    icao: IcaoCode,
    tokens: TokenSet,
}

/// Tests subscriptions to server notifications work properly
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn server_subscriptions() {
    let _guard = qt::init_logging(qt::Level::DEBUG, "corro_types");

    let mut pool = TestSubsDb::new(corrosion::schema::SCHEMA, "server_subscriptions").await;

    let peer = corrosion::Peer::new(Ipv6Addr::from_bits(0xaabbccddeeff), 15111, 0, 0);

    let mut server_set = BTreeMap::<Endpoint, Server>::new();

    for i in (0..30u32).step_by(3) {
        let icao = IcaoCode::new_testing([b'A' + (i as u8 / 3); 4]);

        server_set.insert(
            Endpoint::new(IpAddr::V4(Ipv4Addr::from_bits(i)).into(), 7777),
            Server {
                icao,
                tokens: [[i as u8]].into(),
            },
        );
        server_set.insert(
            Endpoint::new(format!("host.{}.example", i + 1).into(), 7777),
            Server {
                icao,
                tokens: [[(i + 1) as u8]].into(),
            },
        );
        server_set.insert(
            Endpoint::new(IpAddr::V6(Ipv6Addr::from_bits((i + 2) as _)).into(), 7777),
            Server {
                icao,
                tokens: [[(i + 2) as u8]].into(),
            },
        );
    }

    // Seed tables
    let mut states = write::Statements::<30>::new();

    {
        let mut s = write::Server::for_peer(peer, &mut states);

        for (ep, srv) in &server_set {
            s.upsert(ep, srv.icao, &srv.tokens);
        }
    }

    pool.transaction(states.iter()).await;
    states.clear();

    let (sh, mut srx) = pool.subscribe_new("SELECT endpoint,icao,tokens FROM servers");

    assert!(matches!(
        srx.recv().await.unwrap(),
        read::QueryEvent::Columns(_)
    ));

    let mut current_set = BTreeMap::new();

    loop {
        let row = srx.recv().await.expect("stream should still be subscribed");
        match row {
            read::QueryEvent::Row(_id, row) => {
                let server = ServerRow::from_sql(&row).expect("failed to deserialize row");
                assert!(
                    current_set
                        .insert(
                            server.endpoint,
                            Server {
                                icao: server.icao,
                                tokens: server.tokens,
                            }
                        )
                        .is_none()
                );
            }
            read::QueryEvent::EndOfQuery { .. } => break,
            other => {
                panic!("unexpected event {other:?}");
            }
        }
    }

    assert_eq!(server_set, current_set);

    // Add a new server
    {
        let mut s = write::Server::for_peer(peer, &mut states);

        let key = Endpoint::new(Ipv4Addr::new(1, 2, 3, 4).into(), 7777);
        server_set.insert(
            key.clone(),
            Server {
                icao: IcaoCode::new_testing([b'Z'; 4]),
                tokens: [[9; 4]].into(),
            },
        );
        let srv = server_set.get(&key).unwrap();
        s.upsert(&key, srv.icao, &srv.tokens);
    }

    pool.transaction(states.iter()).await;
    states.clear();
    pool.send_changes(&sh).await;

    {
        match srx.recv().await.expect("expected a change") {
            read::QueryEvent::Change(kind, _rid, row, _id) => {
                assert_eq!(kind, ChangeType::Insert);
                let ns = ServerRow::from_sql(&row).expect("failed to deserialize insert");
                current_set.insert(
                    ns.endpoint,
                    Server {
                        icao: ns.icao,
                        tokens: ns.tokens,
                    },
                );

                assert_eq!(server_set, current_set);
            }
            other => {
                panic!("unexpected event {other:?}");
            }
        }
    };

    // Change an existing server
    {
        let mut s = write::Server::for_peer(peer, &mut states);

        let key = Endpoint {
            address: IpAddr::V4(Ipv4Addr::from_bits(0)).into(),
            port: 7777,
        };
        let srv = server_set.get_mut(&key).unwrap();
        srv.icao = IcaoCode::new_testing([b'Y'; 4]);
        s.update(&key, Some(srv.icao), None);
    }

    pool.transaction(states.iter()).await;
    states.clear();
    pool.send_changes(&sh).await;

    {
        match srx.recv().await.expect("expected a change") {
            read::QueryEvent::Change(kind, _rid, row, _id) => {
                assert_eq!(kind, ChangeType::Update);
                let ns = ServerRow::from_sql(&row).expect("failed to deserialize update");
                assert!(
                    current_set
                        .insert(
                            ns.endpoint,
                            Server {
                                icao: ns.icao,
                                tokens: ns.tokens,
                            },
                        )
                        .is_some()
                );

                assert_eq!(server_set, current_set);
            }
            other => {
                panic!("unexpected event {other:?}");
            }
        }
    }

    // Remove 2 servers
    {
        let mut s = write::Server::for_peer(peer, &mut states);

        let icao = IcaoCode::new_testing([b'A'; 4]);
        server_set.retain(|key, val| {
            if val.icao == icao {
                s.remove_immediate(key);
                false
            } else {
                true
            }
        });

        assert_eq!(4, s.statements.len());
    }

    pool.transaction(states.iter()).await;
    states.clear();
    pool.send_changes(&sh).await;

    {
        for _ in 0..2 {
            match srx.recv().await.expect("expected a change") {
                read::QueryEvent::Change(kind, _rid, row, _id) => {
                    assert_eq!(kind, ChangeType::Delete);
                    let ns = ServerRow::from_sql(&row).expect("failed to deserialize delete");
                    assert!(current_set.remove(&ns.endpoint).is_some());
                }
                other => {
                    panic!("unexpected event {other:?}");
                }
            }
        }
    }

    assert_eq!(server_set, current_set);

    pool.remove_handle(sh).await;

    {
        let (handle, mut srx) = pool.subscribe_new("SELECT endpoint,icao,tokens FROM servers");
        assert!(matches!(
            srx.recv().await.unwrap(),
            read::QueryEvent::Columns(_)
        ));

        current_set.clear();

        loop {
            let row = srx.recv().await.expect("stream should still be subscribed");
            match row {
                read::QueryEvent::Row(_id, row) => {
                    let server = ServerRow::from_sql(&row).expect("failed to deserialize row");
                    assert!(
                        current_set
                            .insert(
                                server.endpoint,
                                Server {
                                    icao: server.icao,
                                    tokens: server.tokens,
                                }
                            )
                            .is_none()
                    );
                }
                read::QueryEvent::EndOfQuery { .. } => break,
                other => {
                    panic!("unexpected event {other:?}");
                }
            }
        }

        assert_eq!(server_set, current_set);
        pool.remove_handle(handle).await;
    }

    // Remove all but 1 server with no active subscribers
    {
        let mut s = write::Server::for_peer(peer, &mut states);
        let remaining = IcaoCode::new_testing([b'Y'; 4]);

        server_set.retain(|key, val| {
            if val.icao != remaining {
                s.remove_immediate(key);
                false
            } else {
                true
            }
        })
    }

    pool.transaction(states.iter()).await;
    states.clear();

    let (handle, mut srx) = pool.subscribe_new("SELECT endpoint,icao,tokens FROM servers");
    assert!(matches!(
        srx.recv().await.unwrap(),
        read::QueryEvent::Columns(_)
    ));

    current_set.clear();

    loop {
        let row = srx.recv().await.expect("stream should still be subscribed");
        match row {
            read::QueryEvent::Row(_id, row) => {
                let server = ServerRow::from_sql(&row).expect("failed to deserialize row");
                assert!(
                    current_set
                        .insert(
                            server.endpoint,
                            Server {
                                icao: server.icao,
                                tokens: server.tokens,
                            }
                        )
                        .is_none()
                );
            }
            read::QueryEvent::EndOfQuery { .. } => break,
            other => {
                panic!("unexpected event {other:?}");
            }
        }
    }

    assert_eq!(server_set, current_set);
    pool.remove_handle(handle).await;

    pool.shutdown().await;
}

use corrosion::pubsub;

const NORMAL_QUERY: &str = "SELECT endpoint,icao,tokens FROM servers";

/// Tests that a single subscription works
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn single_sub() {
    let _guard = qt::init_logging(qt::Level::DEBUG, "corro_types");

    let pool = TestSubsDb::new(corrosion::schema::SCHEMA, "single_sub").await;
    let ctx = pool.pubsub_ctx();

    let sub = pubsub::subscribe(
        pubsub::SubParams {
            query: corro_api_types::Statement::Simple(NORMAL_QUERY.to_owned()),
            from: None,
            skip_rows: false,
            max_buffer: 0,
            change_threshold: 0,
            process_interval: Duration::from_secs(1),
        },
        &ctx,
    )
    .await
    .unwrap();

    let mut rx = sub.rx;

    let mut cur_cid = 0;

    // We always get a state of the world first, in our case the DB is empty
    corrosion_tests::assert_sub_event_eq::<ServerRow>(
        &mut rx,
        &TypedQueryEvent::Columns(vec!["endpoint".into(), "icao".into(), "tokens".into()]),
    )
    .await;
    corrosion_tests::assert_sub_event_eq::<ServerRow>(
        &mut rx,
        &TypedQueryEvent::EndOfQuery {
            time: 0.,
            change_id: Some(cur_cid.into()),
        },
    )
    .await;

    let peer = corrosion::Peer::new(Ipv6Addr::from_bits(0xaabbccddeeff), 15111, 0, 0);
    let mut server_set = BTreeMap::<Endpoint, Server>::new();

    let mut states = write::Statements::<30>::new();
    let key = Endpoint::new(Ipv4Addr::new(1, 2, 3, 4).into(), 7777);
    let icao = IcaoCode::new_testing([b'T'; 4]);
    let new_icao = IcaoCode::new_testing([b'N'; 4]);
    let tokens = TokenSet::from([[8; 8]]);

    for _ in 0..5 {
        // Insert
        {
            {
                let mut s = write::Server::for_peer(peer, &mut states);

                server_set.insert(
                    key.clone(),
                    Server {
                        icao,
                        tokens: tokens.clone(),
                    },
                );
                let srv = server_set.get(&key).unwrap();
                s.upsert(&key, srv.icao, &srv.tokens);
            }

            pool.broadcast_changes(&mut states).await;
            cur_cid += 1;

            corrosion_tests::assert_sub_event_eq(
                &mut rx,
                &TypedQueryEvent::Change(
                    ChangeType::Insert,
                    0.into(),
                    ServerRow {
                        icao,
                        endpoint: key.clone(),
                        tokens: tokens.clone(),
                    },
                    cur_cid.into(),
                ),
            )
            .await;
        }

        // Mutate
        {
            {
                let mut s = write::Server::for_peer(peer, &mut states);

                server_set.get_mut(&key).unwrap().icao = new_icao;
                s.update(&key, Some(new_icao), None);
            }

            pool.broadcast_changes(&mut states).await;
            cur_cid += 1;

            corrosion_tests::assert_sub_event_eq(
                &mut rx,
                &TypedQueryEvent::Change(
                    ChangeType::Update,
                    0.into(),
                    ServerRow {
                        icao: new_icao,
                        endpoint: key.clone(),
                        tokens: tokens.clone(),
                    },
                    cur_cid.into(),
                ),
            )
            .await;
        }

        // Delete
        {
            {
                let mut s = write::Server::for_peer(peer, &mut states);

                server_set.remove(&key);
                s.remove_immediate(&key);
            }

            pool.broadcast_changes(&mut states).await;
            cur_cid += 1;

            corrosion_tests::assert_sub_event_eq(
                &mut rx,
                &TypedQueryEvent::Change(
                    ChangeType::Delete,
                    0.into(),
                    ServerRow {
                        icao: new_icao,
                        endpoint: key.clone(),
                        tokens: tokens.clone(),
                    },
                    cur_cid.into(),
                ),
            )
            .await;
        }
    }
}

/// Tests that multiple subscriptions for the same query works
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn multiple_subs() {
    let _guard = qt::init_logging(qt::Level::DEBUG, "corro_types");

    let pool = TestSubsDb::new(corrosion::schema::SCHEMA, "multiple_subs").await;
    let ctx = pool.pubsub_ctx();

    let original = pubsub::subscribe(
        pubsub::SubParams {
            query: corro_api_types::Statement::Simple(NORMAL_QUERY.to_owned()),
            from: None,
            skip_rows: false,
            max_buffer: 0,
            change_threshold: 0,
            process_interval: Duration::from_secs(1),
        },
        &ctx,
    )
    .await
    .unwrap();

    let mut orx = original.rx;

    let mut multi_subs = {
        let mut ms = Vec::with_capacity(3);

        for _ in 0..3 {
            let ns = pubsub::subscribe(
                pubsub::SubParams {
                    query: corro_api_types::Statement::Simple(NORMAL_QUERY.to_owned()),
                    from: None,
                    skip_rows: false,
                    max_buffer: 0,
                    change_threshold: 0,
                    process_interval: Duration::from_secs(1),
                },
                &ctx,
            )
            .await
            .unwrap();

            assert_eq!(original.query_hash, ns.query_hash);
            ms.push(ns.rx);
        }

        ms
    };

    let mut cur_cid = 0;

    let mut assert_all = async |tqe| {
        corrosion_tests::assert_sub_event_eq::<ServerRow>(&mut orx, &tqe).await;

        for sub in &mut multi_subs {
            corrosion_tests::assert_sub_event_eq::<ServerRow>(sub, &tqe).await;
        }
    };

    // We always get a state of the world first, in our case the DB is empty
    assert_all(TypedQueryEvent::Columns(vec![
        "endpoint".into(),
        "icao".into(),
        "tokens".into(),
    ]))
    .await;
    assert_all(TypedQueryEvent::EndOfQuery {
        time: 0.,
        change_id: Some(cur_cid.into()),
    })
    .await;

    let peer = corrosion::Peer::new(Ipv6Addr::from_bits(0xaabbccddeeff), 15111, 0, 0);
    let mut server_set = BTreeMap::<Endpoint, Server>::new();

    let mut states = write::Statements::<30>::new();
    let key = Endpoint::new(Ipv4Addr::new(1, 2, 3, 4).into(), 7777);
    let icao = IcaoCode::new_testing([b'T'; 4]);
    let new_icao = IcaoCode::new_testing([b'N'; 4]);
    let tokens = TokenSet::from([[8; 8]]);

    for _ in 0..5 {
        // Insert
        {
            {
                let mut s = write::Server::for_peer(peer, &mut states);

                server_set.insert(
                    key.clone(),
                    Server {
                        icao,
                        tokens: tokens.clone(),
                    },
                );
                let srv = server_set.get(&key).unwrap();
                s.upsert(&key, srv.icao, &srv.tokens);
            }

            pool.broadcast_changes(&mut states).await;
            cur_cid += 1;

            assert_all(TypedQueryEvent::Change(
                ChangeType::Insert,
                0.into(),
                ServerRow {
                    icao,
                    endpoint: key.clone(),
                    tokens: tokens.clone(),
                },
                cur_cid.into(),
            ))
            .await;
        }

        // Mutate
        {
            {
                let mut s = write::Server::for_peer(peer, &mut states);

                server_set.get_mut(&key).unwrap().icao = new_icao;
                s.update(&key, Some(new_icao), None);
            }

            pool.broadcast_changes(&mut states).await;
            cur_cid += 1;

            assert_all(TypedQueryEvent::Change(
                ChangeType::Update,
                0.into(),
                ServerRow {
                    icao: new_icao,
                    endpoint: key.clone(),
                    tokens: tokens.clone(),
                },
                cur_cid.into(),
            ))
            .await;
        }

        // Delete
        {
            {
                let mut s = write::Server::for_peer(peer, &mut states);

                server_set.remove(&key);
                s.remove_immediate(&key);
            }

            pool.broadcast_changes(&mut states).await;
            cur_cid += 1;

            assert_all(TypedQueryEvent::Change(
                ChangeType::Delete,
                0.into(),
                ServerRow {
                    icao: new_icao,
                    endpoint: key.clone(),
                    tokens: tokens.clone(),
                },
                cur_cid.into(),
            ))
            .await;
        }
    }
}

/// Tests that [`pubsub::BufferingSubStream`] works correctly
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn buffers() {
    use pubsub::query_to_sub_event;

    async fn assert_events(bss: &mut pubsub::BufferingSubStream, exp: &[QueryEvent]) {
        let mut buf = tokio::time::timeout(Duration::from_millis(40), bss.next())
            .await
            .expect("didn't receive event in expected time")
            .expect("stream ended");

        let mut ss = pubsub::SubscriptionStream::length_prefixed(&mut buf).unwrap();
        let mut ex = exp.iter();

        loop {
            match (ss.next(), ex.next()) {
                (Some(ss), Some(ex)) => {
                    assert_eq!(&ss.expect("failed to deserialize queryevent"), ex);
                }
                (Some(ss), None) => {
                    panic!("unexpected event: {ss:?}");
                }
                (None, Some(ex)) => {
                    panic!("failed to receive expected event: {ex:?}");
                }
                (None, None) => {
                    break;
                }
            }
        }
    }

    let (tx, rx) = tokio::sync::mpsc::channel(100);

    let mut bss = pubsub::BufferingSubStream::new(
        pubsub::Subscription {
            id: uuid::Uuid::nil(),
            query_hash: String::new(),
            rx,
        },
        1024,
        std::time::Duration::from_millis(20),
    );

    let mut b = bytes::BytesMut::new();

    // A single small buffer will be buffered until the interval is reached
    {
        let tiny = QueryEvent::Row(0.into(), vec!["tiny".into()]);
        tx.send(query_to_sub_event(&mut b, tiny.clone()).unwrap())
            .await
            .unwrap();

        assert_events(&mut bss, &[tiny]).await;
    }

    // A single buffer that is over the maximum
    {
        let max = QueryEvent::Row(0.into(), vec![vec![8u8; 2048].into()]);
        tx.send(query_to_sub_event(&mut b, max.clone()).unwrap())
            .await
            .unwrap();

        assert_events(&mut bss, &[max]).await;
    }

    // A stream of equally sized buffers
    {
        let chunk = QueryEvent::Row(0.into(), vec![vec![8u8; 250].into()]);

        for _ in 0..100 {
            tx.send(query_to_sub_event(&mut b, chunk.clone()).unwrap())
                .await
                .unwrap();
        }

        let mut count = 0;
        while count < 99 {
            let mut buf = bss.next().await.unwrap();

            for schunk in pubsub::SubscriptionStream::length_prefixed(&mut buf).unwrap() {
                assert_eq!(schunk.unwrap(), chunk);
                count += 1;
            }
        }
    }

    // Push a trailing change
    let tiny = QueryEvent::Row(0.into(), vec!["tiny".into()]);
    tx.send(query_to_sub_event(&mut b, tiny.clone()).unwrap())
        .await
        .unwrap();

    drop(tx);
    let mut last = bss.drain().await.unwrap();
    assert!(bss.drain().await.is_none());

    let mut ss = pubsub::SubscriptionStream::length_prefixed(&mut last).unwrap();
    assert_eq!(ss.next().unwrap().unwrap(), tiny);
    assert!(ss.next().is_none());
}
