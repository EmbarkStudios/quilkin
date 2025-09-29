use corro_types::pubsub::ChangeType;
use corrosion::client::{
    read::{self, FromSqlValue, ServerRow},
    write::{self, UpdateBuilder},
};
use quilkin_types::{Endpoint, IcaoCode, TokenSet};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

/// Tests subscriptions to server notifications work properly
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn server_subscriptions() {
    let tw = corrosion_utils::Trip::new();
    let mut pool = corrosion_utils::TestSubsDb::new(corrosion::schema::SCHEMA).await;

    #[derive(PartialEq, Debug, Clone)]
    struct Server {
        icao: IcaoCode,
        tokens: TokenSet,
    }

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
    pool.send_changes(&sh);

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
        s.update(UpdateBuilder::new(&key).update_icao(srv.icao));
    }

    pool.transaction(states.iter()).await;
    states.clear();
    pool.send_changes(&sh);

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
    pool.send_changes(&sh);

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

    tw.shutdown().await;
}
