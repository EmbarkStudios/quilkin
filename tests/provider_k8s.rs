use ::corrosion::{
    persistent::{mutator::BroadcastingTransactor, proto::v1, server::DbMutator},
    pubsub,
};
use futures::StreamExt;
use k8s_openapi::{api::core::v1::NodeAddress, apimachinery::pkg::apis::meta::v1::ObjectMeta};
use kube::runtime::watcher::Event;
use kube_core::DeserializeGuard;
use quilkin::{
    config, net,
    providers::{
        self, corrosion,
        k8s::agones::{
            self, GameServer, GameServerSpec, GameServerState, GameServerStatus,
            GameServerStatusPort,
        },
    },
};
use quilkin_types::{Endpoint, IcaoCode};
use std::sync::Arc;

fn setup_tracing() {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};
    let layer = tracing_subscriber::fmt::layer()
        .with_test_writer()
        .with_filter(tracing_subscriber::filter::LevelFilter::from_level(
            tracing::Level::TRACE,
        ))
        .with_filter(tracing_subscriber::EnvFilter::new(
            "quilkin=trace,corrosion=trace,corro_types=trace",
        ));
    let sub = tracing_subscriber::Registry::default().with(layer);
    let disp = tracing::dispatcher::Dispatch::new(sub);
    tracing::dispatcher::set_global_default(disp).unwrap();
}

/// Test that ensures the state of the xDs version of a cluster matches valid
///
/// Note this only provides valid events with the valid data, other tests in
/// this file are used to test invalid data
#[test]
fn corrosion_matches_xds() {
    let clusters = config::Watch::new(net::ClusterMap::new());
    let state = Arc::new(corrosion::push::LocalState::default());
    let (mutator, mut rx) = corrosion::ServerMutator::testing(state.clone());

    let mut processor = providers::k8s::EventProcessor {
        clusters: clusters.clone(),
        address_selector: Some(config::AddressSelector {
            name: "addr".into(),
            kind: config::AddrKind::Ipv6,
        }),
        mutator: Some(mutator),
        locality: None,
        servers: Default::default(),
    };

    #[track_caller]
    fn matches(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<providers::corrosion::push::Mutation>,
        clusters: &config::Watch<net::ClusterMap>,
        state: &providers::corrosion::push::LocalState,
    ) {
        // Drain the receiver, we don't care about events in this test
        while rx.try_recv().is_ok() {}

        {
            let xds_map = {
                let read = clusters.read();
                let set = read.get(&None).unwrap();

                set.to_map()
            };

            let corro_map = state.to_map();

            assert_eq!(xds_map, corro_map);
        }
    }

    fn tokens(id: u16) -> std::collections::BTreeMap<String, String> {
        let mut md = std::collections::BTreeMap::new();

        let ts: quilkin_types::TokenSet = (1..id + 1).map(|i| vec![i as u8; i as usize]).collect();
        md.insert(agones::QUILKIN_TOKEN_LABEL.into(), ts.serialize_to_string());

        md
    }

    macro_rules! game_server {
        ($id:expr) => {{
            let gs = GameServer {
                metadata: ObjectMeta {
                    name: Some($id.to_string()),
                    uid: Some(uuid::Uuid::from_u128($id as u128).to_string()),
                    annotations: Some(tokens($id)),
                    ..Default::default()
                },
                spec: GameServerSpec {
                    container: None,
                    ports: vec![],
                    health: Default::default(),
                    scheduling: agones::SchedulingStrategy::Packed,
                    sdk_server: Default::default(),
                    template: Default::default(),
                },
                status: Some(GameServerStatus {
                    state: GameServerState::Allocated,
                    ports: Some(vec![GameServerStatusPort {
                        name: "addr".into(),
                        port: $id,
                    }]),
                    address: std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, $id).to_string(),
                    addresses: vec![NodeAddress {
                        type_: "addr".into(),
                        address: std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, $id).to_string(),
                    }],
                    node_name: $id.to_string(),
                    reserved_until: None,
                }),
            };

            DeserializeGuard(Ok(gs))
        }};
    }

    // Add a single server
    {
        processor.process_event(Event::Apply(game_server!(0)));
        matches(&mut rx, &clusters, &state);
    }

    // Init -> apply many servers
    {
        processor.process_event(Event::Init);

        for i in 0..10 {
            processor.process_event(Event::InitApply(game_server!(i)));
        }

        processor.process_event(Event::InitDone);
        matches(&mut rx, &clusters, &state);
    }

    // Init -> apply with only updates of existing servers
    {
        processor.process_event(Event::Init);

        for i in 0..10 {
            processor.process_event(Event::InitApply(game_server!(i)));
        }

        processor.process_event(Event::InitDone);
        matches(&mut rx, &clusters, &state);
    }

    // Remove, update, and add multiple servers
    {
        for i in 0..10 {
            if i % 2 == 0 {
                processor.process_event(Event::Delete(game_server!(i)));
            }
            processor.process_event(Event::Apply(game_server!(i)));
        }

        matches(&mut rx, &clusters, &state);
    }

    // Same, but in an init block
    {
        processor.process_event(Event::Init);

        for i in 0..10 {
            if i % 2 == 1 {
                processor.process_event(Event::InitApply(game_server!(i)));
            }
        }

        processor.process_event(Event::InitDone);

        matches(&mut rx, &clusters, &state);
    }
}

/// The corrosion implementation uses the k8s assigned [uid](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#uids)
/// to identify servers, this test ensures that if it is somehow missing or invalid
/// that the server is ignored
#[test]
fn handles_missing_invalid_uid() {
    let clusters = config::Watch::new(net::ClusterMap::new());
    let state = Arc::new(corrosion::push::LocalState::default());
    let (mutator, mut rx) = corrosion::ServerMutator::testing(state.clone());

    let mut processor = providers::k8s::EventProcessor {
        clusters: clusters.clone(),
        address_selector: Some(config::AddressSelector {
            name: "addr".into(),
            kind: config::AddrKind::Ipv6,
        }),
        mutator: Some(mutator),
        locality: None,
        servers: Default::default(),
    };

    fn game_server(uid: Option<String>) -> DeserializeGuard<GameServer> {
        let gs = GameServer {
            metadata: ObjectMeta {
                name: Some("name".into()),
                uid,
                annotations: None,
                ..Default::default()
            },
            spec: GameServerSpec {
                container: None,
                ports: vec![],
                health: Default::default(),
                scheduling: agones::SchedulingStrategy::Packed,
                sdk_server: Default::default(),
                template: Default::default(),
            },
            status: Some(GameServerStatus {
                state: GameServerState::Allocated,
                ports: Some(vec![GameServerStatusPort {
                    name: "addr".into(),
                    port: 7777,
                }]),
                address: std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1).to_string(),
                addresses: vec![NodeAddress {
                    type_: "addr".into(),
                    address: std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1).to_string(),
                }],
                node_name: "name".into(),
                reserved_until: None,
            }),
        };

        DeserializeGuard(Ok(gs))
    }

    processor.process_event(Event::Apply(game_server(None)));
    assert!(rx.try_recv().is_err());

    processor.process_event(Event::Init);
    for i in 0..10 {
        processor.process_event(Event::InitApply(game_server(if i % 2 == 0 {
            None
        } else {
            Some(i.to_string())
        })));
    }
    processor.process_event(Event::InitDone);
    assert!(rx.try_recv().is_err());

    let valid = uuid::Uuid::from_u128(0xdefaced);
    processor.process_event(Event::Apply(game_server(Some(valid.to_string()))));
    assert!(matches!(
        rx.try_recv(),
        Ok(providers::corrosion::push::Mutation::Upsert(_))
    ));

    processor.process_event(Event::Delete(game_server(Some("invalid".into()))));
    assert!(rx.try_recv().is_err());

    processor.process_event(Event::Delete(game_server(Some(valid.to_string()))));
    assert!(matches!(
        rx.try_recv(),
        Ok(providers::corrosion::push::Mutation::Remove(_))
    ));
}

/// Tests the accumulator and change iterator
#[test]
fn accumulates_mutations() {
    let state = Arc::new(corrosion::push::LocalState::default());
    let (mutator, mut rx) = corrosion::ServerMutator::testing(state.clone());

    let mut acc = corrosion::push::Accumulator::new(IcaoCode::new_testing([b'A'; 4]));

    for i in 0..100 {
        mutator.upsert_server(
            uuid::Uuid::from_u128(i),
            Endpoint {
                address: std::net::Ipv4Addr::from_bits(i as u32).into(),
                port: i as u16,
            },
            (0..i).map(|i| vec![i as u8; i as usize]).collect(),
        );
    }

    while let Ok(change) = rx.try_recv() {
        acc.accumulate(&state, change);
    }

    assert!(matches!(acc.take(), (Some(_), None, None)));

    for i in 101..10000 {
        mutator.upsert_server(
            uuid::Uuid::from_u128(i),
            Endpoint {
                address: std::net::Ipv4Addr::from_bits(i as u32).into(),
                port: i as u16,
            },
            std::iter::once(vec![i as u8; 3]).collect(),
        );
    }

    while let Ok(change) = rx.try_recv() {
        acc.accumulate(&state, change);
    }

    let upserts = acc.take().0.unwrap();
    let upserts_check = upserts.clone();

    let upserts = v1::ServerChange::Upsert(upserts);

    // The ServerIter is used to split a set of changes into 64k max blocks since
    // we use a u16 to length prefix
    let Ok(iter) = v1::ServerIter::new(upserts) else {
        unreachable!()
    };

    let mut check_index = 0;

    // Each block is a serialized ServerChange, but cut down to fit in 64k,
    // this just checks that the block can be deserialized and is equal to the
    // original set of upserts we are encoding
    for block in iter {
        let len = u16::from_le_bytes([block[0], block[1]]) as usize;
        assert_eq!(block.len(), len + 2);

        let mut dblock: Vec<v1::ServerChange> = serde_json::from_slice(&block[2..]).unwrap();
        assert_eq!(dblock.len(), 1);
        let v1::ServerChange::Upsert(dblock) = dblock.pop().unwrap() else {
            unreachable!();
        };

        let check = &upserts_check[check_index..];
        for (actual, expected) in dblock.iter().zip(check.iter()) {
            assert_eq!(actual, expected);
        }

        check_index += dblock.len();
    }

    assert_eq!(check_index, upserts_check.len());
}

/// Valides we can push and pull changes via a corrosion DB
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn applies_changes() {
    setup_tracing();

    struct Pusher {
        state: Arc<corrosion::push::LocalState>,
        mutator: corrosion::ServerMutator,
        rx: tokio::sync::mpsc::UnboundedReceiver<corrosion::push::Mutation>,
        acc: corrosion::push::Accumulator,
    }

    let state = Arc::new(corrosion::push::LocalState::default());
    let (mutator, rx) = corrosion::ServerMutator::testing(state.clone());

    let acc = corrosion::push::Accumulator::new(IcaoCode::new_testing([b'B'; 4]));

    let temp = tempfile::TempDir::new().expect("failed to create temp dir");

    let root = camino::Utf8Path::from_path(temp.path()).expect("non-utf8 path");
    let sub_path = root.join("subs");
    let db_path = root.join("db.db");

    let db = ::corrosion::db::InitializedDb::setup(&db_path, ::corrosion::schema::SCHEMA)
        .await
        .expect("failed to initialize DB");

    let subs = pubsub::SubsManager::default();

    let btx = BroadcastingTransactor::new(
        db.actor_id,
        db.clock.clone(),
        db.pool.clone(),
        subs.clone(),
        Default::default(),
        None,
    )
    .await;

    let mut pusher = Pusher {
        state,
        mutator,
        rx,
        acc,
    };

    const N: usize = 100;
    const PEER: std::net::SocketAddrV6 = std::net::SocketAddrV6::new(
        std::net::Ipv4Addr::new(5, 4, 3, 1).to_ipv6_compatible(),
        23423,
        0,
        0,
    );
    const PORT: u16 = 4567;
    let icao = IcaoCode::new_testing([b'Y'; 4]);

    let (trip, _w, _s) = ::corrosion::Tripwire::new_simple();
    let ctx = pubsub::PubsubContext::new(
        subs,
        sub_path,
        db.pool.clone(),
        db.schema.clone(),
        trip,
        ::corrosion::types::pubsub::MatcherLoopConfig {
            changes_threshold: 0,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let sub = ctx
        .subscribe(pubsub::SubParamsv1 {
            query: pubsub::SERVER_QUERY.into(),
            from: None,
            skip_rows: true,
            max_buffer: 0,
            max_time: std::time::Duration::from_millis(10),
            process_interval: std::time::Duration::from_millis(10),
            change_threshold: 0,
        })
        .await
        .unwrap();

    let mut bss = pubsub::BufferingSubStream::new(sub, 0, std::time::Duration::from_millis(0));

    let local = quilkin::config::Watch::new(quilkin::net::ClusterMap::new());

    async fn update_db(
        pusher: &mut Pusher,
        db: &BroadcastingTransactor,
        mutation: impl FnOnce(&mut corrosion::ServerMutator),
    ) {
        mutation(&mut pusher.mutator);

        while let Ok(change) = pusher.rx.try_recv() {
            pusher.acc.accumulate(&pusher.state, change);
        }

        let (upserts, updates, removes) = pusher.acc.take();

        if let Some(up) = upserts {
            db.execute(PEER, &[v1::ServerChange::Upsert(up)]).await;
        }

        if let Some(up) = updates {
            db.execute(PEER, &[v1::ServerChange::Update(up)]).await;
        }

        if let Some(rm) = removes {
            db.execute(PEER, &[v1::ServerChange::Remove(rm)]).await;
        }
    }

    async fn apply_state(
        bss: &mut pubsub::BufferingSubStream,
        local: &quilkin::config::Watch<quilkin::net::ClusterMap>,
    ) -> u64 {
        fn apply(
            mut block: bytes::Bytes,
            local: &quilkin::config::Watch<quilkin::net::ClusterMap>,
        ) -> u64 {
            let ss = pubsub::SubscriptionStream::length_prefixed(&mut block).unwrap();

            let cm = local.write();
            cm.corrosion_apply(ss).0
        }

        let mut cid = local
            .read()
            .get(&Some(quilkin::net::endpoint::Locality::new(
                "corrosion",
                "",
                "",
            )))
            .map_or(0, |es| es.change_id().0);

        while let Ok(Some(block)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), bss.next()).await
        {
            cid = cid.max(apply(block, local));
        }

        cid
    }

    async fn end2end(
        pusher: &mut Pusher,
        db: &BroadcastingTransactor,
        bss: &mut pubsub::BufferingSubStream,
        local: &quilkin::config::Watch<quilkin::net::ClusterMap>,
        mutation: impl FnOnce(&mut corrosion::ServerMutator),
    ) -> u64 {
        update_db(pusher, db, mutation).await;
        let changes = apply_state(bss, local).await;

        assert_eq!(
            pusher.state.to_map(),
            local
                .read()
                .get(&Some(quilkin::net::endpoint::Locality::new(
                    "corrosion",
                    "",
                    ""
                )))
                .unwrap()
                .to_map()
        );

        changes
    }

    btx.connected(PEER, icao, PORT).await;

    // Initialize the set
    end2end(&mut pusher, &btx, &mut bss, &local, |mutator| {
        for i in 0..N {
            mutator.upsert_server(
                uuid::Uuid::from_u128(i as _),
                Endpoint {
                    address: std::net::Ipv4Addr::from_bits(i as u32).into(),
                    port: i as u16,
                },
                std::iter::once(vec![i as u8; 3]).collect(),
            );
        }
    })
    .await;

    // Do an update of 1/2 and remove the rest
    end2end(&mut pusher, &btx, &mut bss, &local, |mutator| {
        for i in 0..N {
            if i % 2 == 0 {
                mutator.upsert_server(
                    uuid::Uuid::from_u128(i as _),
                    Endpoint {
                        address: std::net::Ipv4Addr::from_bits(i as u32).into(),
                        port: i as u16,
                    },
                    std::iter::once(vec![i as u8; 4]).collect(),
                );
            } else {
                mutator.remove_server(uuid::Uuid::from_u128(i as _));
            }
        }
    })
    .await;

    // Upsert the ones we removed
    let cid = end2end(&mut pusher, &btx, &mut bss, &local, |mutator| {
        for i in 0..N {
            if i % 2 != 0 {
                mutator.upsert_server(
                    uuid::Uuid::from_u128(i as _),
                    Endpoint {
                        address: std::net::Ipv4Addr::from_bits(i as u32).into(),
                        port: i as u16,
                    },
                    std::iter::once(vec![i as u8; 4]).collect(),
                );
            }
        }
    })
    .await;

    // Pretend as if the pusher completely goes away
    btx.disconnected(PEER).await;
    assert_eq!(
        cid,
        end2end(&mut pusher, &btx, &mut bss, &local, |_mutator| {}).await
    );

    // Redo all our upserts as if we reconnected, which should result in no
    // changes sent to subscribers since we only soft deleted the DB entries
    btx.connected(PEER, icao, PORT).await;
    assert_eq!(
        cid,
        end2end(&mut pusher, &btx, &mut bss, &local, |mutator| {
            for i in 0..N {
                mutator.upsert_server(
                    uuid::Uuid::from_u128(i as _),
                    Endpoint {
                        address: std::net::Ipv4Addr::from_bits(i as u32).into(),
                        port: i as u16,
                    },
                    std::iter::once(vec![i as u8; 4]).collect(),
                );
            }
        })
        .await
    );
}
