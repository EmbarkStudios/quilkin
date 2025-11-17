//! Tests for basic connection, operation, and disconnection between a UDP
//! server and clients

use std::time::Duration;

use corro_api_types::Statement;
use corro_types::base::CrsqlDbVersion;
use corrosion::{
    Peer, db,
    persistent::{
        ErrorCode, client,
        executor::BroadcastingTransactor,
        proto::{ExecResponse, ExecResult, v1 as p},
        server,
    },
    pubsub,
};
use corrosion_tests::{self as ct, Cell};
use quilkin_types::{Endpoint, IcaoCode, TokenSet};

#[derive(Clone)]
struct InstaPrinter {
    db: corro_types::agent::SplitPool,
    btx: BroadcastingTransactor,
}

impl InstaPrinter {
    async fn print(&self) -> String {
        let conn = self.db.read().await.unwrap();

        let statement = conn
            .prepare("SELECT endpoint,icao,json(contributors) FROM servers")
            .unwrap();
        let mut servers = ct::query_to_string(statement, |srow, prow| {
            prow.add_cell(Cell::new(&srow.get::<_, String>(0).unwrap()));
            prow.add_cell(Cell::new(&srow.get::<_, String>(1).unwrap()));
            prow.add_cell(Cell::new(&srow.get::<_, String>(2).unwrap()));
        });
        let statement = conn
            .prepare("SELECT ip,icao,json(servers) FROM dc")
            .unwrap();
        let dc = ct::query_to_string(statement, |srow, prow| {
            prow.add_cell(Cell::new(&srow.get::<_, String>(0).unwrap()));
            prow.add_cell(Cell::new(&srow.get::<_, String>(1).unwrap()));
            prow.add_cell(Cell::new(&srow.get::<_, String>(2).unwrap()));
        });

        servers.push('\n');
        servers.push_str(&dc);
        servers
    }

    async fn do_mutate(&self, ops: &[Statement]) -> (usize, Option<CrsqlDbVersion>, Duration) {
        self.btx
            .make_broadcastable_changes(None, |tx| {
                corrosion_tests::exec_interr(tx, ops.iter()).map_err(|e| {
                    corro_types::agent::ChangeError::Rusqlite {
                        source: e,
                        actor_id: None,
                        version: None,
                    }
                })
            })
            .await
            .unwrap()
    }
}

#[async_trait::async_trait]
impl server::Mutator for InstaPrinter {
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        {
            let mut dc = db::write::Datacenter(&mut dc);
            dc.insert(peer, qcmp_port, icao);
        }

        self.do_mutate(&dc).await;
    }

    async fn execute(&self, peer: Peer, statements: &[p::ServerChange]) -> ExecResponse {
        let mut v = smallvec::SmallVec::<[_; 20]>::new();
        {
            for s in statements {
                match s {
                    p::ServerChange::Upsert(i) => {
                        let mut srv = db::write::Server::for_peer(peer, &mut v);
                        for i in i {
                            srv.upsert(&i.endpoint, i.icao, &i.tokens);
                        }
                    }
                    p::ServerChange::Remove(r) => {
                        let mut srv = db::write::Server::for_peer(peer, &mut v);
                        for r in r {
                            srv.remove_immediate(r);
                        }
                    }
                    p::ServerChange::Update(u) => {
                        let mut srv = db::write::Server::for_peer(peer, &mut v);
                        for u in u {
                            srv.update(&u.endpoint, u.icao, u.tokens.as_ref());
                        }
                    }
                    p::ServerChange::UpdateMutator(mu) => {
                        let mut dc = db::write::Datacenter(&mut v);
                        dc.update(peer, mu.qcmp_port, mu.icao);
                    }
                }
            }
        }

        let (rows_affected, version, dur) = self.do_mutate(&v).await;

        ExecResponse {
            results: vec![ExecResult::Execute {
                rows_affected,
                time: 0.,
            }],
            time: dur.as_secs_f64(),
            version: version.map(|v| v.0),
            actor_id: None,
        }
    }

    async fn disconnected(&self, peer: Peer) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        let mut dc = db::write::Datacenter(&mut dc);
        dc.remove(peer, None);

        {
            let mut conn = self.db.write_priority().await.unwrap();
            let tx = conn.transaction().unwrap();
            ct::exec(&tx, dc.0.iter()).unwrap();
            tx.commit().unwrap();
        }
    }
}

#[derive(Clone)]
struct ServerSub {
    ctx: pubsub::PubsubContext,
}

#[async_trait::async_trait]
impl server::SubManager for ServerSub {
    async fn subscribe(
        &self,
        subp: pubsub::SubParamsv1,
    ) -> Result<pubsub::Subscription, pubsub::MatcherUpsertError> {
        self.ctx.subscribe(subp).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_quic_stream() {
    let db = ct::TestSubsDb::new(corrosion::schema::SCHEMA, "test_quic_stream").await;

    let ip = InstaPrinter {
        db: db.pool.clone(),
        btx: db.btx.clone(),
    };

    let ss = ServerSub {
        ctx: db.pubsub_ctx(),
    };

    let server =
        server::Server::new_unencrypted((std::net::Ipv6Addr::LOCALHOST, 0).into(), ip.clone(), ss)
            .unwrap();

    let icao = IcaoCode::new_testing([b'Y'; 4]);

    let client = client::Client::connect_insecure(server.local_addr())
        .await
        .unwrap();

    let mutator = client::MutationClient::connect(client.clone(), 2001, icao)
        .await
        .unwrap();
    insta::assert_snapshot!("connect", ip.print().await);

    // TODO: Pull this out into an actual type used in quilkin when we integrate the corrosion stuff in
    let mut actual = std::collections::BTreeMap::<Endpoint, (IcaoCode, TokenSet)>::new();
    let mut expected = std::collections::BTreeMap::<Endpoint, (IcaoCode, TokenSet)>::new();

    let (subscriber, sub) = client::SubscriptionClient::connect(
        client,
        corrosion::pubsub::SubParamsv1 {
            query: corrosion::api::Statement::Simple(pubsub::SERVER_QUERY.into()),
            from: None,
            skip_rows: false,
            max_buffer: 0,
            max_time: Duration::from_millis(10),
            process_interval: Duration::from_millis(10),
            change_threshold: 0,
        },
    )
    .await
    .unwrap();

    let mut srx = sub.rx;

    let mut mutate = async |sc: &[p::ServerChange]| {
        for sc in sc {
            match sc {
                p::ServerChange::Upsert(up) => {
                    for u in up {
                        expected.insert(u.endpoint.clone(), (u.icao, u.tokens.clone()));
                    }
                }
                p::ServerChange::Update(up) => {
                    for u in up {
                        let s = expected
                            .get_mut(&u.endpoint)
                            .expect("failed to find expected endpoint");
                        if let Some(ic) = u.icao {
                            s.0 = ic;
                        }
                        if let Some(ts) = u.tokens.clone() {
                            s.1 = ts;
                        }
                    }
                }
                p::ServerChange::Remove(r) => {
                    for r in r {
                        expected.remove(r);
                    }
                }
                p::ServerChange::UpdateMutator(_) => unreachable!(),
            }
        }

        mutator
            .transactions(sc)
            .await
            .expect("failed to apply transactions");
    };

    let mut process_sub = async || -> bool {
        let change = tokio::time::timeout(Duration::from_millis(10000), srx.recv())
            .await
            .expect("timed out waiting for server change")
            .expect("expected change");

        use corrosion::{
            api::{TypedQueryEvent as tqe, sqlite::ChangeType},
            db::read::{self, FromSqlValue},
        };

        let (cty, row) = match change {
            tqe::Change(cty, _, row, _) => (cty, row),
            tqe::Row(_, row) => (ChangeType::Insert, row),
            _ => return false,
        };

        let row = read::ServerRow::from_sql(&row).expect("failed to deserialize row");

        match cty {
            ChangeType::Insert => {
                actual.insert(row.endpoint, (row.icao, row.tokens));
            }
            ChangeType::Update => {
                let r = actual
                    .get_mut(&row.endpoint)
                    .expect("expected endpoint not found");
                r.0 = row.icao;
                r.1 = row.tokens;
            }
            ChangeType::Delete => {
                actual.remove(&row.endpoint);
            }
        }

        true
    };

    mutate(&[p::ServerChange::Upsert(vec![
        p::ServerUpsert {
            endpoint: Endpoint {
                address: std::net::Ipv4Addr::new(1, 2, 3, 4).into(),
                port: 2002,
            },
            icao,
            tokens: [[20; 2]].into(),
        },
        p::ServerUpsert {
            endpoint: Endpoint {
                address: std::net::Ipv4Addr::new(9, 9, 9, 9).into(),
                port: 2003,
            },
            icao,
            tokens: [[30; 3]].into(),
        },
        p::ServerUpsert {
            endpoint: Endpoint {
                address: std::net::Ipv6Addr::from_bits(0xf0ccac1a).into(),
                port: 2004,
            },
            icao,
            tokens: [[40; 4]].into(),
        },
        p::ServerUpsert {
            endpoint: Endpoint {
                address: quilkin_types::AddressKind::Name("game.boop.com".into()),
                port: 2005,
            },
            icao,
            tokens: [[50; 5]].into(),
        },
    ])])
    .await;

    for _ in 0..4 {
        while !process_sub().await {}
    }

    insta::assert_snapshot!("initial_insert", ip.print().await);

    mutate(&[
        p::ServerChange::Remove(vec![Endpoint {
            address: std::net::Ipv4Addr::new(9, 9, 9, 9).into(),
            port: 2003,
        }]),
        p::ServerChange::Update(vec![p::ServerUpdate {
            endpoint: Endpoint {
                address: std::net::Ipv6Addr::from_bits(0xf0ccac1a).into(),
                port: 2004,
            },
            icao: Some(IcaoCode::new_testing([b'X'; 4])),
            tokens: None,
        }]),
    ])
    .await;

    for _ in 0..2 {
        while !process_sub().await {}
    }

    insta::assert_snapshot!("remove_and_update", ip.print().await);

    mutator.shutdown().await;
    insta::assert_snapshot!("disconnect", ip.print().await);
    subscriber.shutdown(ErrorCode::Ok).await;

    assert_eq!(expected, actual);
}
