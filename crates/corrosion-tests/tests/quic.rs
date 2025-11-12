//! Tests for basic connection, operation, and disconnection between a UDP
//! server and clients

use std::sync::Arc;

use corrosion::{Peer, db, pubsub, persistent::{server, client, proto::{v1 as p, ExecResponse, ExecResult}}};
use corrosion_tests::{self as ct, Cell};
use quilkin_types::{Endpoint, IcaoCode, TokenSet};

#[derive(Clone)]
struct InstaPrinter {
    db: corro_types::agent::SplitPool,
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
}

#[async_trait::async_trait]
impl server::Mutator for InstaPrinter {
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        let mut dc = db::write::Datacenter(&mut dc);
        dc.insert(peer, qcmp_port, icao);

        {
            let mut conn = self.db.write_priority().await.unwrap();
            let tx = conn.transaction().unwrap();
            ct::exec(&tx, dc.0.iter()).unwrap();
            tx.commit().unwrap();
        }
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

        let rows_affected = {
            let mut conn = self.db.write_normal().await.unwrap();
            let tx = conn.transaction().unwrap();
            let rows = ct::exec(&tx, v.iter()).unwrap();
            tx.commit().unwrap();
            rows
        };

        ExecResponse {
            results: vec![ExecResult::Execute {
                rows_affected,
                time: 0.,
            }],
            time: 0.,
            version: None,
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
    async fn subscribe(&self, subp: pubsub::SubParamsv1) -> Result<pubsub::Subscription, pubsub::MatcherUpsertError> {
        unimplemented!();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_quic_stream() {
    let db = ct::TestSubsDb::new(corrosion::schema::SCHEMA, "test_quic_stream").await;

    let ip = InstaPrinter {
        db: db.pool.clone(),
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

    let mutator = client::MutationClient::connect(client, 2001, icao)
        .await
        .unwrap();
    insta::assert_snapshot!("connect", ip.print().await);

    // TODO: Pull this out into an actual type used in quilkin when we integrate the corrosion stuff in
    let mut actual = std::collections::BTreeMap::<IcaoCode, (Endpoint, TokenSet)>::new();
    let mut expected = std::collections::BTreeMap::<IcaoCode, (Endpoint, TokenSet)>::new();

    
    

    mutator
        .transactions(&[p::ServerChange::Upsert(vec![
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
        .await
        .unwrap();

    insta::assert_snapshot!("initial_insert", ip.print().await);

    mutator
        .transactions(&[
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
        .await
        .unwrap();

    insta::assert_snapshot!("remove_and_update", ip.print().await);

    mutator.shutdown().await;
    insta::assert_snapshot!("disconnect", ip.print().await);

    assert_eq!(expected, actual);
}
