//! Tests for basic connection, operation, and disconnection between a UDP
//! server and clients

use corrosion::{Peer, client as c, persistent as p};
use corrosion_utils as tu;
use quilkin_types::{Endpoint, IcaoCode};

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
        let mut servers = tu::query_to_string(statement, |srow, prow| {
            prow.add_cell(tu::Cell::new(&srow.get::<_, String>(0).unwrap()));
            prow.add_cell(tu::Cell::new(&srow.get::<_, String>(1).unwrap()));
            prow.add_cell(tu::Cell::new(&srow.get::<_, String>(2).unwrap()));
        });
        let statement = conn
            .prepare("SELECT ip,icao,json(servers) FROM dc")
            .unwrap();
        let dc = tu::query_to_string(statement, |srow, prow| {
            prow.add_cell(tu::Cell::new(&srow.get::<_, String>(0).unwrap()));
            prow.add_cell(tu::Cell::new(&srow.get::<_, String>(1).unwrap()));
            prow.add_cell(tu::Cell::new(&srow.get::<_, String>(2).unwrap()));
        });

        servers.push('\n');
        servers.push_str(&dc);
        servers
    }
}

#[async_trait::async_trait]
impl p::server::AgentExecutor for InstaPrinter {
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        let mut dc = c::write::Datacenter(&mut dc);
        dc.insert(peer, qcmp_port, icao);

        {
            let mut conn = self.db.write_priority().await.unwrap();
            let tx = conn.transaction().unwrap();
            tu::exec(&tx, dc.0.iter()).unwrap();
            tx.commit().unwrap();
        }
    }

    async fn execute(&self, peer: Peer, statements: &[p::ServerChange]) -> p::ExecResult {
        let mut v = smallvec::SmallVec::<[_; 20]>::new();
        {
            let mut srv = c::write::Server::for_peer(peer, &mut v);

            for s in statements {
                match s {
                    p::ServerChange::Insert(i) => {
                        for i in i {
                            srv.upsert(&i.endpoint, i.icao, &i.tokens);
                        }
                    }
                    p::ServerChange::Remove(r) => {
                        for r in r {
                            srv.remove_immediate(r);
                        }
                    }
                    p::ServerChange::Update(u) => {
                        for u in u {
                            let mut ub = c::write::UpdateBuilder::new(&u.endpoint);
                            if let Some(icao) = u.icao {
                                ub = ub.update_icao(icao);
                            }

                            if let Some(ts) = &u.tokens {
                                ub = ub.update_tokens(ts);
                            }
                            srv.update(ub);
                        }
                    }
                }
            }
        }

        let rows_affected = {
            let mut conn = self.db.write_normal().await.unwrap();
            let tx = conn.transaction().unwrap();
            let rows = tu::exec(&tx, v.iter()).unwrap();
            tx.commit().unwrap();
            rows
        };

        p::ExecResult::Execute {
            rows_affected,
            time: 0.,
        }
    }

    async fn disconnected(&self, peer: Peer) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        let mut dc = c::write::Datacenter(&mut dc);
        dc.remove(peer, None);

        {
            let mut conn = self.db.write_priority().await.unwrap();
            let tx = conn.transaction().unwrap();
            tu::exec(&tx, dc.0.iter()).unwrap();
            tx.commit().unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_quic_stream() {
    let ip = InstaPrinter {
        db: tu::new_split_pool("quic-basic", corrosion::schema::SCHEMA).await,
    };

    let server =
        p::server::Server::new_unencrypted((std::net::Ipv6Addr::LOCALHOST, 0).into(), ip.clone())
            .unwrap();

    let icao = IcaoCode::new_testing([b'Y'; 4]);

    let client = p::client::Client::connect_insecure(server.local_addr(), 2001, icao)
        .await
        .unwrap();

    insta::assert_snapshot!("connect", ip.print().await);

    client
        .transactions(&[p::ServerChange::Insert(vec![
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

    client
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

    client.shutdown().await;
    insta::assert_snapshot!("disconnect", ip.print().await);
}
