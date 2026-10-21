//! Tests the DB mutations and queries work as expected

use corro_api_types::SqliteValue;
use corro_types::{agent::SplitPool, api::Statement};
use corrosion::db::{
    read::{self, FromSqlValue, ServerRow},
    write,
};
use corrosion_tests::{self as ct, Cell};
use quilkin_types::{AddressKind, Capabilities, Endpoint, IcaoCode};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

async fn exec_all<const N: usize>(v: &mut smallvec::SmallVec<[Statement; N]>, sp: &SplitPool) {
    let mut conn = sp.write_priority().await.unwrap();
    let tx = conn.transaction().unwrap();
    ct::exec(&tx, v.iter()).unwrap();
    tx.commit().unwrap();
    v.clear();
}

async fn read_server_row(id: usize, sp: &SplitPool) -> ServerRow {
    let conn = sp.read().await.unwrap();
    conn.query_row(
        "SELECT endpoint,icao,tokens FROM servers WHERE rowid = ?",
        [id],
        |row| {
            let v = vec![
                row.get::<_, SqliteValue>(0).unwrap(),
                row.get::<_, SqliteValue>(1).unwrap(),
                row.get::<_, SqliteValue>(2).unwrap(),
            ];
            Ok(ServerRow::from_sql(&v).unwrap())
        },
    )
    .unwrap()
}

fn make_row(i: u32) -> ServerRow {
    let address = match i % 3 {
        0 => AddressKind::Ip(Ipv4Addr::from_bits(i).into()),
        1 => AddressKind::Name(format!("boop.{i}.net")),
        2 => AddressKind::Ip(Ipv6Addr::from_bits(i as _).into()),
        _ => unreachable!(),
    };

    let endpoint = Endpoint {
        address,
        port: i as u16,
    };

    ServerRow {
        endpoint,
        icao: IcaoCode::new_testing(*b"BOOP"),
        tokens: [i.to_ne_bytes()].into(),
    }
}

const PREP_PEER: SocketAddrV6 = SocketAddrV6::new(Ipv6Addr::from_bits(0xaaffeeff), 8999, 0, 0);

async fn prep(name: &str, count: u32) -> SplitPool {
    let sp = ct::new_split_pool(name, corrosion::schema::SCHEMA).await;

    const MAX: usize = 100;

    let mut iv = smallvec::SmallVec::<[_; MAX]>::new();
    let mut s = write::Server::for_peer(PREP_PEER, &mut iv);

    for i in 0..count {
        let row = make_row(i);

        if s.statements.len() == MAX {
            exec_all(s.statements, &sp).await;
        }

        s.upsert(&row.endpoint, row.icao, &row.tokens);
    }

    if !s.statements.is_empty() {
        exec_all(s.statements, &sp).await;
    }

    sp
}

/// Tests basics of inserting servers and datacenters
#[tokio::test]
async fn inserts_and_reads_servers() {
    let sp = prep("inserts_and_reads_servers", 1000).await;

    {
        let r = sp.read().await.unwrap();
        assert_eq!(
            1000,
            r.query_row("SELECT COUNT(*) FROM servers", [], |r| r.get::<_, u32>(0))
                .unwrap()
        );
    }

    for i in 0..3u32 {
        let row = read_server_row(i as usize + 1, &sp).await;
        let expected = make_row(i);

        assert_eq!(row, expected);
    }
}

/// Tests that servers that have no datacenter contributors are reaped after
/// some amount of time
#[tokio::test]
async fn collects_old_servers() {
    let sp = prep("collects_old_servers", 1000).await;
    let fake_time = time::UtcDateTime::now() - std::time::Duration::from_secs(60 * 60);

    {
        let r = sp.read().await.unwrap();
        assert_eq!(
            1000,
            r.query_row("SELECT COUNT(*) FROM servers", [], |r| r.get::<_, u32>(0))
                .unwrap()
        );
    }

    let mut v = smallvec::SmallVec::<[_; 2]>::new();

    // Remove the DC as a contributor, but set the time of the update to an hour in the past
    {
        let mut dc = corrosion::db::write::Datacenter(&mut v);
        dc.remove(PREP_PEER, Some(fake_time));
        exec_all(dc.0, &sp).await;
    }

    {
        let r = sp.read().await.unwrap();
        assert_eq!(
            1000,
            r.query_row("SELECT COUNT(*) FROM servers", [], |r| r.get::<_, u32>(0))
                .unwrap()
        );
    }

    // Add a new server that should not be deleted since it still has a contributor
    {
        let mut s = write::Server::for_peer(PREP_PEER, &mut v);
        s.upsert(
            &Endpoint {
                address: AddressKind::Ip(Ipv6Addr::from_bits(0x888888888888).into()),
                port: 8888,
            },
            IcaoCode::new_testing([b'V'; 4]),
            &[8888u64.to_ne_bytes()].into(),
        );

        exec_all(s.statements, &sp).await;
    }

    // Do the actual removal of the servers with no contributors that are older than 30 minutes
    {
        v.push(write::Server::<2>::reap_old(
            std::time::Duration::from_secs(60 * 30),
        ));
        exec_all(&mut v, &sp).await;
    }

    let only_row = {
        let conn = sp.read().await.unwrap();
        let statement = conn
            .prepare("SELECT endpoint,icao,tokens,json(contributors) FROM servers")
            .unwrap();
        ct::query_to_string(statement, |sql, row| {
            row.add_cell(Cell::new(
                &read::parse_endpoint(&sql.get::<_, String>(0).unwrap())
                    .unwrap()
                    .to_string(),
            ));
            row.add_cell(Cell::new(&sql.get::<_, String>(1).unwrap()));
            row.add_cell(Cell::new(&format!(
                "{:?}",
                read::deserialize_token_set(Some(&sql.get::<_, String>(2).unwrap())).unwrap()
            )));
            row.add_cell(Cell::new(
                &serde_json::from_str::<serde_json::Value>(&sql.get::<_, String>(3).unwrap())
                    .unwrap()
                    .to_string(),
            ));
        })
    };

    insta::assert_snapshot!("only_one", only_row);
}

/// Tests that servers can be updated
#[tokio::test]
async fn updates_servers() {
    let sp = prep("updates_servers", 1).await;

    let only_row = async || {
        let conn = sp.read().await.unwrap();
        let statement = conn
            .prepare("SELECT endpoint,icao,tokens FROM servers WHERE rowid = 1")
            .unwrap();
        ct::query_to_string(statement, |sql, row| {
            row.add_cell(Cell::new(
                &read::parse_endpoint(&sql.get::<_, String>(0).unwrap())
                    .unwrap()
                    .to_string(),
            ));
            row.add_cell(Cell::new(&sql.get::<_, String>(1).unwrap()));
            row.add_cell(Cell::new(&format!(
                "{:?}",
                read::deserialize_token_set(Some(&sql.get::<_, String>(2).unwrap())).unwrap()
            )));
        })
    };

    insta::assert_snapshot!("initial_us", only_row().await);

    let ep = Endpoint {
        address: AddressKind::Ip(std::net::Ipv4Addr::from_bits(0).into()),
        port: 0,
    };

    let mut v = smallvec::SmallVec::<[_; 2]>::new();
    // Update just the ICAO
    {
        let mut s = write::Server::for_peer(PREP_PEER, &mut v);
        s.update(&ep, Some(IcaoCode::new_testing([b'Z'; 4])), None);
        exec_all(s.statements, &sp).await;
    }

    insta::assert_snapshot!("updated_icao_us", only_row().await);

    // Update just the tokenset
    {
        let mut s = write::Server::for_peer(PREP_PEER, &mut v);
        s.update(&ep, None, Some(&[[b'Z'; 20]; 1].into()));
        exec_all(s.statements, &sp).await;
    }

    insta::assert_snapshot!("updated_tokenset_us", only_row().await);

    // Update both
    {
        let mut s = write::Server::for_peer(PREP_PEER, &mut v);
        s.update(
            &ep,
            Some(IcaoCode::new_testing([b'Y'; 4])),
            Some(&[[b'Y'; 10]; 1].into()),
        );
        exec_all(s.statements, &sp).await;
    }

    insta::assert_snapshot!("update_both_us", only_row().await);
}

/// A node is registered when it joins gossip and its row goes when it leaves,
/// so the table only names nodes something is currently connected to.
#[tokio::test]
async fn registers_and_deregisters_nodes() {
    let sp = prep("registers_and_deregisters_nodes", 1).await;

    let nodes = async || -> Vec<(String, u16)> {
        let conn = sp.read().await.unwrap();
        let mut statement = conn.prepare("SELECT ip,port FROM node").unwrap();
        statement
            .query_map((), |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    assert!(nodes().await.is_empty(), "nothing has joined yet");

    let mut v = smallvec::SmallVec::<[_; 2]>::new();
    {
        let mut node = write::Node(&mut v);
        node.upsert(
            PREP_PEER,
            7600,
            IcaoCode::new_testing([b'E'; 4]),
            Capabilities::GAMESERVERS,
        );
        exec_all(node.0, &sp).await;
    }

    assert_eq!(nodes().await, vec![(PREP_PEER.ip().to_string(), 7600)]);

    // Rejoining on a different port updates in place; a node is its address.
    {
        let mut node = write::Node(&mut v);
        node.upsert(
            PREP_PEER,
            7601,
            IcaoCode::new_testing([b'E'; 4]),
            Capabilities::GAMESERVERS,
        );
        exec_all(node.0, &sp).await;
    }

    assert_eq!(nodes().await, vec![(PREP_PEER.ip().to_string(), 7601)]);

    {
        let mut node = write::Node(&mut v);
        node.remove(PREP_PEER);
        exec_all(node.0, &sp).await;
    }

    assert!(nodes().await.is_empty(), "the node left");
}

/// A coordinate survives the round trip through the database, and a row is
/// only reported as having one once it does.
#[tokio::test]
async fn stores_and_reads_coordinates() {
    let sp = prep("stores_and_reads_coordinates", 1).await;

    let read = async || -> read::NodeRow {
        let conn = sp.read().await.unwrap();
        conn.query_row(corrosion::pubsub::NODE_QUERY, [], |row| {
            let v = (0..8)
                .map(|i| row.get::<_, SqliteValue>(i).unwrap())
                .collect::<Vec<_>>();
            Ok(read::NodeRow::from_sql(&v).unwrap())
        })
        .unwrap()
    };

    let mut v = smallvec::SmallVec::<[_; 2]>::new();
    {
        let mut node = write::Node(&mut v);
        node.upsert(
            PREP_PEER,
            7600,
            IcaoCode::new_testing([b'E'; 4]),
            Capabilities::GAMESERVERS,
        );
        exec_all(node.0, &sp).await;
    }

    // Registered but unsolved: no coordinate rather than one at the origin.
    assert_eq!(read().await.coordinate, None);

    let outgoing = [1.5, 0.0, f64::MIN_POSITIVE];
    let incoming = [2.5, 3.25, 0.125];
    {
        let mut node = write::Node(&mut v);
        node.coordinate(PREP_PEER, &outgoing, &incoming, 7);
        exec_all(node.0, &sp).await;
    }

    let row = read().await;
    let coordinate = row.coordinate.expect("solved");

    // Exact: a coordinate that drifts in storage is a coordinate that lies.
    assert_eq!(coordinate.outgoing, outgoing);
    assert_eq!(coordinate.incoming, incoming);
    assert_eq!(coordinate.round, 7);

    // The identity columns are untouched by a coordinate write.
    assert_eq!(row.qcmp_port, 7600);
    assert_eq!(row.capabilities, Capabilities::GAMESERVERS);
}

/// A node that stopped renewing is reaped, which is the backstop for a
/// departure that was never observed.
#[tokio::test]
async fn reaps_nodes_that_stopped_renewing() {
    let sp = prep("reaps_nodes_that_stopped_renewing", 1).await;

    let count = async || -> usize {
        let conn = sp.read().await.unwrap();
        conn.query_row("SELECT COUNT(*) FROM node", (), |row| row.get(0))
            .unwrap()
    };

    let mut v = smallvec::SmallVec::<[_; 2]>::new();
    {
        let mut node = write::Node(&mut v);
        node.upsert(
            PREP_PEER,
            7600,
            IcaoCode::new_testing([b'E'; 4]),
            Capabilities::GAMESERVERS,
        );
        exec_all(node.0, &sp).await;
    }

    // A node that renewed a moment ago survives.
    v.push(write::Node::<2>::reap_old(std::time::Duration::from_secs(
        600,
    )));
    exec_all(&mut v, &sp).await;
    assert_eq!(count().await, 1);

    // Age it past the threshold.
    v.push(corrosion::api::Statement::Simple(
        "UPDATE node SET joined = unixepoch('now') - 1000".into(),
    ));
    v.push(write::Node::<2>::reap_old(std::time::Duration::from_secs(
        600,
    )));
    exec_all(&mut v, &sp).await;
    assert_eq!(count().await, 0);
}

/// Tests that datacenters can be updated
#[tokio::test]
async fn updates_datacenters() {
    let sp = prep("updates_datacenters", 1).await;

    let only_row = async || {
        let conn = sp.read().await.unwrap();
        let statement = conn
            .prepare("SELECT ip,icao,port FROM dc WHERE rowid = 1")
            .unwrap();
        ct::query_to_string(statement, |sql, row| {
            row.add_cell(Cell::new(&sql.get::<_, String>(0).unwrap()));
            row.add_cell(Cell::new(&sql.get::<_, String>(1).unwrap()));
            row.add_cell(Cell::new(&sql.get::<_, u16>(2).unwrap().to_string()));
        })
    };

    insta::assert_snapshot!("initial_ud", only_row().await);

    let mut v = smallvec::SmallVec::<[_; 2]>::new();
    // Update just the ICAO
    {
        let mut dc = write::Datacenter(&mut v);
        dc.update(PREP_PEER, None, Some(IcaoCode::new_testing([b'Z'; 4])));
        exec_all(dc.0, &sp).await;
    }

    insta::assert_snapshot!("updated_icao_ud", only_row().await);

    // Update just the port
    {
        let mut dc = write::Datacenter(&mut v);
        dc.update(PREP_PEER, Some(9876), None);
        exec_all(dc.0, &sp).await;
    }

    insta::assert_snapshot!("updated_port_ud", only_row().await);

    // Update both
    {
        let mut dc = write::Datacenter(&mut v);
        dc.update(
            PREP_PEER,
            Some(1234),
            Some(IcaoCode::new_testing([b'B'; 4])),
        );
        exec_all(dc.0, &sp).await;
    }

    insta::assert_snapshot!("update_both_ud", only_row().await);
}

/// An unknown code must neither replace the one already stored, nor keep a
/// server out of the index that lets its peer stop contributing it
#[tokio::test]
async fn servers_survive_an_unknown_icao() {
    let sp = prep("servers_survive_an_unknown_icao", 1).await;

    let known = make_row(0);
    let newcomer = Endpoint {
        address: AddressKind::Ip(Ipv6Addr::from_bits(0x5151).into()),
        port: 5151,
    };

    let mut v = smallvec::SmallVec::<[_; 4]>::new();
    {
        let mut s = write::Server::for_peer(PREP_PEER, &mut v);
        s.upsert(&known.endpoint, None, &known.tokens);
        s.upsert(&newcomer, None, &[5151u64.to_ne_bytes()].into());
        exec_all(s.statements, &sp).await;
    }

    assert_eq!(
        known.icao,
        read_server_row(1, &sp).await.icao,
        "an unknown code replaced the one already stored"
    );

    // An hour back, so both are past the reap threshold once uncontributed
    {
        let mut dc = write::Datacenter(&mut v);
        dc.remove(
            PREP_PEER,
            Some(time::UtcDateTime::now() - std::time::Duration::from_secs(60 * 60)),
        );
        exec_all(dc.0, &sp).await;
    }
    {
        v.push(write::Server::<4>::reap_old(
            std::time::Duration::from_secs(60 * 30),
        ));
        exec_all(&mut v, &sp).await;
    }

    let remaining: u32 = {
        let r = sp.read().await.unwrap();
        r.query_row("SELECT COUNT(*) FROM servers", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(
        0, remaining,
        "a server added under an unknown code was left uncollectable"
    );
}
