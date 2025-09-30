//! Tests the DB mutations and queries work as expected

use corro_api_types::SqliteValue;
use corro_types::{agent::SplitPool, api::Statement};
use corrosion::client::read::{FromSqlValue, ServerRow};
use corrosion_utils as tu;
use quilkin_types::{AddressKind, Endpoint, IcaoCode};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

async fn exec_all<const N: usize>(v: &mut smallvec::SmallVec<[Statement; N]>, sp: &SplitPool) {
    let mut conn = sp.write_priority().await.unwrap();
    let tx = conn.transaction().unwrap();
    tu::exec(&tx, v.iter()).unwrap();
    tx.commit().unwrap();
    v.clear();
}

async fn read_server_row(id: usize, sp: &SplitPool) -> ServerRow {
    let conn = sp.read().await.unwrap();
    conn.query_row(
        "SELECT endpoint,icao,tokens FROM servers WHERE rowid = ?",
        [id],
        |row| {
            let mut v = Vec::with_capacity(3);
            v.push(row.get::<_, SqliteValue>(0).unwrap());
            v.push(row.get::<_, SqliteValue>(1).unwrap());
            v.push(row.get::<_, SqliteValue>(2).unwrap());
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
        icao: IcaoCode::new_testing([b'B', b'O', b'O', b'P']),
        tokens: [i.to_ne_bytes()].into(),
    }
}

const PREP_PEER: SocketAddrV6 = SocketAddrV6::new(Ipv6Addr::from_bits(0xaaffeeff), 8999, 0, 0);

async fn prep(name: &str) -> SplitPool {
    let sp = tu::new_split_pool(name, corrosion::schema::SCHEMA).await;

    const MAX: usize = 100;

    let mut iv = smallvec::SmallVec::<[_; MAX]>::new();
    let mut s = corrosion::client::write::Server::for_peer(PREP_PEER, &mut iv);

    for i in 0..1000u32 {
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
    let sp = prep("inserts_and_reads_servers").await;

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
    let sp = prep("collects_old_servers").await;
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
        let mut dc = corrosion::client::write::Datacenter(&mut v);
        dc.remove(PREP_PEER, Some(fake_time));

        let mut conn = sp.write_priority().await.unwrap();
        let tx = conn.transaction().unwrap();
        tu::exec(&tx, dc.0.iter()).unwrap();
        tx.commit().unwrap();
        dc.0.clear();
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
        let mut s = corrosion::client::write::Server::for_peer(PREP_PEER, &mut v);
        s.upsert(
            &Endpoint {
                address: AddressKind::Ip(Ipv6Addr::from_bits(0x888888888888).into()),
                port: 8888,
            },
            IcaoCode::new_testing([b'V'; 4]),
            &[8888u64.to_ne_bytes()].into(),
        );

        let mut conn = sp.write_priority().await.unwrap();
        let tx = conn.transaction().unwrap();
        tu::exec(&tx, s.statements.iter()).unwrap();
        tx.commit().unwrap();
        s.statements.clear();
    }

    // Do the actual removal of the servers with no contributors that are older than 30 minutes
    {
        let mut s = corrosion::client::write::Server::for_peer(PREP_PEER, &mut v);
        s.reap_old(std::time::Duration::from_secs(60 * 30));

        let mut conn = sp.write_priority().await.unwrap();
        let tx = conn.transaction().unwrap();
        tu::exec(&tx, s.statements.iter()).unwrap();
        tx.commit().unwrap();
    }

    let only_row = {
        let conn = sp.read().await.unwrap();
        let statement = conn
            .prepare("SELECT endpoint,icao,tokens,json(contributors) FROM servers")
            .unwrap();
        tu::query_to_string(statement, |sql, row| {
            row.add_cell(tu::Cell::new(
                &corrosion::client::read::parse_endpoint(&sql.get::<_, String>(0).unwrap())
                    .unwrap()
                    .to_string(),
            ));
            row.add_cell(tu::Cell::new(&sql.get::<_, String>(1).unwrap()));
            row.add_cell(tu::Cell::new(&format!(
                "{:?}",
                corrosion::client::read::deserialize_token_set(&sql.get::<_, String>(2).unwrap())
                    .unwrap()
            )));
            row.add_cell(tu::Cell::new(
                &serde_json::from_str::<serde_json::Value>(&sql.get::<_, String>(3).unwrap())
                    .unwrap()
                    .to_string(),
            ));
        })
    };

    insta::assert_snapshot!("only_one", only_row);
}
