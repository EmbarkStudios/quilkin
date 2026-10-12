//! Endpoint churn must not grow the `CRDT` bookkeeping without bound.
//!
//! `cr-sqlite` keeps a sentinel clock row and a primary key entry for every row ever
//! deleted. The `servers` table stays flat under churn while those two do not, so without
//! [`reap_tombstones`] the database grows until the volume is exhausted.

use corro_types::agent::ChangeError;
use corrosion::{
    db::{DBMaintenance, InitializedDb, reap_tombstones, write},
    persistent::mutator::BroadcastingTransactor,
};
use quilkin_types::{AddressKind, Endpoint, IcaoCode};
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV6},
    time::Duration,
};

/// A stable population that heartbeats every round, plus this many endpoints
/// replaced per round.
const POPULATION: u32 = 200;
const CHURN: u32 = 100;
const ROUNDS: u32 = 10;
/// `servers` has four non-primary-key columns, so a live row is four clock rows.
const COLUMNS: u64 = 4;

fn endpoint(i: u32) -> Endpoint {
    Endpoint {
        address: AddressKind::Ip(Ipv4Addr::from_bits(i).into()),
        port: (i % 60000) as u16 + 1,
    }
}

struct Counts {
    servers: u64,
    clocks: u64,
    pks: u64,
    tombstones: u64,
}

async fn counts(pool: &corro_types::agent::SplitPool) -> Counts {
    let conn = pool.read().await.unwrap();
    let one = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, u64>(0)).unwrap();

    Counts {
        servers: one("SELECT COUNT(*) FROM servers"),
        clocks: one("SELECT COUNT(*) FROM servers__crsql_clock"),
        pks: one("SELECT COUNT(*) FROM servers__crsql_pks"),
        tombstones: one(
            "SELECT COUNT(*) FROM servers__crsql_clock WHERE col_name = -1 AND col_version % 2 = 0",
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn churn_does_not_grow_clock_tables() {
    let temp = tempfile::TempDir::new().unwrap();
    let db_path = camino::Utf8Path::from_path(temp.path())
        .unwrap()
        .join("churn.db");

    let db = InitializedDb::setup(
        &db_path,
        corrosion::schema::SCHEMA,
        Some(DBMaintenance::default()),
    )
    .await
    .unwrap();

    let btx = BroadcastingTransactor::new(
        db.actor_id,
        db.clock.clone(),
        db.pool.clone(),
        corro_types::pubsub::SubsManager::default(),
        None,
        db.bookie,
        db.booked,
        None,
    );

    let peer = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9001, 0, 0);
    let icao = IcaoCode::new_testing(*b"TEST");
    let tokens = [[0u8; 4]; 1].into();

    let apply = async |stmts: smallvec::SmallVec<[corrosion::api::Statement; 32]>| {
        btx.make_broadcastable_changes(None, move |tx| {
            write::exec_interruptible(tx, &stmts).map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })
        })
        .await
        .unwrap();
    };

    let mut next = POPULATION;
    for round in 0..ROUNDS {
        // the agents re-upsert everything they still see
        for chunk in (0..POPULATION).collect::<Vec<_>>().chunks(32) {
            let mut v = smallvec::SmallVec::<[_; 32]>::new();
            let mut s = write::Server::for_peer(peer, &mut v);
            for i in chunk {
                s.upsert(&endpoint(*i), icao, &tokens);
            }
            apply(v).await;
        }

        let mut v = smallvec::SmallVec::<[_; 32]>::new();
        {
            let mut s = write::Server::for_peer(peer, &mut v);
            for i in next..next + CHURN {
                s.upsert(&endpoint(i), icao, &tokens);
            }
            if round > 0 {
                for i in next - CHURN..next {
                    s.remove_immediate(&endpoint(i));
                }
            }
        }
        apply(v).await;
        next += CHURN;

        // tombstones younger than the retention window have to survive, or a peer
        // that has yet to converge can resurrect the row
        let (collected, _) = reap_tombstones(
            &db.pool,
            "servers",
            Duration::from_secs(60 * 60),
            &db.clock,
            2000,
        )
        .await
        .unwrap();
        assert_eq!(collected, 0, "round {round} collected a live tombstone");

        reap_tombstones(&db.pool, "servers", Duration::ZERO, &db.clock, 2000)
            .await
            .unwrap();
    }

    let after = counts(&db.pool).await;
    let live = u64::from(POPULATION + CHURN);

    assert_eq!(after.servers, live, "live rows should be untouched");
    assert_eq!(after.tombstones, 0, "every tombstone should be collected");
    assert_eq!(
        after.clocks,
        live * COLUMNS,
        "clock rows should track the live set, not every endpoint ever seen"
    );
    assert_eq!(after.pks, live, "primary keys should track the live set");
}
