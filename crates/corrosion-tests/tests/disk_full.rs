//! Tests for `SQLITE_FULL` detection and recovery via [`BroadcastingTransactor::with_full_recovery`].

use corro_types::agent::ChangeError;
use corrosion::{
    db::{self, DBLimits, DBMaintenance, InitializedDb},
    persistent::mutator::BroadcastingTransactor,
};
use std::{
    net::{Ipv4Addr, SocketAddrV6},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn disk_full_error() -> ChangeError {
    ChangeError::Rusqlite {
        source: rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DiskFull,
                extended_code: 13,
            },
            None,
        ),
        actor_id: None,
        version: None,
    }
}

fn other_error() -> ChangeError {
    ChangeError::Rusqlite {
        source: rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::ConstraintViolation,
                extended_code: 19,
            },
            None,
        ),
        actor_id: None,
        version: None,
    }
}

async fn make_transactor() -> corrosion_tests::TestSubsDb {
    corrosion_tests::TestSubsDb::new(corrosion::schema::SCHEMA, "disk_full").await
}

// ── is_disk_full ─────────────────────────────────────────────────────────────

#[test]
fn recognises_sqlite_full_error() {
    let err = rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error {
            code: rusqlite::ErrorCode::DiskFull,
            extended_code: 13,
        },
        None,
    );
    assert!(db::is_disk_full(&err));
}

#[test]
fn ignores_non_full_sqlite_errors() {
    for code in [
        rusqlite::ErrorCode::ConstraintViolation,
        rusqlite::ErrorCode::DatabaseBusy,
        rusqlite::ErrorCode::CannotOpen,
        rusqlite::ErrorCode::DatabaseCorrupt,
    ] {
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code,
                extended_code: 0,
            },
            None,
        );
        assert!(!db::is_disk_full(&err), "{code:?} should not be disk-full");
    }
}

#[test]
fn ignores_non_sqlite_failure_errors() {
    assert!(!db::is_disk_full(&rusqlite::Error::InvalidQuery));
    assert!(!db::is_disk_full(&rusqlite::Error::QueryReturnedNoRows));
}

// ── with_full_recovery retry logic ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn succeeds_on_first_attempt_without_recovery() {
    let tdb = make_transactor().await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let result = tdb
        .btx
        .with_full_recovery(None, {
            let cc = call_count.clone();
            move |_tx| {
                cc.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, ChangeError>(0)
            }
        })
        .await;

    assert!(result.is_ok(), "unexpected error: {result:?}");
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn retries_once_after_disk_full_then_succeeds() {
    let tdb = make_transactor().await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let result = tdb
        .btx
        .with_full_recovery(None, {
            let cc = call_count.clone();
            move |_tx| {
                let n = cc.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(disk_full_error())
                } else {
                    Ok(0usize)
                }
            }
        })
        .await;

    assert!(result.is_ok(), "should succeed after one retry: {result:?}");
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn retries_twice_with_purge_then_succeeds() {
    let tdb = make_transactor().await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let result = tdb
        .btx
        .with_full_recovery(None, {
            let cc = call_count.clone();
            move |_tx| {
                let n = cc.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(disk_full_error())
                } else {
                    Ok(0usize)
                }
            }
        })
        .await;

    assert!(
        result.is_ok(),
        "should succeed after purge retry: {result:?}"
    );
    assert_eq!(call_count.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn propagates_error_when_all_three_attempts_fail() {
    let tdb = make_transactor().await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let result = tdb
        .btx
        .with_full_recovery(None, {
            let cc = call_count.clone();
            move |_tx| {
                cc.fetch_add(1, Ordering::SeqCst);
                Err::<usize, _>(disk_full_error())
            }
        })
        .await;

    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), ChangeError::Rusqlite { source, .. } if db::is_disk_full(&source)),
        "returned error should be the disk-full error"
    );
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        3,
        "should attempt exactly three times"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn does_not_retry_non_disk_full_errors() {
    let tdb = make_transactor().await;
    let call_count = Arc::new(AtomicUsize::new(0));

    let result = tdb
        .btx
        .with_full_recovery(None, {
            let cc = call_count.clone();
            move |_tx| {
                cc.fetch_add(1, Ordering::SeqCst);
                Err::<usize, _>(other_error())
            }
        })
        .await;

    assert!(result.is_err());
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        1,
        "non-disk-full errors must not be retried"
    );
}

// ── recover_space ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn recover_space_checkpoint_and_vacuum_succeed_on_empty_db() {
    let sp = corrosion_tests::new_split_pool("recover_empty", corrosion::schema::SCHEMA).await;
    db::recover_space(&sp, None)
        .await
        .expect("recover_space should not fail on an empty db");
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_space_purges_oldest_entries_by_cont_update() {
    let sp = corrosion_tests::new_split_pool("recover_purge", corrosion::schema::SCHEMA).await;

    {
        let mut conn = sp.write_priority().await.unwrap();
        let tx = conn.transaction().unwrap();
        for i in 0u32..5 {
            let ep = format!("|{}:{}", Ipv4Addr::from(i + 1), i + 1);
            tx.execute(
                "INSERT INTO servers (endpoint, icao, tokens, contributors, cont_update) \
                 VALUES (?, 'XXXX', NULL, '{}', ?)",
                rusqlite::params![ep, i as i64],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // Purge the 3 oldest (cont_update 0, 1, 2).
    db::recover_space(&sp, Some(3))
        .await
        .expect("purge should succeed");

    let remaining: Vec<i64> = {
        let conn = sp.read().await.unwrap();
        let mut stmt = conn
            .prepare("SELECT cont_update FROM servers ORDER BY cont_update")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };

    assert_eq!(remaining, vec![3, 4], "should keep the two newest entries");
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_space_purge_count_exceeding_rows_deletes_all() {
    let sp = corrosion_tests::new_split_pool("recover_purge_all", corrosion::schema::SCHEMA).await;

    {
        let mut conn = sp.write_priority().await.unwrap();
        let tx = conn.transaction().unwrap();
        for i in 0u32..3 {
            let ep = format!("|{}:{}", Ipv4Addr::from(i + 1), i + 1);
            tx.execute(
                "INSERT INTO servers (endpoint, icao, tokens, contributors, cont_update) \
                 VALUES (?, 'XXXX', NULL, '{}', ?)",
                rusqlite::params![ep, i as i64],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    db::recover_space(&sp, Some(1000))
        .await
        .expect("should not error when purge count > rows");

    let remaining: i64 = sp
        .read()
        .await
        .unwrap()
        .query_row("SELECT COUNT(*) FROM servers", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 0);
}

// ── end-to-end: real SQLITE_FULL ─────────────────────────────────────────────

/// Fills a file-backed database with a tiny `max_page_count` until it returns
/// `SQLITE_FULL`, then verifies that `with_full_recovery` resolves it.
#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_recovery_from_real_sqlite_full() {
    use corrosion::db::write;

    let temp = tempfile::TempDir::new().unwrap();
    let db_path = camino::Utf8Path::from_path(temp.path())
        .unwrap()
        .join("test.db");

    // The schema + CRSQL internal tables consume ~20-40 pages; 150 gives a tight
    // budget for data rows.
    let db = InitializedDb::setup(
        &db_path,
        corrosion::schema::SCHEMA,
        Some(DBMaintenance {
            limits: DBLimits {
                max_page_count: Some(150),
                journal_size_limit: None,
                ..Default::default()
            },
            ..Default::default()
        }),
    )
    .await
    .expect("setup failed");

    let btx = BroadcastingTransactor::new(
        db.actor_id,
        db.clock.clone(),
        db.pool.clone(),
        corro_types::pubsub::SubsManager::default(),
        None,
    )
    .await
    .expect("setup failed")
    .with_full_purge_count(50);

    let peer = SocketAddrV6::new(std::net::Ipv6Addr::LOCALHOST, 9001, 0, 0);

    // Fill the database until SQLITE_FULL is returned.
    let mut triggered_full = false;
    for i in 0u32..2000 {
        let ep = quilkin_types::Endpoint {
            address: quilkin_types::AddressKind::Ip(Ipv4Addr::from(i + 1).into()),
            port: (i % 65535) as u16 + 1,
        };

        let result = btx
            .make_broadcastable_changes(None, move |tx| {
                let mut v = smallvec::SmallVec::<[_; 2]>::new();
                write::Server::for_peer(peer, &mut v).upsert(
                    &ep,
                    quilkin_types::IcaoCode::new_testing(*b"TEST"),
                    &[[0u8; 4]; 1].into(),
                );
                write::exec_interruptible(tx, &v).map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: None,
                    version: None,
                })
            })
            .await;

        if let Err(ChangeError::Rusqlite { ref source, .. }) = result
            && db::is_disk_full(source)
        {
            triggered_full = true;
            break;
        }
        result.unwrap();
    }

    assert!(
        triggered_full,
        "expected SQLITE_FULL with max_page_count=150"
    );

    let recovery_peer =
        SocketAddrV6::new(std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2), 9002, 0, 0);

    // with_full_recovery should succeed by purging old entries.
    let result = btx
        .with_full_recovery(None, move |tx| {
            let mut v = smallvec::SmallVec::<[_; 2]>::new();
            write::Server::for_peer(recovery_peer, &mut v).upsert(
                &quilkin_types::Endpoint {
                    address: quilkin_types::AddressKind::Ip(Ipv4Addr::new(99, 99, 99, 99).into()),
                    port: 9999,
                },
                quilkin_types::IcaoCode::new_testing(*b"RECV"),
                &[[1u8; 4]; 1].into(),
            );
            write::exec_interruptible(tx, &v).map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })
        })
        .await;

    assert!(
        result.is_ok(),
        "with_full_recovery should succeed after purging: {result:?}"
    );
}
