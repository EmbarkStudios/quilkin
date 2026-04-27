use corro_types::{
    actor::ActorId,
    agent::{SplitPool, WriteConn},
    schema::Schema,
    sqlite::CrConn,
};
use std::{sync::Arc, time::Duration};

pub mod read;
pub mod write;

pub struct DBMaintenance {
    /// The interval at which to run maintenance
    pub interval: Duration,
    /// The Write-Ahead-Log threshold, in MiB
    pub wal_threshold: u32,
    /// The maximum number of unused free pages that can exist (a page is 4KiB)
    pub max_free_pages: u64,
}

impl Default for DBMaintenance {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5 * 60),
            // 2 GiB
            wal_threshold: 2 * 1024,
            max_free_pages: 10000,
        }
    }
}

pub struct InitializedDb {
    /// Pool used to interact with the DB
    pub pool: SplitPool,
    /// The CRSQL clock
    pub clock: Arc<uhlc::HLC>,
    /// The parsed and verified schema
    pub schema: Arc<Schema>,
    /// The unique actor ID for this database connection, distinguishing it from
    /// other actors that gossip DB changes
    pub actor_id: ActorId,
}

impl InitializedDb {
    /// Attempts to initialize a CRSQL database with the specified schema, creating
    /// it if it doesn't exist
    ///
    /// If `maintenance` is specified, spawns a task to maintain the WAL and vacuum the DB
    pub async fn setup(
        db_path: &crate::Path,
        schema: &str,
        maintenance: Option<DBMaintenance>,
    ) -> eyre::Result<Self> {
        let partial_schema = corro_types::schema::parse_sql(schema)?;

        let actor_id = {
            // we need to set auto_vacuum before any tables are created
            let db_conn = rusqlite::Connection::open(db_path)?;
            db_conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")?;

            let conn = CrConn::init(db_conn)?;
            conn.query_row("SELECT crsql_site_id();", [], |row| {
                row.get::<_, ActorId>(0)
            })?
        };

        let write_sema = Arc::new(tokio::sync::Semaphore::new(1));
        let pool = SplitPool::create(&db_path, write_sema.clone()).await?;

        let clock = Arc::new(
            uhlc::HLCBuilder::default()
                .with_id(actor_id.try_into().unwrap())
                .with_max_delta(std::time::Duration::from_millis(300))
                .build(),
        );

        let schema = {
            let mut conn = pool.write_priority().await?;

            let old_schema = {
                corro_types::agent::migrate(clock.clone(), &mut conn)?;
                let mut schema = corro_types::schema::init_schema(&conn)?;
                schema.constrain()?;

                schema
            };

            tokio::task::block_in_place(|| update_schema(&mut conn, old_schema, partial_schema))?
        };

        if let Some(dbm) = maintenance {
            spawn_db_maintenance(db_path, pool.clone(), dbm);
        }

        Ok(Self {
            pool,
            clock,
            schema: Arc::new(schema),
            actor_id,
        })
    }
}

/// We currently only support updating the schema at startup
fn update_schema(
    conn: &mut WriteConn,
    old_schema: Schema,
    new_schema: Schema,
) -> eyre::Result<Schema> {
    // clone the previous schema and apply
    let mut new_schema = {
        let mut schema = old_schema.clone();
        for (name, def) in new_schema.tables.iter() {
            // overwrite table because users are expected to return a full table def
            schema.tables.insert(name.clone(), def.clone());
        }
        schema
    };

    new_schema.constrain()?;

    let tx = conn.immediate_transaction()?;

    corro_types::schema::apply_schema(&tx, &old_schema, &mut new_schema)?;

    for tbl_name in new_schema.tables.keys() {
        tx.execute("DELETE FROM __corro_schema WHERE tbl_name = ?", [tbl_name])?;

        let n = tx.execute("INSERT INTO __corro_schema SELECT tbl_name, type, name, sql, 'api' AS source FROM sqlite_schema WHERE tbl_name = ? AND type IN ('table', 'index') AND name IS NOT NULL AND sql IS NOT NULL", [tbl_name])?;
        tracing::info!("Updated {n} rows in __corro_schema for table {tbl_name}");
    }

    tx.commit()?;
    Ok(new_schema)
}

/// We keep a write-ahead-log, which under write-pressure can grow to multiple gigabytes and needs periodic truncation.
///
/// We don't want to schedule this task too often since it locks the whole DB.
fn wal_checkpoint(conn: &rusqlite::Connection, timeout: Duration) -> eyre::Result<()> {
    tracing::debug!("handling db_cleanup (WAL truncation)");
    let start = std::time::Instant::now();

    let orig: u64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
    conn.pragma_update(None, "busy_timeout", timeout.as_millis() as u64)?;

    let busy: bool = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
    if busy {
        tracing::warn!(
            ?timeout,
            "could not truncate sqlite WAL, database busy - with timeout"
        );
    } else {
        tracing::info!(elapsed = ?start.elapsed(), "successfully truncated sqlite WAL");
    }

    drop(conn.pragma_update(None, "busy_timeout", orig));

    Ok(())
}

async fn wal_checkpoint_over_threshold(
    wal_path: &crate::Path,
    pool: &SplitPool,
    threshold: u64,
) -> eyre::Result<bool> {
    let wal_size = wal_path.metadata()?.len();
    let should_truncate = wal_size > threshold;

    if should_truncate {
        let conn = if wal_size > (5 * threshold) {
            tracing::warn!(
                size = wal_size,
                "wal_size is over 5x the threshold, trying to get a priority conn"
            );
            pool.write_priority().await?
        } else {
            pool.write_low().await?
        };

        fn calc_busy_timeout(wal_size: u64, threshold: u64) -> u64 {
            let wal_size_gb = wal_size / (1024 * 1024 * 1024);
            let threshold_gb = threshold / (1024 * 1024 * 1024);
            let base_timeout = 30000;
            if wal_size_gb <= threshold_gb {
                return base_timeout;
            }

            // Double the timeout every 5gb and cap at 16 minutes
            let diff = ((wal_size_gb - threshold_gb) / 5).min(5);
            // add extra (five * diff) seconds for every extra 1gb over 4gb
            let linear_increase = (wal_size_gb % 5) * 5000 * (diff + 1);
            let timeout = base_timeout * 2u64.pow(diff as u32) + linear_increase;
            // we are using a 16min timeout, something is wrong if we get here
            if diff >= 5 {
                tracing::warn!("WAL size is too large, setting busy timeout {timeout}ms");
            }
            timeout
        }

        let timeout = calc_busy_timeout(wal_size, threshold);
        tokio::task::block_in_place(|| wal_checkpoint(&conn, Duration::from_millis(timeout)))?;
    }

    Ok(should_truncate)
}

/// Continuously runs an [incremental vacuum](https://sqlite.org/lang_vacuum.html) until the number of unused free pages is below the limit
async fn vacuum_db(pool: &SplitPool, lim: u64) -> eyre::Result<()> {
    let mut freelist: u64 = {
        let conn = pool.read().await?;

        let vacuum: u64 = conn.pragma_query_value(None, "auto_vacuum", |row| row.get(0))?;
        eyre::ensure!(vacuum == 2, "auto_vacuum has to be set to INCREMENTAL");

        conn.pragma_query_value(None, "freelist_count", |row| row.get(0))?
    };

    let (busy_timeout, cache_size) = {
        // update settings in write conn
        let conn = pool.write_low().await?;
        let orig: u64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
        let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |row| row.get(0))?;
        conn.pragma_update(None, "cache_size", 100000)?;
        (orig, cache_size)
    };

    while freelist >= lim {
        let conn = pool.write_low().await?;

        tokio::task::block_in_place(|| {
            let mut prepped = conn.prepare("pragma incremental_vacuum(1000)")?;
            let mut rows = prepped.query([])?;

            while let Ok(Some(_)) = rows.next() {}

            freelist = conn.pragma_query_value(None, "freelist_count", |row| row.get(0))?;

            Ok::<(), eyre::Error>(())
        })?;

        drop(conn);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let conn = pool.write_low().await?;
    conn.pragma_update(None, "busy_timeout", busy_timeout)?;
    conn.pragma_update(None, "cache_size", cache_size)?;

    Ok(())
}

fn spawn_db_maintenance(path: &crate::Path, pool: SplitPool, dbm: DBMaintenance) {
    let wal_path = {
        let mut wp = path.to_path_buf();
        wp.set_extension(format!("{}-wal", path.extension().unwrap_or_default()));
        wp
    };
    let wal_threshold = dbm.wal_threshold as u64 * 1024 * 1024;

    tokio::spawn(async move {
        // try to initially truncate the WAL
        match wal_checkpoint_over_threshold(wal_path.as_path(), &pool, wal_threshold).await {
            Ok(truncated) if truncated => {
                tracing::info!("initially truncated WAL");
            }
            Err(error) => {
                tracing::error!(%error, "could not initially truncate WAL");
            }
            _ => {}
        }

        // large sleep right at the start to give node time to sync
        tokio::time::sleep(Duration::from_secs(60)).await;

        let mut vacuum_interval = tokio::time::interval(dbm.interval);

        loop {
            vacuum_interval.tick().await;
            if let Err(error) = vacuum_db(&pool, dbm.max_free_pages).await {
                tracing::error!(%error, "could not check freelist and vacuum");
            }

            if let Err(error) =
                wal_checkpoint_over_threshold(wal_path.as_path(), &pool, wal_threshold).await
            {
                tracing::error!(%error, "could not wal_checkpoint truncate");
            }
        }
    });
}

/// Spawns an async task that periodically prints out locks that have surpassed
/// the desired limits
#[inline]
pub fn spawn_lock_contention_printer(
    registry: crate::types::agent::LockRegistry,
    warn_threshold: Duration,
    error_threshold: Duration,
    check_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    assert!(error_threshold > warn_threshold);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);

        let mut top = indexmap::IndexMap::<usize, corro_types::agent::LockMeta>::default();
        loop {
            interval.tick().await;

            top.extend(
                registry
                    .map
                    .read()
                    .iter()
                    .filter(|(_, meta)| meta.started_at.elapsed() >= warn_threshold)
                    .take(10) // this is an ordered map, so taking the first few is gonna be the highest values
                    .map(|(k, v)| (*k, v.clone())),
            );

            if top.is_empty() {
                continue;
            }

            tracing::warn!(
                held_locks = top.len(),
                "lock registry shows locks held for a long time!"
            );

            for (id, lock) in top.drain(..) {
                let duration = lock.started_at.elapsed();

                if matches!(lock.state, corro_types::agent::LockState::Locked)
                    && duration >= error_threshold
                {
                    tracing::error!(
                        label = lock.label, id, kind = ?lock.kind, state = ?lock.state, ?duration, "lock exceeded error threshold"
                    );
                } else {
                    tracing::warn!(
                        label = lock.label, id, kind = ?lock.kind, state = ?lock.state, ?duration, "lock exceeded warning threshold"
                    );
                }
            }
        }
    })
}
