use corro_types::{
    actor::{ActorId, ClusterId},
    agent::{Booked, Bookie, SplitPool, WriteConn},
    schema::Schema,
    sqlite::{CrConn, SqlitePoolError},
};
use std::{sync::Arc, time::Duration};

pub mod read;
pub mod write;

/// Wraps [`SplitPool::read`] to enforce `temp_store = MEMORY` and `query_only = ON`.
/// Use this instead of [`SplitPool::read`] directly.
pub trait SplitPoolReadExt {
    fn read_readonly(
        &self,
    ) -> impl std::future::Future<Output = Result<sqlite_pool::Connection<CrConn>, SqlitePoolError>> + Send;
}

impl SplitPoolReadExt for SplitPool {
    async fn read_readonly(&self) -> Result<sqlite_pool::Connection<CrConn>, SqlitePoolError> {
        let conn = self.read().await?;
        // flag-only pragmas; failure means the connection itself is broken
        conn.pragma_update(None, "temp_store", "MEMORY")
            .expect("temp_store is a flag pragma; failure means the connection is broken");
        conn.pragma_update(None, "query_only", true)
            .expect("query_only is a flag pragma; failure means the connection is broken");
        Ok(conn)
    }
}

/// WAL synchronous mode. Affects whether `fsync` is called after each commit.
///
/// In WAL mode [`Normal`][WalSynchronous::Normal] is safe from database corruption (data can only
/// be lost on a simultaneous OS crash + hardware failure) and avoids the per-commit `fsync`.
/// For corrosion nodes that replicate state across peers, [`Normal`][WalSynchronous::Normal] is
/// the recommended setting.
#[derive(Clone, Copy, Default, Debug)]
pub enum WalSynchronous {
    /// Default `SQLite` WAL behaviour: `fsync` after every commit. No data loss on power failure.
    #[default]
    Full,
    /// Skip per-commit `fsync`; only sync during WAL checkpoints. Eliminates commit stalls on
    /// slow fsync paths (e.g. WSL2, network filesystems). Safe when data is replicated.
    Normal,
}

impl WalSynchronous {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::Normal => "NORMAL",
        }
    }
}

/// Hard size limits. Volume allocation must exceed `(max_page_count × page_size) + journal_size_limit`.
#[derive(Default)]
pub struct DBLimits {
    /// Max database file size in pages (4 KiB each; 262,144 = 1 GiB). `None` leaves the built-in ceiling.
    pub max_page_count: Option<u64>,
    /// Max WAL size in bytes after a checkpoint. `None` leaves the `SQLite` default (unlimited).
    pub journal_size_limit: Option<i64>,
    /// WAL synchronous mode. `None` leaves the `SQLite` default ([`WalSynchronous::Full`]).
    pub synchronous: Option<WalSynchronous>,
    /// Auto-checkpoint threshold in WAL pages. `Some(0)` disables automatic checkpointing entirely,
    /// deferring all WAL cleanup to the manual [`wal_checkpoint_over_threshold`] path.
    /// `None` leaves the `SQLite` default (1000 pages ≈ 4 MiB).
    pub wal_autocheckpoint: Option<u32>,
}

/// Returns `true` if `err` is `SQLITE_FULL`.
pub fn is_disk_full(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DiskFull,
                ..
            },
            _
        )
    )
}

/// Checkpoints the WAL and runs incremental vacuum. If `purge_count` is `Some(n)`,
/// deletes the `n` oldest `servers` rows first to free pages for reuse.
pub async fn recover_space(pool: &SplitPool, purge_count: Option<usize>) -> eyre::Result<()> {
    let conn = pool.write_priority().await?;
    tokio::task::block_in_place(|| {
        if let Some(count) = purge_count {
            conn.execute(
                "DELETE FROM servers WHERE endpoint IN \
                 (SELECT endpoint FROM servers ORDER BY cont_update ASC LIMIT ?)",
                rusqlite::params![count],
            )?;
            tracing::info!(count, "purged oldest servers to recover disk space");
        }
        let busy: bool = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
        if busy {
            tracing::warn!("WAL checkpoint during space recovery was busy");
        }
        conn.execute_batch("PRAGMA incremental_vacuum")?;
        Ok::<_, eyre::Error>(())
    })?;
    Ok(())
}

pub struct DBMaintenance {
    /// The interval at which to run maintenance
    pub interval: Duration,
    /// The Write-Ahead-Log threshold, in MiB
    pub wal_threshold: u32,
    /// The maximum number of unused free pages that can exist (a page is 4KiB)
    pub max_free_pages: u64,
    pub limits: DBLimits,
}

impl Default for DBMaintenance {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5 * 60),
            // 2 GiB
            wal_threshold: 2 * 1024,
            max_free_pages: 10000,
            limits: DBLimits::default(),
        }
    }
}

pub struct InitializedDb {
    /// Pool used to interact with the DB
    pub pool: SplitPool,
    /// Bookie
    pub bookie: Bookie,
    /// All booked versions
    pub booked: Booked,
    /// The CRSQL clock
    pub clock: crate::Clock,
    /// The parsed and verified schema
    pub schema: Arc<Schema>,
    /// The unique actor ID for this database connection, distinguishing it from
    /// other actors that gossip DB changes
    pub actor_id: ActorId,
    /// The cluster identifier
    ///
    /// This essentially determines the "version" of a database so that nodes only gossip changes with other nodes with
    /// the same cluster id
    pub cluster_id: ClusterId,
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

        /// We might want to make this configurable, but for now just hardcode to 1GiB, note that this is a negative value
        /// to inidicate it is in KiB, if it was positive it would be the cache size in pages
        const ONE_GIB: i64 = -(1024 * 1024);

        let write_sema = Arc::new(tokio::sync::Semaphore::new(1));
        let pool = SplitPool::create(&db_path, write_sema.clone(), ONE_GIB).await?;

        let clock = crate::Clock(Arc::new(
            uhlc::HLCBuilder::default()
                .with_id(actor_id.try_into().unwrap())
                .with_max_delta(std::time::Duration::from_millis(300))
                .build(),
        ));

        let schema = {
            let mut conn = pool.write_priority().await?;

            let old_schema = {
                corro_types::agent::migrate(clock.0.clone(), &mut conn)?;
                let mut schema = corro_types::schema::init_schema(&conn)?;
                schema.constrain()?;

                schema
            };

            let schema = tokio::task::block_in_place(|| {
                update_schema(&mut conn, old_schema, partial_schema)
            })?;

            let limits = maintenance.as_ref().map(|m| &m.limits);
            tokio::task::block_in_place(|| run_startup_checks(&conn, limits))?;

            schema
        };

        let cluster_id = {
            let conn = pool.read().await?;

            match conn.query_row(
                "SELECT value FROM __corro_state WHERE key = 'cluster_id'",
                [],
                |row| row.get(0),
            ) {
                Ok(value) => value,
                Err(rusqlite::Error::QueryReturnedNoRows) => Default::default(),
                Err(e) => return Err(e.into()),
            }
        };

        if let Some(dbm) = maintenance {
            spawn_db_maintenance(db_path, pool.clone(), dbm);
        }

        use eyre::WrapErr;
        let start = std::time::Instant::now();
        let conn = pool
            .read_readonly()
            .await
            .context("failed to acquire read")?;

        let (bookie, booked) =
            tokio::task::block_in_place(|| -> eyre::Result<(Bookie, Booked)> {
                let all_booked = corro_types::agent::BookedVersions::load_all_from_conn(&conn)
                    .context("failed to load booked versions")?;

                let bookie = Bookie::new(all_booked);
                let booked = bookie.ensure(actor_id);
                Ok((bookie, booked))
            })?;

        tracing::info!(elapsed = ?start.elapsed(), "Loaded booked versions");

        Ok(Self {
            pool,
            bookie,
            booked,
            clock,
            schema: Arc::new(schema),
            actor_id,
            cluster_id,
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

/// Verifies invariants and applies one-time configuration after schema migration.
/// Failures are fatal — the database cannot be used safely.
fn run_startup_checks(conn: &rusqlite::Connection, limits: Option<&DBLimits>) -> eyre::Result<()> {
    use eyre::WrapErr;

    conn.query_row("SELECT crsql_db_version()", [], |row| row.get::<_, i64>(0))
        .context("cr-sqlite extension not loaded")?;

    let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    eyre::ensure!(mode == "wal", "journal_mode is '{mode}', expected 'wal'");

    let options: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA compile_options")?;
        stmt.query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?
    };
    eyre::ensure!(
        !options.iter().any(|o| o == "THREADSAFE=0"),
        "SQLite built with THREADSAFE=0; multi-threaded access is undefined behavior"
    );

    // mmap bypasses fsync — C extension writes go directly to the file without WAL protection
    let mmap_size: i64 = conn.pragma_query_value(None, "mmap_size", |row| row.get(0))?;
    if mmap_size != 0 {
        tracing::warn!(
            mmap_size,
            "mmap enabled; C extension writes bypass fsync and can corrupt the database directly"
        );
    }

    // catches corruption left by a previous crash before building on top of it
    let check: String = conn.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    eyre::ensure!(check == "ok", "database corruption detected: {check}");

    // /tmp in Kubernetes pods is often a size-limited emptyDir; SQLITE_IOERR mid-query
    // if it fills up
    conn.pragma_update(None, "temp_store", "MEMORY")?;

    // WAL mode is required; NORMAL skips the per-commit fsync that WAL FULL would add.
    // Data can only be lost on a simultaneous OS crash + hardware failure, which is acceptable
    // because corrosion nodes replicate state across peers.
    conn.pragma_update(None, "synchronous", "NORMAL")?;

    if let Some(limits) = limits {
        if let Some(max_page_count) = limits.max_page_count {
            conn.pragma_update(None, "max_page_count", max_page_count)?;
            tracing::info!(max_page_count, "max_page_count limit applied");
        }
        if let Some(journal_size_limit) = limits.journal_size_limit {
            conn.pragma_update(None, "journal_size_limit", journal_size_limit)?;
            tracing::info!(journal_size_limit, "journal_size_limit applied");
        }
        if let Some(sync) = limits.synchronous {
            conn.pragma_update(None, "synchronous", sync.as_str())?;
            tracing::info!(?sync, "WAL synchronous override applied");
        }
        if let Some(n) = limits.wal_autocheckpoint {
            conn.pragma_update(None, "wal_autocheckpoint", n)?;
            tracing::info!(n, "wal_autocheckpoint set");
        }
    }

    Ok(())
}

/// We keep a write-ahead-log, which under write-pressure can grow to multiple gigabytes and needs periodic truncation.
///
/// We don't want to schedule this task too often since it locks the whole DB.
fn wal_checkpoint(conn: &rusqlite::Connection, timeout: Duration) -> eyre::Result<()> {
    let start = std::time::Instant::now();

    let orig: u64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
    conn.pragma_update(None, "busy_timeout", timeout.as_millis() as u64)?;

    let busy: bool = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
    if busy {
        tracing::warn!(?timeout, "WAL truncation incomplete: database still busy");
    } else {
        tracing::info!(elapsed = ?start.elapsed(), "WAL truncated");
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
        let conn = pool.read_readonly().await?;

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
