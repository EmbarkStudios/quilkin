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
    /// WAL synchronous mode. `None` leaves the startup setting ([`WalSynchronous::Normal`]).
    pub synchronous: Option<WalSynchronous>,
    /// Auto-checkpoint threshold in WAL pages. `Some(0)` disables automatic checkpointing entirely,
    /// deferring all WAL cleanup to the manual [`wal_checkpoint_over_threshold`] path.
    /// `None` leaves the `SQLite` default (1000 pages ≈ 4 MiB).
    pub wal_autocheckpoint: Option<u32>,
}

/// Percentage of the volume the main database file may occupy.
const DB_CAPACITY_PCT: u64 = 75;
/// Percentage of the volume the WAL may retain after a checkpoint.
const WAL_CAPACITY_PCT: u64 = 12;

/// Caps for a volume of `capacity` bytes, as `(max_page_count, journal_size_limit)`.
///
/// The two together are held under 100% so the remainder stays free for recovery writes.
/// `None` if the volume cannot fit even a single page under the database cap.
fn derive_limits(capacity: u64, page_size: u64) -> Option<(u64, i64)> {
    let page_size = page_size.max(1);
    let max_page_count = capacity.checked_mul(DB_CAPACITY_PCT)? / 100 / page_size;
    let journal_size_limit = capacity.checked_mul(WAL_CAPACITY_PCT)? / 100;
    (max_page_count > 0).then_some((max_page_count, journal_size_limit as i64))
}

/// Total size in bytes of the filesystem holding `path`.
#[cfg(unix)]
fn volume_capacity(path: &crate::Path) -> Option<u64> {
    let stat = rustix::fs::statvfs(path.as_str()).ok()?;
    stat.f_blocks.checked_mul(stat.f_frsize)
}

#[cfg(not(unix))]
fn volume_capacity(_path: &crate::Path) -> Option<u64> {
    None
}

impl DBLimits {
    /// Fills whichever size caps are unset from the capacity of the volume holding `db_path`.
    ///
    /// Left unset, `SQLite` grows until the filesystem is exhausted, and at that point
    /// `wal_checkpoint`, `incremental_vacuum` and the row purge in [`recover_space`] all need
    /// to write and so all fail: the database is wedged until an operator intervenes. Capping
    /// the database and WAL below capacity turns that into `SQLITE_FULL` while free space
    /// remains, which [`crate::persistent::mutator::Transactor::with_full_recovery`] can act on.
    pub fn apply_volume_defaults(
        &mut self,
        conn: &rusqlite::Connection,
        db_path: &crate::Path,
    ) -> eyre::Result<()> {
        if self.max_page_count.is_some() && self.journal_size_limit.is_some() {
            return Ok(());
        }

        let Some(capacity) = volume_capacity(db_path) else {
            tracing::warn!(%db_path, "volume capacity unavailable, database growth is unbounded");
            return Ok(());
        };

        let page_size: i64 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let Some((max_page_count, journal_size_limit)) = derive_limits(capacity, page_size as u64)
        else {
            tracing::warn!(
                capacity,
                page_size,
                "volume too small to derive size limits"
            );
            return Ok(());
        };

        self.max_page_count.get_or_insert(max_page_count);
        self.journal_size_limit.get_or_insert(journal_size_limit);
        tracing::info!(capacity, "derived size limits from volume capacity");

        Ok(())
    }
}

/// Returns `true` if `err` is `SQLITE_FULL`.
///
/// A full database arrives in two shapes: as itself, and as the generic error
/// `cr-sqlite` re-raises when one of its own triggers is what ran out of room,
/// which names the original code in its message and nowhere else. Both have to
/// reach [`crate::persistent::mutator::Transactor::with_full_recovery`], since
/// recovery is what frees the space to carry on.
pub fn is_disk_full(err: &rusqlite::Error) -> bool {
    let rusqlite::Error::SqliteFailure(ffi, message) = err else {
        return false;
    };

    ffi.code == rusqlite::ErrorCode::DiskFull
        || message.as_deref().is_some_and(|m| m.ends_with("code FULL"))
}

/// Smallest lift [`LiftedPageLimit`] applies, so a database too small for the
/// proportional one still gets room to work in.
const MIN_RECOVERY_PAGES: i64 = 64;

/// Raises `max_page_count` for as long as it is held, restoring it on the way out.
///
/// Recovering space costs space. Deleting a row from a CRSQL table writes before
/// it frees, since the row's tombstone is an insert into the table's clock, and
/// checkpointing moves WAL frames into a database file that may have to grow to
/// take them. At the limit neither can run, so the work that would bring a full
/// database back under its limit is the work the limit forbids.
///
/// The lift comes out of the reserve [`derive_limits`] leaves unallocated for
/// this, and lasts only as long as the recovery.
struct LiftedPageLimit<'c> {
    conn: &'c rusqlite::Connection,
    limit: i64,
}

impl<'c> LiftedPageLimit<'c> {
    fn new(conn: &'c rusqlite::Connection) -> rusqlite::Result<Self> {
        let limit: i64 = conn.query_row("PRAGMA max_page_count", [], |row| row.get(0))?;
        conn.pragma_update(
            None,
            "max_page_count",
            limit.saturating_add((limit / 10).max(MIN_RECOVERY_PAGES)),
        )?;

        Ok(Self { conn, limit })
    }
}

impl Drop for LiftedPageLimit<'_> {
    fn drop(&mut self) {
        // Only after the pages have been reclaimed, since SQLite will not take a
        // limit below the size the database currently is
        if let Err(error) = self.conn.pragma_update(None, "max_page_count", self.limit) {
            tracing::error!(%error, limit = self.limit, "failed to restore the page limit");
        }
    }
}

/// Checkpoints the WAL and runs incremental vacuum. If `purge_count` is `Some(n)`,
/// deletes the `n` oldest `servers` rows first to free pages for reuse.
pub async fn recover_space(pool: &SplitPool, purge_count: Option<usize>) -> eyre::Result<()> {
    let conn = pool.write_priority().await?;
    tokio::task::block_in_place(|| {
        let _lifted = LiftedPageLimit::new(&conn)?;

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
    /// How long a deleted row's tombstone is kept before [`reap_tombstones`] collects it.
    ///
    /// `None` disables collection, which lets the clock tables grow without bound under
    /// row churn. Must exceed the time a partitioned peer can take to converge, otherwise
    /// that peer can resurrect a row whose tombstone has already been collected.
    pub tombstone_retention: Option<Duration>,
    /// Tombstones collected per table per tick, bounding how long the write lock is held.
    pub tombstone_batch: usize,
    pub limits: DBLimits,
}

impl Default for DBMaintenance {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5 * 60),
            // 2 GiB
            wal_threshold: 2 * 1024,
            max_free_pages: 10000,
            // orders of magnitude above gossip convergence, and short enough that
            // churn cannot accumulate a meaningful amount of dead weight
            tombstone_retention: Some(Duration::from_secs(60 * 60)),
            tombstone_batch: 2000,
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
        mut maintenance: Option<DBMaintenance>,
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

            if let Some(dbm) = maintenance.as_mut() {
                tokio::task::block_in_place(|| dbm.limits.apply_volume_defaults(&conn, db_path))?;
            }

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
            let tables = schema.tables.keys().cloned().collect();
            spawn_db_maintenance(db_path, pool.clone(), dbm, clock.clone(), tables);
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

    let cache_size: i64 = {
        // update settings in write conn
        let conn = pool.write_low().await?;
        let cache_size = conn.pragma_query_value(None, "cache_size", |row| row.get(0))?;
        conn.pragma_update(None, "cache_size", 100000)?;
        cache_size
    };

    let result = async {
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

        Ok(())
    }
    .await;

    // restore the cache size even if vacuuming failed
    let conn = pool.write_low().await?;
    conn.pragma_update(None, "cache_size", cache_size)?;

    result
}

/// Collects the `CRDT` bookkeeping left behind by deleted rows.
///
/// `cr-sqlite` keeps a sentinel row in `<table>__crsql_clock` for every row ever deleted,
/// so that a delete can still win against a concurrent write from a peer that has yet to
/// see it, and a matching entry in `<table>__crsql_pks`. Neither is ever removed: under
/// row churn the base table stays flat while those two grow without bound, which is what
/// eventually exhausts the volume. Once `retention` has passed every peer has long since
/// converged and the tombstone is only taking up space.
///
/// Deletes at most `limit` tombstones, plus any primary keys orphaned by an earlier pass.
/// Returns the `(tombstone, primary key)` row counts. The clock tables are internal to
/// `cr-sqlite` and carry no triggers, so this does not itself produce a change to gossip.
pub async fn reap_tombstones(
    pool: &SplitPool,
    table: &str,
    retention: Duration,
    clock: &crate::Clock,
    limit: usize,
) -> eyre::Result<(usize, usize)> {
    let now = corro_types::broadcast::Timestamp::from(clock.0.new_timestamp()).to_ntp64();
    let cutoff = corro_types::broadcast::Timestamp(now - uhlc::NTP64::from(retention));

    let mut conn = pool.write_low().await?;

    tokio::task::block_in_place(|| {
        let tx = conn.immediate_transaction()?;

        // col_name -1 marks the delete sentinel, and an even col_version means the row is
        // currently deleted rather than resurrected by a later insert
        let tombstones = tx
            .prepare_cached(&format!(
                "DELETE FROM {table}__crsql_clock WHERE key IN (
                     SELECT key FROM {table}__crsql_clock
                     WHERE col_name = -1 AND col_version % 2 = 0 AND ts < ?
                     LIMIT ?
                 )"
            ))?
            .execute(rusqlite::params![cutoff, limit])?;

        // a key whose clock rows have all gone can never be referenced again
        let pks = tx
            .prepare_cached(&format!(
                "DELETE FROM {table}__crsql_pks WHERE __crsql_key IN (
                     SELECT __crsql_key FROM {table}__crsql_pks
                     WHERE NOT EXISTS (
                         SELECT 1 FROM {table}__crsql_clock WHERE key = __crsql_key
                     )
                     LIMIT ?
                 )"
            ))?
            .execute(rusqlite::params![limit])?;

        tx.commit()?;
        Ok::<_, rusqlite::Error>((tombstones, pks))
    })
    .map_err(Into::into)
}

fn spawn_db_maintenance(
    path: &crate::Path,
    pool: SplitPool,
    dbm: DBMaintenance,
    clock: crate::Clock,
    tables: Vec<String>,
) {
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

            if let Some(retention) = dbm.tombstone_retention {
                for table in &tables {
                    match reap_tombstones(&pool, table, retention, &clock, dbm.tombstone_batch)
                        .await
                    {
                        Ok((0, 0)) => {}
                        Ok((tombstones, pks)) => {
                            tracing::debug!(table, tombstones, pks, "collected tombstones");
                        }
                        Err(error) => {
                            tracing::error!(%error, table, "could not collect tombstones");
                        }
                    }
                }
            }

            if let Err(error) =
                wal_checkpoint_over_threshold(wal_path.as_path(), &pool, wal_threshold).await
            {
                tracing::error!(%error, "could not wal_checkpoint truncate");
            }
        }
    });
}

#[cfg(test)]
mod test {
    use super::*;

    /// The caps have to leave the volume with room to spare. `SQLITE_FULL` is only
    /// recoverable while writes still land: `recover_space` checkpoints, vacuums and
    /// purges rows, and every one of those needs to write.
    #[test]
    fn derived_limits_leave_recovery_headroom() {
        const PAGE: u64 = 4096;

        // 64 MiB is the volume the relays wedged on
        for capacity in [64 << 20, 256 << 20, 1 << 30, 64u64 << 30] {
            let (max_page_count, journal_size_limit) =
                derive_limits(capacity, PAGE).expect("limits should be derivable");
            let used = max_page_count * PAGE + journal_size_limit as u64;
            let reserved = capacity.checked_sub(used).expect("caps exceed the volume");

            assert!(
                reserved >= capacity / 20,
                "{capacity} byte volume left only {reserved} bytes for recovery"
            );
        }
    }

    /// An operator sizing the caps against their own volume has to win over the
    /// derived ones, including when they set only one of the two.
    #[test]
    fn configured_limits_are_kept() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut limits = DBLimits {
            max_page_count: Some(1234),
            ..Default::default()
        };

        limits
            .apply_volume_defaults(&conn, crate::Path::new("/"))
            .unwrap();

        assert_eq!(limits.max_page_count, Some(1234));
        assert!(
            limits.journal_size_limit.is_some(),
            "the unset cap should still be derived"
        );
    }
}
