use corro_types::{
    actor::ActorId,
    agent::{SplitPool, WriteConn},
    schema::Schema,
    sqlite::CrConn,
};
use std::sync::Arc;

pub mod read;
pub mod write;

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
    pub async fn setup(db_path: &crate::Path, schema: &str) -> eyre::Result<Self> {
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

use std::time::Duration;

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
