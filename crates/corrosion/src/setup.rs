//! Handles the conventional (ie non test setup) of the various components
//! needed for functioning of a relay:
//! 1. A database
//! 2. An executor of SQL transactions
//! 3. Queries that can subscribe to DB changes
//! 4. Gossip of DB changes to other relay nodes

pub use camino::Utf8PathBuf as PathBuf;
use corro_types::{
    self as ct, actor::ActorId, agent::SplitPool, pubsub::SubsManager, sqlite::CrConn,
};
use rusqlite::{Connection, OptionalExtension};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tripwire::Tripwire;

/// Configuration for all of the components needed for a relay
pub struct Config {
    /// A directory where all SQLite databases are stored
    pub db_root: PathBuf,
}

impl Config {
    pub async fn setup(self, tripwire: Tripwire) -> eyre::Result<()> {
        std::fs::create_dir_all(&self.db_root)?;

        // Path to the core database
        let db_path = self.db_root.join("quilkin.db");

        //let members = Members::default();

        let actor_id = {
            // we need to set auto_vacuum before any tables are created
            let db_conn = Connection::open(&db_path)?;
            db_conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")?;

            let conn = CrConn::init(db_conn)?;
            conn.query_row("SELECT crsql_site_id();", [], |row| {
                row.get::<_, ActorId>(0)
            })?
        };

        let write_sema = Arc::new(Semaphore::new(1));
        let pool = SplitPool::create(&db_path, write_sema.clone()).await?;

        let clock = Arc::new(
            uhlc::HLCBuilder::default()
                .with_id(actor_id.try_into().unwrap())
                .with_max_delta(Duration::from_millis(300))
                .build(),
        );

        let schema = {
            let mut conn = pool.write_priority().await?;
            ct::agent::migrate(clock.clone(), &mut conn)?;
            let mut schema = ct::schema::init_schema(&conn)?;
            schema.constrain()?;

            schema
        };

        let subs_manager = SubsManager::default();
        let subs_path = self.db_root.join("subscriptions");

        let subs_cache = crate::pubsub::restore_subscriptions(
            &subs_manager,
            &subs_path,
            &pool,
            &schema,
            &tripwire,
        )
        .await?;

        let cluster_id = {
            let conn = pool.read().await?;
            conn.query_row(
                "SELECT value FROM __corro_state WHERE key = 'cluster_id'",
                [],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_default()
        };

        Ok(())
    }
}

/// Spawns an async task that periodically prints out locks that have surpassed
/// the desired limits
#[inline]
pub fn spawn_lock_contention_printer(
    registry: corro_types::agent::LockRegistry,
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
