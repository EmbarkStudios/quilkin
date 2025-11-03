use crate::{Peer, db, persistent as p};
use corro_types::{
    actor::ActorId,
    agent::{Booked, BookedVersions, ChangeError, LockRegistry, SplitPool},
    base::{CrsqlDbVersion, CrsqlSeq},
    broadcast, change,
    pubsub::SubsManager,
    updates::match_changes,
};
use quilkin_types::IcaoCode;
use rusqlite::Transaction;
use sqlite_pool::InterruptibleTransaction;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub type BroadcastTx = tokio::sync::mpsc::Sender<broadcast::BroadcastInput>;

/// A DB mutator that will broadcast changes to any subscribers when a mutation
/// occurs that matches a subscriber's query
#[derive(Clone)]
pub struct BroadcastingTransactor {
    pool: SplitPool,
    book: Booked,
    subs: SubsManager,
    tx: Option<BroadcastTx>,
    clock: Arc<uhlc::HLC>,
    id: ActorId,
}

impl BroadcastingTransactor {
    pub async fn new(
        id: ActorId,
        clock: Arc<uhlc::HLC>,
        pool: SplitPool,
        subs: SubsManager,
        lock_registry: LockRegistry,
        tx: Option<BroadcastTx>,
    ) -> Self {
        let book = Booked::new(BookedVersions::new(id), lock_registry);

        tokio::spawn({
            let pool = pool.clone();
            // acquiring the lock here means everything will have to wait for it to be ready
            let mut booked = book.write_owned::<&str, _>("init", None).await;
            async move {
                let conn = pool.read().await?;
                **booked = tokio::task::block_in_place(|| BookedVersions::from_conn(&conn, id))
                    .expect("loading BookedVersions from db failed");
                Ok::<_, eyre::Report>(())
            }
        });

        Self {
            pool,
            book,
            clock,
            subs,
            id,
            tx,
        }
    }

    pub async fn make_broadcastable_changes<F, T>(
        &self,
        timeout: Option<Duration>,
        f: F,
    ) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
    where
        F: FnOnce(&InterruptibleTransaction<Transaction<'_>>) -> Result<T, ChangeError>,
    {
        let mut conn = self.pool.write_priority().await?;

        let mut book_writer = self
            .book
            .write::<&str, _>("make_broadcastable_changes(booked writer)", None)
            .await;

        let actor_id = self.id;
        let start = Instant::now();
        let clock = self.clock.clone();
        let ts = broadcast::Timestamp::from(clock.new_timestamp());

        tokio::task::block_in_place(move || {
            let tx = conn
                .immediate_transaction()
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: None,
                })?;

            let tx = InterruptibleTransaction::new(tx, timeout, "local_changes");

            let _unused = tx
                .prepare_cached("SELECT crsql_set_ts(?)")
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: None,
                })?
                .query_row([&ts], |row| row.get::<_, String>(0))
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: None,
                })?;

            // Execute whatever might mutate state data
            let ret = f(&tx)?;

            let insert_info = insert_local_changes(actor_id, &clock, &tx, &mut book_writer)?;
            tx.commit().map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: insert_info.as_ref().map(|info| info.db_version),
            })?;

            let elapsed = start.elapsed();

            match insert_info {
                None => Ok((ret, None, elapsed)),
                Some(change::InsertChangesInfo {
                    db_version,
                    last_seq,
                    ts,
                    snap,
                }) => {
                    tracing::debug!(%db_version, ?last_seq, "committed tx");

                    book_writer.commit_snapshot(snap);

                    let pool = self.pool.clone();
                    let subs = self.subs.clone();
                    let tx = self.tx.clone();

                    spawn::spawn_counted(async move {
                        broadcast_changes(&pool, actor_id, &subs, tx, db_version, last_seq, ts)
                            .await
                    });

                    Ok::<_, ChangeError>((ret, Some(db_version), elapsed))
                }
            }
        })
    }
}

#[async_trait::async_trait]
impl super::server::AgentExecutor for BroadcastingTransactor {
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        {
            let mut dc = db::write::Datacenter(&mut dc);
            dc.insert(peer, qcmp_port, icao);
        }

        let res = self
            .make_broadcastable_changes(None, move |tx| {
                db::write::exec_interruptible(tx, dc).map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(self.id),
                    version: None,
                })
            })
            .await;

        match res {
            Ok((_rows, _version, elapsed)) => {
                tracing::debug!(%peer, ?elapsed, "peer added");
            }
            Err(error) => {
                tracing::error!(%peer, %error, "failed to insert dc");
            }
        }
    }

    async fn execute(&self, peer: Peer, statements: &[p::ServerChange]) -> p::ExecResponse {
        let start = std::time::Instant::now();

        let mut v = smallvec::SmallVec::<[_; 32]>::new();
        {
            let mut srv = db::write::Server::for_peer(peer, &mut v);

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
                            srv.update(&u.endpoint, u.icao, u.tokens.as_ref());
                        }
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(statements.len());

        let res = self
            .make_broadcastable_changes(None, |tx| {
                let mut rows = 0;
                for statement in v {
                    match db::write::exec_single_interruptible(tx, statement) {
                        Ok(rows_affected) => {
                            rows += rows_affected;
                            results.push(p::ExecResult::Execute {
                                rows_affected,
                                time: start.elapsed().as_secs_f64(),
                            });
                        }
                        Err(error) => {
                            results.push(p::ExecResult::Error {
                                error: error.to_string(),
                            });
                        }
                    }
                }

                Ok(rows)
            })
            .await;

        let version = match res {
            Ok((rows_affected, version, elapsed)) => {
                tracing::debug!(%peer, ?elapsed, rows_affected, "updated servers");
                version.map(u64::from)
            }
            Err(error) => {
                tracing::error!(%peer, %error, "failed to update servers");
                results.push(p::ExecResult::Error {
                    error: error.to_string(),
                });
                None
            }
        };

        p::ExecResponse {
            results,
            time: start.elapsed().as_secs_f64(),
            version,
            actor_id: Some(self.id.to_string()),
        }
    }

    async fn disconnected(&self, peer: Peer) {
        let mut dc = smallvec::SmallVec::<[_; 2]>::new();
        {
            let mut dc = db::write::Datacenter(&mut dc);
            dc.remove(peer, None);
        }

        let res = self
            .make_broadcastable_changes(None, move |tx| {
                db::write::exec_interruptible(tx, dc).map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(self.id),
                    version: None,
                })
            })
            .await;

        match res {
            Ok((_rows, _version, elapsed)) => {
                tracing::debug!(%peer, ?elapsed, "peer removed");
            }
            Err(error) => {
                tracing::error!(%peer, %error, "failed to remove dc");
            }
        }
    }
}

pub fn insert_local_changes(
    actor_id: ActorId,
    clock: &uhlc::HLC,
    tx: &rusqlite::Connection,
    book_writer: &mut tokio::sync::RwLockWriteGuard<'_, BookedVersions>,
) -> Result<Option<change::InsertChangesInfo>, ChangeError> {
    let db_version: CrsqlDbVersion = tx
        .prepare_cached("SELECT crsql_peek_next_db_version()")
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_row((), |row| row.get(0))
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    let version_info: (Option<CrsqlSeq>, Option<broadcast::Timestamp>) = tx
        .prepare_cached(
            "SELECT MAX(seq), MAX(ts) FROM crsql_changes WHERE site_id = ? AND db_version = ?;",
        )
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_row((actor_id, db_version), |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    match version_info {
        (None, None) => Ok(None),
        (None, Some(ts)) => {
            tracing::warn!(%db_version, ?ts, "found db_version without seq");
            Ok(None)
        }
        (Some(last_seq), ts) => {
            let ts = ts.unwrap_or_else(|| {
                tracing::warn!(%db_version, ?ts, "found db_version without seq");
                broadcast::Timestamp::from(clock.new_timestamp())
            });

            tracing::debug!(%db_version, %last_seq, ?ts, "found db_version");

            let db_versions = db_version..=db_version;

            let mut snap = book_writer.snapshot();
            snap.insert_db(tx, [db_versions].into())
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(db_version),
                })?;

            Ok(Some(change::InsertChangesInfo {
                db_version,
                last_seq,
                ts,
                snap,
            }))
        }
    }
}

pub async fn broadcast_changes(
    pool: &SplitPool,
    actor_id: ActorId,
    subs: &SubsManager,
    tx: Option<BroadcastTx>,
    db_version: CrsqlDbVersion,
    last_seq: CrsqlSeq,
    ts: broadcast::Timestamp,
) -> Result<(), broadcast::BroadcastError> {
    let conn = pool.read().await?;

    tokio::task::block_in_place(|| {
        let mut prepped = conn.prepare_cached(
            r#"
                SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                    FROM crsql_changes
                    WHERE db_version = ?
                    AND site_id = crsql_site_id()
                    ORDER BY seq ASC
            "#,
        )?;
        let rows = prepped.query_map([db_version], change::row_to_change)?;
        let chunked =
            change::ChunkedChanges::new(rows, CrsqlSeq(0), last_seq, change::MAX_CHANGES_BYTE_SIZE);
        for changes_seqs in chunked {
            match changes_seqs {
                Ok((changes, seqs)) => {
                    tracing::trace!(num_changes = changes.len(), ?seqs, "broadcasting changes");

                    let changeset = corro_types::broadcast::Changeset::FullV2 {
                        actor_id,
                        version: db_version,
                        changes,
                        seqs,
                        last_seq,
                        ts,
                    };
                    match_changes(subs, &changeset, db_version);

                    if let Some(tx) = tx.clone() {
                        tokio::spawn(async move {
                            if tx
                                .send(broadcast::BroadcastInput::AddBroadcast(
                                    broadcast::BroadcastV1::Change(broadcast::ChangeV1 {
                                        actor_id,
                                        changeset,
                                    }),
                                ))
                                .await
                                .is_err()
                            {
                                tracing::error!("could not send change message for broadcast");
                            }
                        });
                    }
                }
                Err(error) => {
                    tracing::error!(
                        %db_version, %error, "could not process crsql change"
                    );
                    break;
                }
            }
        }
        Ok::<_, rusqlite::Error>(())
    })?;

    Ok(())
}
