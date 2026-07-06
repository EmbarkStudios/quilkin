//! Contains the [`BroadcastingTransactor`] used to process database mutations
//! and send events to subscribers whose queries match the applied mutations

use crate::{
    Peer, db,
    db::SplitPoolReadExt,
    persistent::proto::{ExecResponse, ExecResult, v1 as p},
};
use corro_types::{
    actor::ActorId,
    agent::{Booked, BookedVersions, Bookie, ChangeError, SplitPool},
    base::{CrsqlDbVersion, CrsqlSeq},
    broadcast, change,
    pubsub::SubsManager,
    updates::match_changes,
};
use eyre::WrapErr;
use quilkin_types::IcaoCode;
use rusqlite::Transaction;
use sqlite_pool::InterruptibleTransaction;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

pub type BroadcastTx = tokio::sync::mpsc::Sender<broadcast::BroadcastInput>;

/// Default for [`BroadcastingTransactor::with_full_purge_count`].
pub const DEFAULT_FULL_PURGE_COUNT: usize = 100;

const MAX_WRITE_BATCH: usize = 64;

struct PendingWrite {
    statements: smallvec::SmallVec<[crate::api::Statement; 32]>,
    start: Instant,
    response_tx: oneshot::Sender<ExecResponse>,
}

fn is_disk_full_change_error(err: &ChangeError) -> bool {
    matches!(err, ChangeError::Rusqlite { source, .. } if db::is_disk_full(source))
}

fn rusqlite_err(source: rusqlite::Error, actor_id: ActorId) -> ChangeError {
    ChangeError::Rusqlite {
        source,
        actor_id: Some(actor_id),
        version: None,
    }
}

/// Executes transactions against the DB, broadcasting committed changes to any
/// subscribers whose query matches the mutation
#[derive(Clone)]
pub struct Transactor {
    pool: SplitPool,
    bookie: Bookie,
    booked: Booked,
    subs: SubsManager,
    tx: Option<BroadcastTx>,
    clock: Arc<uhlc::HLC>,
    id: ActorId,
    full_purge_count: usize,
}

/// A [`Transactor`] that coalesces concurrently queued writes into a single
/// transaction
#[derive(Clone)]
pub struct BroadcastingTransactor {
    inner: Transactor,
    write_tx: tokio::sync::mpsc::Sender<PendingWrite>,
}

impl BroadcastingTransactor {
    pub async fn new(
        id: ActorId,
        clock: Arc<uhlc::HLC>,
        pool: SplitPool,
        subs: SubsManager,
        tx: Option<BroadcastTx>,
    ) -> eyre::Result<Self> {
        let start = Instant::now();
        let all_booked = {
            let conn = pool
                .read_readonly()
                .await
                .context("failed to acquire read")?;
            BookedVersions::load_all_from_conn(&conn).context("failed to load booked versions")?
        };
        tracing::info!("Loaded booked versions in {:?}", start.elapsed());

        let bookie = Bookie::new(all_booked);
        let booked = bookie.ensure(id);

        let (write_tx, write_rx) = tokio::sync::mpsc::channel(256);

        let inner = Transactor {
            pool,
            bookie,
            booked,
            clock,
            subs,
            id,
            tx,
            full_purge_count: DEFAULT_FULL_PURGE_COUNT,
        };

        // The write loop is given a `Transactor` rather than `Self` so that
        // `write_rx` closes, ending the loop, once every `Self` is dropped
        let txr = inner.clone();
        tokio::spawn(async move { write_loop(txr, write_rx).await });

        Ok(Self { inner, write_tx })
    }

    /// Sets the number of oldest `servers` rows purged on the second recovery attempt.
    pub fn with_full_purge_count(mut self, count: usize) -> Self {
        self.inner.full_purge_count = count;
        self
    }

    #[inline]
    pub fn actor_id(&self) -> ActorId {
        self.inner.actor_id()
    }

    /// See [`Transactor::with_full_recovery`]
    #[inline]
    pub async fn with_full_recovery<F, T>(
        &self,
        timeout: Option<Duration>,
        f: F,
    ) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
    where
        F: Fn(&InterruptibleTransaction<Transaction<'_>>) -> Result<T, ChangeError>,
    {
        self.inner.with_full_recovery(timeout, f).await
    }

    /// See [`Transactor::make_broadcastable_changes`]
    #[inline]
    pub async fn make_broadcastable_changes<F, T>(
        &self,
        timeout: Option<Duration>,
        f: F,
    ) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
    where
        F: FnOnce(&InterruptibleTransaction<Transaction<'_>>) -> Result<T, ChangeError>,
    {
        self.inner.make_broadcastable_changes(timeout, f).await
    }
}

impl Transactor {
    /// Executes `f` as a broadcastable write, retrying up to twice on `SQLITE_FULL`:
    ///
    /// 1. First failure: checkpoint WAL + incremental vacuum, then retry.
    /// 2. Second failure: purge the oldest `full_purge_count` server entries, then retry.
    /// 3. Third failure: return the error.
    ///
    /// `f` must be `Fn` — clone any per-attempt data inside the closure body.
    pub async fn with_full_recovery<F, T>(
        &self,
        timeout: Option<Duration>,
        f: F,
    ) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
    where
        F: Fn(&InterruptibleTransaction<Transaction<'_>>) -> Result<T, ChangeError>,
    {
        let strategies: [(Option<usize>, &str); 2] = [
            (None, "checkpointing WAL and running incremental vacuum"),
            (
                Some(self.full_purge_count),
                "purging oldest server entries then checkpointing",
            ),
        ];

        for (purge_count, description) in strategies {
            let result = self.make_broadcastable_changes(timeout, &f).await;
            let Err(ref e) = result else {
                return result;
            };
            if !is_disk_full_change_error(e) {
                return result;
            }
            tracing::warn!("SQLITE_FULL: {description}");
            if let Err(e) = db::recover_space(&self.pool, purge_count).await {
                // non-fatal: space may have been freed by concurrent ops
                tracing::error!(%e, "space recovery failed");
            }
        }

        self.make_broadcastable_changes(timeout, &f).await
    }

    #[inline]
    pub fn actor_id(&self) -> ActorId {
        self.id
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

        let actor_id = self.id;
        let start = Instant::now();
        let clock = self.clock.clone();
        let ts = broadcast::Timestamp::from(clock.new_timestamp());

        tokio::task::block_in_place(move || {
            let bookie_write = self.bookie.write_lock_blocking();
            let mut book_writer = bookie_write.write_tx(&self.booked);

            let tx = conn
                .immediate_transaction()
                .map_err(|e| rusqlite_err(e, actor_id))?;

            let tx = InterruptibleTransaction::new(tx, timeout, "local_changes");

            let _unused = tx
                .prepare_cached("SELECT crsql_set_ts(?)")
                .and_then(|mut s| s.query_row([&ts], |row| row.get::<_, String>(0)))
                .map_err(|e| rusqlite_err(e, actor_id))?;

            let ret = f(&tx)?;

            let insert_info = insert_local_changes(actor_id, &clock, &tx, &mut book_writer)?;
            tx.commit().map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: insert_info.as_ref().map(|i| i.db_version),
            })?;

            let elapsed = start.elapsed();

            match insert_info {
                None => Ok((ret, None, elapsed)),
                Some(change::InsertChangesInfo {
                    db_version,
                    last_seq,
                    ts,
                }) => {
                    tracing::debug!(%db_version, ?last_seq, "committed tx");

                    book_writer.commit();

                    let pool = self.pool.clone();
                    let subs = self.subs.clone();
                    let tx = self.tx.clone();

                    spawn::spawn_counted(async move {
                        if let Err(error) =
                            broadcast_changes(&pool, actor_id, &subs, tx, db_version, last_seq, ts)
                                .await
                        {
                            tracing::error!(%error, "failed to broadcast changes");
                        }
                    });

                    Ok::<_, ChangeError>((ret, Some(db_version), elapsed))
                }
            }
        })
    }
}

impl BroadcastingTransactor {
    async fn commit_datacenter<const N: usize>(
        &self,
        peer: Peer,
        dc: smallvec::SmallVec<[corro_api_types::Statement; N]>,
        ok_msg: &'static str,
        err_msg: &'static str,
    ) {
        let id = self.actor_id();
        let res = self
            .with_full_recovery(None, move |tx| {
                db::write::exec_interruptible(tx, &dc).map_err(|e| rusqlite_err(e, id))
            })
            .await;
        match res {
            Ok((_, _, elapsed)) => tracing::debug!(%peer, ?elapsed, "{ok_msg}"),
            Err(error) => tracing::error!(%peer, %error, "{err_msg}"),
        }
    }
}

#[async_trait::async_trait]
impl super::server::DbMutator for BroadcastingTransactor {
    async fn connected(&self, peer: Peer, icao: IcaoCode, qcmp_port: u16) {
        let mut dc = smallvec::SmallVec::<[_; 1]>::new();
        db::write::Datacenter(&mut dc).insert(peer, qcmp_port, icao);
        self.commit_datacenter(peer, dc, "peer added", "failed to insert dc")
            .await;
    }

    async fn submit(&self, peer: Peer, statements: &[p::ServerChange]) -> oneshot::Receiver<ExecResponse> {
        let start = Instant::now();

        let mut v = smallvec::SmallVec::<[_; 32]>::new();
        for s in statements {
            match s {
                p::ServerChange::Upsert(i) => {
                    let mut srv = db::write::Server::for_peer(peer, &mut v);
                    for i in i {
                        srv.upsert(&i.endpoint, i.icao, &i.tokens);
                    }
                }
                p::ServerChange::Remove(r) => {
                    let mut srv = db::write::Server::for_peer(peer, &mut v);
                    for r in r {
                        srv.remove_immediate(r);
                    }
                }
                p::ServerChange::Update(u) => {
                    let mut srv = db::write::Server::for_peer(peer, &mut v);
                    for u in u {
                        srv.update(&u.endpoint, u.icao, u.tokens.as_ref());
                    }
                }
                p::ServerChange::UpdateMutator(mu) => {
                    let mut dc = db::write::Datacenter(&mut v);
                    dc.update(peer, mu.qcmp_port, mu.icao);
                }
            }
        }

        let (response_tx, response_rx) = oneshot::channel();
        if let Err(tokio::sync::mpsc::error::SendError(pw)) = self
            .write_tx
            .send(PendingWrite {
                statements: v,
                start,
                response_tx,
            })
            .await
        {
            tracing::error!(%peer, "write coalescer channel closed");
            drop(pw.response_tx.send(ExecResponse {
                results: vec![ExecResult::Error {
                    error: "write coalescer unavailable".into(),
                }],
                time: start.elapsed().as_secs_f64(),
                version: None,
                actor_id: Some(self.actor_id().to_string()),
            }));
        }

        response_rx
    }

    async fn disconnected(&self, peer: Peer) {
        let mut dc = smallvec::SmallVec::<[_; 2]>::new();
        db::write::Datacenter(&mut dc).remove(peer, None);
        self.commit_datacenter(peer, dc, "peer removed", "failed to remove dc")
            .await;
    }
}

pub fn insert_local_changes(
    actor_id: ActorId,
    clock: &uhlc::HLC,
    tx: &rusqlite::Connection,
    book_writer: &mut BookedVersions,
) -> Result<Option<change::InsertChangesInfo>, ChangeError> {
    let db_version: CrsqlDbVersion = tx
        .prepare_cached("SELECT crsql_peek_next_db_version()")
        .and_then(|mut s| s.query_row((), |row| row.get(0)))
        .map_err(|e| rusqlite_err(e, actor_id))?;

    let version_info: (Option<CrsqlSeq>, Option<broadcast::Timestamp>) = tx
        .prepare_cached(
            "SELECT MAX(seq), MAX(ts) FROM crsql_changes WHERE site_id = ? AND db_version = ?;",
        )
        .and_then(|mut s| s.query_row((actor_id, db_version), |row| Ok((row.get(0)?, row.get(1)?))))
        .map_err(|e| rusqlite_err(e, actor_id))?;

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

            book_writer
                .insert_db(tx, [db_versions].into())
                .map_err(|e| ChangeError::Rusqlite {
                    source: e,
                    actor_id: Some(actor_id),
                    version: Some(db_version),
                })?;

            Ok(Some(change::InsertChangesInfo {
                db_version,
                last_seq,
                ts,
            }))
        }
    }
}

/// Executes a request's statements inside a savepoint so that a failed request
/// rolls back atomically without aborting the rest of the batch
///
/// `SQLITE_FULL` aborts the whole batch so that [`Transactor::with_full_recovery`]
/// can free space and retry it
fn exec_request(
    tx: &InterruptibleTransaction<Transaction<'_>>,
    req: &PendingWrite,
    actor_id: ActorId,
) -> Result<Vec<ExecResult>, ChangeError> {
    let mut results = Vec::with_capacity(req.statements.len());

    tx.execute_batch("SAVEPOINT request")
        .map_err(|e| rusqlite_err(e, actor_id))?;

    for stmt in &req.statements {
        match db::write::exec_single_interruptible(tx, stmt) {
            Ok(rows_affected) => {
                results.push(ExecResult::Execute {
                    rows_affected,
                    time: req.start.elapsed().as_secs_f64(),
                });
            }
            Err(e) if db::is_disk_full(&e) => return Err(rusqlite_err(e, actor_id)),
            Err(e) => {
                tx.execute_batch("ROLLBACK TO request; RELEASE request")
                    .map_err(|e| rusqlite_err(e, actor_id))?;
                results.clear();
                results.push(ExecResult::Error {
                    error: e.to_string(),
                });
                return Ok(results);
            }
        }
    }

    tx.execute_batch("RELEASE request")
        .map_err(|e| rusqlite_err(e, actor_id))?;

    Ok(results)
}

async fn write_loop(txr: Transactor, mut rx: tokio::sync::mpsc::Receiver<PendingWrite>) {
    while let Some(first) = rx.recv().await {
        // coalesce: drain any concurrently queued writes into the same transaction
        let mut batch = vec![first];
        while batch.len() < MAX_WRITE_BATCH {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }

        let id = txr.actor_id();

        let result = txr
            .with_full_recovery(None, |tx| {
                batch
                    .iter()
                    .map(|req| exec_request(tx, req, id))
                    .collect::<Result<Vec<_>, _>>()
            })
            .await;

        match result {
            Ok((all_results, db_version, elapsed)) => {
                let version = db_version.map(u64::from);
                tracing::debug!(
                    batch_size = batch.len(),
                    ?elapsed,
                    "committed coalesced write batch"
                );
                for (req, req_results) in batch.into_iter().zip(all_results) {
                    drop(req.response_tx.send(ExecResponse {
                        results: req_results,
                        time: req.start.elapsed().as_secs_f64(),
                        version,
                        actor_id: Some(id.to_string()),
                    }));
                }
            }
            Err(e) => {
                tracing::error!(%e, batch_size = batch.len(), "write batch failed");
                for req in batch {
                    drop(req.response_tx.send(ExecResponse {
                        results: vec![ExecResult::Error {
                            error: e.to_string(),
                        }],
                        time: req.start.elapsed().as_secs_f64(),
                        version: None,
                        actor_id: Some(id.to_string()),
                    }));
                }
            }
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
    let conn = pool.read_readonly().await?;

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
                    tracing::debug!(num_changes = changes.len(), ?seqs, "broadcasting changes");

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
