mod throttle;

use super::{ChangeAndSource, GossipMetrics};
use crate::Clock;
use corro_types::{
    actor::ActorId,
    agent::{self, ChangeError, CurrentVersion, KnownDbVersion, PartialVersion},
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq},
    bookie::{Booked, Bookie},
    broadcast::{self as bx, ChangeV1, Changeset, ChangesetParts},
    sqlite::unnest_param,
};
use futures::StreamExt;
use rangemap::RangeInclusiveSet;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::Instrument;

/// State for dynamic batch processing
struct HandleChangesState {
    queue: VecDeque<(ChangeAndSource, Instant)>,
    buf_cost: usize,
    current_batch_size: usize,
    processing_task: Option<tokio::task::JoinHandle<Result<(), ChangeError>>>,
    max_wait: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,

    /// When emergency halving last occurred; prevents batch size re-inflation
    /// during the grace period to avoid oscillation (halve → burst → halve …)
    halved_at: Option<Instant>,

    // Configuration
    max_queue_len: usize,
    min_batch_size: usize,
    step_base: usize,
    max_batch_size: usize,
    batch_threshold_ratio: f64,
    timeout_duration: Duration,

    /// Duration after emergency halving during which batch size increases are blocked.
    ///
    /// Uses the transaction timeout as a reasonable proxy for "one full processing cycle".
    tx_timeout: Duration,

    metrics: &'static GossipMetrics,
}

impl HandleChangesState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        min_batch_size: usize,
        step_base: usize,
        max_batch_size: usize,
        batch_threshold_ratio: f64,
        timeout_duration: Duration,
        tx_timeout: Duration,
        max_queue_len: usize,
        metrics: &'static GossipMetrics,
    ) -> Self {
        Self {
            queue: VecDeque::with_capacity(max_queue_len),
            buf_cost: 0,
            current_batch_size: min_batch_size,
            processing_task: None,
            max_wait: Some(Box::pin(tokio::time::sleep(timeout_duration))),
            halved_at: None,
            min_batch_size,
            step_base,
            max_batch_size,
            batch_threshold_ratio,
            timeout_duration,
            tx_timeout,
            max_queue_len,
            metrics,
        }
    }

    /// Whether we're still in the grace period after an emergency halving.
    #[inline]
    fn in_grace_period(&self) -> bool {
        self.halved_at
            .is_some_and(|t| t.elapsed() < self.tx_timeout)
    }

    /// Calculate exponential batch size based on cost.
    ///
    /// During the grace period after emergency halving, the result is clamped
    /// to `current_batch_size` to prevent re-inflation and oscillation.
    #[inline]
    fn calculate_batch_size(&self, cost: usize) -> usize {
        let computed = if self.step_base == 0 || cost < self.step_base {
            self.min_batch_size
        } else {
            let size = self.step_base * (1 << (cost / self.step_base).ilog2());
            size.clamp(self.min_batch_size, self.max_batch_size)
        };
        if self.in_grace_period() {
            computed.min(self.current_batch_size)
        } else {
            computed
        }
    }

    /// Get the threshold for immediate spawning based on configured ratio
    #[inline]
    fn batch_threshold(&self) -> usize {
        (self.current_batch_size as f64 * self.batch_threshold_ratio) as usize
    }

    /// Drain a batch from the queue and spawn processing task
    fn drain_and_spawn(
        &mut self,
        ctx: &ChangeCtx,
        target_size: usize,
        reason: &'static str,
    ) -> usize {
        // Must complete previous task before spawning a new one
        if self.processing_task.is_some() {
            unreachable!("a processing task already exists");
        }

        let mut batch = Vec::with_capacity(self.current_batch_size);
        let mut batch_cost = 0;

        while let Some((change, queued_at)) = self.queue.pop_front() {
            let cost = change.0.processing_cost();
            batch_cost += cost;
            self.buf_cost -= cost;
            batch.push((change, queued_at));

            if batch_cost >= target_size {
                break;
            }
        }

        if batch.is_empty() {
            unreachable!("batch is empty");
        }

        let ctx = ctx.clone();
        self.processing_task = Some(tokio::spawn(process_multiple_changes(
            ctx,
            batch,
            self.tx_timeout,
        )));

        self.metrics.change_batches_inc(reason);
        self.metrics.change_batch_size(self.current_batch_size);

        batch_cost
    }

    /// Handle task completion and decide whether to spawn next batch
    fn handle_task_completion(
        &mut self,
        ctx: &ChangeCtx,
        result: Result<Result<(), corro_types::agent::ChangeError>, tokio::task::JoinError>,
    ) {
        // Handle task result
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(%error, "error processing change batch");

                if let Some(issue) = error.fatal_db_issue() {
                    super::fatal_db_issue(issue, &ctx.shutdown);
                }

                // Check for memory errors and emergency reduce batch size
                // TODO: requeue the changes
                if error.is_oom_error() || error.is_interrupt_error() {
                    if self.current_batch_size == self.min_batch_size {
                        tracing::error!(current_batch_size = %self.current_batch_size, min_batch_size = %self.min_batch_size, "batch too large for the database to process, but already at min_batch_size — min_batch_size may be too large or transaction timeout may be misconfigured");
                    } else {
                        tracing::error!(current_batch_size = %self.current_batch_size, "batch too large for the database to process, halving batch size");
                    }
                    self.current_batch_size =
                        (self.current_batch_size / 2).max(self.min_batch_size);
                    self.halved_at = Some(std::time::Instant::now());
                }
            }
            Err(error) => {
                tracing::error!(%error, "batch processing task panicked");
            }
        }

        self.processing_task = None;

        // Should we spawn immediately with queued changes?
        if self.buf_cost >= self.batch_threshold() {
            // YES - enough changes queued, spawn immediately
            // Check if we might want to burst
            if self.buf_cost > self.current_batch_size {
                self.current_batch_size = self.calculate_batch_size(self.buf_cost);
            }
            self.drain_and_spawn(ctx, self.current_batch_size, "immediate-after-completion");
        } else {
            // NO - not enough changes yet, start timeout
            self.max_wait = Some(Box::pin(tokio::time::sleep(self.timeout_duration)));
        }
    }

    /// Handle timeout - spawn whatever we have
    ///
    /// The amount of changes MUST be smaller than the threshold
    /// as otherwise other code paths would have spawned a batch
    fn handle_timeout(&mut self, ctx: &ChangeCtx) {
        self.metrics.changes_queued(self.buf_cost, self.queue.len());

        self.max_wait = None;

        if !self.queue.is_empty() {
            // Just process whatever we have and then downsize the batch size
            self.drain_and_spawn(ctx, usize::MAX, "timeout");

            // Adjust batch size based on what we had processed
            // This will decrease the batch size unless it's already at the minimum
            self.current_batch_size = self.calculate_batch_size(self.buf_cost);
        } else if self.processing_task.is_none() && self.max_wait.is_none() {
            // Empty queue, no task running, no timeout set
            self.current_batch_size = self.calculate_batch_size(0);
            // If no task is running and no timeout is set, start timeout
            // This ensures changes get processed even if they don't hit the threshold
            self.max_wait = Some(Box::pin(tokio::time::sleep(self.timeout_duration)));
        }
    }

    // Drops oldest items when the queue is full
    #[inline]
    fn maybe_drop_old_change(&mut self) -> Option<ChangeV1> {
        let maybe_dropped_change = if self.queue.len() >= self.max_queue_len {
            if let Some((dropped_change, _)) = self.queue.pop_front() {
                self.buf_cost -= dropped_change.0.processing_cost();
                Some(dropped_change.0)
            } else {
                None
            }
        } else {
            None
        };

        if maybe_dropped_change.is_some() {
            self.metrics.change_dropped_inc();
        }

        maybe_dropped_change
    }

    /// Handle new change arrival - check if we should spawn immediately
    fn handle_new_change(&mut self, ctx: &ChangeCtx, change: ChangeAndSource) {
        let cost = change.0.processing_cost();
        self.queue.push_back((change, Instant::now()));
        self.buf_cost += cost;

        // Check if we should spawn immediately (threshold-based)
        if self.buf_cost < self.batch_threshold() || self.processing_task.is_some() {
            return;
        }

        // Cancel any pending timeout
        self.max_wait = None;

        // Check if we might want to burst
        if self.buf_cost > self.current_batch_size {
            self.current_batch_size = self.calculate_batch_size(self.buf_cost);
        }

        // Drain and spawn
        self.drain_and_spawn(ctx, self.current_batch_size, "size-threshold-on-new-change");
    }
}

#[derive(Clone)]
pub struct ChangeOptions {
    /// How many unapplied changesets corrosion will buffer before starting to drop them
    pub processing_queue_len: usize,
    /// Minimum amount of changes corrosion will try to apply at once in the same transaction
    pub apply_queue_min_batch_size: usize,
    /// `batch_size = clamp(min_batch_size, step_base * 2 ** floor(log2(x/step_base)), max_batch_size)`
    pub apply_queue_step_base: usize,
    /// Maximum amount of changes corrosion will try to apply at once in the same transaction
    pub apply_queue_max_batch_size: usize,
    /// Threshold ratio (0.0-1.0) for immediate batch spawning when queue reaches this fraction of batch size
    ///
    /// It's used to decide whether to wait for more changes for `apply_queue_timeout` ms or spawn a batch immediately
    pub apply_queue_batch_threshold_ratio: f64,
    /// Amount of time corrosion will wait before proceeding to apply a batch of changes
    ///
    /// We wait either for `apply_queue_timeout` or until at least `apply_queue_min_batch_size` changes accumulate
    pub apply_queue_timeout: Duration,
    /// Max amount of a time allowed for an individual SQL transaction
    pub sql_tx_timeout: Duration,
}

impl Default for ChangeOptions {
    fn default() -> Self {
        Self {
            processing_queue_len: 20_000,
            apply_queue_min_batch_size: 100,
            apply_queue_step_base: 500,
            apply_queue_max_batch_size: 16_000,
            apply_queue_batch_threshold_ratio: 0.9,
            apply_queue_timeout: Duration::from_millis(10),
            sql_tx_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Clone)]
pub struct ChangeCtx {
    pub pool: agent::SplitPool,
    pub sub_manager: corro_types::pubsub::SubsManager,
    pub update_manager: corro_types::updates::UpdatesManager,
    pub bookie: Bookie,
    pub actor_id: ActorId,
    pub clock: Clock,
    pub bcast_tx: mpsc::Sender<bx::BroadcastInput>,
    pub apply_tx: mpsc::Sender<ChangeApply>,
    pub clear_buf_tx: mpsc::Sender<(ActorId, RangeInclusiveSet<CrsqlDbVersion>)>,
    pub shutdown: tokio::sync::watch::Sender<()>,
    pub metrics: &'static super::GossipMetrics,
}

/// Spawns a task for [`handle_changes`]
pub fn spawn_changes_handler(
    ctx: ChangeCtx,
    opts: ChangeOptions,
    changes_rx: mpsc::Receiver<super::Change>,
    tripwire: tripwire::Tripwire,
) {
    spawn::spawn_counted(async move {
        handle_changes(ctx, opts, changes_rx, tripwire).await;
    });
}

/// Bundle incoming changes to optimise transaction sizes with `SQLite`
///
/// *Performance tradeoff*: introduce latency (with a max timeout) to
/// apply changes more efficiently.
///
/// This function used by broadcast receivers and sync receivers
pub async fn handle_changes(
    ctx: ChangeCtx,
    opts: ChangeOptions,
    mut changes_rx: mpsc::Receiver<super::Change>,
    mut tripwire: tripwire::Tripwire,
) {
    // Initialize batch processor state
    let mut state = HandleChangesState::new(
        opts.apply_queue_min_batch_size,
        opts.apply_queue_step_base,
        opts.apply_queue_max_batch_size,
        opts.apply_queue_batch_threshold_ratio,
        opts.apply_queue_timeout,
        opts.sql_tx_timeout,
        opts.processing_queue_len,
        ctx.metrics,
    );

    let max_seen_cache_len = opts.processing_queue_len;

    // unlikely, but max_seen_cache_len can be less than 10, in that case we want to just clear the whole cache
    let keep_seen_cache_size = if max_seen_cache_len > 10 {
        10.max(max_seen_cache_len / 10)
    } else {
        0
    };

    let mut seen = indexmap::IndexMap::<_, RangeInclusiveSet<CrsqlSeq>>::new();

    loop {
        let changes = tokio::select! {
            biased;

            // Processing task finished
            res = async { state.processing_task.as_mut().unwrap().await }, if state.processing_task.is_some() => {
                state.handle_task_completion(&ctx, res);
                continue;
            },

            // New changes arrive
            maybe_change_src = changes_rx.recv() => match maybe_change_src {
                Some(change) => change,
                None => break,
            },

            // Timeout fires
            _ = async { state.max_wait.as_mut().unwrap().await }, if state.max_wait.is_some() => {
                state.handle_timeout(&ctx);

                // Cleanup seen cache
                if seen.len() > max_seen_cache_len {
                    seen.drain(..seen.len() - keep_seen_cache_size);
                }

                continue;
            },

            // Corrosion is shutting down
            _ = &mut tripwire => {
                break;
            }
        };

        ctx.metrics.change_received_inc(changes.len());

        for (change, src) in changes {
            // Skip changes from ourselves
            // TODO: seems better to just ensure we don't send to ourselves?
            if change.actor_id == ctx.actor_id {
                continue;
            }

            // Skip changes we've already seen recently in the seen cache
            if let Some(mut seqs) = change.seqs() {
                let v = change.versions().start();
                if let Some(seen_seqs) = seen.get(&(change.actor_id, v))
                    && seqs.all(|seq| seen_seqs.contains(&seq))
                {
                    continue;
                }
            } else if change
                .versions()
                .all(|v| seen.contains_key(&(change.actor_id, v)))
            {
                if src.is_broadcast() {
                    ctx.metrics.change_broadcast_duplicate_inc("cache");
                }
                continue;
            }

            let src_str = src.as_str();

            // Update logical clock if needed
            let recv_lag = change.ts().and_then(|ts| {
                let mut our_ts = ctx.clock.new_timestamp();
                if ts > our_ts {
                    if let Err(error) = ctx.clock.update_with_timestamp(change.actor_id, ts) {
                        tracing::error!(remote_actor_id = %change.actor_id, error, "could not update clock from actor");
                        return None;
                    }

                    ctx.metrics.change_clock_inc(src_str);

                    // update our_ts to the new timestamp
                    our_ts = ctx.clock.new_timestamp();
                }
                Some((our_ts.0 - ts.0).to_duration())
            });

            if src.is_broadcast() {
                ctx.metrics.change_broadcast_received_inc("change");
            }

            // Skip changes we've already seen in the bookie
            if let Some(booked) = ctx.bookie.get(&change.actor_id)
                && booked.read().contains_all(change.versions(), change.seqs())
            {
                if src.is_broadcast() {
                    ctx.metrics.change_broadcast_duplicate_inc("bookie");
                }
                continue;
            }

            if let Some(recv_lag) = recv_lag {
                ctx.metrics
                    .change_recv_lag_sample(recv_lag.as_secs_f64(), src_str);
            }

            // If we need to drop an old change, drop it from the seen cache as well
            while let Some(dropped_change) = state.maybe_drop_old_change() {
                for v in dropped_change.versions() {
                    let indexmap::map::Entry::Occupied(mut entry) =
                        seen.entry((change.actor_id, v))
                    else {
                        continue;
                    };

                    if let Some(seqs) = dropped_change.seqs() {
                        entry.get_mut().remove(seqs.into());
                    } else {
                        entry.swap_remove_entry();
                    }
                }
            }

            // Register the new change in the seen cache
            // this will only run once for a non-empty changeset
            for v in change.versions() {
                let entry = seen.entry((change.actor_id, v)).or_default();
                if let Some(seqs) = change.seqs() {
                    entry.extend([seqs.into()]);
                }
            }

            // Rebroadcast changes received from broadcast
            if src.is_broadcast()
                && !change.is_empty()
                && ctx
                    .bcast_tx
                    .try_send(bx::BroadcastInput::Rebroadcast(bx::BroadcastV1::Change(
                        change.clone(),
                    )))
                    .is_err()
            {
                tracing::debug!("broadcasts are full or done!");
            }

            // Handle the new change - queue it and potentially spawn a batch
            state.handle_new_change(&ctx, (change, src));
        }
    }
}

async fn process_multiple_changes(
    ctx: ChangeCtx,
    changes: Vec<(ChangeAndSource, Instant)>,
    tx_timeout: Duration,
) -> Result<(), corro_types::agent::ChangeError> {
    let start = Instant::now();
    //counter!("corro.agent.changes.processing.started").increment(changes.len() as u64);

    const PROCESSING_WARN_THRESHOLD: Duration = Duration::from_secs(5);

    let mut seen = HashSet::new();
    let mut unknown_changes = BTreeMap::<ActorId, (Booked, Vec<_>)>::new();

    for ((change, src), queued_at) in changes {
        ctx.metrics.changes_queued_time(queued_at.elapsed());

        let versions = change.versions();
        let seqs = change.seqs();

        if !seen.insert((change.actor_id, versions, seqs)) {
            continue;
        }

        let booked = ctx.bookie.ensure(change.actor_id);
        if booked.read().contains_all(change.versions(), change.seqs()) {
            continue;
        }

        unknown_changes
            .entry(change.actor_id)
            .and_modify(|(_, per_actor_changes)| per_actor_changes.push((change.clone(), src)))
            .or_insert_with(|| (booked, vec![(change, src)]));
    }

    let elapsed = start.elapsed();
    if elapsed >= PROCESSING_WARN_THRESHOLD {
        tracing::warn!(
            ?elapsed,
            "process_multiple_changes: removing duplicates took too long"
        );
    }

    if unknown_changes.is_empty() {
        return Ok(());
    }

    let mut conn = ctx.pool.write_normal().await?;
    let ctx = ctx.clone();

    let changesets = tokio::task::block_in_place(move || {
        let bookie_write = ctx.bookie.write_lock_blocking();

        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        let mut tx = sqlite_pool::InterruptibleTransaction::new(
            tx,
            Some(tx_timeout),
            "process_multiple_changes",
        );

        let mut processed = BTreeMap::<ActorId, Vec<_>>::new();
        let mut changesets = Vec::new();
        let mut booked_writes = BTreeMap::new();

        for (actor_id, (booked, _)) in &unknown_changes {
            booked_writes.insert(*actor_id, bookie_write.write_tx(booked));
        }

        let sub_start = Instant::now();
        for (actor_id, (_, changes)) in unknown_changes {
            let booked_write = booked_writes
                .get_mut(&actor_id)
                .expect("booked write guard should be present for every actor");

            let max = booked_write.last();
            let mut seen = rangemap::RangeInclusiveMap::new();

            for (change, src) in changes {
                let seqs = change.seqs();
                if booked_write.contains_all(change.versions(), change.seqs()) {
                    continue;
                }

                let versions = change.versions();

                // check if we've seen this version here...
                if versions.clone().all(|version| match seqs {
                    Some(mut check_seqs) => match seen.get(&version) {
                        Some(maybe_partial) => match maybe_partial {
                            Some(agent::PartialVersion { seqs, .. }) => {
                                check_seqs.all(|seq| seqs.contains(&seq))
                            }
                            // other kind of known version
                            None => true,
                        },
                        None => false,
                    },
                    None => seen.contains_key(&version),
                }) {
                    continue;
                }

                // optimizing this, insert later!
                let known = if change.is_complete() && change.is_empty() {
                    let end = change.versions().end();

                    // update db_version in db if it's greater than the max
                    // since we aren't passing any changes to crsql
                    if max.is_none_or(|max| end > max) {
                        drop(
                            tx.prepare_cached("SELECT crsql_set_db_version(?, ?)")
                                .map_err(|source| ChangeError::Rusqlite {
                                    source,
                                    actor_id: Some(change.actor_id),
                                    version: Some(end),
                                })?
                                .query_row((change.actor_id, end), |row| row.get::<_, String>(0))
                                .map_err(|source| ChangeError::Rusqlite {
                                    source,
                                    actor_id: Some(change.actor_id),
                                    version: Some(end),
                                })?,
                        );
                    }

                    KnownDbVersion::Cleared
                } else {
                    if let Some(seqs) = change.seqs()
                        && seqs.end() < seqs.start()
                    {
                        tracing::warn!(%actor_id, versions = ?change.versions(), "received an invalid change, seqs start is greater than seqs end: {seqs:?}");
                        continue;
                    }

                    let (known, changeset) = {
                        match process_single_version(&ctx.clock, ctx.metrics, &mut tx, change) {
                            Ok(res) => res,
                            Err(error) => {
                                tracing::error!(%actor_id, versions = ?versions, %error, "error processing single version");

                                if let Some(issue) = error
                                    .sqlite_error_code()
                                    .and_then(ChangeError::fatal_db_issue_for_code)
                                {
                                    super::fatal_db_issue(issue, &ctx.shutdown);
                                }

                                // the transaction was rolled back, so we need to return.
                                if tx.is_autocommit() {
                                    return Err(ChangeError::Rusqlite {
                                        source: error,
                                        actor_id: None,
                                        version: None,
                                    });
                                } else {
                                    continue;
                                }
                            }
                        }
                    };

                    if let KnownDbVersion::Current(CurrentVersion { db_version, .. }) = &known {
                        changesets.push((actor_id, changeset, *db_version, src));
                    }

                    known
                };

                let partial = match known {
                    KnownDbVersion::Partial(partial) => Some(partial),
                    _ => None,
                };

                seen.insert(versions.into(), partial.clone());
                processed
                    .entry(actor_id)
                    .or_default()
                    .push((versions, partial));
            }
        }

        let elapsed = sub_start.elapsed();
        if elapsed >= PROCESSING_WARN_THRESHOLD {
            tracing::warn!(
                ?elapsed,
                "process_multiple_changes:: process_single_version took too long"
            );
        }

        let sub_start = Instant::now();

        let mut all_changes = Vec::with_capacity(processed.len());
        let mut completed_versions = BTreeMap::new();
        for (actor_id, processed) in processed.iter() {
            let booked_write = booked_writes
                .get_mut(actor_id)
                .expect("booked write guard should be present for every actor");

            let (changes, completed_partials) = booked_write.compute_and_apply(processed);
            let clear_versions = changes
                .clear_versions
                .iter()
                .map(|v| *v..=*v)
                .collect::<RangeInclusiveSet<CrsqlDbVersion>>();
            completed_versions.insert(*actor_id, (completed_partials, clear_versions));
            all_changes.push(changes);
        }

        corro_types::bookie::BookieDbParams::from_changes(&all_changes)
            .execute(&tx)
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        tx.commit().map_err(|source| {
            if let Some(issue) = source
                .sqlite_error_code()
                .and_then(ChangeError::fatal_db_issue_for_code)
            {
                super::fatal_db_issue(issue, &ctx.shutdown);
            }

            ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            }
        })?;

        let elapsed = sub_start.elapsed();
        if elapsed >= PROCESSING_WARN_THRESHOLD {
            tracing::warn!(
                ?elapsed,
                "process_multiple_changes: commiting transaction took too long"
            );
        }

        for (_, booked_write) in booked_writes {
            booked_write.commit();
        }

        // for (_, changeset, _, _) in changesets.iter() {
        //     if let Some(ts) = changeset.ts() {
        //         let dur = (agent.clock().new_timestamp().get_time() - ts.0).to_duration();
        //         histogram!("corro.agent.changes.commit.lag.seconds").record(dur);
        //         agent.metrics_tracker().observe_lag(dur.as_secs_f64());
        //     }
        // }

        // schedule apply for partials that became complete
        for (actor_id, (versions, clear_versions)) in completed_versions {
            // try to send synchronously since we're in a blocking task and spawning a task to send a message is kind of gross
            match ctx.apply_tx.try_reserve() {
                Ok(permit) => permit.send((actor_id, versions)),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let tx = ctx.apply_tx.clone();
                    tokio::task::spawn(async move {
                        if tx.send((actor_id, versions)).await.is_err() {
                            tracing::error!(
                                "unable to apply further changes, receiver was closed after task spawn"
                            );
                        }
                    });
                }
                Err(_) => {
                    tracing::error!("unable to apply further changes, receiver was closed");
                }
            }

            // the corrosion code seems to think this one isn't critical, or that it won't usually ever be full?
            if ctx
                .clear_buf_tx
                .try_send((actor_id, clear_versions))
                .is_err()
            {
                tracing::warn!("could not schedule buffered meta clear");
            }
        }

        Ok::<_, ChangeError>(changesets)
    })?;

    let change_chunk_size: usize = changesets.iter().map(|cs| cs.1.len()).sum();

    ctx.metrics
        .changes_processed(start.elapsed(), "remote", Some(change_chunk_size));

    tokio::spawn(async move {
        for (_actor_id, changeset, db_version, _src) in changesets {
            corro_types::updates::match_changes(&ctx.sub_manager, &changeset, db_version);
            corro_types::updates::match_changes(&ctx.update_manager, &changeset, db_version);
        }
    });

    Ok(())
}

fn process_single_version(
    clock: &Clock,
    metrics: &'static GossipMetrics,
    tx: &mut sqlite_pool::InterruptibleTransaction<rusqlite::Transaction<'_>>,
    change: ChangeV1,
) -> rusqlite::Result<(KnownDbVersion, bx::Changeset)> {
    let ChangeV1 {
        actor_id,
        changeset,
    } = change;

    let versions = changeset.versions();

    let sp = tx.savepoint()?;
    let mut changes_per_table = BTreeMap::new();
    let (known, changeset) = if changeset.is_complete() {
        let (known, changeset) = process_complete_version(
            clock,
            &sp,
            actor_id,
            versions,
            changeset
                .into_parts()
                .expect("no changeset parts, this shouldn't be happening!"),
            &mut changes_per_table,
        )?;

        (known, changeset)
    } else {
        let parts = changeset.into_parts().unwrap();
        let known = process_incomplete_version(&sp, &parts, &mut changes_per_table)?;

        (known, parts.into())
    };

    sp.commit()?;

    for (table_name, count) in changes_per_table {
        metrics.changes_committed_inc(&table_name, count, "remote");
    }

    Ok((known, changeset))
}

fn process_complete_version(
    clock: &Clock,
    sp: &sqlite_pool::InterruptibleTransaction<rusqlite::Savepoint<'_>>,
    actor_id: ActorId,
    versions: CrsqlDbVersionRange,
    parts: ChangesetParts,
    changes_per_table: &mut BTreeMap<String, u64>,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangesetParts {
        version,
        changes,
        seqs,
        last_seq,
        ts,
    } = parts;

    let len = changes.len();

    debug_assert!(
        len <= seqs.len(),
        "change from actor {actor_id} version {version} has len {len} but seqs range is {seqs:?} and last_seq is {last_seq}"
    );

    // Insert all the changes in a single statement
    // This will return a non zero rowid only if the change impacted the database
    let mut stmt = sp.prepare_cached(
        r#"
    INSERT INTO crsql_changes ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
        SELECT value0, value1, value2, value3, value4, value5, value6, value7, value8, value9
        FROM unnest(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        -- WARNING: This returns a row BEFORE inserting not after
        RETURNING db_version, seq, last_insert_rowid()
    "#,
    )?;
    let params = rusqlite::params![
        unnest_param(changes.iter().map(|c| c.table.as_str())),
        unnest_param(changes.iter().map(|c| &c.pk)),
        unnest_param(changes.iter().map(|c| c.cid.as_str())),
        unnest_param(changes.iter().map(|c| &c.val)),
        unnest_param(changes.iter().map(|c| &c.col_version)),
        unnest_param(changes.iter().map(|c| &c.db_version)),
        unnest_param(changes.iter().map(|c| &c.site_id)),
        unnest_param(changes.iter().map(|c| &c.cl)),
        unnest_param(changes.iter().map(|c| &c.seq)),
        unnest_param(changes.iter().map(|_| &ts)),
    ];
    let mut last_rowids = stmt
        .query_map(params, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<Vec<(CrsqlDbVersion, CrsqlSeq, i64)>>>()?;

    let last_rowids_len = last_rowids.len();

    // RETURNING returns rows BEFORE the insert, not after
    // so the rowids we get will be shifted by one
    // we need to shift them back
    if last_rowids_len > 0 {
        for i in 0..(last_rowids_len - 1) {
            last_rowids[i].2 = last_rowids[i + 1].2;
        }
        last_rowids[last_rowids_len - 1].2 = sp
            .prepare_cached("SELECT last_insert_rowid()")?
            .query_row(rusqlite::params![], |row| row.get(0))?;
    }

    // Now determine which exact changes impacted the database
    // This is mostly for keeping accurate metrics
    let mut impactful_changeset = vec![];
    for (change, (db_version, seq, rowid)) in changes.into_iter().zip(last_rowids) {
        // Those asserts are only a sanity check
        assert!(db_version == change.db_version);
        assert!(seq == change.seq);

        if rowid == 0 {
            continue;
        }

        let table_name = change.table.0.to_string();
        impactful_changeset.push(change);
        *changes_per_table.entry(table_name).or_default() += 1;
    }

    let (known_version, new_changeset) = if impactful_changeset.is_empty() {
        (
            KnownDbVersion::Cleared,
            Changeset::Empty {
                versions,
                ts: Some(clock.new_timestamp()),
            },
        )
    } else {
        (
            KnownDbVersion::Current(CurrentVersion {
                db_version: version,
                last_seq,
                ts,
            }),
            Changeset::Full {
                version,
                changes: impactful_changeset,
                seqs,
                last_seq,
                ts,
            },
        )
    };

    Ok((known_version, new_changeset))
}

fn process_incomplete_version(
    sp: &sqlite_pool::InterruptibleTransaction<rusqlite::Savepoint<'_>>,
    parts: &ChangesetParts,
    changes_per_table: &mut BTreeMap<String, u64>,
) -> rusqlite::Result<KnownDbVersion> {
    let ChangesetParts {
        changes,
        seqs,
        last_seq,
        ts,
        ..
    } = parts;

    let mut stmt = sp.prepare_cached(
                r#"
                INSERT INTO __corro_buffered_changes
                    ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
                SELECT
                    value0, value1, value2, value3, value4, value5, value6, value7, value8, value9
                FROM
                    unnest(:table_arr, :pk_arr, :cid_arr, :val_arr, :col_version_arr, :db_version_arr, :site_id_arr, :cl_arr, :seq_arr, :ts_arr)
                -- Otherwise sqlite will think ON CONFLICT is part of a JOIN
                WHERE TRUE
                ON CONFLICT (site_id, db_version, seq)
                    DO NOTHING
                RETURNING
                    "table"
            "#,
            )?;
    let table_names = stmt.query_map(
        rusqlite::named_params! {
            ":table_arr": unnest_param(changes.iter().map(|change| change.table.as_str())),
            ":pk_arr": unnest_param(changes.iter().map(|change| &change.pk)),
            ":cid_arr": unnest_param(changes.iter().map(|change| change.cid.as_str())),
            ":val_arr": unnest_param(changes.iter().map(|change| &change.val)),
            ":col_version_arr": unnest_param(changes.iter().map(|change| change.col_version)),
            ":db_version_arr": unnest_param(changes.iter().map(|change| change.db_version)),
            ":site_id_arr": unnest_param(changes.iter().map(|change| &change.site_id)),
            ":cl_arr": unnest_param(changes.iter().map(|change| change.cl)),
            ":seq_arr": unnest_param(changes.iter().map(|change| change.seq)),
            ":ts_arr": unnest_param(changes.iter().map(|_| ts)),
        },
        |row| row.get::<_, String>(0),
    )?;

    for res in table_names {
        let tn = res?;
        *changes_per_table.entry(tn).or_default() += 1;
    }

    Ok(KnownDbVersion::Partial(PartialVersion {
        seqs: RangeInclusiveSet::from_iter([(*seqs).into()]),
        last_seq: *last_seq,
        ts: *ts,
    }))
}

pub type ChangeApply = (ActorId, Vec<CrsqlDbVersion>);

fn match_changes_from_db_version<H>(
    manager: &impl corro_types::updates::Manager<H>,
    conn: &rusqlite::Connection,
    db_version: CrsqlDbVersion,
    actor_id: ActorId,
) -> rusqlite::Result<()>
where
    H: corro_types::updates::Handle + Send + 'static,
{
    let handles = manager.get_handles();
    if handles.is_empty() {
        return Ok(());
    }

    let mut candidates = handles
        .iter()
        .map(|(id, handle)| (id, (corro_types::pubsub::MatchCandidates::new(), handle)))
        .collect::<BTreeMap<_, _>>();

    {
        let mut prepped = conn.prepare_cached(
            r#"
        SELECT "table", pk, cid, cl
            FROM crsql_changes
            WHERE db_version = ?
              AND site_id = ?
            ORDER BY seq ASC
        "#,
        )?;

        let rows = prepped.query_map((db_version, actor_id), |row| {
            Ok((
                row.get::<_, corro_api_types::TableName>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, corro_api_types::ColumnName>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        for change_res in rows {
            let (table, pk, column, cl) = change_res?;

            for (_id, (candidates, handle)) in candidates.iter_mut() {
                let change = corro_types::pubsub::MatchableChange {
                    table: &table,
                    pk: &pk,
                    column: &column,
                    cl,
                };
                handle.filter_matchable_change(candidates, change);
            }
        }
    }

    // The original corrosion code spawns individual tasks for each case of the channel being full or closed, we just spawn
    // a single task for each category
    let mut full = Vec::new();
    let mut closed = Vec::new();

    for (id, (candidates, handle)) in candidates {
        let mut match_count = 0;
        for (table, pks) in candidates.iter() {
            let count = pks.len();
            match_count += count;
            handle
                .get_counter(table)
                .matched_count
                .increment(pks.len() as u64);
        }

        tracing::trace!(sub_id = %id, %db_version, match_count, "found candidates");

        let Err(err) = handle.changes_tx().try_send(candidates) else {
            continue;
        };

        match err {
            mpsc::error::TrySendError::Full(item) => {
                full.push((handle.changes_tx(), item));
            }
            mpsc::error::TrySendError::Closed(_) => {
                let Some(handle) = manager.remove(id) else {
                    continue;
                };

                closed.push(handle);
            }
        }
    }

    if !full.is_empty() {
        tokio::spawn(async move {
            let total = full.len();

            let start = Instant::now();

            let mut sends = futures::stream::iter(
                full.into_iter()
                    .map(|(tx, item)| async move { tx.send(item).await }),
            )
            .buffer_unordered(10);

            let mut errors = 0;

            while let Some(res) = sends.next().await {
                if res.is_err() {
                    errors += 1;
                }
            }

            tracing::debug!(total, errors, elapsed = ?start.elapsed(), "asynchronously sent changes to channels which were full");
        });
    }

    if !closed.is_empty() {
        tokio::spawn(async move {
            for handle in closed {
                handle.cleanup().await;
            }
        });
    }

    Ok(())
}

async fn process_fully_buffered_changes(
    ctx: &ChangeCtx,
    bookie: &Bookie,
    actor_id: ActorId,
    version: CrsqlDbVersion,
    tx_timeout: Duration,
) -> Result<bool, ChangeError> {
    let rows_impacted = {
        let booked = bookie.ensure(actor_id);
        let mut conn = ctx.pool.write_normal().await?;
        tracing::trace!(%actor_id, %version, "acquired write (normal) connection to process fully buffered changes");

        tokio::task::block_in_place(move || {
            let bookie_write = bookie.write_lock_blocking();
            let bookedw = bookie_write.write_tx(&booked);
            tracing::trace!(%actor_id, %version, "acquired Booked write lock to process fully buffered changes");

            let mut bookedw = bookedw;
            let last_seq = if let Some(PartialVersion { seqs, last_seq, .. }) =
                bookedw.partials.get(&version)
            {
                if seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).count() != 0 {
                    let gaps = seqs
                        .gaps(&(CrsqlSeq(0)..=*last_seq))
                        .collect::<RangeInclusiveSet<CrsqlSeq>>();
                    tracing::error!(%actor_id, %version, ?gaps, "found sequence gaps, aborting!", );
                    // TODO: return an error here
                    return Ok(false);
                }
                *last_seq
            } else {
                tracing::warn!(%actor_id, %version, "version not found in cache, returning");
                return Ok(false);
            };

            let base_tx = conn
                .immediate_transaction()
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            let tx = sqlite_pool::InterruptibleTransaction::new(
                base_tx,
                Some(tx_timeout),
                "process_buffered_changes",
            );

            tracing::trace!(%actor_id, %version, %last_seq, "Processing buffered changes to crsql_changes");

            let rows_present: bool = tx.prepare_cached("SELECT EXISTS (SELECT 1 FROM __corro_buffered_changes WHERE site_id = ? AND db_version = ?)")
                                    .map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?
                                    .query_row(rusqlite::params![actor_id, version], |row| row.get(0))
                                    .map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;

            let start = Instant::now();

            if rows_present {
                // insert all buffered changes into crsql_changes directly from the buffered changes table
                let count = tx
                    .prepare_cached(
                        r#"
                        INSERT INTO crsql_changes ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
                            SELECT                 "table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts
                                FROM __corro_buffered_changes
                                    WHERE site_id = ?
                                    AND db_version = ?
                                    ORDER BY db_version ASC, seq ASC
                                    "#,
                    ).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?
                    .execute(rusqlite::params![actor_id.as_bytes(), version]).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;
                tracing::trace!(%actor_id, %version, count, elapsed = ?start.elapsed(), "Inserted rows from buffered into crsql_changes");
            } else {
                tracing::trace!(%actor_id, %version, "No buffered rows, skipped insertion into crsql_changes");
            }

            let rows_impacted: i64 = tx
                .prepare_cached("SELECT crsql_rows_impacted()")
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?
                .query_row((), |row| row.get(0))
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            tracing::trace!(%actor_id, %version, rows_impacted, "rows impacted by buffered changes insertion");

            bookedw
                .insert_db(&tx, [version..=version].into())
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            // the version transitions from partial → fully applied, clean up its seq bookkeeping
            bookedw
                .clear_partials(&tx, RangeInclusiveSet::from([version..=version]))
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            tx.commit().map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: Some(version),
            })?;

            bookedw.commit();

            if let Err(error) = ctx
                .clear_buf_tx
                .try_send((actor_id, rangemap::range_inclusive_set![version..=version]))
            {
                tracing::error!(%error, "could not schedule buffered data clear");
            }

            Ok::<_, ChangeError>(rows_impacted > 0)
        })
    }?;

    if rows_impacted {
        let conn = ctx.pool.read().await?;
        tokio::task::block_in_place(|| {
            if let Err(error) =
                match_changes_from_db_version(&ctx.sub_manager, &conn, version, actor_id)
            {
                tracing::error!(%error, %version, "could not match changes for subs from db version");
            }
        });

        tokio::task::block_in_place(|| {
            if let Err(error) =
                match_changes_from_db_version(&ctx.update_manager, &conn, version, actor_id)
            {
                tracing::error!(%error, %version, "could not match changes for updates from db version");
            }
        });
    }

    Ok(rows_impacted)
}

pub fn spawn_buffered_apply(
    ctx: ChangeCtx,
    opts: ChangeOptions,
    bookie: Bookie,
    mut apply_rx: mpsc::Receiver<ChangeApply>,
    mut tripwire: tripwire::Tripwire,
) {
    async fn find_fully_buffered_partial(
        pool: &corro_types::agent::SplitPool,
    ) -> Result<Option<(ActorId, CrsqlDbVersion)>, ChangeError> {
        let conn = pool.read().await.map_err(ChangeError::SqlitePool)?;

        tokio::task::block_in_place(|| {
            conn.prepare_cached(
                "SELECT site_id, db_version FROM __corro_seq_bookkeeping
                 WHERE start_seq = 0 AND end_seq = last_seq
                 LIMIT 1",
            )
            .and_then(
                |mut stmt| match stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?))) {
                    Ok(v) => Ok(Some(v)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e),
                },
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })
        })
    }

    spawn::spawn_counted(async move {
        // we can burst timeout up to an additional 2 min
        let max_timeout_increase: u64 = 6;
        let step_timeout_secs: u64 = 20;

        let throttle_min = Duration::from_secs(5 * 60);
        let throttle_max = Duration::from_secs(60 * 60);

        let mut retry_interval = tokio::time::interval(Duration::from_secs(5 * 60));

        // map to throttle retries for failed versions that took too long to apply
        let mut limit_retries = throttle::ThrottleMap::new(throttle_min, throttle_max);

        let do_process =
            async |partial_version: (ActorId, CrsqlDbVersion),
                   limit_retries: &mut throttle::ThrottleMap| {
                async {
                if let Some(blocked_until) = limit_retries.is_throttled(&partial_version) {
                    let next_retry = blocked_until.duration_since(Instant::now());
                    tracing::debug!(
                        ?next_retry,
                        "previous attempt to apply buffered changes took too long, will retry"
                    );
                    return;
                }

                let throttle_count = limit_retries
                    .throttle_count(&partial_version)
                    .min(max_timeout_increase);
                let tx_timeout =
                    opts.sql_tx_timeout + Duration::from_secs(step_timeout_secs * throttle_count);

                tracing::debug!(?tx_timeout, "picked up buffered changes");

                let start = Instant::now();
                let res =
                    process_fully_buffered_changes(&ctx, &bookie, partial_version.0, partial_version.1, tx_timeout)
                        .await;
                let elapsed = start.elapsed();
                match res {
                    Ok(false) => {
                        tracing::debug!("did not apply buffered changes");
                        limit_retries.remove(&partial_version);
                    }
                    Ok(true) => {
                        tracing::debug!("succesfully applied buffered changes");
                        ctx.metrics.changes_processed(elapsed, "buffered", None);
                        limit_retries.remove(&partial_version);
                    }
                    Err(error) => {
                        let is_interrupt_error = error.is_interrupt_error();
                        tracing::debug!(elapsed = ?tx_timeout, %error, "could not apply fully buffered changes");
                        if let Some(issue) = error.fatal_db_issue() {
                            super::fatal_db_issue(issue, &ctx.shutdown);
                        }

                        // processing time came close to timeout, limit retry with exponential backoff
                        if is_interrupt_error {
                            limit_retries.throttle(partial_version);
                        }
                    }
                }
            }.instrument(tracing::debug_span!("background apply", actor_id = %partial_version.0, version = %partial_version.1)).await;
            };

        retry_interval.tick().await;

        loop {
            // TODO: The original corrosion code was sending one change at a time but I changed it so it was n changes per
            // actor instead which would be beneficial in this write code by consolidating DB access
            tokio::select! {
                biased;
                _ = &mut tripwire => break,
                maybe_version = apply_rx.recv() => {
                    let Some((actor, versions)) = maybe_version else {
                        break;
                    };

                    for version in versions {
                        do_process((actor, version), &mut limit_retries).await;
                    }
                },
                _ = retry_interval.tick() => {
                    match find_fully_buffered_partial(&ctx.pool).await {
                        Ok(Some(pair)) => {
                            do_process(pair, &mut limit_retries).await;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::debug!(%error, "could not query for fully buffered partials");
                        },
                    }
                },
            }
        }
    });
}

/// Clean up `__corro_buffered_changes` rows for versions that have been fully applied.
/// `__corro_seq_bookkeeping` is managed through the bookie via `insert_partials_db`/`insert_db`.
pub fn spawn_buffered_cleanup(
    ctx: ChangeCtx,
    opts: ChangeOptions,
    mut clear_buf_rx: mpsc::Receiver<(ActorId, RangeInclusiveSet<CrsqlDbVersion>)>,
    mut tripwire: tripwire::Tripwire,
) {
    async fn find_orphaned_buffered_changes(
        pool: &corro_types::agent::SplitPool,
    ) -> Result<Option<(ActorId, CrsqlDbVersion)>, ChangeError> {
        let conn = pool.read().await.map_err(ChangeError::SqlitePool)?;
        tokio::task::block_in_place(|| {
            conn.prepare_cached(
                "SELECT DISTINCT bc.site_id, bc.db_version
                 FROM __corro_buffered_changes bc
                 WHERE NOT EXISTS (
                     SELECT 1 FROM __corro_seq_bookkeeping bk
                     WHERE bk.site_id = bc.site_id AND bk.db_version = bc.db_version
                 )
                 LIMIT 1",
            )
            .and_then(
                |mut stmt| match stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?))) {
                    Ok(v) => Ok(Some(v)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e),
                },
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })
        })
    }

    spawn::spawn_counted(async move {
        // check for orphaned buffered changes every 5 minutes
        let mut retry_interval = tokio::time::interval(Duration::from_secs(5 * 60));
        retry_interval.tick().await;

        loop {
            let (actor_id, versions) = tokio::select! {
                biased;
                _ = &mut tripwire => break,
                maybe_partials = clear_buf_rx.recv() => {
                    let Some(partials) = maybe_partials else {
                        break;
                    };

                    partials
                }
                _ = retry_interval.tick() => {
                    match find_orphaned_buffered_changes(&ctx.pool).await {
                        Ok(Some((actor_id, version))) => (actor_id, rangemap::range_inclusive_set![version..=version]),
                        Ok(None) => continue,
                        Err(error) => {
                            tracing::debug!(%error, "could not query for orphaned buffered changes");
                            continue;
                        }
                    }
                }
            };

            let to_clear = if let Some(booked) = ctx.bookie.get(&actor_id) {
                let booked_read = booked.read();
                // The original corrosion code seems to have a bug where a continue is done on the inner loop instead of the
                // outer loop
                // for version in versions {
                //     if booked_read.get_partial(&version).is_some() {
                //         warn!(%actor_id, %version, "clear_buffered_meta: received partial version that's still in bookie, skipping");
                //         continue;
                //     }
                // }

                let before = versions.len();

                // This is a poor version of retain since rangemap does not have that functionality
                let to_clear: rangemap::RangeInclusiveSet<_> = versions
                    .into_iter()
                    .filter(|v| {
                        for v in v.start().0..=v.end().0 {
                            if booked_read.get_partial(&CrsqlDbVersion(v)).is_some() {
                                return false;
                            }
                        }

                        true
                    })
                    .collect();

                if to_clear.is_empty() {
                    tracing::debug!(%actor_id, "received set of partial versions that were all still in the bookie, skipping");
                    continue;
                } else if before > to_clear.len() {
                    tracing::debug!(%actor_id, still_booked = before - to_clear.len(), "received set of partial versions, some of which were still booked");
                }

                to_clear
            } else {
                versions
            };

            let pool = ctx.pool.clone();
            let tx_timeout = opts.sql_tx_timeout;
            let self_actor_id = ctx.actor_id;
            let mut task_tripwire = tripwire.clone();

            const TO_CLEAR_COUNT: usize = 1000;

            spawn::spawn_counted(async move {
                loop {
                    let res = {
                        let mut conn = pool.write_low().await?;

                        tokio::task::block_in_place(|| {
                            let tx = sqlite_pool::InterruptibleTransaction::new(
                                conn.immediate_transaction()?,
                                Some(tx_timeout),
                                "clear_buffered_meta",
                            );

                            let mut buf_count = 0;

                            for range in to_clear.iter() {
                                buf_count += tx
                                .prepare_cached("DELETE FROM __corro_buffered_changes WHERE (site_id, db_version, seq) IN (SELECT site_id, db_version, seq FROM __corro_buffered_changes WHERE site_id = ? AND db_version >= ? AND db_version <= ? LIMIT ?)")?
                                .execute(rusqlite::params![actor_id, range.start().0, range.end().0, TO_CLEAR_COUNT])?;
                            }

                            tx.commit()?;

                            Ok::<_, rusqlite::Error>(buf_count)
                        })
                    };

                    match res {
                        Ok(buf_count) => {
                            if buf_count > 0 {
                                tracing::debug!(buf_count, "cleared buffered change rows");
                            }
                            if buf_count < TO_CLEAR_COUNT {
                                break;
                            }
                        }
                        Err(error) => {
                            tracing::debug!(%error, "could not clear buffered meta for versions");
                        }
                    }

                    tokio::select! {
                        _ = &mut task_tripwire => {
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
                }

                Ok::<_, eyre::Report>(())
            }.instrument(tracing::debug_span!("clear buffered version", %actor_id, %self_actor_id)));
        }
    });
}
