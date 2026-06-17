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
use rangemap::RangeInclusiveSet;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

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

pub enum ClearVersionRange {
    Single(CrsqlDbVersionRange),
    Set(RangeInclusiveSet<CrsqlDbVersion>),
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
    pub apply_tx: mpsc::Sender<(ActorId, Vec<CrsqlDbVersion>)>,
    pub clear_buf_tx: mpsc::Sender<(ActorId, ClearVersionRange)>,
    pub shutdown: tokio::sync::watch::Sender<()>,
    pub metrics: &'static super::GossipMetrics,
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
    mut rx_changes: mpsc::Receiver<super::Change>,
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
            maybe_change_src = rx_changes.recv() => match maybe_change_src {
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

            // the corrosion code doesn't seem to think this one isn't critical, or that it won't usually ever be full?
            if ctx
                .clear_buf_tx
                .try_send((actor_id, ClearVersionRange::Set(clear_versions)))
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

pub fn process_complete_version(
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

    // TODO: Figure out a better assertion. This assertion is disabled for now to reduce false negatives. We can receive a valid complete changeset
    // where the number of changes is less than the seqs range because some rows have been overridden by a newer update.
    // let details = json!({"len": len, "seqs": seqs.start_int(), "seqs_end": seqs.end_int(), "actor_id": actor_id, "version": version});
    // assert_always!(
    //     len <= seqs.len(),
    //     "number of changes is equal to the seq len",
    //     &details
    // );
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

pub fn process_incomplete_version(
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
