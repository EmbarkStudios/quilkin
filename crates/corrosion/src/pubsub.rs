//! Reimplementation of some pieces of <https://github.com/superfly/corrosion/blob/main/crates/corro-agent/src/api/public/pubsub.rs>
//!
//! The Corrosion code was too linked to HTTP, which we may or may not use

use crate::codec::PrefixedBuf;
use bytes::Bytes;
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
pub use corro_agent::api::public::pubsub::MatcherUpsertError;
pub use corro_agent::api::public::pubsub::SubscriptionEvent;
use corro_api_types::{QueryEventMeta, Statement};
use corro_types::{
    agent::SplitPool,
    pubsub::{Matcher, MatcherCreated, MatcherLoopConfig},
    schema::Schema,
    updates::Handle,
};
pub use corro_types::{
    api::{ChangeId, QueryEvent, TypedQueryEvent},
    pubsub::{MatchCandidates, MatcherError, MatcherHandle, SubsManager},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc},
    task::block_in_place,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};
use tripwire::Tripwire;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum CatchUpError {
    #[error(transparent)]
    Pool(#[from] corro_types::sqlite::SqlitePoolError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Send(#[from] mpsc::error::SendError<SubscriptionEvent>),
    #[error(transparent)]
    Serialization(#[from] std::io::Error),
    #[error(transparent)]
    Matcher(#[from] MatcherError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

pub type BodySender = mpsc::Sender<Bytes>;

pub const SERVER_QUERY: &str = "SELECT endpoint,icao,tokens FROM servers";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubParamsv1 {
    /// The query being subscribed to
    #[serde(rename = "q")]
    pub query: Statement,
    /// If known, the last change id that has been seen, allowing only changes
    /// newer than this to be sent
    #[serde(default, rename = "f")]
    pub from: Option<ChangeId>,
    #[serde(default, rename = "s")]
    pub skip_rows: bool,
    /// The maximum amount in bytes that can be buffered before being sent
    ///
    /// If events are buffered below this threshold, they will be emitted on
    /// the next `max_time` interval
    #[serde(default, rename = "mb")]
    pub max_buffer: u16,
    /// The maximum amount of time that buffered events beneath the `max_buffer`
    /// threshold will be kept before being sent
    ///
    /// Defaults to 10ms
    #[serde(default = "max_time", rename = "mt")]
    pub max_time: Duration,
    /// The [`Matcher`] will buffer change notifications up to this time before emitting
    /// them, or until the notification buffer reaches the [`Self::change_threshold`]
    ///
    /// Note that this will only apply to the matcher if this is the first subscriber
    /// as it affects the single unique [`Matcher`] for each query, not this
    /// individual subscriber
    #[serde(default = "process_interval", rename = "d")]
    pub process_interval: Duration,
    /// The [`Matcher`] will buffer change notifications up to this count before
    /// emitting them, or up until the next [`Self::process_interval`]
    ///
    /// Note that this will only apply to the matcher if this is the first subscriber
    /// as it affects the single unique [`Matcher`] for each query, not this
    /// individual subscriber
    #[serde(default = "change_threshold", rename = "t")]
    pub change_threshold: usize,
}

/// The default [`SubParams::process_interval`]
pub const fn process_interval() -> Duration {
    Duration::from_millis(600)
}

/// The default [`SubParams::change_threshold`]
pub const fn change_threshold() -> usize {
    1000
}

/// The default [`SubParams::max_time`]
pub const fn max_time() -> Duration {
    Duration::from_millis(10)
}

async fn expand_sql(sp: &SplitPool, stmt: &Statement) -> Result<String, MatcherUpsertError> {
    let conn = sp.read().await?;

    let mut prepped = conn.prepare(stmt.query())?;
    match stmt {
        Statement::Simple(_)
        | Statement::Verbose {
            params: None,
            named_params: None,
            ..
        } => {}
        Statement::WithParams(_, params)
        | Statement::Verbose {
            params: Some(params),
            ..
        } => {
            for (i, param) in params.iter().enumerate() {
                prepped.raw_bind_parameter(i + 1, param)?;
            }
        }
        Statement::WithNamedParams(_, params)
        | Statement::Verbose {
            named_params: Some(params),
            ..
        } => {
            for (k, v) in params.iter() {
                let idx = match prepped.parameter_index(k)? {
                    Some(idx) => idx,
                    None => continue,
                };
                prepped.raw_bind_parameter(idx, v)?;
            }
        }
    }

    prepped
        .expanded_sql()
        .ok_or(MatcherUpsertError::CouldNotExpand)
}

fn handle_sub_event(
    max_size: u16,
    buf: &mut PrefixedBuf,
    event: SubscriptionEvent,
    last_change_id: &mut ChangeId,
) -> Option<Bytes> {
    if let QueryEventMeta::EndOfQuery(Some(change_id)) | QueryEventMeta::Change(change_id) =
        event.meta
    {
        if !last_change_id.is_zero() && change_id > *last_change_id + 1 {
            warn!(
                ?change_id,
                ?last_change_id,
                "non-contiguous change id (> + 1) received"
            );
        } else if !last_change_id.is_zero() && change_id == *last_change_id {
            warn!(?change_id, ?last_change_id, "duplicate change id received");
        } else if change_id < *last_change_id {
            warn!(?change_id, ?last_change_id, "smaller change id received");
        }
        *last_change_id = change_id;
    }

    buf.extend_capped(&event.buff, max_size)
}

const MAX_EVENTS_BUFFER_SIZE: usize = 10240;

#[inline]
pub fn error_to_sub_event(
    buf: &mut PrefixedBuf,
    e: impl compact_str::ToCompactString,
) -> SubscriptionEvent {
    query_to_sub_event(buf, QueryEvent::Error(e.to_compact_string())).unwrap()
}

#[inline]
pub fn query_to_sub_event(
    buf: &mut PrefixedBuf,
    query_evt: QueryEvent,
) -> std::io::Result<SubscriptionEvent> {
    Ok(SubscriptionEvent {
        buff: buf.write_json(&query_evt)?,
        meta: query_evt.meta(),
    })
}

pub async fn catch_up_sub_from(
    matcher: &MatcherHandle,
    from: ChangeId,
    evt_tx: &mpsc::Sender<SubscriptionEvent>,
) -> Result<ChangeId, CatchUpError> {
    let (q_tx, mut q_rx) = mpsc::channel(MAX_EVENTS_BUFFER_SIZE);

    let task = tokio::spawn({
        let evt_tx = evt_tx.clone();
        async move {
            let mut buf = PrefixedBuf::new();
            while let Some(event) = q_rx.recv().await {
                evt_tx.send(query_to_sub_event(&mut buf, event)?).await?;
            }
            Ok::<_, CatchUpError>(())
        }
    });

    let last_change_id = {
        let conn = matcher.pool().get().await?;
        block_in_place(|| matcher.changes_since(from, &conn, q_tx))?
    };

    task.await??;

    Ok(last_change_id)
}

pub async fn catch_up_sub_anew(
    matcher: &MatcherHandle,
    evt_tx: &mpsc::Sender<SubscriptionEvent>,
) -> Result<ChangeId, CatchUpError> {
    let (q_tx, mut q_rx) = mpsc::channel(MAX_EVENTS_BUFFER_SIZE);

    let task = tokio::spawn({
        let evt_tx = evt_tx.clone();
        async move {
            let mut buf = PrefixedBuf::new();
            while let Some(event) = q_rx.recv().await {
                evt_tx.send(query_to_sub_event(&mut buf, event)?).await?;
            }
            Ok::<_, CatchUpError>(())
        }
    });

    let last_change_id = {
        let mut conn = matcher.pool().get().await?;
        block_in_place(|| {
            let conn_tx = conn.transaction()?;
            matcher.all_rows(&conn_tx, q_tx)
        })?
    };

    task.await??;

    Ok(last_change_id)
}

/// Forwards subscription events from the broadcaster to the individual subscriber
async fn forward_sub_to_sender(
    handle: MatcherHandle,
    mut sub_rx: broadcast::Receiver<SubscriptionEvent>,
    tx: mpsc::Sender<SubscriptionEvent>,
    skip_rows: bool,
) {
    info!(sub_id = %handle.id(), "forwarding subscription events to a sender");

    loop {
        let eve = tokio::select! {
            res = sub_rx.recv() => {
                match res {
                    Ok(eve) => eve,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(sub_id = %handle.id(), skipped, "subscription skipped events");
                        return;
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        info!(sub_id = %handle.id(), "events sender closed");
                        return;
                    },
                }
            },
            _ = handle.cancelled() => {
                info!(sub_id = %handle.id(), "subscription cancelled, aborting forwarding bytes to subscriber");
                return;
            },
        };

        if skip_rows
            && matches!(
                eve.meta,
                QueryEventMeta::Columns | QueryEventMeta::Row(_) | QueryEventMeta::EndOfQuery(_)
            )
        {
            continue;
        }
        if let Err(e) = tx.send(eve).await {
            warn!(sub_id = %handle.id(), "could not send subscription event to channel: {e}");
            return;
        }
    }
}

/// Given a new subscriber to an existing matcher, attempts to catch up the
/// subscriber to the current state of the database
pub async fn catch_up_sub(
    matcher: MatcherHandle,
    params: SubParamsv1,
    mut sub_rx: broadcast::Receiver<SubscriptionEvent>,
    evt_tx: mpsc::Sender<SubscriptionEvent>,
) {
    tracing::debug!(?params, "catching up sub");

    let mut buf = PrefixedBuf::new();

    // buffer events while we catch up...
    let (queue_tx, mut queue_rx) = mpsc::channel(MAX_EVENTS_BUFFER_SIZE);

    let cancel = CancellationToken::new();
    // just in case...
    let _drop_guard = cancel.clone().drop_guard();

    let queue_task = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            loop {
                let eve = tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    },
                    Ok(res) = sub_rx.recv() => res,
                    else => break
                };

                if let QueryEventMeta::Change(change_id) = eve.meta
                    && let Err(_e) = queue_tx.try_send((eve.buff, change_id))
                {
                    return Err(eyre::eyre!(
                        "catching up too slowly, gave up after buffering {MAX_EVENTS_BUFFER_SIZE} events"
                    ));
                }
            }
            Ok(sub_rx)
        }
    });

    let mut last_change_id = {
        let res = match params.from {
            Some(ChangeId(0)) | None => {
                if params.skip_rows {
                    let conn = match matcher.pool().get().await {
                        Ok(conn) => conn,
                        Err(e) => {
                            drop(evt_tx.send(error_to_sub_event(&mut buf, e)).await);
                            return;
                        }
                    };
                    block_in_place(|| matcher.max_change_id(&conn)).map_err(CatchUpError::from)
                } else {
                    catch_up_sub_anew(&matcher, &evt_tx).await
                }
            }
            Some(from) => catch_up_sub_from(&matcher, from, &evt_tx).await,
        };

        match res {
            Ok(change_id) => change_id,
            Err(e) => {
                if !matches!(e, CatchUpError::Send(_)) {
                    drop(evt_tx.send(error_to_sub_event(&mut buf, e)).await);
                }
                return;
            }
        }
    };

    let mut min_change_id = last_change_id + 1;
    info!(sub_id = %matcher.id(), "minimum expected change id: {min_change_id:?}");

    let mut pending_event = None;

    let last_sub_change_id = match queue_rx.try_recv() {
        Ok((event_buf, change_id)) => {
            info!(sub_id = %matcher.id(), "last change id received by subscription: {change_id:?}");
            pending_event = Some((event_buf, change_id));
            Some(change_id)
        }
        Err(mpsc::error::TryRecvError::Empty) => {
            let last_change_id_sent = matcher.last_change_id_sent();
            info!(?last_change_id_sent, "last change id sent by subscription");
            if last_change_id_sent <= last_change_id {
                None
            } else {
                Some(last_change_id_sent)
            }
        }
        Err(mpsc::error::TryRecvError::Disconnected) => {
            // abnormal
            drop(
                evt_tx
                    .send(error_to_sub_event(&mut buf, "exceeded events buffer"))
                    .await,
            );
            return;
        }
    };

    if let Some(change_id) = last_sub_change_id {
        debug!(sub_id = %matcher.id(), "got a change to check: {change_id:?}");
        for i in 0..5 {
            min_change_id = last_change_id + 1;

            if change_id >= min_change_id {
                // missed some updates!
                info!(sub_id = %matcher.id(), "attempt #{} to catch up subscription from change id: {change_id:?} (last: {last_change_id:?})", i+1);

                let res = catch_up_sub_from(&matcher, last_change_id, &evt_tx).await;

                match res {
                    Ok(new_last_change_id) => last_change_id = new_last_change_id,
                    Err(e) => {
                        if !matches!(e, CatchUpError::Send(_)) {
                            drop(evt_tx.send(error_to_sub_event(&mut buf, e)).await);
                        }
                        return;
                    }
                }
            } else {
                break;
            }
            // TODO: corrosion just has a comment "sleep for 100 milliseconds", which is utterly
            // useless, _WHY_ are we sleeping!?
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if change_id >= min_change_id {
            drop(
                evt_tx
                    .send(error_to_sub_event(
                        &mut buf,
                        "could not catch up to changes in 5 attempts",
                    ))
                    .await,
            );
            return;
        }
    }

    info!(
        ?last_change_id,
        ?last_sub_change_id,
        "subscription is caught up, no gaps in change id"
    );

    if let Some((event_buf, change_id)) = pending_event {
        info!(sub_id = %matcher.id(), "had a pending event we popped from the queue, id: {change_id:?} (last change id: {last_change_id:?})");
        if change_id > last_change_id {
            info!(sub_id = %matcher.id(), "change was more recent, sending!");
            if let Err(_e) = evt_tx
                .send(SubscriptionEvent {
                    buff: event_buf,
                    meta: QueryEventMeta::Change(change_id),
                })
                .await
            {
                warn!("could not send buffered events to subscriber, receiver must be gone!");
                return;
            }

            last_change_id = change_id;
        }
    }

    // cancel the task that was buffering changes as we've now caught up to where
    // the head change was when the queue started
    cancel.cancel();

    info!(?last_change_id, "processing buffered changes");
    while let Some((event_buf, change_id)) = queue_rx.recv().await {
        if change_id <= last_change_id {
            debug!(?change_id, "change already applied");
            continue;
        }

        info!(?change_id, "buffered change");
        if let Err(_e) = evt_tx
            .send(SubscriptionEvent {
                buff: event_buf,
                meta: QueryEventMeta::Change(change_id),
            })
            .await
        {
            warn!(sub_id = %matcher.id(), "could not send buffered events to subscriber, receiver must be gone!");
            return;
        }

        last_change_id = change_id;
    }

    let sub_rx = match queue_task.await {
        Ok(Ok(sub_rx)) => sub_rx,
        Ok(Err(e)) => {
            drop(evt_tx.send(error_to_sub_event(&mut buf, e)).await);
            return;
        }
        Err(e) => {
            drop(evt_tx.send(error_to_sub_event(&mut buf, e)).await);
            return;
        }
    };

    forward_sub_to_sender(matcher, sub_rx, evt_tx, params.skip_rows).await;
}

const MAX_UNSUB_TIME: Duration = Duration::from_secs(10 * 60);
// this should be a fraction of the `MAX_UNSUB_TIME`
const RECEIVERS_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// Processes a single matcher mpsc channel to sent it to the broadcast channel
/// in the case of multiple subscriptions to the same query
pub async fn process_sub_channel(
    subs: SubsManager,
    id: Uuid,
    tx: broadcast::Sender<SubscriptionEvent>,
    mut evt_rx: mpsc::Receiver<QueryEvent>,
) {
    let mut buf = PrefixedBuf::new();

    let mut deadline = if tx.receiver_count() == 0 {
        Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)))
    } else {
        None
    };

    // even if there are no more subscribers
    // useful for queries that don't change often so we can cleanup...
    let mut subs_check = tokio::time::interval(RECEIVERS_CHECK_INTERVAL);

    loop {
        let deadline_check = async {
            if let Some(sleep) = deadline.as_mut() {
                sleep.await;
            } else {
                std::future::pending::<()>().await;
            }
        };

        let query_evt = tokio::select! {
            biased;
            Some(query_evt) = evt_rx.recv() => query_evt,
            _ = deadline_check => {
                if tx.receiver_count() == 0 {
                    info!(sub_id = %id, "All listeners for subscription are gone and didn't come back within {MAX_UNSUB_TIME:?}");
                    break;
                }

                // reset the deadline if there are receivers!
                deadline = None;
                continue;
            },
            _ = subs_check.tick() => {
                if tx.receiver_count() == 0 {
                    if deadline.is_none() {
                        deadline = Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)));
                    }
                } else {
                    deadline = None;
                };
                continue;
            },
            else => {
                break;
            }
        };

        let is_still_active = match query_to_sub_event(&mut buf, query_evt) {
            Ok(b) => tx.send(b).is_ok(),
            Err(e) => {
                drop(tx.send(error_to_sub_event(&mut buf, e)));
                break;
            }
        };

        if is_still_active {
            deadline = None;
        } else {
            debug!(sub_id = %id, "no active listeners to receive subscription event");
            if deadline.is_none() {
                deadline = Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)));
            }
        }
    }

    warn!("subscription query channel done");

    // remove and get handle from the agent's "matchers"
    if let Some(handle) = subs.remove(&id) {
        info!("Removed subscription from process_sub_channel");
        // clean up the subscription
        handle.cleanup().await;
    } else {
        warn!("subscription handle was already gone. odd!");
    }
}

/// Upserts a subscription
///
/// If this is a subscription to a new query, a task is created for funneling
/// mpsc events though a broadcaster in case new subscribers are made for the same
/// query
///
/// If it's a subscription to an existing query that is cached, it will be added
/// to the broadcaster and caught up to events that have already occurred
pub async fn upsert_sub(
    handle: MatcherHandle,
    maybe_created: Option<MatcherCreated>,
    subs: &SubsManager,
    bcast_write: &mut MatcherCache,
    params: SubParamsv1,
    tx: mpsc::Sender<SubscriptionEvent>,
) -> Result<Uuid, MatcherUpsertError> {
    if let Some(created) = maybe_created {
        if params.from.is_some() {
            handle.cleanup().await;
            subs.remove(&handle.id());
            return Err(MatcherUpsertError::SubFromWithoutMatcher);
        }

        let (sub_tx, sub_rx) = broadcast::channel(MAX_EVENTS_BUFFER_SIZE);

        tokio::spawn(
            forward_sub_to_sender(handle.clone(), sub_rx, tx, params.skip_rows)
                .instrument(tracing::info_span!("forward", sub_id = %handle.id())),
        );

        bcast_write.insert(handle.id(), sub_tx.clone());

        tokio::spawn(
            process_sub_channel(subs.clone(), handle.id(), sub_tx, created.evt_rx)
                .instrument(tracing::info_span!("process", sub_id = %handle.id())),
        );

        Ok(handle.id())
    } else {
        let id = handle.id();
        let sub_tx = bcast_write
            .get(id)
            .ok_or(MatcherUpsertError::MissingBroadcaster)?;

        let span = tracing::info_span!("catchup", sub_id = %handle.id());
        tokio::spawn(catch_up_sub(handle, params, sub_tx, tx).instrument(span));

        Ok(id)
    }
}

pub type MatcherCacheInner = std::collections::HashMap<Uuid, broadcast::Sender<SubscriptionEvent>>;

#[derive(Default)]
pub struct MatcherCache(MatcherCacheInner);

impl MatcherCache {
    #[inline]
    pub fn insert(&mut self, id: Uuid, tx: broadcast::Sender<SubscriptionEvent>) {
        self.0.insert(id, tx);
    }

    #[inline]
    pub fn get(&self, id: Uuid) -> Option<broadcast::Receiver<SubscriptionEvent>> {
        self.0.get(&id).map(|tx| tx.subscribe())
    }
}

pub type SharedMatcherCache = Arc<tokio::sync::RwLock<MatcherCache>>;

/// The context needed to create and manage subscriptions
#[derive(Clone)]
pub struct PubsubContext {
    /// The schema for the database
    pub schema: Arc<Schema>,
    /// The subscription manager
    pub subs: SubsManager,
    /// The database pool
    pub pool: SplitPool,
    /// A subscription cache.
    ///
    /// This is needed due to how Corrosion works, where each individual [`Matcher`]
    /// only has an mpsc channel, so if there are multiple subscribers to the
    /// same query, and thus the same [`Matcher`], they need to be funnelled through
    /// a broadcaster so that each individual subscriber gets the events
    pub cache: SharedMatcherCache,
    pub tripwire: Tripwire,
    /// The path where subscriptions are stored
    pub path: PathBuf,
}

impl PubsubContext {
    /// Creates a subscription for the specified [`PubusbContext`]
    ///
    /// Database mutations that match the query specified in the params will
    /// cause subscription events to be emitted to the receiver
    pub async fn subscribe(&self, params: SubParamsv1) -> Result<Subscription, MatcherUpsertError> {
        let query = expand_sql(&self.pool, &params.query).await?;
        let mut bcast_write = self.cache.write().await;

        let (handle, created) = self.subs.get_or_insert(
            &query,
            &self.path,
            &self.schema,
            &self.pool,
            self.tripwire.clone(),
            MatcherLoopConfig {
                changes_threshold: params.change_threshold,
                process_buffer_interval: params.process_interval,
                ..Default::default()
            },
        )?;

        let (tx, rx) = mpsc::channel(MAX_EVENTS_BUFFER_SIZE);

        let query_hash = handle.hash().to_owned();
        let id = upsert_sub(handle, created, &self.subs, &mut bcast_write, params, tx).await?;

        Ok(Subscription { id, query_hash, rx })
    }
}

pub struct Subscription {
    pub id: Uuid,
    pub query_hash: String,
    pub rx: mpsc::Receiver<SubscriptionEvent>,
}

/// Creates a subscription for the specified [`PubusbContext`]
///
/// Database mutations that match the query specified in the params will
/// cause subscription events to be emitted to the receiver
pub async fn subscribe(
    params: SubParamsv1,
    ctx: &PubsubContext,
) -> Result<Subscription, MatcherUpsertError> {
    let query = expand_sql(&ctx.pool, &params.query).await?;
    let mut bcast_write = ctx.cache.write().await;

    let (handle, created) = ctx.subs.get_or_insert(
        &query,
        &ctx.path,
        &ctx.schema,
        &ctx.pool,
        ctx.tripwire.clone(),
        MatcherLoopConfig {
            changes_threshold: params.change_threshold,
            process_buffer_interval: params.process_interval,
            ..Default::default()
        },
    )?;

    let (tx, rx) = mpsc::channel(MAX_EVENTS_BUFFER_SIZE);

    let query_hash = handle.hash().to_owned();
    let id = upsert_sub(handle, created, &ctx.subs, &mut bcast_write, params, tx).await?;

    Ok(Subscription { id, query_hash, rx })
}

/// An async stream that buffers events, used by senders to batch changes
///
/// This buffers up to a maximum size, or a certain amount of time has passed
/// since the last emitted buffer set
pub struct BufferingSubStream {
    rx: mpsc::Receiver<SubscriptionEvent>,
    id: Uuid,
    max_size: u16,
    max_time: Duration,
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
    change_id: ChangeId,
    buffer: PrefixedBuf,
}

impl BufferingSubStream {
    /// Buffers changes from the subscription
    #[inline]
    pub fn new(sub: Subscription, max_size: u16, max_time: Duration) -> Self {
        let sleep = Box::pin(tokio::time::sleep(max_time));

        Self {
            rx: sub.rx,
            id: sub.id,
            max_size,
            max_time,
            sleep,
            change_id: ChangeId(0),
            buffer: PrefixedBuf::new(),
        }
    }

    /// Drains the stream, must be called until `None` is returned
    #[inline]
    pub async fn drain(&mut self) -> Option<Bytes> {
        let span = tracing::info_span!("sub event", sub_id = %self.id);
        {
            while let Some(event) = self.rx.recv().await {
                let _sg = span.enter();
                if let Some(bytes) =
                    handle_sub_event(self.max_size, &mut self.buffer, event, &mut self.change_id)
                {
                    return Some(bytes);
                }
            }
        }

        self.buffer.freeze()
    }
}

use std::task::Poll;

impl futures::Stream for BufferingSubStream {
    type Item = Bytes;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // First try to pull any buffers from the receiver
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(eve)) => {
                let span = tracing::info_span!("sub event", sub_id = %self.id);

                // We can't borrow multiple mutable fields from a pin, thus this weird copy
                let mut cid = self.change_id;
                let maybe_buf = span
                    .in_scope(|| handle_sub_event(self.max_size, &mut self.buffer, eve, &mut cid));
                self.change_id = cid;

                // This will be Some if the buffer was over the configured maximum
                if let Some(buf) = maybe_buf {
                    let new_time = tokio::time::Instant::now() + self.max_time;
                    self.sleep.as_mut().reset(new_time);
                    return Poll::Ready(Some(buf));
                }
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }

        // If we didn't receive a new buffer, check if we have buffered events
        // and the configured timeout has elapsed
        if let Poll::Ready(()) = self.sleep.as_mut().poll(cx) {
            let new_time = tokio::time::Instant::now() + self.max_time;
            self.sleep.as_mut().reset(new_time);

            if let Some(buf) = self.buffer.freeze() {
                return Poll::Ready(Some(buf));
            }
        }

        Poll::Pending
    }
}

/// Reads the next length-prefixed chunk from the bytes
///
/// Returns `Some` with the chunk if there is a complete chunk,
/// advancing the input to the next chunk
#[inline]
pub fn read_length_prefixed_bytes(b: &mut Bytes) -> Option<Bytes> {
    if b.len() < 2 {
        return None;
    }

    let len = (b[0] as u16 | ((b[1] as u16) << 8)) as usize;

    if len > b.len() - 2 {
        return None;
    }

    let _b = b.split_to(2);
    Some(b.split_to(len))
}

/// The read side of a [`BufferingSubStream`]
///
/// This is intentionally sans-io for easier testing
pub struct SubscriptionStream {
    buf: Bytes,
}

impl SubscriptionStream {
    #[inline]
    pub fn new(buf: Bytes) -> Self {
        Self { buf }
    }

    #[inline]
    pub fn length_prefixed(buf: &mut Bytes) -> Option<Self> {
        Some(Self {
            buf: read_length_prefixed_bytes(buf)?,
        })
    }
}

impl Iterator for SubscriptionStream {
    type Item = Result<QueryEvent, serde_json::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let b = read_length_prefixed_bytes(&mut self.buf)?;
        Some(serde_json::from_slice(&b))
    }
}

/// Initialize subscription state and tasks
///
/// 1. Get subscriptions state directory from config
/// 2. Load existing subscriptions and restore them in [`SubsManager`]
/// 3. Spawn subscription processor task
pub async fn restore_subscriptions(
    subs_manager: &SubsManager,
    subs_path: &Path,
    pool: &SplitPool,
    schema: &Schema,
    tripwire: &Tripwire,
    loop_cfg: MatcherLoopConfig,
) -> eyre::Result<SharedMatcherCache> {
    let mut subs_bcast_cache = MatcherCacheInner::default();

    // If we error trying to restore a subscription, delete it
    let mut to_cleanup = Vec::new();

    if let Ok(mut dir) = tokio::fs::read_dir(&subs_path).await {
        loop {
            let entry = match dir.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(error) => {
                    error!(%error, %subs_path, "failed to read entry in subscription path");
                    continue;
                }
            };

            let Some(sub_id) = entry
                .file_name()
                .to_str()
                .and_then(|fname| fname.parse().ok())
            else {
                warn!(%subs_path, filename=?entry.file_name(), "invalid file found in subscription path");
                continue;
            };

            let res = subs_manager.restore(
                sub_id,
                subs_path,
                schema,
                pool,
                tripwire.clone(),
                loop_cfg.clone(),
            );

            match res {
                Ok((_, created)) => {
                    info!(%sub_id, "Restored subscription");

                    let (sub_tx, _) = tokio::sync::broadcast::channel(MAX_EVENTS_BUFFER_SIZE);

                    tokio::spawn(process_sub_channel(
                        subs_manager.clone(),
                        sub_id,
                        sub_tx.clone(),
                        created.evt_rx,
                    ));

                    subs_bcast_cache.insert(sub_id, sub_tx);
                }
                Err(error) => {
                    error!(%sub_id, %error, "could not restore subscription");
                    to_cleanup.push(sub_id);
                }
            }
        }
    }

    for sub_id in to_cleanup {
        debug!(%sub_id, "Cleaning up unclean subscription");
        if let Err(error) = Matcher::cleanup(sub_id, &Matcher::sub_path(subs_path, sub_id)) {
            warn!(%error, %sub_id, "failed to cleanup subscription");
        }
    }

    Ok(Arc::new(tokio::sync::RwLock::new(MatcherCache(
        subs_bcast_cache,
    ))))
}
