use super::{GossipMetrics, transport::Transport};
use bytes::{Bytes, BytesMut};
use corro_types::{
    actor::{self, Actor},
    broadcast::{self as bx, FocaCmd, FocaInput},
    sqlite::unnest_param,
};
use foca::{Identity, Timer};
use parking_lot::RwLock;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tripwire::PreemptibleFutureExt;

type Foca = foca::Foca<
    Actor,
    foca::BincodeCodec<bincode::config::Configuration>,
    StdRng,
    foca::NoCustomBroadcast,
>;

pub struct DispatchRuntime {
    pub to_send: mpsc::Sender<(Actor, Bytes)>,
    pub to_schedule: mpsc::Sender<(Duration, Timer<Actor>)>,
    pub notifications: mpsc::Sender<foca::OwnedNotification<Actor>>,
    pub active: bool,
    pub buf: BytesMut,
    pub metrics: &'static GossipMetrics,
}

impl foca::Runtime<Actor> for DispatchRuntime {
    fn notify(&mut self, notification: foca::Notification<'_, Actor>) {
        match &notification {
            foca::Notification::Active => {
                self.active = true;
            }
            foca::Notification::Idle | foca::Notification::Defunct => {
                self.active = false;
            }
            _ => {}
        };

        if self
            .notifications
            .try_send(notification.to_owned())
            .is_err()
        {
            self.metrics
                .channel_error_inc("full", "dispatch.notifications");
        }
    }

    fn send_to(&mut self, to: Actor, data: &[u8]) {
        self.buf.extend_from_slice(data);

        if self
            .to_send
            .try_send((to, self.buf.split().freeze()))
            .is_err()
        {
            self.metrics.channel_error_inc("full", "dispatch.to_send");
        }
    }

    fn submit_after(&mut self, event: Timer<Actor>, after: Duration) {
        if self.to_schedule.try_send((after, event)).is_err() {
            self.metrics
                .channel_error_inc("full", "dispatch.to_schedule");
        }
    }
}

#[derive(Clone)]
pub struct SwimCtx {
    pub pool: corro_types::agent::SplitPool,
    pub members: super::Members,
    pub actor_id: actor::ActorId,
    pub member_id: Option<actor::MemberId>,
    pub cluster_id: actor::ClusterId,
    pub metrics: &'static GossipMetrics,
}

/// Entry point for the SWIM-based gossip
#[allow(clippy::too_many_arguments)]
pub fn swim_loop(
    ctx: SwimCtx,
    actor: Actor,
    transport: Transport,
    mut foca_rx: mpsc::Receiver<FocaInput>,
    bcast_rx: mpsc::Receiver<bx::BroadcastInput>,
    to_send: mpsc::Sender<(Actor, Bytes)>,
    notifications_tx: mpsc::Sender<foca::OwnedNotification<Actor>>,
    tripwire: tripwire::Tripwire,
    member_states: Vec<(std::net::SocketAddr, foca::Member<Actor>)>,
) {
    let rng = StdRng::from_os_rng();

    let config = make_foca_config(1.try_into().unwrap());

    let mut foca = Foca::with_custom_broadcast(
        actor,
        config.clone(),
        rng,
        foca::BincodeCodec(bincode::config::Configuration::default()),
        foca::NoCustomBroadcast,
    );

    let config = Arc::new(RwLock::new(config));

    let (to_schedule, mut to_schedule_rx) = mpsc::channel(512);

    let mut runtime = DispatchRuntime {
        to_send,
        to_schedule,
        notifications: notifications_tx.clone(),
        active: false,
        buf: Default::default(),
        metrics: ctx.metrics,
    };

    let (timer_tx, mut timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(timer_tx);

    {
        let mut tripwire = tripwire.clone();
        spawn::spawn_counted(async move {
            while let tripwire::Outcome::Completed(Some((duration, timer))) =
                to_schedule_rx.recv().preemptible(&mut tripwire).await
            {
                timer_spawner.spawn((duration, timer));
            }
        });
    }

    let cluster_size = Arc::new(std::sync::atomic::AtomicU32::new(1));

    // foca SWIM operations loop.
    // NOTE: every turn of that loop should be fast or else we risk being a down suspect
    spawn::spawn_counted({
        let config = config.clone();
        let cluster_size = cluster_size.clone();
        let ctx = ctx.clone();
        let mut tripwire = tripwire.clone();

        async move {
            let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_cluster_size = std::num::NonZeroU32::new(1).unwrap();

            let mut last_states = member_states
                .into_iter()
                .map(|(_, member)| (member, None))
                .collect::<Vec<_>>();
            let mut diff_last_states_every = tokio::time::interval(Duration::from_secs(60));

            enum Branch {
                Foca(FocaInput),
                HandleTimer(Timer<Actor>, Instant),
                DiffMembers,
                Metrics,
            }

            loop {
                let (branch, bdisc) = tokio::select! {
                    biased;
                    timer = timer_rx.recv() => {
                        let Some((timer, seq)) = timer else {
                            tracing::warn!("no more foca timers, breaking");
                            break;
                        };

                        (Branch::HandleTimer(timer, seq), "handle_timer")
                    },
                    input = foca_rx.recv() => {
                        let Some(input) = input else {
                            tracing::warn!("no more foca inputs");
                            break;
                        };

                        (Branch::Foca(input), "foca")
                    },
                    _ = metrics_interval.tick() => {
                        (Branch::Metrics, "metrics")
                    },
                    _ = diff_last_states_every.tick() => {
                        (Branch::DiffMembers, "diff_members")
                    }
                    _ = &mut tripwire => {
                        break
                    },
                };

                let start = Instant::now();

                match branch {
                    Branch::Foca(input) => 'foca: {
                        match input {
                            FocaInput::Announce(actor) => {
                                if let Err(error) = foca.announce(actor, &mut runtime) {
                                    tracing::error!(%error, "foca announce error");
                                }
                            }
                            FocaInput::Data(data) => {
                                if let Err(error) = foca.handle_data(&data, &mut runtime) {
                                    tracing::error!(%error, "error handling foca data");
                                }
                            }
                            FocaInput::ClusterSize(size) => {
                                if size != last_cluster_size {
                                    let new_config = make_foca_config(size);
                                    if let Err(error) = foca.set_config(new_config.clone()) {
                                        tracing::error!(%error, "foca set_config error");
                                    } else {
                                        last_cluster_size = size;
                                        *config.write() = new_config;
                                    }
                                }

                                cluster_size
                                    .store(size.get(), std::sync::atomic::Ordering::Release);
                            }
                            FocaInput::ApplyMany(updates) => {
                                if let Err(error) =
                                    foca.apply_many(updates.into_iter(), true, &mut runtime)
                                {
                                    tracing::error!(%error, "foca apply_many error");
                                }
                            }
                            FocaInput::Cmd(cmd) => match cmd {
                                FocaCmd::Rejoin(callback) => {
                                    let Some(renewed) = foca.identity().renew() else {
                                        break 'foca;
                                    };

                                    let new_id = foca.change_identity(renewed, &mut runtime);

                                    if callback.send(new_id).is_err() {
                                        tracing::warn!(
                                            "could not send back result after rejoining cluster"
                                        );
                                    }
                                }
                                FocaCmd::MembershipStates(sender) => {
                                    for member in foca.iter_membership_state() {
                                        if let Err(error) = sender.send(member.clone()).await {
                                            tracing::error!(
                                                %error,
                                                "could not send back foca membership"
                                            );
                                            break;
                                        }
                                    }
                                }
                                FocaCmd::ChangeIdentity(id, callback) => {
                                    if callback
                                        .send(foca.change_identity(id, &mut runtime))
                                        .is_err()
                                    {
                                        tracing::warn!(
                                            "could not send back result after changing identity"
                                        );
                                    }
                                }
                            },
                        }
                    }
                    Branch::HandleTimer(timer, seq) => {
                        handle_timer(&mut foca, &mut runtime, &mut timer_rx, timer, seq);
                    }
                    Branch::Metrics => {
                        // trace!("handling Branch::Metrics");
                        // {
                        //     gauge!("corro.gossip.members").set(foca.num_members() as f64);
                        //     gauge!("corro.gossip.member.states")
                        //         .set(foca.iter_membership_state().count() as f64);
                        //     gauge!("corro.gossip.updates_backlog")
                        //         .set(foca.updates_backlog() as f64);
                        // }
                        // {
                        //     let config = config.read();
                        //     gauge!("corro.gossip.config.max_transmissions")
                        //         .set(config.max_transmissions.get() as f64);
                        //     gauge!("corro.gossip.config.num_indirect_probes")
                        //         .set(config.num_indirect_probes.get() as f64);
                        // }
                        // gauge!("corro.gossip.cluster_size").set(last_cluster_size.get() as f64);
                    }
                    Branch::DiffMembers => {
                        diff_member_states(&ctx, &foca, &mut last_states, &notifications_tx);
                    }
                };

                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    tracing::warn!(
                        ?elapsed,
                        branch = bdisc,
                        "swim loop took excessive time to execute branch"
                    );
                }
            }

            // leave the cluster gracefully
            if let Err(error) = foca.leave_cluster(&mut runtime) {
                tracing::error!(%error, "could not leave cluster gracefully");
            }

            let leave_deadline = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(leave_deadline);

            let mut foca_done = false;
            let mut timer_done = false;

            loop {
                tokio::select! {
                    biased;
                    _ = &mut leave_deadline => {
                        break;
                    },
                    timer = timer_rx.recv(), if !timer_done => match timer {
                        Some((timer, seq)) => {
                            handle_timer(&mut foca, &mut runtime, &mut timer_rx, timer, seq);
                        },
                        None => {
                            timer_done = true;
                        }
                    },
                    input = foca_rx.recv(), if !foca_done => match input {
                        Some(FocaInput::Data(data)) => {
                            drop(foca.handle_data(&data, &mut runtime));
                        },
                        None => {
                            foca_done = true;
                        }
                        _ => {

                        }
                    },
                    else => {
                        break;
                    }
                }
            }

            if let Some(handle) =
                diff_member_states(&ctx, &foca, &mut last_states, &notifications_tx)
            {
                tracing::debug!("Waiting on task to update member states...");
                if let Err(error) = handle.await {
                    tracing::error!(%error, "could not await task to update member states");
                }
            }

            tracing::info!("foca runtime loop is done, leaving cluster");
        }
    });

    spawn::spawn_counted(handle_broadcasts(
        ctx, bcast_rx, transport, config, tripwire,
    ));
}

fn make_foca_config(cluster_size: std::num::NonZeroU32) -> foca::Config {
    let mut config = foca::Config::new_wan(cluster_size);

    config.remove_down_after = Duration::from_secs(2 * 24 * 3600);
    config.max_packet_size = std::num::NonZeroUsize::new(1178).unwrap();

    config
}

#[derive(Clone)]
struct TimerSpawner {
    send: mpsc::UnboundedSender<(Duration, Timer<Actor>)>,
}

impl TimerSpawner {
    pub fn new(timer_tx: mpsc::Sender<(Timer<Actor>, Instant)>) -> Self {
        let (send, mut recv) = mpsc::unbounded_channel();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        std::thread::Builder::new()
            .name("foca-runtime-timer".to_owned())
            .spawn({
                move || {
                    let local = tokio::task::LocalSet::new();

                    local.spawn_local(async move {
                        while let Some((duration, timer)) = recv.recv().await {
                            let seq = Instant::now() + duration;

                            let timer_tx = timer_tx.clone();
                            tokio::task::spawn_local(async move {
                                tokio::time::sleep(duration).await;
                                timer_tx.send((timer, seq)).await.ok();
                            });
                        }
                        // If the while loop returns, then all the LocalSpawner
                        // objects have been dropped.
                    });

                    // This will return once all senders are dropped and all
                    // spawned tasks have returned.
                    rt.block_on(local);
                }
            })
            .expect("failed to spawn foca runtime timer thread");

        Self { send }
    }

    #[inline]
    pub fn spawn(&self, task: (Duration, Timer<Actor>)) {
        self.send
            .send(task)
            .expect("Thread with LocalSet has shut down.");
    }
}

fn handle_timer(
    foca: &mut Foca,
    runtime: &mut DispatchRuntime,
    timer_rx: &mut mpsc::Receiver<(Timer<Actor>, Instant)>,
    timer: Timer<Actor>,
    seq: Instant,
) {
    let mut v = vec![(timer, seq)];

    // drain the channel, in case there's a race among timers
    while let Ok((timer, seq)) = timer_rx.try_recv() {
        v.push((timer, seq));
    }

    // sort by instant these were scheduled
    v.sort_by_key(|a| a.1);

    for (timer, _) in v {
        let Err(error) = foca.handle_timer(timer, &mut *runtime) else {
            continue;
        };

        tracing::error!(%error, "foca: error handling timer");
    }
}

fn diff_member_states(
    ctx: &SwimCtx,
    foca: &Foca,
    last_states: &mut Vec<(foca::Member<Actor>, Option<u64>)>,
    notifications_tx: &mpsc::Sender<foca::OwnedNotification<Actor>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut foca_states = Vec::<&foca::Member<Actor>>::new();
    for member in foca.iter_membership_state() {
        let Some(existing) = foca_states
            .iter_mut()
            .find(|fs| fs.id().id() == member.id().id())
        else {
            foca_states.push(member);
            continue;
        };

        if existing.id().ts() < member.id().ts() {
            *existing = member;
        }
    }

    let (to_delete, to_update, foca_notifications) = {
        let members = ctx.members.0.read();

        let to_update = foca_states
            .iter()
            .filter_map(|member| {
                let member = *member;

                if member.id().member_id() != ctx.member_id {
                    return None;
                }

                let rtt = members
                    .rtts
                    .get(&member.id().addr())
                    .and_then(|rtts| rtts.buf.iter().min().copied());

                let Some(prev) = last_states
                    .iter_mut()
                    .find(|ls| ls.0.id().id() == member.id().id())
                else {
                    last_states.push((member.clone(), rtt));
                    return Some((member.clone(), rtt));
                };

                (prev.0 != *member || prev.1 != rtt).then(|| {
                    last_states.push((member.clone(), rtt));
                    (member.clone(), rtt)
                })
            })
            .collect::<Vec<_>>();

        let mut to_delete = vec![];

        last_states.retain(|(mem, _)| {
            if let Some(foca_state) = foca_states.iter().find(|fs| fs.id().id() == mem.id().id())
                && foca_state.id().member_id() == ctx.member_id
            {
                true
            } else {
                to_delete.push(mem.id().id());
                false
            }
        });

        let mut foca_notifications = vec![];
        foca_notifications.extend(foca_states.iter().filter_map(|fs| {
            let member = *fs;
            (foca_state_is_active(&member.state()) && members.get(&member.id().id()).is_none())
                .then(|| foca::OwnedNotification::MemberUp(member.id().clone()))
        }));

        foca_notifications.extend(members.states.iter().filter_map(|(id, member_state)| {
            let Some(fs) = foca_states.iter().find(|fs| fs.id().id() == *id) else {
                return Some(foca::OwnedNotification::MemberDown(
                    member_state.to_actor(*id),
                ));
            };

            (!foca_state_is_active(&fs.state()))
                .then(|| foca::OwnedNotification::MemberDown(fs.id().clone()))
        }));

        (to_delete, to_update, foca_notifications)
    };

    if !foca_notifications.is_empty() {
        tracing::info!(
            count = foca_notifications.len(),
            "Sending out foca notifications for members",
        );
        // best effort update since we'd retry this function.
        for notification in foca_notifications {
            if notifications_tx.try_send(notification).is_err() {
                ctx.metrics
                    .channel_error_inc("full", "dispatch.notifications");
            }
        }
    }

    if to_update.is_empty() && to_delete.is_empty() {
        return None;
    }

    tracing::info!(
        update = to_update.len(),
        delete = to_delete.len(),
        "Scheduling cluster membership state update",
    );

    let updated_at = time::OffsetDateTime::now_utc();

    let pool = ctx.pool.clone();
    Some(tokio::spawn(async move {
        let mut conn = match pool.write_low().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::error!(%error, "could not acquire a r/w conn to process member events");
                return;
            }
        };

        let mut upserted = 0;
        let mut deleted = 0;

        let res = tokio::task::block_in_place(|| {
            let tx = conn.immediate_transaction()?;

            upserted += tx
                .prepare_cached(
                    "
                INSERT INTO __corro_members (actor_id, address, foca_state, rtt_min, updated_at)
                    SELECT                   value0,   value1,  value2,     value3, value4
                    FROM              unnest(?,        ?,       ?,          ?,       ?)
                    -- Otherwise sqlite will think ON CONFLICT is part of a JOIN
                    WHERE true
                    ON CONFLICT (actor_id)
                        DO UPDATE SET
                            foca_state = excluded.foca_state,
                            address = excluded.address,
                            rtt_min = COALESCE(excluded.rtt_min, rtt_min),
                            updated_at = excluded.updated_at
                        WHERE excluded.updated_at > updated_at
            ",
                )?
                .execute(rusqlite::params![
                    unnest_param(to_update.iter().map(|(member, _)| member.id().id())),
                    unnest_param(
                        to_update
                            .iter()
                            .map(|(member, _)| member.id().addr().to_string())
                    ),
                    unnest_param(
                        to_update
                            .iter()
                            .map(|(member, _)| serde_json::to_string(&member).unwrap())
                    ),
                    unnest_param(to_update.iter().map(|(_, rtt_min)| rtt_min)),
                    unnest_param(to_update.iter().map(|_| updated_at)),
                ])?;

            deleted += tx
                .prepare_cached(
                    r#"DELETE FROM __corro_members
                            WHERE actor_id IN (SELECT value0 FROM UNNEST(?))
                            AND updated_at < ?
                        "#,
                )?
                .execute(rusqlite::params![
                    unnest_param(to_delete.iter()),
                    updated_at
                ])?;

            tx.commit()?;

            Ok::<_, rusqlite::Error>(())
        });

        if let Err(error) = res {
            tracing::error!(%error, "could not insert state changes from SWIM cluster into sqlite");
        }

        tracing::info!(upserted, deleted, "membership states changed");
    }))
}

struct PendingBroadcast {
    payload: Bytes,
    is_local: bool,
    /// Corrosion was using a `HashSet` for this, I don't really see this being more than several relays ever at a time so
    /// a Vec will be cheaper in all cases
    sent_to: Vec<SocketAddr>,
    send_count: u8,
}

impl PendingBroadcast {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            is_local: false,
            sent_to: Default::default(),
            send_count: 0,
        }
    }

    pub fn new_local(payload: Bytes) -> Self {
        Self {
            payload,
            is_local: true,
            sent_to: Default::default(),
            send_count: 0,
        }
    }

    #[inline]
    pub fn contains(&self, addr: &SocketAddr) -> bool {
        self.sent_to.iter().any(|s| s == addr)
    }

    #[inline]
    pub fn insert(&mut self, addr: SocketAddr) {
        if !self.contains(&addr) {
            self.sent_to.push(addr);
        }
    }
}

type BroadcastRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::QuantaClock,
    governor::middleware::StateInformationMiddleware,
>;

async fn handle_broadcasts(
    ctx: SwimCtx,
    mut bcast_rx: mpsc::Receiver<bx::BroadcastInput>,
    transport: Transport,
    config: Arc<RwLock<foca::Config>>,
    mut tripwire: tripwire::Tripwire,
) {
    use bytes::BufMut;
    use futures::stream::FusedStream;
    use speedy::Writable;
    use tokio_stream::StreamExt;
    use tokio_util::codec::Encoder;

    // max broadcast size
    let broadcast_cutoff: usize = 64 * 1024;

    let mut bcast_codec = tokio_util::codec::LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();

    let mut bcast_buf = BytesMut::new();
    let mut local_bcast_buf = BytesMut::new();
    let mut single_bcast_buf = BytesMut::new();

    let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

    let mut rng = StdRng::from_os_rng();

    let mut idle_pendings = futures_util::stream::FuturesUnordered::<
        std::pin::Pin<Box<dyn Future<Output = PendingBroadcast> + Send + 'static>>,
    >::new();

    let mut bcast_interval = tokio::time::interval(Duration::from_millis(500));

    enum Branch {
        Broadcast(bx::BroadcastInput),
        BroadcastDeadline,
        WokePendingBroadcast(PendingBroadcast),
        Tripped,
        Metrics,
    }

    let mut tripped = false;
    let mut ser_buf = BytesMut::new();

    const MAX_INFLIGHT_BROADCAST: usize = 500;
    const MAX_QUEUE_LEN: usize = 20_000;

    let mut join_set = tokio::task::JoinSet::new();
    let mut to_broadcast = VecDeque::new();
    let mut to_local_broadcast = VecDeque::new();

    let bytes_per_sec = governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(10 * 1024 * 1024).unwrap(),
    ))
    .with_middleware();

    let mut rate_limited = false;

    loop {
        let branch = tokio::select! {
            biased;
            input = bcast_rx.recv() => {
                let Some(input) = input else {
                    tracing::warn!("no more SWIM inputs");
                    break;
                };

                Branch::Broadcast(input)
            },
            _ = join_set.join_next(), if !join_set.is_empty() => {
                // drains the joinset
                continue;
            },
            _ = bcast_interval.tick() => {
                Branch::BroadcastDeadline
            },
            maybe_woke = idle_pendings.next(), if !idle_pendings.is_terminated() => {
                let Some(woke) = maybe_woke else {
                    continue;
                };

                Branch::WokePendingBroadcast(woke)
            },
            _ = &mut tripwire, if !tripped => {
                tripped = true;
                Branch::Tripped
            },
            _ = metrics_interval.tick() => {
                Branch::Metrics
            }
        };

        match branch {
            Branch::Tripped => {
                break;
            }
            Branch::BroadcastDeadline => {
                if !bcast_buf.is_empty() {
                    to_broadcast.push_front(PendingBroadcast::new(bcast_buf.split().freeze()));
                }
                if !local_bcast_buf.is_empty() {
                    to_broadcast.push_front(PendingBroadcast::new_local(
                        local_bcast_buf.split().freeze(),
                    ));
                }
            }
            Branch::Broadcast(input) => {
                let (bcast, is_local) = match input {
                    bx::BroadcastInput::Rebroadcast(bcast) => (bcast, false),
                    bx::BroadcastInput::AddBroadcast(bcast) => (bcast, true),
                };

                if let Err(error) = (bx::UniPayload::V1 {
                    data: bx::UniPayloadV1::Broadcast(bcast.clone()),
                    cluster_id: ctx.cluster_id,
                })
                .write_to_stream((&mut ser_buf).writer())
                {
                    tracing::error!(%error, "could not encode UniPayload::Broadcast");
                    ser_buf.clear();
                    continue;
                }

                if is_local {
                    if let Err(error) =
                        bcast_codec.encode(ser_buf.split().freeze(), &mut single_bcast_buf)
                    {
                        tracing::error!(%error, "could not encode local broadcast");
                        single_bcast_buf.clear();
                        continue;
                    }

                    let payload = single_bcast_buf.split().freeze();

                    local_bcast_buf.extend_from_slice(&payload);

                    to_local_broadcast.push_front(payload);

                    if local_bcast_buf.len() >= broadcast_cutoff {
                        to_broadcast.push_front(PendingBroadcast::new_local(
                            local_bcast_buf.split().freeze(),
                        ));
                    }
                } else {
                    if let Err(error) = bcast_codec.encode(ser_buf.split().freeze(), &mut bcast_buf)
                    {
                        tracing::error!(%error, "could not encode broadcast");
                        bcast_buf.clear();
                        continue;
                    }

                    if bcast_buf.len() >= broadcast_cutoff {
                        to_broadcast.push_front(PendingBroadcast::new(bcast_buf.split().freeze()));
                    }
                }
            }
            Branch::WokePendingBroadcast(pending) => {
                to_broadcast.push_front(pending);
            }
            Branch::Metrics => {
                // gauge!("corro.broadcast.pending.count").set(idle_pendings.len() as f64);
                // gauge!("corro.broadcast.processing.jobs").set(join_set.len() as f64);
                // gauge!("corro.broadcast.buffer.capacity").set(bcast_buf.capacity() as f64);
                // gauge!("corro.broadcast.serialization.buffer.capacity")
                //     .set(ser_buf.capacity() as f64);
            }
        }

        let prev_rate_limited = std::mem::take(&mut rate_limited);

        // start with local broadcasts, they're higher priority
        let mut ring0 = std::collections::HashSet::new();
        while !to_local_broadcast.is_empty() && join_set.len() < MAX_INFLIGHT_BROADCAST {
            // UNWRAP: we just checked that it wasn't empty
            let payload = to_local_broadcast.pop_front().unwrap();

            let members = ctx.members.0.read();
            let mut spawn_count = 0;
            let mut ring0_count = 0;
            for addr in members.ring0(ctx.cluster_id) {
                if join_set.len() >= MAX_INFLIGHT_BROADCAST {
                    tracing::debug!(
                        MAX_INFLIGHT_BROADCAST,
                        "breaking, max inflight broadcast reached",
                    );
                    break;
                }
                ring0_count += 1;
                ring0.insert(addr);

                match try_transmit_broadcast(
                    &bytes_per_sec,
                    payload.clone(),
                    transport.clone(),
                    addr,
                    ctx.metrics,
                ) {
                    Err(error) => {
                        match error {
                            TransmitError::TooBig(_) | TransmitError::InsufficientCapacity(_) => {
                                // not sure this would ever happen
                                tracing::error!(%error, "could not spawn broadcast transmission");
                            }
                            TransmitError::QuotaExceeded(_) => {
                                // exceeded our quota, stop trying to send this through
                                rate_limited = true;
                                ctx.metrics.broadcast_rate_limited_inc();
                                break;
                            }
                        }
                    }
                    Ok(fut) => {
                        join_set.spawn(fut);
                        spawn_count += 1;
                    }
                }
            }

            // couldn't send it anywhere!
            if rate_limited && spawn_count == 0 && ring0_count > 0 {
                // push it back in front since this got nowhere and it's still the
                // freshest item we have in the queue
                to_local_broadcast.push_front(payload);
                break;
            }

            ctx.metrics.broadcast_spawned_inc("local", spawn_count);
        }

        if !rate_limited && !to_broadcast.is_empty() && join_set.len() < MAX_INFLIGHT_BROADCAST {
            let (members_count, ring0_count) = {
                let members = ctx.members.0.read();
                let members_count = members.states.len();
                let ring0_count = members.ring0(ctx.cluster_id).count();
                (members_count, ring0_count)
            };

            let (choose_count, max_transmissions) = {
                let config = config.read();
                let max_transmissions = config.max_transmissions.get();
                let dynamic_count =
                    (members_count - ring0_count) / (max_transmissions as usize * 10);
                let count = config.num_indirect_probes.get().max(dynamic_count);

                if prev_rate_limited {
                    // we've been rate limited on the last loop, try sending to less nodes...
                    (count.min(dynamic_count / 2), max_transmissions / 2)
                } else {
                    (count, max_transmissions)
                }
            };

            while !to_broadcast.is_empty() && join_set.len() < MAX_INFLIGHT_BROADCAST {
                let mut pending = to_broadcast.pop_front().unwrap();

                let broadcast_to = {
                    ctx.members
                        .0
                        .read()
                        .states
                        .iter()
                        .filter_map(|(member_id, state)| {
                            // don't broadcast to ourselves... or ring0 if local broadcast
                            // (ring0 could have changed since the time we sent the local broadcast
                            // so we check the ring0 variable that's created at start of local_broacast
                            // instead of state.is_ring0())
                            if *member_id == ctx.actor_id
                                || state.cluster_id != ctx.cluster_id
                                || (pending.is_local && ring0.contains(&state.addr))
                                || pending.contains(&state.addr)
                            // don't broadcast to this peer
                            {
                                None
                            } else {
                                Some(state.addr)
                            }
                        })
                        .choose_multiple(
                            &mut rng,
                            // prevent going over max count
                            choose_count.min(MAX_INFLIGHT_BROADCAST.saturating_sub(join_set.len())),
                        )
                };

                let mut spawn_count = 0;

                for addr in broadcast_to {
                    match try_transmit_broadcast(
                        &bytes_per_sec,
                        pending.payload.clone(),
                        transport.clone(),
                        addr,
                        ctx.metrics,
                    ) {
                        Err(error) => {
                            tracing::warn!(%error, "could not spawn broadcast transmission");

                            match error {
                                TransmitError::TooBig(_)
                                | TransmitError::InsufficientCapacity(_) => {
                                    // not sure this would ever happen
                                }
                                TransmitError::QuotaExceeded(_) => {
                                    // exceeded our quota, stop trying to send this through
                                    ctx.metrics.broadcast_rate_limited_inc();
                                    break;
                                }
                            }
                        }
                        Ok(fut) => {
                            join_set.spawn(fut);
                            pending.insert(addr);
                            spawn_count += 1;
                        }
                    }
                }

                ctx.metrics.broadcast_spawned_inc("global", spawn_count);

                let Some(send_count) = pending.send_count.checked_add(1) else {
                    continue;
                };

                pending.send_count = send_count;

                if send_count >= max_transmissions {
                    continue;
                }

                idle_pendings.push(Box::pin(async move {
                    // slow our send pace if we've been previously rate limited
                    let sleep_ms_base = if prev_rate_limited { 500 } else { 100 };
                    // send with increasing latency as we've already sent the updates out
                    tokio::time::sleep(Duration::from_millis(sleep_ms_base * send_count as u64))
                        .await;
                    pending
                }));
            }
        }

        if drop_oldest_broadcast(&mut to_broadcast, &mut to_local_broadcast, MAX_QUEUE_LEN)
            .is_some()
        {
            ctx.metrics.broadcast_dropped_inc();
        }
    }

    tracing::info!("SWIM broadcasts are done");
}

#[inline]
fn foca_state_is_active(state: &foca::State) -> bool {
    matches!(*state, foca::State::Alive | foca::State::Suspect)
}

#[derive(Debug, thiserror::Error)]
enum TransmitError {
    #[error("payload > u32::MAX: {0}")]
    TooBig(usize),
    #[error(transparent)]
    InsufficientCapacity(#[from] governor::InsufficientCapacity),
    #[error("{0}")]
    QuotaExceeded(governor::NotUntil<governor::clock::QuantaInstant>),
}

fn try_transmit_broadcast(
    bytes_per_sec: &BroadcastRateLimiter,
    payload: Bytes,
    transport: Transport,
    addr: SocketAddr,
    metrics: &'static GossipMetrics,
) -> Result<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>, TransmitError> {
    let len = payload.len();

    let Some(len_u32) = len.try_into().ok().and_then(std::num::NonZeroU32::new) else {
        return Err(TransmitError::TooBig(len));
    };

    match bytes_per_sec.check_n(len_u32) {
        Ok(Ok(state)) => {
            metrics.broadcast_remaining_burst(state.remaining_burst_capacity());
        }
        Ok(Err(e)) => return Err(TransmitError::QuotaExceeded(e)),
        Err(e) => return Err(e.into()),
    }

    Ok(Box::pin(async move {
        match tokio::time::timeout(Duration::from_secs(5), transport.send_uni(addr, payload)).await
        {
            Err(_e) => {
                tracing::warn!(%addr, "timed out writing broadcast to uni broadcast stream");
            }
            Ok(Err(error)) => {
                tracing::error!(%error, %addr, "could not write to uni broadcast stream");
            }
            Ok(Ok(_)) => {
                metrics.stream_bytes_inc(
                    len as _,
                    quinn::Dir::Uni,
                    super::metrics::Direction::Out,
                    super::transport::TrafficClass::Broadcast,
                );
            }
        }
    }))
}

// Drop the oldest, most sent item or the oldest local item
fn drop_oldest_broadcast(
    queue: &mut VecDeque<PendingBroadcast>,
    local_queue: &mut VecDeque<Bytes>,
    max: usize,
) -> Option<PendingBroadcast> {
    if queue.len() + local_queue.len() <= max {
        return None;
    }

    // start by dropping from global queue
    if let Some((i, _)) = queue
        .iter()
        .enumerate()
        .max_by_key(|(_, val)| val.send_count)
    {
        queue.remove(i)
    } else {
        local_queue.pop_back().map(PendingBroadcast::new_local)
    }
}

/// A central dispatcher for SWIM cluster management messages
///
/// TODO: This is just a copy of what corrosion does, but IMO should probably just be in the main loop
pub fn spawn_sender(
    transport: Transport,
    mut to_send_rx: mpsc::Receiver<(Actor, Bytes)>,
    mut tripwire: tripwire::Tripwire,
) {
    spawn::spawn_counted(async move {
        while let tripwire::Outcome::Completed(Some((actor, data))) =
            to_send_rx.recv().preemptible(&mut tripwire).await
        {
            let Err(error) = transport.send_datagram(actor.addr(), data).await else {
                continue;
            };

            tracing::error!(%error, address = %actor.addr(), "could not write datagram");
        }

        if tripwire.is_shutting_down() {
            // Drain the queue
            let mut total = 0;
            let mut errors = 0;

            let start = Instant::now();

            while let Ok((actor, data)) = to_send_rx.try_recv() {
                let res = transport.send_datagram(actor.addr(), data).await;

                total += 1;
                errors += if res.is_err() { 1 } else { 0 };
            }

            tracing::debug!(elapsed = ?start.elapsed(), total, errors, "drained SWIM datagrams before shutting down");
        }
    });
}

/// Poll for updates from the cluster membership system (`foca`/ SWIM) and apply any incoming changes to the local
/// actor/agent state.
pub fn spawn_notification_handler(
    metrics: &'static GossipMetrics,
    members: super::Members,
    mut notification_rx: mpsc::Receiver<foca::OwnedNotification<Actor>>,
    foca_tx: mpsc::Sender<FocaInput>,
    mut tripwire: tripwire::Tripwire,
) {
    use corro_types::members::MemberAddedResult;
    use foca::OwnedNotification as ON;

    spawn::spawn_counted(async move {
        while let tripwire::Outcome::Completed(Some(notification)) =
            notification_rx.recv().preemptible(&mut tripwire).await
        {
            let kind = match notification {
                ON::MemberUp(actor) => {
                    let res = members.0.write().add_member(&actor);
                    tracing::info!(?actor, member_added_result = ?res, "Member Up");

                    match res {
                        MemberAddedResult::NewMember | MemberAddedResult::Removed => {
                            if matches!(res, MemberAddedResult::Removed) {
                                metrics.member_removed_inc("member_id_mismatch");
                            } else {
                                metrics.member_added_inc();
                            }

                            let members_len = { members.0.read().states.len() as u32 };

                            // actually added a member
                            // notify of new cluster size
                            if let Ok(size) = members_len.try_into()
                                && let Err(error) = foca_tx.send(FocaInput::ClusterSize(size)).await
                            {
                                tracing::error!(%error, "could not send new foca cluster size");
                            }
                        }
                        MemberAddedResult::Updated | MemberAddedResult::Ignored => {
                            // anything else to do here?
                        }
                    }

                    "memberup"
                }
                ON::MemberDown(actor) => {
                    let removed = { members.0.write().remove_member(&actor) };
                    tracing::info!(?actor, removed, "Member Down");

                    if removed {
                        metrics.member_removed_inc("member_down");

                        // actually removed a member
                        // notify of new cluster size
                        let member_len = { members.0.read().states.len() as u32 };
                        if let Ok(size) = member_len.try_into()
                            && let Err(error) = foca_tx.send(FocaInput::ClusterSize(size)).await
                        {
                            tracing::error!(%error, "could not send new foca cluster size");
                        }
                    }

                    "memberdown"
                }
                ON::Rename(a, b) => {
                    let mut lock = members.0.write();
                    lock.remove_member(&a);
                    lock.add_member(&b);

                    tracing::info!(old = ?a, new = ?b, "Member Rename");

                    "rename"
                }
                ON::Active => {
                    tracing::info!("Current node is considered ACTIVE");
                    "active"
                }
                ON::Idle => {
                    tracing::warn!("Current node is considered IDLE");
                    "idle"
                }
                // this happens when we leave the cluster
                ON::Defunct => {
                    tracing::debug!("Current node is considered DEFUNCT");
                    "defunct"
                }
                ON::Rejoin(id) => {
                    tracing::info!(?id, "Rejoined the cluster");
                    "rejoin"
                }
            };

            metrics.swim_notification_inc(kind);
        }
    });
}

/// Load the existing known member state and addresses
pub async fn load_member_states(
    pool: &corro_types::agent::SplitPool,
) -> Vec<(std::net::SocketAddr, foca::Member<Actor>)> {
    let conn = match pool.read().await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!(%error, "could not acquire conn for foca member states");
            return Vec::new();
        }
    };

    use eyre::WrapErr;

    let res = tokio::task::block_in_place(
        || -> eyre::Result<Vec<(std::net::SocketAddr, foca::Member<Actor>)>> {
            let mut prepped = conn
                .prepare("SELECT address,foca_state FROM __corro_members")
                .context("could not prepare query")?;
            let members = prepped
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?.parse().map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?,
                        row.get::<_, String>(1)?,
                    ))
                })
                .and_then(|rows| {
                    rows.collect::<rusqlite::Result<Vec<(std::net::SocketAddr, String)>>>()
                })
                .context("could not query")?;

            Ok(members.into_iter().filter_map(|(address, state)| match serde_json::from_str::<foca::Member<Actor>>(state.as_str()) {
            Ok(fs) => Some((address, fs)),
            Err(error) => {
                tracing::error!(%error, state, "could not deserialize foca member state");
                None
            }
        }).collect::<Vec<(std::net::SocketAddr, foca::Member<Actor>)>>())
        },
    );

    res.inspect_err(|error| {
        tracing::error!(%error, "unable to read foca member state");
    })
    .unwrap_or_default()
}
