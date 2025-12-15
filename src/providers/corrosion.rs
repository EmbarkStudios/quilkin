use eyre::{Context, ContextCompat as _};
use std::{
    net,
    sync::{Arc, atomic},
    time::Duration,
};
use tracing_futures::Instrument as _;

type CorrosionAddrs = Vec<std::net::SocketAddr>;
type HealthCheck = Arc<atomic::AtomicBool>;
type State = Arc<crate::config::Config>;

impl super::Providers {
    fn maybe_spawn_corrosion(
        &self,
        config: &State,
        health_check: &HealthCheck,
    ) -> crate::Result<()> {
        if !self.corrosion_enabled() {
            return Ok(());
        }

        // TODO: We currently cheat to determine if we are pushing or pulling changes
        // since we'll be running this in parallel with xDs for some amount of time,
        // but will need to change when we rip out xDs at some point
        assert!(self.grpc_pull_enabled() || self.grpc_push_enabled());

        let config = config.clone();
        let health_check = health_check.clone();
        let endpoints = self.endpoints.clone();

        if self.grpc_pull_enabled() {
            // We're a proxy, getting changes from a remote relay
            Self::task(
                "corrosion_provider".into(),
                health_check.clone(),
                move || {
                    let config = config.clone();
                    let endpoints = endpoints.clone();
                    let health_check = health_check.clone();
                    let tx = notifier.clone();

                    async move {
                        let identifier = config.id();
                        let stream = crate::net::xds::delta_subscribe(
                            config,
                            identifier,
                            endpoints,
                            health_check.clone(),
                            tx,
                            Self::SUBS,
                        )
                        .await
                        .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                        health_check.store(true, Ordering::SeqCst);

                        stream.await.wrap_err("join handle error")?
                    }
                },
            )
        } else {
            // We're an agent, pushing changes to a remote relay
            Self::task(
                "corrosion_provider".into(),
                health_check.clone(),
                move || {
                    let config = config.clone();
                    let endpoints = endpoints.clone();
                    let health_check = health_check.clone();
                    let tx = notifier.clone();

                    async move {
                        let identifier = config.id();
                        let stream = crate::net::xds::delta_subscribe(
                            config,
                            identifier,
                            endpoints,
                            health_check.clone(),
                            tx,
                            Self::SUBS,
                        )
                        .await
                        .map_err(|_err| eyre::eyre!("failed to acquire delta stream"))?;

                        health_check.store(true, Ordering::SeqCst);

                        stream.await.wrap_err("join handle error")?
                    }
                },
            )
        }
    }
}

use corrosion::{
    db::read::FromSqlValue,
    persistent::client,
    pubsub::{self, SubParamsv1 as SubParams},
};

const BACKOFF_INITIAL_DELAY: Duration = Duration::from_millis(500);
const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(30);
const BACKOFF_MAX_JITTER: Duration = Duration::from_secs(2);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

struct Sub {
    client: client::SubscriptionClient,
    stream: client::SubClientStream,
}

struct SubState {
    /// The root client, we need to keep this alive as long as we have
    /// subscriptions that use it
    client: client::Client,
    /// Subscription to servers
    servers: Sub,
    /// Subscription to cluster agents
    clusters: Sub,
    /// Subscription to the filter config
    filter: Sub,
}

#[derive(Copy, Clone)]
#[repr(usize)]
enum Which {
    Servers,
    Clusters,
    Filter,
}

impl std::fmt::Display for Which {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Servers => "servers",
            Self::Clusters => "clusters",
            Self::Filter => "filter",
        };

        f.write_str(s)
    }
}

struct QuerySet<T> {
    set: [Option<T>; 3],
}

impl<T> QuerySet<T> {
    fn new() -> Self {
        Self {
            set: [None, None, None],
        }
    }

    fn assume_initialized(self) -> (T, T, T) {
        let (a, b, c) = self.set.into();
        (a.unwrap(), b.unwrap(), c.unwrap())
    }
}

impl<T> std::ops::Index<Which> for QuerySet<T> {
    type Output = Option<T>;

    #[inline]
    fn index(&self, index: Which) -> &Self::Output {
        &self.set[index as usize]
    }
}

impl<T> std::ops::IndexMut<Which> for QuerySet<T> {
    fn index_mut(&mut self, index: Which) -> &mut Self::Output {
        &mut self.set[index as usize]
    }
}

type ChangeIds = QuerySet<corrosion::pubsub::ChangeId>;

/// Attempts to connect to and subscribe to the queries to keep this proxy up to
/// date with cluster status
async fn connect_and_sub(addr: net::SocketAddr, change_ids: &ChangeIds) -> crate::Result<SubState> {
    let root = client::Client::connect_insecure(addr)
        .await
        .context("failed to connect")?;

    let mut js = tokio::task::JoinSet::new();
    js.spawn({
        let root = root.clone();
        let from = change_ids[Which::Servers];

        async move {
            let mut sp = SubParams::new(pubsub::SERVER_QUERY);
            sp.from = from;
            (
                client::SubscriptionClient::connect(root, sp).await,
                Which::Servers,
            )
        }
    });
    js.spawn({
        let root = root.clone();
        let from = change_ids[Which::Servers];

        async move {
            let mut sp = SubParams::new(pubsub::DC_QUERY);
            sp.from = from;
            (
                client::SubscriptionClient::connect(root, sp).await,
                Which::Clusters,
            )
        }
    });
    js.spawn({
        let root = root.clone();
        let from = change_ids[Which::Servers];

        async move {
            let mut sp = SubParams::new(pubsub::FILTER_QUERY);
            sp.from = from;
            (
                client::SubscriptionClient::connect(root, sp).await,
                Which::Filter,
            )
        }
    });

    let mut sub_set = QuerySet::new();

    while let Some(result) = js.join_next().await {
        let (result, which) = result.wrap_err("failed to join subscribe task")?;
        let (client, stream) =
            result.with_context(|| format!("failed to subscribe to '{which}' query"))?;
        sub_set[which] = Some(Sub { client, stream });
    }

    let (servers, clusters, filter) = sub_set.assume_initialized();

    Ok(SubState {
        client: root,
        servers,
        clusters,
        filter,
    })
}

use rand::Rng;
use tryhard::backoff_strategies::{BackoffStrategy as _, ExponentialBackoff};

async fn corrosion_subscribe(
    state: State,
    endpoints: CorrosionAddrs,
    hc: HealthCheck,
) -> crate::Result<()> {
    let mut backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);

    // Each query keeps track of the latest change id it has received, if we
    // disconnect from a remote server, we can send this when subscribing to
    // (hopefully) be able to catch up to the state of that server more quickly
    let mut change_ids = QuerySet::new();

    let retry_config =
        tryhard::RetryFutureConfig::new(u32::MAX).custom_backoff(|attempt, error: &_| {
            tracing::info!(attempt, "Retrying to connect");
            // reset after success
            if attempt <= 1 {
                backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);
            }

            let mut delay = backoff.delay(attempt, &error).min(BACKOFF_MAX_DELAY);
            delay += Duration::from_millis(
                rand::rng().random_range(0..BACKOFF_MAX_JITTER.as_millis() as _),
            );

            tracing::warn!(?error, "Unable to connect to the corrosion server");
            tryhard::RetryPolicy::Delay(delay)
        });

    let connect_to_corrosion = tryhard::retry_fn(|| {
        tracing::info!(
            server_count = endpoints.len(),
            "attempting to connect to corrosion server"
        );

        // Attempt to connect to multiple servers in parallel, otherwise
        // down/slow servers in the list can unneccessarily delay connections
        // to healthy servers.
        //
        // Currently we just go with the first server that we can successfully
        // connect and subscribe to, but in the future we could subscribe
        // to multiple servers simultaneously 
        let mut js = tokio::task::JoinSet::new();

        for addr in endpoints.iter().cloned() {
            js.spawn(async move {
                let res = connect_and_sub(addr, &change_ids)
                    .instrument(tracing::debug_span!("connect_and_sub"))
                    .await;

                (res, addr)
            });
        }

        async move {
            match tokio::time::timeout(CONNECTION_TIMEOUT, async {
                while let Some(join_result) = js.join_next().await {
                    match join_result {
                        Ok((result, addr)) => {
                            match result {
                                Ok(sub_state) => {
                                    return Ok((sub_state, addr));
                                }
                                Err(error) => {
                                    tracing::warn!(address = %addr, %error, "failed to connect");
                                }
                            }
                        }
                        Err(join_error) => {
                            if join_error.is_panic() {
                                tracing::error!(
                                    ?join_error,
                                    "panic occurred in task attempting to connect to xDS endpoint"
                                );
                            }
                        }
                    }
                }

                eyre::bail!("no successful connections could be made to {} possible corrosion servers", endpoints.len());
            })
            .await
            {
                Ok(Ok(cae)) => Ok(cae),
                Ok(Err(err)) => Err(err),
                Err(_) => eyre::bail!("timed out after {CONNECTION_TIMEOUT:?} attempting to connect to one of {} possible corrosion servers", endpoints.len()),
            }
        }
    })
    .with_config(retry_config);

    loop {
        let (sstate, address) = match connect_to_corrosion
            .instrument(tracing::trace_span!("corrosion_subscribe"))
            .await
        {
            Ok(c) => c,
            Err(error) => {
                tracing::warn!(%error, "unable to subscribe to a corrosion server");
                continue;
            }
        };

        tracing::info!(%address, "successfully subscribed to corrosion server");
        hc.store(true, atomic::Ordering::Relaxed);

        process_subscription_events(&state, sstate, &mut change_ids)
            .await
            .instrument(tracing::debug_span!("corrosion subscription events", %address));

        hc.store(false, atomic::Ordering::Relaxed);
    }
}

async fn process_subscription_events(
    state: &State,
    mut sstate: SubState,
    change_ids: &mut ChangeIds,
) -> crate::Result<()> {
    use corrosion::db::read as db;
    use pubsub::{ChangeType, QueryEvent};

    let process_server_event = |event: Option<QueryEvent>| -> crate::Result<()> {
        let event = event.context("subscription was closed")?;
        let Some(servers) = state.dyn_cfg.servers() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        match event {
            // The state of row that matches our query changed
            QueryEvent::Change(ct, _rid, row, cid) => {
                let server = db::ServerRow::from_sql(&row)?;

                match ct {
                    ChangeType::Insert | ChangeType::Update => {
                        servers.upsert(server.endpoint, server.icao, server.tokens);
                    }
                    ChangeType::Delete => {
                        if let Some((micao, mtoken_set)) = servers.remove(&server.endpoint) {
                            let diffs = server.tokens.0.symmetric_difference(&mtoken_set.0).count();
                            if server.icao != micao || diffs != 0 {
                                tracing::warn!(endpoint = %server.endpoint, our_icao = %micao, remote_icao = %server.icao, token_set_diffs = diffs, "received removal event for endpoint which differed from the local version");
                            }
                        } else {
                            tracing::warn!(endpoint = %server.endpoint, "received removal event for endpoint not in local server set");
                        }
                    }
                }

                change_ids[Which::Servers] = Some(cid);
            }
            // The state of a row in the initial query
            QueryEvent::Row(_rid, row) => {
                let server = db::ServerRow::from_sql(&row)?;
                servers.upsert(server.endpoint, server.icao, server.tokens);
            }
            QueryEvent::Error(error) => {}
            // Marks the end of the initial query to catch us up to the current state of the server
            QueryEvent::EndOfQuery { time, change_id } => {
                tracing::debug!(elapsed = ?Duration::from_secs_f64(time), ?change_id, "received initial state of 'servers'");
                change_ids[Which::Servers] = change_id;
            }
            QueryEvent::Columns(_) => {
                // irrelevant
            }
        }

        Ok(())
    };

    let process_cluster_event = |event: Option<QueryEvent>| -> crate::Result<()> {
        let event = event.context("subscription was closed")?;
        let Some(dcs) = state.dyn_cfg.datacenters() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        use crate::config::Datacenter;

        match event {
            // The state of row that matches our query changed
            QueryEvent::Change(ct, _rid, row, cid) => {
                let dc = db::DatacenterRow::from_sql(&row)?;

                match ct {
                    ChangeType::Insert | ChangeType::Update => {
                        dcs.modify(|dcs| {
                            dcs.insert(
                                dc.ip,
                                Datacenter {
                                    qcmp_port: dc.qcmp_port,
                                    icao_code: dc.icao,
                                },
                            );
                        });
                    }
                    ChangeType::Delete => {
                        dcs.modify(|dcs| {
                            dcs.remove(dc.ip);
                        });
                    }
                }

                change_ids[Which::Clusters] = Some(cid);
            }
            // The state of a row in the initial query
            QueryEvent::Row(_rid, row) => {
                let dc = db::DatacenterRow::from_sql(&row)?;
                dcs.modify(|dcs| {
                    dcs.insert(
                        dc.ip,
                        Datacenter {
                            qcmp_port: dc.qcmp_port,
                            icao_code: dc.icao,
                        },
                    );
                });
            }
            QueryEvent::Error(error) => {}
            // Marks the end of the initial query to catch us up to the current state of the server
            QueryEvent::EndOfQuery { time, change_id } => {
                tracing::debug!(elapsed = ?Duration::from_secs_f64(time), ?change_id, "received initial state of 'servers'");
                change_ids[Which::Clusters] = change_id;
            }
            QueryEvent::Columns(_) => {
                // irrelevant
            }
        }

        Ok(())
    };

    loop {
        tokio::select! {
            sc = sstate.servers.stream.rx.recv() => {
                process_server_event(sc).context("processing 'servers' event")?;
            }
            dc = sstate.clusters.stream.rx.recv() => {
                process_cluster_event(dc).context("processing 'clusters' event")?;
            }
            fc = sstate.filter.stream.rx.recv() => {
                process_filter_event(fc).context("processing 'filter' event")?;
            }
        }
    }
}
