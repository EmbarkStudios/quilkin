use super::*;

use rand::Rng;
use tryhard::backoff_strategies::{BackoffStrategy as _, ExponentialBackoff};

const BACKOFF_INITIAL_DELAY: Duration = Duration::from_millis(500);
const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(30);
const BACKOFF_MAX_JITTER: Duration = Duration::from_secs(2);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

struct Sub {
    #[allow(unused)]
    client: client::SubscriptionClient,
    stream: client::SubClientStream,
}

struct SubState {
    /// The root client, we need to keep this alive as long as we have
    /// subscriptions that use it
    #[allow(unused)]
    client: client::Client,
    /// Subscription to servers
    servers: Sub,
    /// Subscription to cluster agents
    clusters: Sub,
    /// Subscription to the filter config
    filter: Sub,
}

pub(super) async fn corrosion_subscribe(
    state: State,
    endpoints: CorrosionAddrs,
    hc: HealthCheck,
) -> crate::Result<()> {
    let mut backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);

    // Each query keeps track of the latest change id it has received, if we
    // disconnect from a remote server, we can send this when subscribing to
    // (hopefully) be able to catch up to the state of that server more quickly
    let mut change_ids = QuerySet::new();

    loop {
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
                let cids = change_ids.clone();
                js.spawn(async move {
                    let res = connect_and_sub(addr, &cids)
                        .instrument(tracing::debug_span!("connect_and_sub"))
                        .await;

                    (res, addr)
                });
            }

            let num_endpoints = endpoints.len();

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

                    eyre::bail!("no successful connections could be made to {num_endpoints} possible corrosion servers");
                })
                .await
                {
                    Ok(Ok(cae)) => Ok(cae),
                    Ok(Err(err)) => Err(err),
                    Err(_) => eyre::bail!("timed out after {CONNECTION_TIMEOUT:?} attempting to connect to one of {num_endpoints} possible corrosion servers"),
                }
            }
        })
        .with_config(retry_config);

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

/// The actual core of the event loop, applies the events from the authoratative
/// server to reflect its state locally
async fn process_subscription_events(
    state: &State,
    mut sstate: SubState,
    change_ids: &mut ChangeIds,
) -> crate::Result<()> {
    use corrosion::db::read as db;
    use pubsub::{ChangeType, QueryEvent};

    let process_server_event = |event: Option<QueryEvent>,
                                cid: &mut Option<ChangeId>|
     -> crate::Result<()> {
        let event = event.context("subscription was closed")?;
        let Some(servers) = state.dyn_cfg.servers() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        match event {
            // The state of row that matches our query changed
            QueryEvent::Change(ct, _rid, row, id) => {
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

                *cid = Some(id);
            }
            // The state of a row in the initial query
            QueryEvent::Row(_rid, row) => {
                let server = db::ServerRow::from_sql(&row)?;
                servers.upsert(server.endpoint, server.icao, server.tokens);
            }
            QueryEvent::Error(error) => {
                tracing::error!(%error, "error from 'servers' subscription");
            }
            // Marks the end of the initial query to catch us up to the current state of the server
            QueryEvent::EndOfQuery { time, change_id } => {
                tracing::debug!(elapsed = ?Duration::from_secs_f64(time), ?change_id, "received initial state of 'servers'");
                *cid = change_id;
            }
            QueryEvent::Columns(_) => {
                // irrelevant
            }
        }

        Ok(())
    };

    let process_cluster_event = |event: Option<QueryEvent>,
                                 cid: &mut Option<ChangeId>|
     -> crate::Result<()> {
        let event = event.context("subscription was closed")?;
        let Some(dcs) = state.dyn_cfg.datacenters() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        use crate::config::Datacenter;

        match event {
            // The state of row that matches our query changed
            QueryEvent::Change(ct, _rid, row, id) => {
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

                *cid = Some(id);
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
            QueryEvent::Error(error) => {
                tracing::error!(%error, "error from 'clusters' subscription");
            }
            // Marks the end of the initial query to catch us up to the current state of the server
            QueryEvent::EndOfQuery { time, change_id } => {
                tracing::debug!(elapsed = ?Duration::from_secs_f64(time), ?change_id, "received initial state of 'clusters'");
                *cid = change_id;
            }
            QueryEvent::Columns(_) => {
                // irrelevant
            }
        }

        Ok(())
    };

    let process_filter_event = |event: Option<QueryEvent>,
                                cid: &mut Option<ChangeId>|
     -> crate::Result<()> {
        let event = event.context("subscription was closed")?;
        let Some(fcf) = state.dyn_cfg.filters() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        let update_filter = |row: &[SqliteValue]| -> crate::Result<()> {
            let column = row.get(0).context("missing 'filter' column")?;

            let filter = column.as_str().with_context(|| {
                format!(
                    "'filter' column is {:?}, not a string",
                    column.column_type()
                )
            })?;

            fcf.store(serde_json::from_str(filter).context("failed to deserialize filter")?);
            Ok(())
        };

        match event {
            // The state of row that matches our query changed
            QueryEvent::Change(ct, _rid, row, id) => {
                match ct {
                    ChangeType::Insert | ChangeType::Update => {
                        update_filter(&row)?;
                    }
                    ChangeType::Delete => {
                        // TODO: what do we actually want to do here?
                        tracing::warn!("ignoring `filter` deletion event");
                    }
                }

                *cid = Some(id);
            }
            // The state of a row in the initial query
            QueryEvent::Row(_rid, row) => {
                update_filter(&row)?;
            }
            QueryEvent::Error(error) => {
                tracing::error!(%error, "error from 'filter' subscription");
            }
            // Marks the end of the initial query to catch us up to the current state of the server
            QueryEvent::EndOfQuery { time, change_id } => {
                tracing::debug!(elapsed = ?Duration::from_secs_f64(time), ?change_id, "received initial state of 'filter'");
                *cid = change_id;
            }
            QueryEvent::Columns(_) => {
                // irrelevant
            }
        }

        Ok(())
    };

    loop {
        let res = tokio::select! {
            sc = sstate.servers.stream.rx.recv() => {
                process_server_event(sc, &mut change_ids[Which::Servers]).context("processing 'servers' event")
            }
            dc = sstate.clusters.stream.rx.recv() => {
                process_cluster_event(dc, &mut change_ids[Which::Clusters]).context("processing 'clusters' event")
            }
            fc = sstate.filter.stream.rx.recv() => {
                process_filter_event(fc, &mut change_ids[Which::Filter]).context("processing 'filter' event")
            }
        };

        if let Err(error) = res {
            tracing::error!(%error, "error processing subscription event");
        }
    }
}
