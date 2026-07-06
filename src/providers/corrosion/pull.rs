use super::*;

use corrosion::persistent;

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
    // Each query keeps track of the latest change id it has received, if we
    // disconnect from a remote server, we can send this when subscribing to
    // (hopefully) be able to catch up to the state of that server more quickly
    let mut change_ids = QuerySet::new();

    loop {
        let connect_to_corrosion = connect_first(&endpoints, |addr| {
            let cids = change_ids.clone();
            async move {
                connect_and_sub(&addr, &cids)
                    .instrument(tracing::debug_span!("connect_and_sub", address = %addr))
                    .await
            }
        });

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

        let _res = {
            let _metrics = crate::metrics::ActiveProviderMetrics::new(address.to_string());

            process_subscription_events(&state, sstate, &mut change_ids)
                .await
                .instrument(tracing::debug_span!("corrosion subscription events", %address))
        };

        tracing::info!(%address, "lost connection to corrosion server");

        hc.store(false, atomic::Ordering::Relaxed);
    }
}

/// Attempts to connect to and subscribe to the queries to keep this proxy up to
/// date with cluster status
async fn connect_and_sub(
    addr: &crate::net::EndpointAddress,
    change_ids: &ChangeIds,
) -> crate::Result<SubState> {
    tracing::debug!("connecting to corrosion server");

    let addr = addr.to_socket_addr_async().await?;
    let root = client::Client::connect_insecure(
        addr,
        persistent::Metrics::new(crate::metrics::registry()),
    )
    .await
    .context("failed to connect")?;

    tracing::debug!("connected to corrosion server");

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
        let from = change_ids[Which::Clusters];

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
        let from = change_ids[Which::Filter];

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

    tracing::debug!("subscribed to corrosion server");

    let (servers, clusters, filter) = sub_set.assume_initialized();

    Ok(SubState {
        client: root,
        servers,
        clusters,
        filter,
    })
}

/// The actual core of the event loop, applies the events from the authoritative
/// server to reflect its state locally
async fn process_subscription_events(
    state: &State,
    mut sstate: SubState,
    change_ids: &mut ChangeIds,
) -> crate::Result<()> {
    use corrosion::{db::read as db, persistent::SubMetrics, pubsub::SubscriptionStream};
    use pubsub::ChangeType;

    let process_server_events = |events: Option<SubscriptionStream>,
                                 cid: &mut Option<ChangeId>,
                                 subm: &mut SubMetrics|
     -> crate::Result<()> {
        let events = events.context("subscription was closed")?;
        let Some(servers) = state.dyn_cfg.clusters() else {
            return Ok(());
        };

        *cid = Some(servers.write().corrosion_apply(events, subm));
        Ok(())
    };

    let process_cluster_events = |events: Option<SubscriptionStream>,
                                  cid: &mut Option<ChangeId>,
                                  subm: &mut SubMetrics|
     -> crate::Result<()> {
        let events = events.context("subscription was closed")?;
        let Some(dcs) = state.dyn_cfg.datacenters() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        use crate::config::Datacenter;

        process_events(events, cid, subm, |ct, row| {
            let dc = db::DatacenterRow::from_sql(row)
                .context("failed to deserialize datacenter row")?;

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

            Ok(())
        });

        Ok(())
    };

    let process_filter_events = |events: Option<SubscriptionStream>,
                                 cid: &mut Option<ChangeId>,
                                 subm: &mut SubMetrics|
     -> crate::Result<()> {
        let events = events.context("subscription was closed")?;
        let Some(fcf) = state.dyn_cfg.filters() else {
            // TODO: Don't subscribe if we don't have this
            return Ok(());
        };

        process_events(events, cid, subm, |ct, row| {
            match ct {
                ChangeType::Insert | ChangeType::Update => {
                    let column = row.first().context("missing 'filter' column")?;

                    let filter = column.as_str().with_context(|| {
                        format!(
                            "'filter' column is {:?}, not a string",
                            column.column_type()
                        )
                    })?;

                    fcf.store(
                        serde_json::from_str(filter).context("failed to deserialize filter")?,
                    );
                }
                ChangeType::Delete => {
                    // TODO: what do we actually want to do here?
                    tracing::warn!("ignoring `filter` deletion event");
                }
            }

            Ok(())
        });

        Ok(())
    };

    loop {
        let mut subm = persistent::SubMetrics {
            total_events: 0,
            failures: 0,
        };

        let res = tokio::select! {
            biased;
            sc = sstate.servers.stream.rx.recv() => {
                let span = tracing::info_span!("servers");
                let _s = span.enter();
                process_server_events(sc, &mut change_ids[Which::Servers], &mut subm).context("processing 'servers' event").map(|_|"servers")
            }
            dc = sstate.clusters.stream.rx.recv() => {
                let span = tracing::info_span!("clusters");
                let _s = span.enter();
                process_cluster_events(dc, &mut change_ids[Which::Clusters], &mut subm).context("processing 'clusters' event").map(|_|"clusters")
            }
            fc = sstate.filter.stream.rx.recv() => {
                let span = tracing::info_span!("filter");
                let _s = span.enter();
                process_filter_events(fc, &mut change_ids[Which::Filter], &mut subm).context("processing 'filter' event").map(|_|"filter")
            }
        };

        match res {
            Ok(stream) => {
                crate::metrics::corrosion::subscription_events(stream)
                    .add(subm.total_events as i64);

                if subm.failures > 0 {
                    crate::metrics::corrosion::subscription_failures(stream)
                        .add(subm.failures as i64);
                }
            }
            Err(error) => {
                tracing::error!(?error, "error processing subscription event");
                return Err(error);
            }
        }
    }
}

/// Applies a block of subscription events, tracking the latest change id seen
///
/// `apply` is called with each row that changed, receiving [`ChangeType::Insert`]
/// for rows in the initial query state. Application failures are logged and
/// counted, but don't stop processing of the remaining events.
fn process_events(
    events: corrosion::pubsub::SubscriptionStream,
    cid: &mut Option<ChangeId>,
    subm: &mut corrosion::persistent::SubMetrics,
    mut apply: impl FnMut(pubsub::ChangeType, &[SqliteValue]) -> crate::Result<()>,
) {
    use pubsub::{ChangeType, QueryEvent};

    let mut successful = 0;

    for event in events {
        subm.total_events += 1;

        let event = match event {
            Ok(e) => e,
            Err(error) => {
                tracing::error!(%error, "failed to deserialize event");
                continue;
            }
        };

        match event {
            // The state of a row that matches our query changed
            QueryEvent::Change(ct, _rid, row, id) => {
                if let Err(error) = apply(ct, &row) {
                    tracing::error!(%error, "failed to apply change");
                    continue;
                }

                *cid = Some(id);
            }
            // The state of a row in the initial query
            QueryEvent::Row(_rid, row) => {
                if let Err(error) = apply(ChangeType::Insert, &row) {
                    tracing::error!(%error, "failed to apply row");
                    continue;
                }
            }
            QueryEvent::Error(error) => {
                tracing::error!(%error, "error event from subscription");
                continue;
            }
            // Marks the end of the initial query to catch us up to the current state of the server
            QueryEvent::EndOfQuery { time, change_id } => {
                tracing::debug!(elapsed = ?Duration::from_secs_f64(time), ?change_id, "received initial state");
                *cid = change_id;
            }
            QueryEvent::Columns(_) => {
                // irrelevant
            }
        }

        successful += 1;
    }

    subm.failures = subm.total_events - successful;
}
