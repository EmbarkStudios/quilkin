use super::*;
use corrosion::persistent::{self, client, proto::v1};
use quilkin_types::{Endpoint, IcaoCode, TokenSet};
use rand::Rng;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{broadcast, mpsc};

pub(super) fn corrosion_mutate(
    qcmp: &crate::config::qcmp::QcmpPort,
    icao: &crate::config::NotifyingIcaoCode,
    endpoints: CorrosionAddrs,
    hc: HealthCheck,
) -> (ServerMutator, Pusher) {
    let (tx, rx) = mpsc::unbounded_channel();
    let ls = Arc::new(LocalState::default());

    let agent_info = AgentInfo {
        qcmp: qcmp.load(),
        icao: icao.load(),
    };

    (
        ServerMutator {
            state: ls.clone(),
            tx,
        },
        Pusher {
            hc,
            endpoints,
            state: ls,
            rx,
            agent_info,
            qcmp: qcmp.subscribe(),
            icao: icao.subscribe(),
        },
    )
}

#[derive(Copy, Clone)]
struct AgentInfo {
    qcmp: u16,
    icao: IcaoCode,
}

pub enum Mutation {
    Upsert(uuid::Uuid),
    Update(uuid::Uuid),
    Remove(Endpoint),
}

/// Keeps track of server state, publishing changes to a remote corrosion database
///
/// Unlike the xDS version of tracking, this specializes for the k8s usecase, so
/// servers are locally uniquely identified by their [uid](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#uids)
/// rather than their endpoint
#[derive(Clone)]
pub struct ServerMutator {
    state: Arc<LocalState>,
    tx: mpsc::UnboundedSender<Mutation>,
}

impl ServerMutator {
    /// Creates a mutator for tests
    #[inline]
    pub fn testing(state: Arc<LocalState>) -> (Self, mpsc::UnboundedReceiver<Mutation>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { state, tx }, rx)
    }

    /// Upserts the server, updating it if it already exists and 1 or more details
    /// differ from the current state, or adding it if it doesn't exist
    #[inline]
    pub fn upsert_server(&self, id: uuid::Uuid, endpoint: Endpoint, ts: TokenSet) {
        match self.state.servers.entry(id) {
            dashmap::Entry::Vacant(ve) => {
                ve.insert((endpoint, ts));
                self.send(Mutation::Upsert(id));
            }
            dashmap::Entry::Occupied(mut oc) => {
                let v = oc.get_mut();
                // This would indicate we got a new pod with a different endpoint
                // address with the same uuid as a different pod that wasn't deleted
                // which should be impossible, honor it, but emit a warning
                if v.0 != endpoint {
                    tracing::warn!(%id, old = %v.0, new = %endpoint, "uuid endpoint has changed, replacing the endpoint");
                    self.send(Mutation::Remove(std::mem::replace(&mut v.0, endpoint)));
                    v.1 = ts;
                    self.send(Mutation::Upsert(id));
                } else if v.1 == ts {
                    tracing::debug!(%id, %endpoint, "ignoring server upsert, token set is the same");
                } else {
                    v.1 = ts;
                    self.send(Mutation::Update(id));
                }
            }
        }
    }

    /// Does a full replacement of the local state
    ///
    /// This is used when k8s does an `Init` -> `InitApply` -> `InitDone` which
    /// is essentially a full state reset, so this can both add, update, and
    /// delete servers
    #[inline]
    pub fn replace(&self, set: BTreeMap<uuid::Uuid, (Endpoint, TokenSet)>) {
        // Remove servers that aren't part of the replacement set
        let mut to_remove = Vec::new();
        for entry in self.state.servers.iter() {
            if !set.contains_key(entry.key()) {
                to_remove.push(*entry.key());
            }
        }

        for uid in to_remove {
            self.remove_server(uid);
        }

        for (uid, (ep, ts)) in set {
            self.upsert_server(uid, ep, ts);
        }
    }

    /// Removes the server with the specified `UUID`
    #[inline]
    pub fn remove_server(&self, id: uuid::Uuid) {
        let Some((_, v)) = self.state.servers.remove(&id) else {
            return;
        };

        self.send(Mutation::Remove(v.0));
    }

    #[inline]
    fn send(&self, to_send: Mutation) {
        drop(self.tx.send(to_send));
    }
}

/// Accumulates mutations to the local state so we can batch mutations when
/// sending to a remote server
pub struct Accumulator {
    upserts: Vec<v1::ServerUpsert>,
    updates: Vec<v1::ServerUpdate>,
    removes: Vec<Endpoint>,
    icao: IcaoCode,
}

impl Accumulator {
    #[inline]
    pub fn new(icao: IcaoCode) -> Self {
        Self {
            upserts: Vec::new(),
            updates: Vec::new(),
            removes: Vec::new(),
            icao,
        }
    }

    /// Adds this mutation if it is still valid
    #[inline]
    pub fn accumulate(&mut self, state: &LocalState, mutation: Mutation) {
        match mutation {
            Mutation::Upsert(id) => {
                if let Some(server) = state.servers.get(&id) {
                    self.upserts.push(v1::ServerUpsert {
                        endpoint: server.0.clone(),
                        icao: self.icao,
                        tokens: server.1.clone(),
                    });
                }
            }
            Mutation::Update(id) => {
                if let Some(server) = state.servers.get(&id) {
                    self.updates.push(v1::ServerUpdate {
                        endpoint: server.0.clone(),
                        tokens: Some(server.1.clone()),
                        icao: None,
                    });
                }
            }
            Mutation::Remove(ep) => {
                self.removes.push(ep);
            }
        }
    }

    /// Resets the accumulator, returning any mutations
    #[inline]
    #[allow(clippy::type_complexity)]
    pub fn take(
        &mut self,
    ) -> (
        Option<Vec<v1::ServerUpsert>>,
        Option<Vec<v1::ServerUpdate>>,
        Option<Vec<Endpoint>>,
    ) {
        let up = (!self.upserts.is_empty()).then_some(std::mem::take(&mut self.upserts));
        let u = (!self.updates.is_empty()).then_some(std::mem::take(&mut self.updates));
        let r = (!self.removes.is_empty()).then_some(std::mem::take(&mut self.removes));

        (up, u, r)
    }
}

#[derive(Default)]
pub struct LocalState {
    servers: dashmap::DashMap<uuid::Uuid, (Endpoint, TokenSet)>,
}

impl LocalState {
    /// For testing, converts the current state to and endpoint -> tokenset map
    #[inline]
    pub fn to_map(&self) -> BTreeMap<Endpoint, TokenSet> {
        self.servers
            .iter()
            .map(|srv| (srv.0.clone(), srv.1.clone()))
            .collect()
    }
}

/// Pushes changes to a remote server
pub struct Pusher {
    hc: HealthCheck,
    endpoints: CorrosionAddrs,
    state: Arc<LocalState>,
    rx: mpsc::UnboundedReceiver<Mutation>,
    qcmp: broadcast::Receiver<u16>,
    icao: broadcast::Receiver<IcaoCode>,
    agent_info: AgentInfo,
}

impl Pusher {
    pub async fn push_changes(mut self) -> crate::Result<()> {
        let mut backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);

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
                    server_count = self.endpoints.len(),
                    "attempting to connect to corrosion server"
                );

                // Attempt to connect to multiple servers in parallel, otherwise
                // down/slow servers in the list can unnecessarily delay connections
                // to healthy servers.
                //
                // Currently we just go with the first server that we can successfully
                // connect to, but in the future we could connect to multiple servers simultaneously 
                let mut js = tokio::task::JoinSet::new();

                for addr in self.endpoints.iter().cloned() {
                    let info = self.agent_info;
                    js.spawn(async move {
                        let res = connect(addr, info)
                            .instrument(tracing::debug_span!("connect"))
                            .await;

                        (res, addr)
                    });
                }

                let num_endpoints = self.endpoints.len();

                async move {
                    match tokio::time::timeout(CONNECTION_TIMEOUT, async {
                        while let Some(join_result) = js.join_next().await {
                            match join_result {
                                Ok((result, addr)) => {
                                    match result {
                                        Ok(client) => {
                                            return Ok((client, addr));
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

            let (client, address) = match connect_to_corrosion
                .instrument(tracing::trace_span!("corrosion_connect"))
                .await
            {
                Ok(c) => c,
                Err(error) => {
                    tracing::warn!(%error, "unable to connect to a corrosion server");
                    continue;
                }
            };

            tracing::info!(%address, "successfully connected to corrosion server");
            self.hc.store(true, atomic::Ordering::Relaxed);

            self.push(client)
                .instrument(tracing::debug_span!("corrosion mutation events", %address))
                .await;

            self.hc.store(false, atomic::Ordering::Relaxed);
        }
    }

    async fn push(&mut self, client: client::MutationClient) {
        // TODO: we could eventually be smarter about this and not send state of
        // of the world if we've previously been connected to this server (or
        // one that had some or all of the same state), but for now it is much
        // simpler to send state of the world upon initial connection
        while self.rx.try_recv().is_ok() {}

        // Transmit the entirety of our current state
        let mut upserts = Vec::with_capacity(self.state.servers.len());
        let icao = self.agent_info.icao;

        for entry in self.state.servers.iter() {
            upserts.push(v1::ServerUpsert {
                endpoint: entry.0.clone(),
                icao,
                tokens: entry.1.clone(),
            });
        }

        let Ok(iter) = v1::ServerIter::new(v1::ServerChange::Upsert(upserts)) else {
            unreachable!()
        };

        for buf in iter {
            if let Err(error) = client.send_raw(buf).await {
                tracing::warn!(%error, "failed to upsert initial server set");
                return;
            }
        }

        let mut accumulator = Accumulator::new(icao);

        // Try to batch updates
        let mut update_interval = tokio::time::interval(std::time::Duration::from_millis(100));

        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a separate task to do the actual serialization and transmission to the remote server
        tokio::task::spawn(async move {
            async fn publish_changes(
                client: &client::MutationClient,
                mut rx: mpsc::UnboundedReceiver<v1::ServerChange>,
            ) -> Result<(), client::TransactionError> {
                while let Some(change) = rx.recv().await {
                    match v1::ServerIter::new(change) {
                        Ok(iter) => {
                            for buf in iter {
                                client.send_raw(buf).await?;
                            }
                        }
                        Err(mutate) => {
                            client.transactions(&[mutate]).await?;
                        }
                    }
                }

                Ok(())
            }

            if let Err(error) = publish_changes(&client, rx).await {
                tracing::error!(%error, "failed to push changes to server");
            }

            client.shutdown().await;
        });

        macro_rules! send {
            ($item:expr) => {
                if tx.send($item).is_err() {
                    tracing::warn!("lost connection to remote server");
                    return;
                }
            };
        }

        // Transmit mutations. If we received mutations in the time between
        // the connection was made we might send duplicate data.
        loop {
            tokio::select! {
                biased;

                change = self.rx.recv() => {
                    let Some(change) = change else {
                        tracing::info!("mutation channel closed");
                        return;
                    };

                    accumulator.accumulate(&self.state, change);
                }
                _ = update_interval.tick() => {
                    let (up, u, r) = accumulator.take();

                    if let Some(upserts) = up {
                        send!(v1::ServerChange::Upsert(upserts));
                    }

                    if let Some(removes) = r {
                        send!(v1::ServerChange::Remove(removes));
                    }

                    if let Some(updates) = u {
                        send!(v1::ServerChange::Update(updates));
                    }
                }
                qcmp = self.qcmp.recv() => {
                    let Ok(qcmp) = qcmp else {
                        tracing::warn!("QCMP broadcaster has been closed");
                        continue;
                    };
                    if self.agent_info.qcmp != qcmp {
                        self.agent_info.qcmp = qcmp;
                        send!(v1::ServerChange::UpdateMutator(v1::MutatorUpdate {
                            qcmp_port: Some(qcmp),
                            icao: None,
                        }));
                    }
                }
                icao = self.icao.recv() => {
                    let Ok(icao) = icao else {
                        tracing::warn!("ICAO broadcaster has been closed");
                        continue;
                    };
                    if self.agent_info.icao != icao {
                        self.agent_info.icao = icao;
                        accumulator.icao = icao;
                        send!(v1::ServerChange::UpdateMutator(v1::MutatorUpdate {
                            icao: Some(icao),
                            qcmp_port: None,
                        }));
                    }
                }
            }
        }
    }
}

async fn connect(addr: net::SocketAddr, info: AgentInfo) -> crate::Result<client::MutationClient> {
    let root = client::Client::connect_insecure(
        addr,
        persistent::Metrics::new(crate::metrics::registry()),
    )
    .await
    .context("failed to connect")?;

    Ok(client::MutationClient::connect(root, info.qcmp, info.icao).await?)
}
