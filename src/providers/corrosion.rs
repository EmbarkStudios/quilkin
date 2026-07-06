use eyre::{Context, ContextCompat as _};
use std::{
    sync::{Arc, atomic},
    time::Duration,
};
use tracing_futures::Instrument as _;
mod pull;
pub mod push;

pub use push::ServerMutator;

type CorrosionAddrs = Vec<crate::net::EndpointAddress>;
type HealthCheck = Arc<atomic::AtomicBool>;
type State = Arc<crate::config::Config>;

use rand::Rng as _;
use tryhard::backoff_strategies::{BackoffStrategy as _, ExponentialBackoff};

const BACKOFF_INITIAL_DELAY: Duration = Duration::from_millis(500);
const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(30);
const BACKOFF_MAX_JITTER: Duration = Duration::from_secs(2);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Repeatedly attempts `connect` against all `endpoints` in parallel with
/// backoff, resolving with the first successful connection
///
/// Connecting to all endpoints in parallel prevents down/slow servers in the
/// list from unnecessarily delaying connections to healthy servers. Currently
/// we just go with the first server that we can successfully connect to, but
/// in the future we could connect to multiple servers simultaneously.
async fn connect_first<T, F, Fut>(
    endpoints: &CorrosionAddrs,
    connect: F,
) -> crate::Result<(T, crate::net::EndpointAddress)>
where
    T: Send + 'static,
    F: Fn(crate::net::EndpointAddress) -> Fut,
    Fut: std::future::Future<Output = crate::Result<T>> + Send + 'static,
{
    let mut backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);

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

    tryhard::retry_fn(|| {
        tracing::debug!(
            server_count = endpoints.len(),
            "attempting to connect to corrosion servers"
        );

        let mut js = tokio::task::JoinSet::new();

        for addr in endpoints.iter().cloned() {
            let fut = connect(addr.clone());
            js.spawn(async move { (fut.await, addr) });
        }

        let num_endpoints = endpoints.len();

        async move {
            match tokio::time::timeout(CONNECTION_TIMEOUT, async {
                while let Some(join_result) = js.join_next().await {
                    match join_result {
                        Ok((result, addr)) => {
                            match result {
                                Ok(connection) => {
                                    return Ok((connection, addr));
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
                                    "panic occurred in task attempting to connect to corrosion endpoint"
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
    .with_config(retry_config)
    .await
}

#[derive(Copy, Clone, Debug)]
pub enum CorrosionMode {
    /// Pushes changes to a remote corrosion DB
    Push,
    /// Pulls changes from a remote corrosion DB
    Pull,
}

impl clap::ValueEnum for CorrosionMode {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Push, Self::Pull]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        use clap::builder::PossibleValue as pv;
        Some(match self {
            Self::Push => pv::new("push"),
            Self::Pull => pv::new("pull"),
        })
    }
}

impl super::Providers {
    /// Potentially spawns a corrosion related provider
    ///
    /// 1. If `[CorrosionMode::Push]`, spawns a `Mutator` and `Pusher` to mutate the local state
    ///    and send those mutations to a remote corrosion DB
    /// 1. If `[CorrosionMode::Pull]`, spawns a provider that subscribes to changes from a remote
    ///    corrosion DB and applies events to the local state
    pub(super) fn maybe_spawn_corrosion(
        &self,
        config: &State,
        health_check: &HealthCheck,
        providers: &mut tokio::task::JoinSet<crate::Result<()>>,
    ) -> Option<ServerMutator> {
        let Some(mode) = self.corrosion_mode else {
            tracing::debug!("corrosion is not enabled");
            return None;
        };

        match mode {
            CorrosionMode::Pull => {
                if self.corrosion_endpoints.is_empty() {
                    tracing::error!(
                        "unable to start corrosion subscriber, no corrosion endpoints were provided"
                    );
                    return None;
                }

                let config = config.clone();
                let health_check = health_check.clone();
                let endpoints = self.corrosion_endpoints.clone();

                // We're a proxy, subscribing to changes from a remote relay
                providers.spawn(Self::task(
                    "corrosion_subscribe".into(),
                    health_check.clone(),
                    move || {
                        let state = config.clone();
                        let endpoints = endpoints.clone();
                        let hc = health_check.clone();

                        async move { pull::corrosion_subscribe(state, endpoints, hc).await }
                    },
                ));

                None
            }
            CorrosionMode::Push => {
                if self.corrosion_endpoints.is_empty() {
                    tracing::error!(
                        "unable to start corrosion publisher, no corrosion endpoints were provided"
                    );
                    return None;
                }
                let Some(qcmp) = config.dyn_cfg.qcmp_port() else {
                    tracing::error!(
                        "cannot create a mutator when there is no QCMP port configured"
                    );
                    return None;
                };
                let icao = &config.dyn_cfg.icao_code;

                let (mutator, pusher) = push::corrosion_mutate(
                    qcmp,
                    icao,
                    self.corrosion_endpoints.clone(),
                    health_check.clone(),
                );

                // We're an agent, pushing changes to a remote relay
                providers.spawn(async move { pusher.push_changes().await });

                Some(mutator)
            }
        }
    }
}

use corrosion::{
    api::{ChangeId, SqliteValue},
    db::read::FromSqlValue,
    persistent::client,
    pubsub::{self, SubParamsv1 as SubParams},
};

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

impl<T> Clone for QuerySet<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            set: self.set.clone(),
        }
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
    #[inline]
    fn index_mut(&mut self, index: Which) -> &mut Self::Output {
        &mut self.set[index as usize]
    }
}

type ChangeIds = QuerySet<corrosion::pubsub::ChangeId>;
