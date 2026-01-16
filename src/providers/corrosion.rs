use eyre::{Context, ContextCompat as _};
use std::{
    net,
    sync::{Arc, atomic},
    time::Duration,
};
use tracing_futures::Instrument as _;
mod pull;
mod push;

pub use push::Mutator;

type CorrosionAddrs = Vec<std::net::SocketAddr>;
type HealthCheck = Arc<atomic::AtomicBool>;
type State = Arc<crate::config::Config>;

use tryhard::backoff_strategies::{BackoffStrategy as _, ExponentialBackoff};

const BACKOFF_INITIAL_DELAY: Duration = Duration::from_millis(500);
const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(30);
const BACKOFF_MAX_JITTER: Duration = Duration::from_secs(2);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// and send those mutations to a remote corrosion DB
    /// 1. If `[CorrosionMode::Pull]`, spawns a provider that subscribes to changes from a remote
    /// corrosion DB and applies events to the local state
    /// 1. If `[CorrosionMode::Db]`, creates or opens a corrosion DB and spins up a
    /// server that remote clients can push or pull changes to/from
    pub(super) fn maybe_spawn_corrosion(
        &self,
        config: &State,
        health_check: &HealthCheck,
        providers: &mut tokio::task::JoinSet<crate::Result<()>>,
    ) -> Option<Mutator> {
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
