use eyre::{Context, ContextCompat as _};
use std::{
    net,
    sync::{Arc, atomic},
    time::Duration,
};
use tracing_futures::Instrument as _;
mod pull;
mod push;

type CorrosionAddrs = Vec<std::net::SocketAddr>;
type HealthCheck = Arc<atomic::AtomicBool>;
type State = Arc<crate::config::Config>;

impl super::Providers {
    pub(super) fn maybe_spawn_corrosion(
        &self,
        config: &State,
        health_check: &HealthCheck,
        providers: &mut tokio::task::JoinSet<crate::Result<()>>,
    ) -> Option<push::Mutator> {
        if !self.corrosion_enabled() {
            return None;
        }

        // TODO: We currently cheat to determine if we are pushing or pulling changes
        // since we'll be running this in parallel with xDs for some amount of time,
        // but will need to change when we rip out xDs at some point
        assert!(self.grpc_pull_enabled() || self.grpc_push_enabled());

        let config = config.clone();
        let health_check = health_check.clone();
        let endpoints = self.endpoints.clone();

        if self.grpc_pull_enabled() {
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
        } else {
            // We're an agent, pushing changes to a remote relay
            providers.spawn(Self::task(
                "corrosion_mutate".into(),
                health_check.clone(),
                move || {
                    let state = config.clone();
                    let endpoints = endpoints.clone();
                    let hc = health_check.clone();

                    async move { push::corrosion_mutate(state, endpoints, hc).await }
                },
            ));
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
