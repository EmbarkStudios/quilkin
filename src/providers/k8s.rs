/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

pub mod agones;

use std::collections::BTreeSet;

use futures::Stream;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{core::DeserializeGuard, runtime::watcher::Event};
use kube_leader_election::{LeaseLock, LeaseLockParams};

use agones::GameServer;

use crate::{
    config, metrics,
    net::{ClusterMap, endpoint::Locality},
};

const CONFIGMAP: &str = "v1/ConfigMap";
const GAMESERVER: &str = "agones.dev/v1/GameServer";

fn track_event<T>(kind: &'static str, event: Event<T>) -> Event<T> {
    let ty = match &event {
        Event::Apply(_) => "apply",
        Event::Init => "init",
        Event::InitApply(_) => "init-apply",
        Event::InitDone => "init-done",
        Event::Delete(_) => "done",
    };

    metrics::k8s::events_total(kind, ty).inc();
    event
}

pub(crate) async fn update_leader_lock(
    client: kube::Client,
    namespace: impl AsRef<str>,
    lease_name: impl Into<String>,
    holder_id: impl Into<String>,
    leader_lock: config::LeaderLock,
) -> crate::Result<()> {
    let leadership = LeaseLock::new(
        client,
        namespace.as_ref(),
        LeaseLockParams {
            holder_id: holder_id.into(),
            lease_name: lease_name.into(),
            lease_ttl: std::time::Duration::from_secs(1),
        },
    );

    loop {
        match leadership.try_acquire_or_renew().await {
            Ok(ll) => leader_lock.store(ll.acquired_lease),
            Err(error) => tracing::warn!(%error),
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
pub fn update_filters_from_configmap(
    client: kube::Client,
    namespace: impl AsRef<str>,
    filters: config::filter::FilterChainConfig,
) -> impl Stream<Item = crate::Result<(), eyre::Error>> {
    async_stream::stream! {
        let mut cmap = None;
        for await event in configmap_events(client, namespace) {
            tracing::trace!("new configmap event");

            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    metrics::k8s::errors_total(CONFIGMAP, &error).inc();
                    yield Err(error.into());
                    continue;
                }
            };

            let configmap = match track_event(CONFIGMAP, event) {
                Event::Apply(configmap) => configmap,
                Event::Init => { yield Ok(()); continue; }
                Event::InitApply(configmap) => {
                    if cmap.is_none() {
                        cmap = Some(configmap);
                    }
                    yield Ok(());
                    continue;
                }
                Event::InitDone => {
                    if let Some(cmap) = cmap.take() {
                        cmap
                    } else {
                        yield Ok(());
                        continue;
                    }
                }
                Event::Delete(_) => {
                    metrics::k8s::filters(false);
                    filters.store(Default::default());
                    yield Ok(());
                    continue;
                }
            };

            let data = configmap.data.ok_or_else(|| eyre::eyre!("configmap data missing"))?;
            let data = data.get("quilkin.yaml").ok_or_else(|| eyre::eyre!("quilkin.yaml property not found"))?;
            let data: serde_json::Map<String, serde_json::Value> = serde_yaml::from_str(data)?;

            if let Some(de_filters) = data
                .get("filters")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
            {
                metrics::k8s::filters(true);
                filters.store(de_filters);
            }

            yield Ok(());
        }
    }
}

fn configmap_events(
    client: kube::Client,
    namespace: impl AsRef<str>,
) -> impl Stream<Item = Result<Event<ConfigMap>, kube::runtime::watcher::Error>> {
    let config_namespace = namespace.as_ref();
    let configmap: kube::Api<ConfigMap> = kube::Api::namespaced(client, config_namespace);
    let config_writer = kube::runtime::reflector::store::Writer::<ConfigMap>::default();
    let configmap_stream = kube::runtime::watcher(
        configmap,
        kube::runtime::watcher::Config::default().labels("quilkin.dev/configmap=true"),
    );
    kube::runtime::reflector(config_writer, configmap_stream)
}

fn gameserver_events(
    client: kube::Client,
    namespace: impl AsRef<str>,
) -> impl Stream<Item = Result<Event<DeserializeGuard<GameServer>>, kube::runtime::watcher::Error>>
{
    let gameservers_namespace = namespace.as_ref();
    let gameservers: kube::Api<DeserializeGuard<GameServer>> =
        kube::Api::namespaced(client, gameservers_namespace);
    let gs_writer =
        kube::runtime::reflector::store::Writer::<DeserializeGuard<GameServer>>::default();
    let mut config = kube::runtime::watcher::Config::default()
        // Default timeout is 5 minutes, far too slow for us to react.
        .timeout(15);

    // Retreive unbounded results.
    config.page_size = None;

    let gameserver_stream = kube::runtime::watcher(gameservers, config);
    kube::runtime::reflector(gs_writer, gameserver_stream)
}

fn validate_gameserver(
    result: DeserializeGuard<GameServer>,
    ads: Option<&crate::config::AddressSelector>,
) -> Option<crate::net::endpoint::Endpoint> {
    match result.0 {
        Ok(server) => {
            if server.is_allocated() {
                if let Some(ep) = server.endpoint(ads) {
                    tracing::trace!(endpoint=%ep.address, metadata=serde_json::to_string(&ep.metadata).unwrap(), "applying server");
                    metrics::k8s::gameservers_total_valid();
                    Some(ep)
                } else {
                    tracing::warn!(
                        server = serde_json::to_string(&server).unwrap(),
                        "skipping invalid server"
                    );
                    metrics::k8s::gameservers_total_invalid();
                    None
                }
            } else {
                tracing::debug!(
                    server = serde_json::to_string(&server).unwrap(),
                    "skipping unallocated server"
                );
                metrics::k8s::gameservers_total_unallocated();
                None
            }
        }
        Err(error) => {
            tracing::debug!(error=%error.error, metadata=serde_json::to_string(&error.metadata).unwrap(), "couldn't decode gameserver event");
            metrics::k8s::errors_total(GAMESERVER, &error);
            None
        }
    }
}

pub fn update_endpoints_from_gameservers(
    client: kube::Client,
    namespace: impl AsRef<str>,
    clusters: config::Watch<ClusterMap>,
    locality: Option<Locality>,
    address_selector: Option<crate::config::AddressSelector>,
) -> impl Stream<Item = crate::Result<(), eyre::Error>> {
    async_stream::stream! {
        let mut servers = BTreeSet::new();

        for await event in gameserver_events(client, namespace) {
            let ads = address_selector.as_ref();
            match track_event(GAMESERVER, event?) {
                Event::Apply(result) => {
                    let span = tracing::trace_span!("k8s::gameservers::apply");
                    let _enter = span.enter();
                    let Some(endpoint) = validate_gameserver(result, ads) else {
                        continue;
                    };
                    tracing::debug!(endpoint=%serde_json::to_value(&endpoint).unwrap(), "Adding endpoint");
                    metrics::k8s::gameservers_total_valid();
                    clusters.write().replace(None, locality.clone(), endpoint);
                }
                Event::Init => {},
                Event::InitApply(result) => {
                    let span = tracing::trace_span!("k8s::gameservers::init_apply");
                    let _enter = span.enter();
                    let Some(endpoint) = validate_gameserver(result, ads) else {
                        continue;
                    };

                    tracing::trace!(%endpoint.address, endpoint.metadata=serde_json::to_string(&endpoint.metadata).unwrap(), "applying server");
                    metrics::k8s::gameservers_total_valid();
                    servers.insert(endpoint);
                }
                Event::InitDone => {
                    let span = tracing::trace_span!("k8s::gameservers::init_done");
                    let _enter = span.enter();
                    tracing::debug!("received restart event from k8s");

                    tracing::trace!(
                        endpoints=%serde_json::to_value(servers.clone()).unwrap(),
                        "Restarting with endpoints"
                    );

                    clusters.write().insert(None, locality.clone(), std::mem::take(&mut servers));
                }
                Event::Delete(result) => {
                    let span = tracing::trace_span!("k8s::gameservers::delete");
                    let _enter = span.enter();
                    let server = match result.0 {
                        Ok(server) => server,
                        Err(error) => {
                            metrics::k8s::errors_total(GAMESERVER, &error);
                            tracing::debug!(%error, metadata=serde_json::to_string(&error.metadata).unwrap(), "couldn't decode gameserver event");
                            continue;
                        }
                    };

                    let found = if let Some(endpoint) = server.endpoint(ads) {
                        tracing::debug!(%endpoint.address, endpoint.metadata=serde_json::to_string(&endpoint.metadata).unwrap(), "deleting by endpoint");
                        clusters.write().remove_endpoint(&endpoint)
                    } else {
                        tracing::debug!(server.metadata.name=%server.metadata.name.clone().unwrap_or_default(), "deleting by server name");
                        clusters.write().remove_endpoint_if(|endpoint| {
                            endpoint.metadata.unknown.get("name") == server.metadata.name.clone().map(From::from).as_ref()
                        })
                    };

                    metrics::k8s::gameservers_deletions_total(found);
                    if !found {
                        tracing::debug!(
                            endpoint=%serde_json::to_value(server.endpoint(ads)).unwrap(),
                            server.metadata.name=%serde_json::to_value(server.metadata.name).unwrap(),
                            "received unknown gameserver to delete from k8s"
                        );
                    } else {
                        tracing::debug!(
                            endpoint=%serde_json::to_value(server.endpoint(ads)).unwrap(),
                            server.metadata.name=%serde_json::to_value(server.metadata.name).unwrap(),
                            "deleted gameserver"
                        );
                    }
                }
            };

            crate::metrics::apply_clusters(&clusters);
            yield Ok(());
        }
    }
}
