/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use eyre::ContextCompat;
use futures::StreamExt;
use rand::Rng;
use tonic::transport::{Endpoint, channel::Channel as TonicChannel};
use tracing::Instrument;
use tryhard::{
    RetryFutureConfig, RetryPolicy,
    backoff_strategies::{BackoffStrategy, ExponentialBackoff},
};

use crate::{
    Result,
    core::Node,
    discovery::{
        DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
        aggregated_discovery_service_client::AggregatedDiscoveryServiceClient,
    },
    generated::quilkin::relay::v1alpha1::aggregated_control_plane_discovery_service_client::AggregatedControlPlaneDiscoveryServiceClient,
    metrics::{KIND_CLIENT, KIND_SERVER},
};

type AdsGrpcClient = AggregatedDiscoveryServiceClient<TonicChannel>;
type MdsGrpcClient = AggregatedControlPlaneDiscoveryServiceClient<TonicChannel>;

pub type AdsClient = Client<AdsGrpcClient>;
pub type MdsClient = Client<MdsGrpcClient>;

pub(crate) const IDLE_REQUEST_INTERVAL: Duration = Duration::from_secs(30);

#[tonic::async_trait]
pub trait ServiceClient: Clone + Sized + Send + 'static {
    type Request: Clone + Send + Sync + Sized + 'static + std::fmt::Debug;
    type Response: Clone + Send + Sync + Sized + 'static + std::fmt::Debug;

    async fn connect_to_endpoint(
        endpoint: tonic::transport::Endpoint,
    ) -> Result<Self, tonic::transport::Error>;
    async fn stream_requests<S: tonic::IntoStreamingRequest<Message = Self::Request> + Send>(
        &mut self,
        stream: S,
    ) -> tonic::Result<tonic::Response<tonic::Streaming<Self::Response>>>;
}

#[tonic::async_trait]
impl ServiceClient for AdsGrpcClient {
    type Request = DiscoveryRequest;
    type Response = DiscoveryResponse;

    async fn connect_to_endpoint(
        endpoint: tonic::transport::Endpoint,
    ) -> Result<Self, tonic::transport::Error> {
        Ok(AdsGrpcClient::connect(
            endpoint
                .tcp_keepalive(Some(crate::HTTP2_KEEPALIVE_INTERVAL))
                .timeout(crate::HTTP2_KEEPALIVE_TIMEOUT),
        )
        .await?
        .max_decoding_message_size(crate::config::max_grpc_message_size())
        .max_encoding_message_size(crate::config::max_grpc_message_size()))
    }

    async fn stream_requests<S: tonic::IntoStreamingRequest<Message = Self::Request> + Send>(
        &mut self,
        stream: S,
    ) -> tonic::Result<tonic::Response<tonic::Streaming<Self::Response>>> {
        self.stream_aggregated_resources(stream).await
    }
}

#[tonic::async_trait]
impl ServiceClient for MdsGrpcClient {
    type Request = DiscoveryResponse;
    type Response = DiscoveryRequest;

    async fn connect_to_endpoint(
        endpoint: tonic::transport::Endpoint,
    ) -> Result<Self, tonic::transport::Error> {
        Ok(MdsGrpcClient::connect(
            endpoint
                .tcp_keepalive(Some(crate::HTTP2_KEEPALIVE_INTERVAL))
                .timeout(crate::HTTP2_KEEPALIVE_TIMEOUT),
        )
        .await?
        .max_decoding_message_size(crate::config::max_grpc_message_size())
        .max_encoding_message_size(crate::config::max_grpc_message_size()))
    }

    async fn stream_requests<S: tonic::IntoStreamingRequest<Message = Self::Request> + Send>(
        &mut self,
        stream: S,
    ) -> tonic::Result<tonic::Response<tonic::Streaming<Self::Response>>> {
        self.stream_aggregated_resources(stream).await
    }
}

/// Client that can talk to an XDS server using the aDS protocol.
#[derive(Clone)]
pub struct Client<C: ServiceClient> {
    client: C,
    identifier: Arc<str>,
    management_servers: Vec<Endpoint>,
    /// The management server endpoint the client is currently connected to
    #[allow(dead_code)]
    connected_endpoint: Endpoint,
}

impl<C: ServiceClient> Client<C> {
    #[tracing::instrument(skip_all, level = "trace", fields(servers = ?management_servers))]
    pub async fn connect(identifier: String, management_servers: Vec<Endpoint>) -> Result<Self> {
        eyre::ensure!(
            !management_servers.is_empty(),
            "at least one endpoint must be specified"
        );
        for ms in &management_servers {
            // make sure that we have everything we will need in our URI
            let uri = ms.uri();
            eyre::ensure!(uri.scheme().is_some(), "endpoint {uri} has no scheme");
            eyre::ensure!(uri.host().is_some(), "endpoint {uri} has no host");
        }

        let (client, connected_endpoint) = Self::connect_with_backoff(&management_servers).await?;
        Ok(Self {
            client,
            identifier: Arc::from(identifier),
            management_servers,
            connected_endpoint,
        })
    }

    async fn connect_with_backoff(management_servers: &[Endpoint]) -> Result<(C, Endpoint)> {
        use crate::config::{
            BACKOFF_INITIAL_DELAY, BACKOFF_MAX_DELAY, BACKOFF_MAX_JITTER, CONNECTION_TIMEOUT,
        };

        let mut backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);

        let retry_config = RetryFutureConfig::new(u32::MAX).custom_backoff(|attempt, error: &_| {
            tracing::info!(attempt, "Retrying to connect");
            // reset after success
            if attempt <= 1 {
                backoff = ExponentialBackoff::new(BACKOFF_INITIAL_DELAY);
            }

            // max delay + jitter of up to 2 seconds
            let mut delay = backoff.delay(attempt, &error).min(BACKOFF_MAX_DELAY);
            delay += Duration::from_millis(
                rand::rng().random_range(0..BACKOFF_MAX_JITTER.as_millis() as _),
            );

            tracing::warn!(?error, "Unable to connect to the xDS server");
            RetryPolicy::Delay(delay)
        });

        let connect_to_server = tryhard::retry_fn(|| {
            let mut js = tokio::task::JoinSet::new();

            tracing::info!(
                server_count = management_servers.len(),
                "attempting to connect to xDS server"
            );
            for ms in management_servers {
                let endpoint = ms.clone();

                js.spawn(async move {
                    let res = C::connect_to_endpoint(endpoint.clone())
                        .instrument(tracing::debug_span!(
                            "AggregatedDiscoveryServiceClient::connect_to_endpoint"
                        ))
                        .await;

                    (res, endpoint)
                });
            }

            async move {
                match tokio::time::timeout(CONNECTION_TIMEOUT, async {
                    while let Some(join_result) = js.join_next().await {
                        match join_result {
                            Ok((result, endpoint)) => {
                                match result {
                                    Ok(client) => {
                                        return Ok((client, endpoint));
                                    }
                                    Err(error) => {
                                        tracing::warn!(uri = %endpoint.uri(), %error, "failed to connect");
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

                    Err(RpcSessionError::NoSuccessfulConnections(management_servers.len()))
                })
                .await
                {
                    Ok(Ok(cae)) => Ok(cae),
                    Ok(Err(err)) => Err(err),
                    Err(_) => Err(RpcSessionError::TimedOut {
                        endpoint_count: management_servers.len(),
                        window: CONNECTION_TIMEOUT,
                    }),
                }
            }
        })
        .with_config(retry_config);

        let client = connect_to_server
            .instrument(tracing::trace_span!("client_connect"))
            .await?;
        tracing::info!(endpoint = %client.1.uri(), "Connected to management server");
        Ok(client)
    }
}

impl MdsClient {
    pub async fn delta_stream<C: crate::config::Configuration>(
        self,
        config: Arc<C>,
        is_healthy: Arc<AtomicBool>,
        mut shutdown: crate::ShutdownSignal,
    ) -> Result<tokio::task::JoinHandle<Result<()>>, Self> {
        const LEADERSHIP_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
        let identifier = String::from(&*self.identifier);

        while config.is_leader() == Some(false) {
            tokio::time::sleep(LEADERSHIP_CHECK_INTERVAL).await;
        }

        let (mut ds, mut stream) =
            match DeltaServerStream::connect(self.client.clone(), identifier.clone()).await {
                Ok(ds) => {
                    tracing::debug!("acquired aggregated delta stream");
                    ds
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to acquire aggregated delta stream");
                    return Err(self);
                }
            };

        let id = identifier.clone();
        let handle = tokio::task::spawn(
            async move {
                tracing::trace!("starting relay client delta stream task");

                loop {
                    if config.is_leader() == Some(false) {
                        tracing::debug!("not leader, delaying task");
                        tokio::time::sleep(LEADERSHIP_CHECK_INTERVAL).await;
                        continue;
                    }

                    {
                        let control_plane = super::server::ControlPlane::from_arc(
                            config.clone(),
                            IDLE_REQUEST_INTERVAL,
                            shutdown.clone(),
                        );

                        let change_watcher = tokio::spawn({
                            control_plane.config.on_changed(control_plane.clone(), shutdown.clone())
                        });

                        tokio::select! {
                            res = control_plane.delta_aggregated_resources(stream) => {
                                match res {
                                    Ok(mut stream) => {
                                        is_healthy.store(true, Ordering::SeqCst);

                                        loop {
                                            if config.is_leader() == Some(false) {
                                                tracing::warn!("lost leader lock mid-stream, disconnecting");
                                                break;
                                            }

                                            const TIMEOUT_INTERVAL: Duration = Duration::from_secs(30);
                                            match tokio::time::timeout(TIMEOUT_INTERVAL, stream.next()).await {
                                                Ok(Some(Ok(response))) => match ds.send_response(response).await {
                                                    Ok(_) => {
                                                        tracing::trace!("ACK successfully sent");
                                                    }
                                                    Err(error) => {
                                                        tracing::warn!(%error, "error sending ACK");
                                                        break;
                                                    }
                                                }
                                                Ok(Some(Err(error))) => {
                                                    tracing::warn!(%error, "error receiving delta response");
                                                    break;
                                                }
                                                Ok(None) => {
                                                    tracing::debug!("delta stream terminated by client");
                                                    break;
                                                }
                                                Err(error) => {
                                                    tracing::trace!(duration=TIMEOUT_INTERVAL.as_secs_f64(), %error, "no requests received");
                                                    continue;
                                                }
                                            }
                                        }

                                        change_watcher.abort();
                                        let _unused = change_watcher.await;
                                    },
                                    Err(error) => {
                                        tracing::warn!(%error, error_debug=?error, "failed to acquire internal delta stream from config");
                                    }
                                }
                            }
                            _ = shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }

                    is_healthy.store(false, Ordering::SeqCst);

                    if shutdown.has_changed().is_ok_and(|b| b) {
                        // We are shutting down, just quit
                        return Ok(());
                    }

                    tracing::debug!("lost connection to relay server, retrying");
                    let new_client = MdsClient::connect_with_backoff(&self.management_servers)
                        .await
                        .unwrap()
                        .0;
                    (ds, stream) =
                        DeltaServerStream::connect(new_client, identifier.clone()).await?;
                }
            }
            .instrument(tracing::trace_span!("handle_delta_discovery_response", id)),
        );

        Ok(handle)
    }
}

pub(crate) struct DeltaClientStream {
    req_tx: tokio::sync::mpsc::Sender<DeltaDiscoveryRequest>,
}

impl DeltaClientStream {
    #[inline]
    async fn connect(
        endpoints: &[Endpoint],
        identifier: String,
    ) -> Result<(Self, tonic::Streaming<DeltaDiscoveryResponse>, Endpoint)> {
        crate::metrics::actions_total(KIND_CLIENT, "connect").inc();
        if let Ok((mut client, ep)) = MdsClient::connect_with_backoff(endpoints).await {
            let (req_tx, requests_rx) =
                tokio::sync::mpsc::channel(100 /*ResourceType::VARIANTS.len()*/);

            // Since we are doing exploratory requests to see if the remote endpoint supports delta streams, we unfortunately
            // need to actually send something before the full roundtrip occurs. This can be removed once delta discovery
            // is fully rolled out
            req_tx
                .send(DeltaDiscoveryRequest {
                    node: Some(Node {
                        id: identifier.clone(),
                        user_agent_name: "quilkin".into(),
                        ..Default::default()
                    }),
                    type_url: "ignore-me".to_owned(),
                    ..Default::default()
                })
                .await?;

            if let Ok(stream) = client
                .subscribe_delta_resources(tokio_stream::wrappers::ReceiverStream::new(requests_rx))
                .in_current_span()
                .await
            {
                return Ok((Self { req_tx }, stream.into_inner(), ep));
            }
        }

        let (mut client, ep) = AdsClient::connect_with_backoff(endpoints).await?;

        let (req_tx, requests_rx) =
            tokio::sync::mpsc::channel(100 /*ResourceType::VARIANTS.len()*/);

        // Since we are doing exploratory requests to see if the remote endpoint supports delta streams, we unfortunately
        // need to actually send something before the full roundtrip occurs. This can be removed once delta discovery
        // is fully rolled out
        req_tx
            .send(DeltaDiscoveryRequest {
                node: Some(Node {
                    id: identifier,
                    user_agent_name: "quilkin".into(),
                    ..Default::default()
                }),
                type_url: "ignore-me".to_owned(),
                ..Default::default()
            })
            .await?;

        let stream = client
            .delta_aggregated_resources(tokio_stream::wrappers::ReceiverStream::new(requests_rx))
            .in_current_span()
            .await?;
        Ok((Self { req_tx }, stream.into_inner(), ep))
    }

    pub(crate) fn new() -> (Self, tokio::sync::mpsc::Receiver<DeltaDiscoveryRequest>) {
        let (req_tx, requests_rx) = tokio::sync::mpsc::channel(1);
        (Self { req_tx }, requests_rx)
    }

    #[inline]
    pub(crate) async fn refresh(
        &self,
        identifier: &str,
        subs: Vec<(&'static str, Vec<String>)>,
        local: &crate::config::LocalVersions,
    ) -> Result<()> {
        crate::metrics::actions_total(KIND_CLIENT, "refresh").inc();
        for (rt, names) in subs {
            let initial_resource_versions = local.get(rt).clone();
            self.req_tx
                .send(DeltaDiscoveryRequest {
                    node: Some(Node {
                        id: identifier.to_owned(),
                        user_agent_name: "quilkin".into(),
                        ..Node::default()
                    }),
                    type_url: (*rt).to_owned(),
                    resource_names_subscribe: names.clone(),
                    initial_resource_versions,
                    // We (currently) never unsubscribe from resources, since we
                    // never actually subscribe to particular ones in the first place
                    resource_names_unsubscribe: Vec::new(),
                    response_nonce: "".into(),
                    error_detail: None,
                })
                .await?;
        }

        Ok(())
    }

    /// Sends an n/ack "response" in response to the remote response
    #[inline]
    pub(crate) async fn send_response(&self, response: DeltaDiscoveryRequest) -> Result<()> {
        crate::metrics::actions_total(KIND_CLIENT, "respond").inc();
        self.req_tx.send(response).await?;
        Ok(())
    }
}

pub(crate) struct DeltaServerStream {
    res_tx: tokio::sync::mpsc::Sender<DeltaDiscoveryResponse>,
}

impl DeltaServerStream {
    #[inline]
    async fn connect(
        mut client: MdsGrpcClient,
        identifier: String,
    ) -> Result<(Self, tonic::Streaming<DeltaDiscoveryRequest>)> {
        crate::metrics::actions_total(KIND_SERVER, "connect").inc();
        let (res_tx, responses_rx) = tokio::sync::mpsc::channel(100);

        res_tx
            .send(DeltaDiscoveryResponse {
                control_plane: Some(crate::core::ControlPlane { identifier }),
                ..Default::default()
            })
            .await?;

        let stream = client
            .delta_aggregated_resources(tokio_stream::wrappers::ReceiverStream::new(responses_rx))
            .in_current_span()
            .await?
            .into_inner();

        Ok((Self { res_tx }, stream))
    }

    #[inline]
    async fn send_response(&self, res: DeltaDiscoveryResponse) -> Result<()> {
        crate::metrics::actions_total(KIND_SERVER, "respond").inc();
        self.res_tx.send(res).await?;
        Ok(())
    }
}

/// Attempts to start a new delta stream to the xDS management server, if the
/// management server does not support delta xDS we return the client as an error
#[allow(clippy::type_complexity)]
pub async fn delta_subscribe<C: crate::config::Configuration>(
    config: Arc<C>,
    identifier: String,
    endpoints: Vec<Endpoint>,
    is_healthy: Arc<AtomicBool>,
    notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    resources: &'static [(&'static str, &'static [(&'static str, Vec<String>)])],
) -> eyre::Result<tokio::task::JoinHandle<Result<()>>> {
    let (mut ds, mut stream, mut connected_endpoint) = match DeltaClientStream::connect(
        &endpoints,
        identifier.clone(),
    )
    .await
    {
        Ok(ds) => ds,
        Err(err) => {
            crate::metrics::errors_total(KIND_CLIENT, "connect").inc();
            tracing::error!(error = ?err, "failed to acquire aggregated delta stream from management server");
            return Err(err);
        }
    };

    async fn handle_first_response(
        stream: &mut tonic::Streaming<DeltaDiscoveryResponse>,
        resources: &'static [(&'static str, &'static [(&'static str, Vec<String>)])],
    ) -> eyre::Result<(String, &'static [(&'static str, Vec<String>)])> {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

        match tokio::time::timeout(TIMEOUT, stream.message()).await {
            Err(_elapsed) => {
                crate::metrics::errors_total(KIND_CLIENT, "timeout").inc();
                eyre::bail!("timed out after {TIMEOUT:?} waiting for first response");
            }
            Ok(result) => {
                let Some(first) = result? else {
                    crate::metrics::errors_total(KIND_CLIENT, "unexpected").inc();
                    eyre::bail!("expected at least one response from the management server");
                };

                let control_plane_identifier = first
                    .control_plane
                    .as_ref()
                    .map(|cp| cp.identifier.clone())
                    .unwrap_or_default();

                if first.type_url != "ignore-me" {
                    crate::metrics::errors_total(KIND_CLIENT, "unexpected").inc();
                    tracing::warn!("expected `ignore-me` response from management server");
                }

                resources
                    .iter()
                    .find_map(|(vers, subs)| (*vers == first.system_version_info).then_some(*subs))
                    .map(|rs| (control_plane_identifier, rs))
                    .with_context(|| {
                        crate::metrics::errors_total(KIND_CLIENT, "no_resource").inc();
                        format!(
                            "failed to find resources with version `{}` to subscribe to",
                            first.system_version_info
                        )
                    })
            }
        }
    }

    let (mut control_plane, resource_subscriptions) = match handle_first_response(
        &mut stream,
        resources,
    )
    .await
    {
        Ok((id, rs)) => (id, rs),
        Err(error) => {
            tracing::error!(%error, "failed to acquire matching resource subscriptions based on response from management sever");
            return Err(error);
        }
    };

    // Send requests for our resource subscriptions, in this first request we
    // won't have any resources, but if we reconnect to management servers in
    // the future we'll send the resources we already have locally to hopefully
    // reduce the amount of response data if those resources are already up
    // to date with the current state of the management server
    let local = Arc::new(crate::config::LocalVersions::new(
        resource_subscriptions.iter().map(|(s, _)| *s),
    ));
    if let Err(err) = ds
        .refresh(&identifier, resource_subscriptions.to_vec(), &local)
        .await
    {
        crate::metrics::errors_total(KIND_CLIENT, "request_failed").inc();
        tracing::error!(error = ?err, "failed to send initial resource requests");
        return Err(err);
    }

    let client_id = identifier.clone();
    let handle = tokio::task::spawn(
        async move {
            tracing::trace!("starting xDS delta stream task");
            let mut stream = stream;
            let mut resource_subscriptions = resource_subscriptions;

            loop {
                tracing::info!(%control_plane, "creating discovery response handler");
                let mut response_stream = crate::config::handle_delta_discovery_responses(
                    control_plane.clone(),
                    stream,
                    config.clone(),
                    local.clone(),
                    None,
                    notifier.clone(),
                );

                tracing::info!(%control_plane, "entering xDS stream loop");
                loop {
                    let next_response =
                        tokio::time::timeout(IDLE_REQUEST_INTERVAL, response_stream.next());

                    match next_response.await {
                        Ok(Some(Ok(response))) => {
                            is_healthy.store(true, Ordering::SeqCst);

                            let node_id = if let Some(node) = &response.node {
                                node.id.clone()
                            } else {
                                "unknown".into()
                            };
                            tracing::trace!(%node_id, "received delta response");
                            if let Err(error) = ds.send_response(response).await {
                                crate::metrics::errors_total(KIND_CLIENT, "ack_failed").inc();
                                tracing::error!(%error, %node_id, "failed to ack delta response");
                            }
                            continue;
                        }
                        Ok(Some(Err(error))) => {
                            if crate::is_broken_pipe(&error) {
                                crate::metrics::actions_total(KIND_CLIENT, "remote_terminate")
                                    .inc();
                                tracing::info!(
                                    %control_plane,
                                    endpoint = %connected_endpoint.uri(),
                                    "remoteterminated the connection",
                                );
                            } else {
                                crate::metrics::errors_total(KIND_CLIENT, "unknown").inc();
                                tracing::warn!(%error, "xds stream error");
                            }
                            break;
                        }
                        Ok(None) => {
                            crate::metrics::actions_total(KIND_CLIENT, "terminate").inc();
                            tracing::warn!(%control_plane, "xDS stream terminated");
                            break;
                        }
                        Err(_) => {
                            tracing::debug!("exceeded idle request interval sending new requests");
                            if let Err(error) = ds
                                .refresh(&identifier, resource_subscriptions.to_vec(), &local)
                                .await
                            {
                                crate::metrics::errors_total(KIND_CLIENT, "refresh").inc();
                                return Err(error.wrap_err("refresh failed"));
                            }
                        }
                    }
                }

                is_healthy.store(false, Ordering::SeqCst);

                loop {
                    tracing::info!(%control_plane, "Lost connection to xDS, retrying");
                    match DeltaClientStream::connect(&endpoints, identifier.clone()).await {
                        Ok(res) => {
                            (ds, stream, connected_endpoint) = res;
                        }
                        Err(error) => {
                            crate::metrics::errors_total(KIND_CLIENT, "connect").inc();
                            tracing::error!(%error, "failed to establish connection");
                            continue;
                        }
                    }

                    match handle_first_response(&mut stream, resources).await {
                        Ok((id, rs)) => {
                            control_plane = id;
                            resource_subscriptions = rs;
                            tracing::info!(%control_plane, "received first response");
                            break;
                        }
                        Err(error) => {
                            tracing::error!(%error, "failed to receive first response");
                        }
                    }
                }

                if let Err(error) = ds
                    .refresh(&identifier, resource_subscriptions.to_vec(), &local)
                    .await
                {
                    crate::metrics::errors_total(KIND_CLIENT, "refresh").inc();
                    return Err(error.wrap_err("refresh failed"));
                }
                tracing::info!(%control_plane, "xDS connection refreshed");
            }
        }
        .instrument(tracing::trace_span!("xds_client_stream", client_id)),
    );

    Ok(handle)
}

#[derive(Debug, thiserror::Error)]
enum RpcSessionError {
    #[error("No successful connections to {0} server(s) established")]
    NoSuccessfulConnections(usize),
    #[error(
        "No connections to {endpoint_count} server(s) established within connection window of {window:?}"
    )]
    TimedOut {
        endpoint_count: usize,
        window: Duration,
    },
}
