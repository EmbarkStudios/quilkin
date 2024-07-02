/*
 * Copyright 2022 Google LLC
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

use std::{io, sync::Arc, time::Duration};

use futures::{Stream, TryFutureExt};
use tokio_stream::StreamExt;
use tracing_futures::Instrument;

use crate::{
    discovery::{
        aggregated_discovery_service_server::{
            AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
        },
        DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
    },
    generated::quilkin::relay::v1alpha1::aggregated_control_plane_discovery_service_server::{
        AggregatedControlPlaneDiscoveryService, AggregatedControlPlaneDiscoveryServiceServer,
    },
    metrics,
    net::TcpListener,
};

pub struct ControlPlane<C> {
    pub config: Arc<C>,
    pub idle_request_interval: Duration,
    tx: tokio::sync::broadcast::Sender<&'static str>,
    pub is_relay: bool,
}

impl<C> Clone for ControlPlane<C> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            idle_request_interval: self.idle_request_interval,
            tx: self.tx.clone(),
            is_relay: self.is_relay,
        }
    }
}

impl<C: crate::config::Configuration> ControlPlane<C> {
    pub fn from_arc(config: Arc<C>, idle_request_interval: Duration) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(10);

        Self {
            config,
            idle_request_interval,
            tx,
            is_relay: false,
        }
    }

    pub fn management_server(
        mut self,
        listener: TcpListener,
    ) -> io::Result<impl std::future::Future<Output = crate::Result<()>>> {
        self.is_relay = false;
        tokio::spawn({
            let this = self.clone();
            self.config.on_changed(this)
        });

        let server = AggregatedDiscoveryServiceServer::new(self)
            .max_encoding_message_size(crate::config::max_grpc_message_size());
        let server = tonic::transport::Server::builder().add_service(server);
        tracing::info!("serving management server on port `{}`", listener.port());
        Ok(server
            .serve_with_incoming(listener.into_stream()?)
            .map_err(From::from))
    }

    pub fn relay_server(
        mut self,
        listener: TcpListener,
    ) -> io::Result<impl std::future::Future<Output = crate::Result<()>>> {
        self.is_relay = true;
        tokio::spawn({
            let this = self.clone();
            self.config.on_changed(this)
        });

        let server = AggregatedControlPlaneDiscoveryServiceServer::new(self)
            .max_encoding_message_size(crate::config::max_grpc_message_size());
        let server = tonic::transport::Server::builder().add_service(server);
        tracing::info!("serving relay server on port `{}`", listener.port());
        Ok(server
            .serve_with_incoming(listener.into_stream()?)
            .map_err(From::from))
    }

    #[inline]
    pub fn push_update(&self, resource_type: &'static str) {
        tracing::debug!(
            %resource_type,
            id = self.config.identifier(),
            is_relay = self.is_relay,
            "pushing update"
        );
        if self.tx.send(resource_type).is_err() {
            tracing::debug!("no client connections currently subscribed");
        }
    }

    pub async fn delta_aggregated_resources<S>(
        &self,
        mut streaming: S,
    ) -> Result<
        impl Stream<Item = Result<DeltaDiscoveryResponse, tonic::Status>> + Send,
        tonic::Status,
    >
    where
        S: Stream<Item = Result<DeltaDiscoveryRequest, tonic::Status>>
            + Send
            + std::marker::Unpin
            + 'static,
    {
        tracing::debug!("starting delta stream");
        let message = streaming.next().await.ok_or_else(|| {
            tracing::error!("No message found");
            tonic::Status::invalid_argument("No message found")
        })??;

        let node_id = if let Some(node) = &message.node {
            node.id.clone()
        } else {
            tracing::error!("Node identifier was not found");
            return Err(tonic::Status::invalid_argument("Node identifier required"));
        };

        let this = Self::clone(self);
        let mut rx = this.tx.subscribe();

        let id = this.config.identifier();
        tracing::debug!(
            id,
            client = node_id,
            count = this.tx.receiver_count(),
            is_relay = this.is_relay,
            "subscribed to config updates"
        );

        let control_plane_id = crate::core::ControlPlane {
            identifier: id.clone(),
        };

        use crate::config::ClientTracker;
        let mut client_tracker = ClientTracker::track_client(node_id.clone());

        let client = node_id.clone();
        let cfg = this.config.clone();
        let responder = move |req: Option<DeltaDiscoveryRequest>,
                              type_url: &str,
                              client_tracker: &mut ClientTracker|
              -> Result<Option<DeltaDiscoveryResponse>, tonic::Status> {
            let cs = if let Some(req) = req {
                metrics::delta_discovery_requests(&client, type_url).inc();

                let cs = if let Some(cs) = client_tracker.get_state(type_url) {
                    cs
                } else if cfg.allow_request_processing(type_url) {
                    client_tracker.track_state(type_url.into())
                } else {
                    return Err(tonic::Status::invalid_argument(format!(
                        "resource type '{type_url}' is not allowed by this stream"
                    )));
                };

                cs.update(req);
                tracing::debug!(kind = type_url, "sending delta update");

                cs
            } else {
                let Some(cs) = client_tracker.get_state(type_url) else {
                    return Ok(None);
                };

                tracing::debug!(kind = type_url, "sending delta for resource update");
                cs
            };

            let req = cfg
                .delta_discovery_request(cs)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;

            if req.resources.is_empty() && req.removed.is_empty() {
                return Ok(None);
            }

            let removed_resources = req.removed.iter().cloned().collect();

            match client_tracker.needs_ack(crate::config::AwaitingAck {
                type_url: type_url.into(),
                removed: req.removed,
                versions: req
                    .resources
                    .iter()
                    .map(|res| (res.name.clone(), res.version.clone()))
                    .collect(),
            }) {
                Ok(nonce) => {
                    let response = DeltaDiscoveryResponse {
                        resources: req.resources,
                        nonce: nonce.to_string(),
                        control_plane: Some(control_plane_id.clone()),
                        type_url: type_url.into(),
                        removed_resources,
                        // Only used for debugging, not really useful
                        system_version_info: String::new(),
                    };

                    tracing::trace!(
                        r#type = &*response.type_url,
                        nonce = &*response.nonce,
                        "delta discovery response"
                    );

                    Ok(Some(response))
                }
                Err(error) => {
                    tracing::error!(%error, "server implementation returned invalid delta response");
                    Err(tonic::Status::internal(error.to_string()))
                }
            }
        };

        let nid = node_id.clone();

        let response = {
            if message.type_url == "ignore-me" {
                tracing::debug!(id, client = nid, "initial delta response");
                DeltaDiscoveryResponse {
                    resources: Vec::new(),
                    nonce: String::new(),
                    control_plane: None,
                    type_url: message.type_url,
                    removed_resources: Vec::new(),
                    // Only used for debugging, not really useful
                    system_version_info: String::new(),
                }
            } else {
                tracing::debug!(client = %node_id, resource_type = %message.type_url, "initial delta response");

                let type_url = message.type_url.clone();
                responder(Some(message), &type_url, &mut client_tracker)?.unwrap()
            }
        };

        let stream = async_stream::try_stream! {
            yield response;

            loop {
                tokio::select! {
                    // The resource(s) have changed, inform the connected client, but only
                    // send the changed resources that the client doesn't already have
                    res = rx.recv() => {
                        match res {
                            Ok(rt) => {
                                match responder(None, rt, &mut client_tracker) {
                                    Ok(Some(res)) => yield res,
                                    Ok(None) => {}
                                    Err(error) => {
                                        tracing::error!(%error, "responder failed to generate response");
                                        continue;
                                    },
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                let tracked_resources: Vec<_> = client_tracker.tracked_resources().collect();
                                for rt in tracked_resources {
                                    yield responder(None, &rt, &mut client_tracker)?.unwrap();
                                }
                            }
                        }
                    }
                    client_request = streaming.next() => {
                        let client_request = match client_request.transpose() {
                            Ok(Some(value)) => value,
                            Ok(None) => break,
                            Err(error) => {
                                tracing::error!(%error, "error receiving delta response");
                                continue;
                            }
                        };

                        if client_request.type_url == "ignore-me" {
                            continue;
                        }

                        let id = client_request.node.as_ref().map(|node| node.id.as_str()).unwrap_or(node_id.as_str());

                        tracing::trace!(resource_type = client_request.type_url, "new delta message");

                        if let Some(error) = &client_request.error_detail {
                            metrics::nacks(id, &client_request.type_url).inc();
                            tracing::error!(nonce = %client_request.response_nonce, ?error, "NACK");
                        } else if let Ok(nonce) = uuid::Uuid::parse_str(&client_request.response_nonce) {
                            match client_tracker.apply_ack(nonce) {
                                Ok(()) => {
                                    tracing::trace!(%nonce, "ACK");
                                }
                                Err(error) => {
                                    tracing::error!(%nonce, %error, "failed to process client ack");
                                }
                            }

                            metrics::delta_discovery_requests(id, &client_request.type_url).inc();
                            continue;
                        }

                        let type_url = client_request.type_url.clone();

                        let Some(response) = responder(Some(client_request), &type_url, &mut client_tracker).unwrap() else { continue; };
                        yield response;
                    }
                }
            }

            tracing::info!("terminating stream");
        };

        Ok(Box::pin(stream.instrument(tracing::info_span!(
            "xds_server_stream",
            id,
            client = nid
        ))))
    }
}

#[tonic::async_trait]
impl<C: crate::config::Configuration> AggregatedDiscoveryService for ControlPlane<C> {
    type StreamAggregatedResourcesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, tonic::Status>> + Send>>;
    type DeltaAggregatedResourcesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryResponse, tonic::Status>> + Send>>;

    #[tracing::instrument(skip_all)]
    async fn stream_aggregated_resources(
        &self,
        _request: tonic::Request<tonic::Streaming<DiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::StreamAggregatedResourcesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "only delta streams are supported",
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn delta_aggregated_resources(
        &self,
        request: tonic::Request<tonic::Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::DeltaAggregatedResourcesStream>, tonic::Status> {
        Ok(tonic::Response::new(Box::pin(
            self.delta_aggregated_resources(request.into_inner())
                .in_current_span()
                .await?,
        )))
    }
}

#[tonic::async_trait]
impl<C: crate::config::Configuration> AggregatedControlPlaneDiscoveryService for ControlPlane<C> {
    type StreamAggregatedResourcesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<DiscoveryRequest, tonic::Status>> + Send>>;
    type DeltaAggregatedResourcesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryRequest, tonic::Status>> + Send>>;

    #[tracing::instrument(skip_all)]
    async fn stream_aggregated_resources(
        &self,
        _responses: tonic::Request<tonic::Streaming<DiscoveryResponse>>,
    ) -> Result<tonic::Response<Self::StreamAggregatedResourcesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "only delta streams are supported",
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn delta_aggregated_resources(
        &self,
        responses: tonic::Request<tonic::Streaming<DeltaDiscoveryResponse>>,
    ) -> Result<tonic::Response<Self::DeltaAggregatedResourcesStream>, tonic::Status> {
        let remote_addr = responses
            .remote_addr()
            .ok_or_else(|| tonic::Status::invalid_argument("no remote address available"))?;

        tracing::info!("control plane discovery delta stream attempt");
        let mut responses = responses.into_inner();
        let Some(identifier) = responses
            .next()
            .await
            .ok_or_else(|| tonic::Status::cancelled("received empty first response"))??
            .control_plane
            .map(|cp| cp.identifier)
        else {
            return Err(tonic::Status::invalid_argument(
                "DeltaDiscoveryResponse.control_plane.identifier is required in the first message",
            ));
        };

        tracing::info!(identifier, "new control plane delta discovery stream");
        let config = self.config.clone();
        let idle_request_interval = self.idle_request_interval;

        let (ds, mut request_stream) = super::client::DeltaClientStream::new();

        let _handle: tokio::task::JoinHandle<crate::Result<()>> = tokio::task::spawn(
            async move {
                tracing::info!(identifier, "sending initial delta discovery request");

                let local = Arc::new(crate::config::LocalVersions::new(
                    config.interested_resources().map(|(n, _)| n),
                ));

                ds.refresh(&identifier, config.interested_resources().collect(), &local)
                    .await
                    .map_err(|error| tonic::Status::internal(error.to_string()))?;

                let mut response_stream = crate::config::handle_delta_discovery_responses(
                    identifier.clone(),
                    responses,
                    config.clone(),
                    local.clone(),
                    Some(remote_addr),
                    None,
                );

                loop {
                    let next_response =
                        tokio::time::timeout(idle_request_interval, response_stream.next());

                    if let Ok(Some(ack)) = next_response.await {
                        tracing::trace!("sending ack request");
                        ds.send_response(ack?)
                            .await
                            .map_err(|_| tonic::Status::internal("this should not be reachable"))?;
                    } else {
                        tracing::trace!("exceeded idle interval, sending request");
                        ds.refresh(&identifier, config.interested_resources().collect(), &local)
                            .await
                            .map_err(|error| tonic::Status::internal(error.to_string()))?;
                    }
                }
            }
            .instrument(tracing::trace_span!("handle_delta_discovery_response")),
        );

        Ok(tonic::Response::new(Box::pin(async_stream::stream! {
            loop {
                let Some(req) = request_stream.recv().await else { break; };
                yield Ok(req);
            }
        })))
    }
}
