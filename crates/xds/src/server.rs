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

use std::{sync::Arc, time::Duration};

use futures::Stream;
use tokio_stream::StreamExt;
use tracing_futures::Instrument;

use crate::{
    ShutdownSignal,
    discovery::{
        DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
        aggregated_discovery_service_server::{
            AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
        },
    },
    generated::quilkin::relay::v1alpha1::aggregated_control_plane_discovery_service_server::{
        AggregatedControlPlaneDiscoveryService, AggregatedControlPlaneDiscoveryServiceServer,
    },
    metrics::{self, KIND_SERVER},
    net::TcpListener,
};

const FORWARDED: &str = "forwarded";
const X_FORWARDED_FOR: &str = "x-forwarded-for";
const ENVOY_EXTERNAL_ADDRESS: &str = "x-envoy-external-address";

/// Returns the true external address of a xDS client if available, checking for
/// if we are behind a proxy and need to parse the forwarded headers. The header
/// precendence is as follows:
///
/// - Forwarded
/// - X-Forwarded-For
/// - X-Envoy-External-Address
/// - Host
fn get_external_remote_addr<T>(request: &tonic::Request<T>) -> Option<std::net::IpAddr> {
    let metadata = request.metadata();
    let forwarded_for = metadata
        .get(FORWARDED)
        .and_then(|header| {
            header.to_str().ok().and_then(|value| {
                forwarded_header_value::ForwardedHeaderValue::from_forwarded(value).ok()
            })
        })
        .and_then(|header| header.remotest().forwarded_for_ip());

    forwarded_for
        .or_else(|| {
            metadata
                .get(X_FORWARDED_FOR)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(",").next().and_then(|value| value.parse().ok()))
        })
        .or_else(|| {
            metadata
                .get(ENVOY_EXTERNAL_ADDRESS)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
        })
        .or_else(|| request.remote_addr().map(|addr| addr.ip()))
        .map(|ip| ip.to_canonical())
}

/// How long a peer gets to complete a TLS handshake before its connection is
/// dropped, so a stalled one cannot hold a task open indefinitely.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The PEM a TLS listener is configured from.
pub struct TlsMaterials {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    /// Verify peer certificates against this PEM bundle.
    pub client_ca: Option<Vec<u8>>,
    /// Serve a peer that offers no certificate.
    pub client_auth_optional: bool,
}

/// The TLS configuration new connections are accepted with.
///
/// Held behind an `ArcSwap` so a rotated certificate reaches new connections
/// without rebinding the listener. A connection already established keeps the
/// configuration it was accepted with.
#[derive(Clone)]
pub struct TlsIdentity {
    config: Arc<arc_swap::ArcSwap<rustls::ServerConfig>>,
    requires_client_auth: bool,
}

impl TlsIdentity {
    pub fn new(materials: &TlsMaterials) -> eyre::Result<Self> {
        Ok(Self {
            // Whether a peer must authenticate comes from flags rather than
            // the files, so a reload cannot change it.
            requires_client_auth: materials.client_ca.is_some() && !materials.client_auth_optional,
            config: Arc::new(arc_swap::ArcSwap::from_pointee(server_config(materials)?)),
        })
    }

    /// Serves new connections with `materials`.
    ///
    /// The replacement is built before anything is swapped, so materials that
    /// do not make a usable configuration leave the running one in place.
    pub fn reload(&self, materials: &TlsMaterials) -> eyre::Result<()> {
        self.config.store(Arc::new(server_config(materials)?));
        Ok(())
    }

    /// Whether a peer has to present a certificate to be served at all, and so
    /// whether the identity it claims can be checked against one.
    pub fn requires_client_auth(&self) -> bool {
        self.requires_client_auth
    }

    /// The configuration new connections are currently accepted with.
    pub fn config(&self) -> Arc<rustls::ServerConfig> {
        self.config.load_full()
    }
}

fn server_config(materials: &TlsMaterials) -> eyre::Result<rustls::ServerConfig> {
    use eyre::Context as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};

    let certs = CertificateDer::pem_slice_iter(&materials.cert)
        .collect::<Result<Vec<_>, _>>()
        .context("failed to read the certificate chain")?;
    let key =
        PrivateKeyDer::from_pem_slice(&materials.key).context("failed to read the private key")?;

    let builder = rustls::ServerConfig::builder();
    let builder = match &materials.client_ca {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(ca) {
                roots
                    .add(cert.context("failed to read the client CA bundle")?)
                    .context("failed to trust a client CA certificate")?;
            }

            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
            let verifier = if materials.client_auth_optional {
                verifier.allow_unauthenticated()
            } else {
                verifier
            };

            builder.with_client_cert_verifier(
                verifier
                    .build()
                    .context("failed to build the client certificate verifier")?,
            )
        }
        None => builder.with_no_client_auth(),
    };

    let mut config = builder
        .with_single_cert(certs, key)
        .context("the certificate and key do not make a usable pair")?;

    // tonic serves gRPC over HTTP/2 only, and nothing else negotiates ALPN for
    // us now that the handshake is ours.
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(config)
}

/// Accepts TLS connections, yielding streams tonic reads connection info from.
///
/// Each handshake runs in its own task, so a peer that stalls partway through
/// one cannot hold up the accept loop. The configuration is read per
/// connection, which is what lets a rotated certificate take effect.
fn tls_incoming(
    listener: tokio::net::TcpListener,
    tls: TlsIdentity,
    mut shutdown: ShutdownSignal,
) -> impl Stream<Item = Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, std::io::Error>>
{
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                _ = shutdown.changed() => return,
            };

            let (stream, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(%error, "failed to accept a connection");
                    continue;
                }
            };

            let acceptor = tokio_rustls::TlsAcceptor::from(tls.config());
            let tx = tx.clone();

            tokio::spawn(async move {
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(stream)) => drop(tx.send(Ok(stream)).await),
                    Ok(Err(error)) => tracing::debug!(%peer, %error, "tls handshake failed"),
                    Err(_) => tracing::debug!(%peer, "tls handshake timed out"),
                }
            });
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// What a peer's certificate says it is.
///
/// `None` when client certificates are not required, which leaves the
/// identifier in the request self-asserted, as it is without them.
#[derive(Clone, Debug, Default)]
pub struct PeerIdentity(Option<Vec<String>>);

impl PeerIdentity {
    /// Reads the DNS names out of the peer's certificate chain.
    fn from_request<T>(request: &tonic::Request<T>, verify: bool) -> Self {
        if !verify {
            return Self(None);
        }

        let mut names = Vec::new();

        // Only the end entity certificate, which is what the peer proved it
        // holds the key for. The rest of the chain is its issuers.
        if let Some(leaf) = request.peer_certs().as_deref().and_then(|c| c.first())
            && let Ok((_, leaf)) = x509_parser::parse_x509_certificate(leaf)
            && let Ok(Some(san)) = leaf.subject_alternative_name()
        {
            names.extend(
                san.value
                    .general_names
                    .iter()
                    .filter_map(|name| match name {
                        x509_parser::extensions::GeneralName::DNSName(name) => {
                            Some((*name).to_owned())
                        }
                        _ => None,
                    }),
            );
        }

        Self(Some(names))
    }

    /// Checks the identifier a peer claims against its certificate.
    ///
    /// The identifier arrives in the request body, so without this a peer
    /// holding any certificate the CA signed could claim to be any node.
    fn verify(&self, claimed: &str) -> Result<(), tonic::Status> {
        let Some(names) = &self.0 else {
            return Ok(());
        };

        if names.iter().any(|name| name == claimed) {
            return Ok(());
        }

        tracing::warn!(
            %claimed,
            certificate = ?names,
            "rejecting a peer claiming an identifier its certificate is not valid for"
        );
        Err(tonic::Status::permission_denied(format!(
            "identifier `{claimed}` is not a DNS name in the peer certificate"
        )))
    }
}

const RESPONSE_PROPAGATION_INTERVAL: Duration = Duration::from_millis(100);

/// Slots in the change broadcast, which only has to outlast a client's gap
/// between drains. A client that overruns it widens to every resource type it
/// tracks, so the figure trades a little memory against that, and is not a
/// correctness bound.
const RESPONSE_BROADCAST_CAPACITY: usize = 128;

/// The resource types a stream re-checks on its next propagation tick.
///
/// Changes are coalesced rather than sent per change, to bound the network load
/// when cluster churn is high. Both buffers are reused across ticks, so a
/// settled stream allocates nothing.
#[derive(Default)]
struct ChangedTypes {
    narrowed: Vec<&'static str>,
    widened: Vec<String>,
    lagged: bool,
}

impl ChangedTypes {
    /// Records that `resource_type` changed.
    fn mark(&mut self, resource_type: &'static str) {
        if !self.narrowed.contains(&resource_type) {
            self.narrowed.push(resource_type);
        }
    }

    /// Records that some change was missed, which loses which types changed but
    /// not that they did.
    fn mark_all(&mut self) {
        self.lagged = true;
    }

    /// Takes the types to re-check, widening to everything `tracker` follows if
    /// a change was missed since the last call.
    ///
    /// The capture list keeps `tracker` out of the returned iterator, so it is
    /// free to be mutated again while the caller iterates.
    fn take<'a>(
        &'a mut self,
        tracker: &crate::config::ClientTracker,
    ) -> impl Iterator<Item = &'a str> + use<'a> {
        self.widened.clear();

        if std::mem::take(&mut self.lagged) {
            self.widened.extend(tracker.tracked_resources());
            self.narrowed.clear();
        }

        let narrowed = self.narrowed.drain(..).map(|rt| -> &str { rt });
        self.widened.iter().map(String::as_str).chain(narrowed)
    }
}

const VERSION_INFO: &str = "9";

pub struct ControlPlane<C> {
    pub config: Arc<C>,
    pub idle_request_interval: Duration,
    tx: tokio::sync::broadcast::Sender<&'static str>,
    pub shutdown: ShutdownSignal,
    /// Set when the server requires client certificates, which is what makes
    /// the identifier in a request checkable.
    verify_peer_identity: bool,
}

impl<C> Clone for ControlPlane<C> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            idle_request_interval: self.idle_request_interval,
            tx: self.tx.clone(),
            shutdown: self.shutdown.clone(),
            verify_peer_identity: self.verify_peer_identity,
        }
    }
}

impl<C: crate::config::Configuration> ControlPlane<C> {
    pub fn from_arc(
        config: Arc<C>,
        idle_request_interval: Duration,
        shutdown: ShutdownSignal,
    ) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(RESPONSE_BROADCAST_CAPACITY);

        Self {
            config,
            idle_request_interval,
            tx,
            shutdown,
            verify_peer_identity: false,
        }
    }

    fn server_builder() -> tonic::transport::Server {
        tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(crate::HTTP2_KEEPALIVE_INTERVAL))
            .http2_keepalive_timeout(Some(crate::HTTP2_KEEPALIVE_TIMEOUT))
    }

    pub fn management_server(
        mut self,
        listener: TcpListener,
        tls: Option<TlsIdentity>,
    ) -> eyre::Result<impl std::future::Future<Output = crate::Result<()>>> {
        self.verify_peer_identity = tls.as_ref().is_some_and(TlsIdentity::requires_client_auth);

        let accept_srx = self.shutdown.clone();
        let srx = self.shutdown.clone();
        tokio::spawn({
            let this = self.clone();
            self.config.on_changed(this, srx)
        });

        let mut shutdown = self.shutdown.clone();
        let server = AggregatedDiscoveryServiceServer::new(self)
            .max_encoding_message_size(crate::config::max_grpc_message_size());
        let server = Self::server_builder().add_service(server);
        tracing::info!("serving management server on port `{}`", listener.port());

        let stop = async move {
            drop(shutdown.changed().await);
        };

        Ok(async move {
            match tls {
                Some(tls) => {
                    server
                        .serve_with_incoming_shutdown(
                            tls_incoming(listener.into_tokio()?, tls, accept_srx),
                            stop,
                        )
                        .await
                }
                None => {
                    server
                        .serve_with_incoming_shutdown(listener.into_stream()?, stop)
                        .await
                }
            }
            .map_err(From::from)
        })
    }

    pub fn relay_server(
        mut self,
        listener: TcpListener,
        tls: Option<TlsIdentity>,
    ) -> eyre::Result<impl std::future::Future<Output = crate::Result<()>>> {
        self.verify_peer_identity = tls.as_ref().is_some_and(TlsIdentity::requires_client_auth);

        let accept_srx = self.shutdown.clone();
        let srx = self.shutdown.clone();
        tokio::spawn({
            let this = self.clone();
            self.config.on_changed(this, srx)
        });

        let mut shutdown = self.shutdown.clone();
        let server = AggregatedControlPlaneDiscoveryServiceServer::new(self)
            .max_encoding_message_size(crate::config::max_grpc_message_size());
        let server = Self::server_builder().add_service(server);
        tracing::info!("serving relay server on port `{}`", listener.port());

        let stop = async move {
            drop(shutdown.changed().await);
        };

        Ok(async move {
            match tls {
                Some(tls) => {
                    server
                        .serve_with_incoming_shutdown(
                            tls_incoming(listener.into_tokio()?, tls, accept_srx),
                            stop,
                        )
                        .await
                }
                None => {
                    server
                        .serve_with_incoming_shutdown(listener.into_stream()?, stop)
                        .await
                }
            }
            .map_err(From::from)
        })
    }

    #[inline]
    pub fn push_update(&self, resource_type: &'static str) {
        tracing::debug!(
            %resource_type,
            id = self.config.identifier(),
            "pushing update"
        );
        crate::metrics::actions_total(KIND_SERVER, "push").inc();
        if self.tx.send(resource_type).is_err() {
            tracing::debug!("no client connections currently subscribed");
        }
    }

    pub async fn delta_aggregated_resources<S>(
        &self,
        mut streaming: S,
        peer: PeerIdentity,
    ) -> Result<
        impl Stream<Item = Result<DeltaDiscoveryResponse, tonic::Status>> + Send + use<S, C>,
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

        peer.verify(&node_id)?;

        let mut rx = self.tx.subscribe();
        let id = self.config.identifier();

        tracing::debug!(
            id,
            client = node_id,
            count = self.tx.receiver_count(),
            "subscribed to config updates"
        );

        let control_plane = Some(crate::core::ControlPlane {
            identifier: id.clone(),
        });

        use crate::config::ClientTracker;
        let mut client_tracker = ClientTracker::track_client(node_id.clone());

        let responder_control_plane = control_plane.clone();
        let client = node_id.clone();
        let cfg = self.config.clone();
        let mut shutdown = self.shutdown.clone();

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
                        control_plane: responder_control_plane.clone(),
                        type_url: type_url.into(),
                        removed_resources,
                        system_version_info: VERSION_INFO.into(),
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
                    control_plane: control_plane.clone(),
                    type_url: message.type_url,
                    removed_resources: Vec::new(),
                    system_version_info: VERSION_INFO.into(),
                }
            } else {
                tracing::debug!(client = %node_id, resource_type = %message.type_url, "initial delta response");

                let type_url = message.type_url.clone();
                responder(Some(message), &type_url, &mut client_tracker)?.unwrap_or(
                    DeltaDiscoveryResponse {
                        resources: Vec::new(),
                        nonce: String::new(),
                        control_plane: control_plane.clone(),
                        type_url,
                        removed_resources: Vec::new(),
                        system_version_info: VERSION_INFO.into(),
                    },
                )
            }
        };

        let stream = async_stream::try_stream! {
            yield response;

            let mut changed = ChangedTypes::default();
            let mut propagation_interval = tokio::time::interval(RESPONSE_PROPAGATION_INTERVAL);
            propagation_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    // Inform the connected client if any of the resources have changed, but only
                    // send the changed resources that the client doesn't already have
                    _ = propagation_interval.tick() => {
                        let types = changed.take(&client_tracker);

                        for rt in types {
                            match responder(None, rt, &mut client_tracker) {
                                Ok(Some(res)) => yield res,
                                Ok(None) => {},
                                Err(error) => {
                                    crate::metrics::errors_total(KIND_SERVER, "respond").inc();
                                    tracing::error!(%error, "responder failed to generate response");
                                }
                            }
                        }
                    }
                    // A resource has changed, coalesce it until the next tick
                    res = rx.recv() => {
                        match res {
                            Ok(rt) => changed.mark(rt),
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            // Routine under backpressure, and self-correcting, so metered rather
                            // than logged
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                crate::metrics::actions_total(KIND_SERVER, "lagged").inc();
                                changed.mark_all();
                            },
                        }
                    }
                    client_request = streaming.next() => {
                        let client_request = match client_request.transpose() {
                            Ok(Some(value)) => value,
                            Ok(None) => break,
                            Err(error) => {
                                crate::metrics::errors_total(KIND_SERVER, "receive").inc();
                                tracing::error!(%error, "error receiving delta response");
                                continue;
                            }
                        };

                        if client_request.type_url == "ignore-me" {
                            continue;
                        }

                        let id = client_request.node.as_ref().map_or(node_id.as_str(), |node| node.id.as_str());

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
                                    crate::metrics::errors_total(KIND_SERVER, "ack").inc();
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
                    _ = shutdown.changed() => {
                        break;
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
        let peer = PeerIdentity::from_request(&request, self.verify_peer_identity);

        Ok(tonic::Response::new(Box::pin(
            self.delta_aggregated_resources(request.into_inner(), peer)
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
    type SubscribeDeltaResourcesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryResponse, tonic::Status>> + Send>>;

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
        let remote_addr = get_external_remote_addr(&responses)
            .ok_or_else(|| tonic::Status::invalid_argument("no remote address available"))?;
        let peer = PeerIdentity::from_request(&responses, self.verify_peer_identity);

        tracing::info!("control plane discovery delta stream attempt");
        let mut responses = responses.into_inner();

        fn handle_first_response(
            res: DeltaDiscoveryResponse,
        ) -> Result<(String, String), tonic::Status> {
            let Some(identifier) = res.control_plane.map(|cp| cp.identifier) else {
                return Err(tonic::Status::invalid_argument(
                    "DeltaDiscoveryResponse.control_plane.identifier is required in the first message",
                ));
            };

            Ok((identifier, res.system_version_info))
        }

        let first_response = responses
            .next()
            .await
            .ok_or_else(|| tonic::Status::cancelled("received empty first response"))??;

        let (identifier, server_version) = handle_first_response(first_response)?;
        peer.verify(&identifier)?;

        tracing::info!(identifier, "new control plane delta discovery stream");
        let config = self.config.clone();
        let mut shutdown = self.shutdown.clone();
        let idle_request_interval = self.idle_request_interval;

        let (ds, mut request_stream) = super::client::DeltaClientStream::new();

        let _handle: tokio::task::JoinHandle<crate::Result<()>> = tokio::task::spawn(
            async move {
                struct RemoveClient<'stack, C: crate::config::Configuration> {
                    config: &'stack C,
                    ip: std::net::IpAddr,
                }

                impl<C: crate::config::Configuration> Drop for RemoveClient<'_, C> {
                    fn drop(&mut self) {
                        self.config.client_disconnected(self.ip);
                    }
                }

                let _remove_client = RemoveClient {
                    config: config.as_ref(),
                    ip: remote_addr,
                };

                tracing::info!(identifier, "sending initial delta discovery request");

                let local = Arc::new(crate::config::LocalVersions::new(
                    config.interested_resources(&server_version).map(|(n, _)| n),
                ));

                ds.refresh(
                    &identifier,
                    config.interested_resources(&server_version).collect(),
                    &local,
                )
                .map_err(|error| tonic::Status::internal(error.to_string()))?;

                let mut response_stream = crate::config::handle_delta_discovery_responses(
                    identifier.clone(),
                    responses,
                    config.clone(),
                    local.clone(),
                    Some(remote_addr),
                    None,
                );

                let res = loop {
                    let next_response =
                        tokio::time::timeout(idle_request_interval, response_stream.next());

                    tokio::select! {
                        nr = next_response => {
                            match nr {
                                Ok(Some(Ok(ack))) => {
                                    tracing::trace!("sending ack request");
                                    ds.ack_response(ack).map_err(|_err| {
                                        crate::metrics::errors_total(KIND_SERVER, "ack_failed").inc();
                                        tonic::Status::internal("this should not be reachable")
                                    })?;
                                }
                                Ok(Some(Err(error))) => {
                                    if crate::is_broken_pipe(&error) {
                                        break Ok(());
                                    } else {
                                        break Err(eyre::eyre!(error));
                                    }
                                }
                                Ok(None) => {
                                    break Ok(());
                                }
                                Err(_) => {
                                    tracing::trace!("exceeded idle interval, sending request");
                                    ds.refresh(
                                        &identifier,
                                        config.interested_resources(&server_version).collect(),
                                        &local,
                                    )
                                    .map_err(|error| tonic::Status::internal(error.to_string()))?;
                                }
                            }
                        }
                        _ = shutdown.changed() => {
                            break Ok(());
                        }
                    }
                };

                if res.is_ok() {
                    tracing::info!("xds stream terminated");
                }

                res
            }
            .instrument(tracing::trace_span!("handle_delta_discovery_response")),
        );

        Ok(tonic::Response::new(Box::pin(async_stream::stream! {
            loop {
                let Some(req) = request_stream.recv().await else {
                    break;
                };
                yield Ok(req);
            }
        })))
    }

    async fn subscribe_delta_resources(
        &self,
        requests: tonic::Request<tonic::Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeDeltaResourcesStream>, tonic::Status> {
        let peer = PeerIdentity::from_request(&requests, self.verify_peer_identity);

        Ok(tonic::Response::new(Box::pin(
            self.delta_aggregated_resources(requests.into_inner(), peer)
                .in_current_span()
                .await?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker(types: &[&str]) -> crate::config::ClientTracker {
        let mut tracker = crate::config::ClientTracker::track_client("node".into());
        for rt in types {
            tracker.track_state((*rt).into());
        }
        tracker
    }

    fn sorted<'a>(types: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
        let mut types: Vec<_> = types.collect();
        types.sort_unstable();
        types
    }

    /// A missed change says nothing about which types it was, so the stream has
    /// to re-check all of them or the client never hears about the one it lost.
    #[test]
    fn a_missed_change_widens_to_every_tracked_type() {
        let tracker = tracker(&["cluster", "datacenter", "filter"]);
        let mut changed = ChangedTypes::default();

        changed.mark("cluster");
        changed.mark_all();

        assert_eq!(
            ["cluster", "datacenter", "filter"].as_slice(),
            sorted(changed.take(&tracker)),
        );
        // Widening is spent once taken, not sticky
        changed.mark("cluster");
        assert_eq!(["cluster"].as_slice(), sorted(changed.take(&tracker)));
    }

    /// Repeats within one interval are the common case under churn, and cost a
    /// response each if they aren't coalesced.
    #[test]
    fn repeated_changes_are_coalesced_and_taken_once() {
        let tracker = tracker(&["cluster", "datacenter"]);
        let mut changed = ChangedTypes::default();

        changed.mark("cluster");
        changed.mark("datacenter");
        changed.mark("cluster");

        assert_eq!(
            ["cluster", "datacenter"].as_slice(),
            sorted(changed.take(&tracker))
        );
        assert_eq!(0, changed.take(&tracker).count());
    }

    #[test]
    fn get_external_address() {
        let mut request = tonic::Request::new("");

        assert_eq!(None, get_external_remote_addr(&request));

        request
            .extensions_mut()
            .insert(tonic::transport::server::TcpConnectInfo {
                local_addr: None,
                remote_addr: Some((std::net::Ipv4Addr::LOCALHOST, 8888).into()),
            });

        assert_eq!(
            Some(std::net::Ipv4Addr::LOCALHOST.into()),
            get_external_remote_addr(&request)
        );

        request
            .metadata_mut()
            .insert(ENVOY_EXTERNAL_ADDRESS, "127.0.0.2".parse().unwrap());
        assert_eq!(
            Some([127, 0, 0, 2].into()),
            get_external_remote_addr(&request)
        );

        request
            .metadata_mut()
            .insert(ENVOY_EXTERNAL_ADDRESS, "::ffff:127.0.0.2".parse().unwrap());
        assert_eq!(
            Some([127, 0, 0, 2].into()),
            get_external_remote_addr(&request)
        );

        request.metadata_mut().insert(
            X_FORWARDED_FOR,
            "127.0.0.3,255.255.255.255".parse().unwrap(),
        );
        assert_eq!(
            Some([127, 0, 0, 3].into()),
            get_external_remote_addr(&request)
        );

        request.metadata_mut().insert(
            FORWARDED,
            "for=127.0.0.4,for=255.255.255.255".parse().unwrap(),
        );
        assert_eq!(
            Some([127, 0, 0, 4].into()),
            get_external_remote_addr(&request)
        );
    }
}
