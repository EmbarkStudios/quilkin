use crate::discovery::{DeltaDiscoveryRequest, DeltaDiscoveryResponse, Resource};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

pub(crate) const BACKOFF_INITIAL_DELAY: Duration = Duration::from_millis(500);
pub(crate) const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(30);
pub(crate) const BACKOFF_MAX_JITTER: Duration = Duration::from_millis(2000);
pub(crate) const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns the configured maximum allowed message size for gRPC messages.
/// When using State Of The World xDS, the message size can get large enough
/// that it can exceed the default limits.
pub fn max_grpc_message_size() -> usize {
    std::env::var("QUILKIN_MAX_GRPC_MESSAGE_SIZE")
        .as_deref()
        .ok()
        .and_then(|var| var.parse().ok())
        .unwrap_or(256 * 1024 * 1024)
}

pub type VersionMap = HashMap<String, String>;
pub type TypeUrl = &'static str;

/// Keeps tracking of the local versions of each resource sent from the management
/// server, allowing reconnections to the same/new management servers to send initial
/// versions to reduce the initial response size
pub struct LocalVersions {
    versions: Vec<(TypeUrl, parking_lot::Mutex<VersionMap>)>,
}

impl LocalVersions {
    #[inline]
    pub fn new(types: impl Iterator<Item = TypeUrl>) -> Self {
        Self {
            versions: types.map(|ty| (ty, Default::default())).collect(),
        }
    }

    #[inline]
    pub fn get(&self, ty: &str) -> parking_lot::MutexGuard<'_, VersionMap> {
        let g = self
            .versions
            .iter()
            .find_map(|(t, hm)| (*t == ty).then_some(hm));

        if let Some(ml) = g {
            ml.lock()
        } else {
            let versions = self.versions.iter().map(|(ty, _)| *ty).collect::<Vec<_>>();
            panic!("unable to retrieve `{ty}` versions, available versions are {versions:?}");
        }
    }
}

pub struct ClientState {
    pub resource_type: String,
    pub versions: VersionMap,
    pub subscribed: BTreeSet<String>,
}

impl ClientState {
    pub fn new(resource_type: String) -> Self {
        Self {
            resource_type,
            versions: Default::default(),
            subscribed: Default::default(),
        }
    }

    pub fn reset(&mut self, versions: VersionMap) {
        drop(std::mem::replace(&mut self.versions, versions));
        self.subscribed.clear();
    }

    pub fn update(&mut self, req: DeltaDiscoveryRequest) {
        // If the request has filled out the initial_versions field, it means the connected management servers has
        // already had a connection with a control plane, so hard reset our state to what it says it has
        if !req.initial_resource_versions.is_empty() {
            self.reset(req.initial_resource_versions);
        }

        // From the spec:
        // A resource_names_subscribe field may contain resource names that
        // the server believes the client is already subscribed to, and
        // furthermore has the most recent versions of. However, the server
        // must still provide those resources in the response; due to
        // implementation details hidden from the server, the client may
        // have “forgotten” those resources despite apparently remaining subscribed.
        if !req.resource_names_subscribe.is_empty() {
            for sub in req.resource_names_subscribe {
                self.versions.remove(&sub);
                self.subscribed.insert(sub);
            }
        }

        if !req.resource_names_unsubscribe.is_empty() {
            for sub in req.resource_names_unsubscribe {
                self.versions.remove(&sub);
                self.subscribed.remove(&sub);
            }
        }
    }

    pub fn version_matches(&self, key: &str, value: &str) -> bool {
        self.versions
            .get(key)
            .map(|v| *v == value)
            .unwrap_or_default()
    }
}

pub struct AwaitingAck {
    pub type_url: String,
    pub versions: HashMap<String, String>,
    pub removed: std::collections::HashSet<String>,
}

pub struct ClientTracker {
    pub node_id: String,
    ack_map: HashMap<uuid::Uuid, AwaitingAck>,
    states: HashMap<String, ClientState>,
}

impl ClientTracker {
    pub fn track_client(node_id: String) -> Self {
        Self {
            node_id,
            ack_map: Default::default(),
            states: Default::default(),
        }
    }

    pub fn get_state(&mut self, rt: &str) -> Option<&mut ClientState> {
        self.states.get_mut(rt)
    }

    pub fn track_state(&mut self, rt: String) -> &mut ClientState {
        self.states
            .entry(rt.clone())
            .or_insert_with(|| ClientState::new(rt))
    }

    pub fn tracked_resources(&self) -> impl Iterator<Item = String> + '_ {
        self.states.keys().cloned()
    }

    pub fn needs_ack(&mut self, ack: AwaitingAck) -> eyre::Result<uuid::Uuid> {
        // Validate that no items are both updated/added and removed
        for rem in &ack.removed {
            eyre::ensure!(
                !ack.versions.contains_key(rem),
                "{rem} is both in the removed list and version map"
            );
        }

        let uuid = uuid::Uuid::new_v4();
        self.ack_map.insert(uuid, ack);
        Ok(uuid)
    }

    pub fn apply_ack(&mut self, uuid: uuid::Uuid) -> eyre::Result<()> {
        let Some(ack_state) = self.ack_map.remove(&uuid) else {
            eyre::bail!("unknown nonce");
        };
        let Some(cs) = self.get_state(&ack_state.type_url) else {
            eyre::bail!("unknown type url");
        };

        for removed in ack_state.removed {
            cs.subscribed.remove(&removed);
            cs.versions.remove(&removed);
        }

        cs.versions.extend(ack_state.versions);
        Ok(())
    }
}

pub trait Configuration: Send + Sync + Sized + 'static {
    fn identifier(&self) -> String;

    /// Returns whether the current instance is considered the leader of a set
    /// of replicas, if leader election is enabled in a config provider.
    fn is_leader(&self) -> Option<bool>;

    fn apply_delta(
        &self,
        resource_type: &str,
        resources: Vec<Resource>,
        removed_resources: &[String],
        remote_addr: Option<std::net::IpAddr>,
    ) -> crate::Result<()>;

    fn allow_request_processing(&self, resource_type: &str) -> bool;

    fn delta_discovery_request(
        &self,
        client_state: &ClientState,
    ) -> crate::Result<DeltaDiscoveryRes>;

    fn on_changed(
        &self,
        subscribed: crate::server::ControlPlane<Self>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static;

    fn interested_resources(
        &self,
        server_version: &str,
    ) -> impl Iterator<Item = (&'static str, Vec<String>)>;

    /// Called when the remote endpoint disconnects from this server
    fn client_disconnected(&self, ip: std::net::IpAddr);
}

pub struct DeltaDiscoveryRes {
    pub resources: Vec<crate::discovery::Resource>,
    pub removed: std::collections::HashSet<String>,
}

/// Processes responses from management servers, applying resources to the proxy
#[tracing::instrument(skip_all, fields(identifier))]
pub fn handle_delta_discovery_responses<C: Configuration>(
    identifier: String,
    stream: impl futures::Stream<Item = tonic::Result<DeltaDiscoveryResponse>> + 'static + Send,
    config: Arc<C>,
    local: Arc<LocalVersions>,
    remote_addr: Option<std::net::IpAddr>,
    mut notifier: Option<tokio::sync::mpsc::UnboundedSender<String>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = tonic::Result<DeltaDiscoveryRequest>> + Send>> {
    Box::pin(async_stream::try_stream! {
        let _stream_metrics = crate::metrics::StreamConnectionMetrics::new(identifier.clone());
        tracing::trace!("awaiting delta response");
        for await response in stream
        {
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    yield Err(error)?;
                    break;
                }
            };

            if response.type_url == "ignore-me" {
                continue;
            }

            let control_plane_identifier = response.control_plane.as_ref().map(|cp| cp.identifier.as_str()).unwrap_or_default();

            crate::metrics::delta_discovery_responses(control_plane_identifier, &response.type_url).inc();
            tracing::trace!(
                version = &*response.system_version_info,
                r#type = &*response.type_url,
                nonce = &*response.nonce,
                "received delta response"
            );

            let type_url = response.type_url;

            let result = {
                tracing::trace!(num_resources = response.resources.len(), kind = type_url, "applying delta resources");

                let version_map: Vec<_> = response.resources.iter().map(|res| (res.name.clone(), res.version.clone())).collect();

                let res = config.apply_delta(&type_url, response.resources, &response.removed_resources, remote_addr);

                if res.is_ok() {
                    let mut lock = local.get(&type_url);

                    // Remove any resources the upstream server has removed/doesn't have,
                    // we do this before applying any new/updated resources in case a
                    // resource is in both lists, though really that would be a bug in
                    // the upstream server
                    for removed in response.removed_resources {
                        lock.remove(&removed);
                    }

                    for (k, v) in version_map {
                        lock.insert(k, v);
                    }
                }

                res
            };

            if let Some(note) = &notifier {
                if note.send(type_url.clone()).is_err() {
                    notifier = None;
                }
            }

            let error_detail = if let Err(error) = result {
                crate::metrics::nacks(control_plane_identifier, &type_url).inc();
                Some(quilkin_proto::generated::google::rpc::Status {
                    code: 3,
                    message: error.to_string(),
                    ..Default::default()
                })
            } else {
                crate::metrics::acks(control_plane_identifier, &type_url).inc();
                None
            };

            yield DeltaDiscoveryRequest {
                type_url,
                response_nonce: response.nonce,
                error_detail,
                ..Default::default()
            }
        }
    })
}
