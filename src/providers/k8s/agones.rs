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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures::TryStreamExt;
use k8s_openapi::{
    api::core::v1::NodeAddress,
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceDefinition, CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, CustomResourceValidation,
    },
    apimachinery::pkg::{apis::meta::v1::ObjectMeta, util::intstr::IntOrString},
};
use kube::core::Resource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::net::endpoint::{Endpoint, Locality};

const QUILKIN_TOKEN_LABEL: &str = "quilkin.dev/tokens";

pub async fn watch(
    gameservers_namespace: String,
    config_namespace: Option<String>,
    health_check: Arc<AtomicBool>,
    locality: Option<Locality>,
    filters: crate::config::filter::FilterChainConfig,
    clusters: crate::config::Watch<crate::net::ClusterMap>,
    address_selector: Option<crate::config::AddressSelector>,
) -> crate::Result<()> {
    let client = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        kube::Client::try_default(),
    )
    .await??;

    let mut configmap_reflector: std::pin::Pin<Box<dyn futures::Stream<Item = _> + Send>> =
        if let Some(cns) = config_namespace {
            Box::pin(super::update_filters_from_configmap(
                client.clone(),
                cns,
                filters,
            ))
        } else {
            Box::pin(futures::stream::pending())
        };

    let gameserver_reflector = super::update_endpoints_from_gameservers(
        client,
        gameservers_namespace,
        clusters,
        locality,
        address_selector,
    );

    tokio::pin!(gameserver_reflector);

    loop {
        let result = tokio::select! {
            result = configmap_reflector.try_next() => result,
            result = gameserver_reflector.try_next() => result,
            _ = tokio::time::sleep(crate::providers::NO_UPDATE_INTERVAL) => {
                tracing::trace!(duration_secs=crate::providers::NO_UPDATE_INTERVAL.as_secs_f64(), "no updates from gameservers or configmap");
                Ok(Some(()))
            }
        };

        match result
            .and_then(|opt| opt.ok_or_else(|| eyre::eyre!("kubernetes watch stream terminated")))
        {
            Ok(_) => {
                crate::metrics::k8s::active(true);
                health_check.store(true, Ordering::SeqCst);
            }
            Err(error) => break Err(error),
        }
    }
}

/// Auto-generated derived type for [`GameServerSpec`] via `CustomResource`
#[derive(Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GameServer {
    #[schemars(skip)]
    pub metadata: ObjectMeta,
    pub spec: GameServerSpec,
    pub status: Option<GameServerStatus>,
}

impl GameServer {
    pub fn endpoint(
        &self,
        address_selector: Option<&crate::config::AddressSelector>,
    ) -> Option<Endpoint> {
        self.status.as_ref().and_then(|status| {
            let port = status
                .ports
                .as_ref()
                .and_then(|ports| ports.first().map(|status| status.port))
                .unwrap_or_default();

            let tokens = self.tokens();
            let extra_metadata = {
                let mut map = serde_json::Map::default();
                map.insert(
                    "name".into(),
                    self.metadata.name.clone().unwrap_or_default().into(),
                );
                map
            };

            let address = if let Some(ads) = address_selector {
                status.addresses.iter().find_map(|adr| {
                    if adr.type_ != ads.name {
                        return None;
                    }

                    use crate::config::AddrKind;
                    match ads.kind {
                        AddrKind::Any => Some(adr.address.clone()),
                        AddrKind::Ipv4 => (!adr.address.contains(':')).then(|| adr.address.clone()),
                        AddrKind::Ipv6 => adr.address.contains(':').then(|| adr.address.clone()),
                    }
                })?
            } else {
                status.address.clone()
            };

            let ep = Endpoint::with_metadata(
                (address, port).into(),
                crate::net::endpoint::metadata::MetadataView::with_unknown(
                    crate::net::endpoint::Metadata { tokens },
                    extra_metadata,
                ),
            );

            Some(ep)
        })
    }

    #[inline]
    fn tokens(&self) -> quilkin_types::TokenSet {
        self.metadata
            .annotations
            .as_ref()
            .and_then(|anno| {
                anno.get(QUILKIN_TOKEN_LABEL).map(|value| {
                    value
                        .split(',')
                        .map(crate::codec::base64::decode)
                        .filter_map(Result::ok)
                        .collect()
                })
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Inner {
    #[schemars(skip)]
    metadata: ObjectMeta,
    spec: GameServerSpec,
    status: Option<GameServerStatus>,
}

impl<'de> serde::Deserialize<'de> for GameServer {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(de).unwrap();

        serde_json::from_value::<Inner>(value.clone())
            .map_err(|error| {
                tracing::trace!(%error, %value, "gameserver failed");
                Error::custom(error)
            })
            .map(
                |Inner {
                     metadata,
                     spec,
                     status,
                 }| Self {
                    metadata,
                    spec,
                    status,
                },
            )
    }
}

impl GameServer {
    pub fn new(name: &str, spec: GameServerSpec) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    pub fn is_allocated(&self) -> bool {
        self.status.as_ref().is_some_and(|status| {
            tracing::trace!(?status.addresses, ?status.state, "checking gameserver");
            matches!(status.state, GameServerState::Allocated)
        })
    }
}

impl serde::Serialize for GameServer {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut obj = ser.serialize_struct("GameServer", 5)?;
        obj.serialize_field("apiVersion", &GameServer::api_version(&()))?;
        obj.serialize_field("kind", &GameServer::kind(&()))?;
        obj.serialize_field("metadata", &self.metadata)?;
        obj.serialize_field("spec", &self.spec)?;
        obj.serialize_field("status", &self.status)?;
        obj.end()
    }
}

impl Resource for GameServer {
    type DynamicType = ();
    type Scope = kube::core::NamespaceResourceScope;

    fn group(_: &()) -> std::borrow::Cow<'_, str> {
        "agones.dev".into()
    }

    fn kind(_: &()) -> std::borrow::Cow<'_, str> {
        "GameServer".into()
    }

    fn version(_: &()) -> std::borrow::Cow<'_, str> {
        "v1".into()
    }

    fn api_version(_: &()) -> std::borrow::Cow<'_, str> {
        "agones.dev/v1".into()
    }

    fn plural(_: &()) -> std::borrow::Cow<'_, str> {
        "gameservers".into()
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl kube::core::crd::v1::CustomResourceExt for GameServer {
    fn crd() -> CustomResourceDefinition {
        let open_api_v3_schema = Some(
            schemars::r#gen::SchemaSettings::openapi3()
                .with(|s| {
                    s.inline_subschemas = true;
                    s.meta_schema = None;
                })
                .with_visitor(kube_core::schema::StructuralSchemaRewriter)
                .into_generator()
                .into_root_schema_for::<Self>(),
        );

        CustomResourceDefinition {
            metadata: ObjectMeta {
                name: Some("gameservers.agones.dev".into()),
                ..<_>::default()
            },
            spec: CustomResourceDefinitionSpec {
                group: "agones.dev".into(),
                scope: "Namespaced".into(),
                names: CustomResourceDefinitionNames {
                    plural: "gameservers".into(),
                    singular: Some("gameserver".into()),
                    kind: "GameServer".into(),
                    ..<_>::default()
                },
                versions: vec![CustomResourceDefinitionVersion {
                    name: "v1".into(),
                    served: true,
                    storage: true,
                    schema: Some(CustomResourceValidation {
                        // Hack to get around k8s and schemars having different
                        // root JSON schema types. Replace this with a From impl
                        open_api_v3_schema: serde_json::from_value(
                            serde_json::to_value(&open_api_v3_schema).unwrap(),
                        )
                        .unwrap(),
                    }),
                    ..<_>::default()
                }],
                ..<_>::default()
            },
            status: None,
        }
    }

    fn crd_name() -> &'static str {
        "gameservers.agones.dev"
    }

    fn api_resource() -> kube::core::dynamic::ApiResource {
        kube::core::dynamic::ApiResource::erase::<Self>(&())
    }

    fn shortnames() -> &'static [&'static str] {
        &[]
    }
}

impl kube::core::object::HasSpec for GameServer {
    type Spec = GameServerSpec;

    fn spec(&self) -> &GameServerSpec {
        &self.spec
    }
    fn spec_mut(&mut self) -> &mut GameServerSpec {
        &mut self.spec
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::de::Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GameServerSpec {
    /// Container specifies which Pod container is the game server. Only
    /// required if there is more than one container defined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// Ports are the array of ports that can be exposed via the game server
    #[serde(deserialize_with = "deserialize_null_default")]
    pub ports: Vec<GameServerPort>,
    /// Configures health checking
    pub health: Health,
    /// Scheduling strategy. Defaults to "Packed"
    pub scheduling: SchedulingStrategy,
    /// Specifies parameters for the Agones SDK Server sidecar container.
    pub sdk_server: SdkServer,
    /// Describes the Pod that will be created for the [`GameServer`].
    pub template: k8s_openapi::api::core::v1::PodTemplateSpec,
}

impl Default for GameServerSpec {
    fn default() -> Self {
        Self {
            container: None,
            ports: vec![],
            health: Default::default(),
            scheduling: SchedulingStrategy::Packed,
            sdk_server: Default::default(),
            template: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Health {
    /// Whether health checking is disabled or not
    #[serde(default)]
    disabled: bool,
    /// The number of seconds each health ping has to occur in
    #[serde(rename = "periodSeconds", default = "default_period_seconds")]
    period_seconds: i32,
    /// How many failures in a row constitutes unhealthy
    #[serde(rename = "failureThreshold", default = "default_failure_threshold")]
    failure_threshold: i32,
    /// Initial delay before checking health
    #[serde(
        rename = "initialDelaySeconds",
        default = "default_initial_delay_seconds"
    )]
    initial_delay_seconds: i32,
}

fn default_period_seconds() -> i32 {
    5
}
fn default_initial_delay_seconds() -> i32 {
    5
}
fn default_failure_threshold() -> i32 {
    5
}

impl Default for Health {
    fn default() -> Self {
        Self {
            disabled: false,
            period_seconds: default_period_seconds(),
            failure_threshold: default_failure_threshold(),
            initial_delay_seconds: default_failure_threshold(),
        }
    }
}

/// Defines a set of Ports that are to be exposed via the [`GameServer`].
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GameServerPort {
    /// Name is the descriptive name of the port
    pub name: String,
    /// Defines the policy for how the [`Self::host_port`] is populated.
    ///
    /// - Dynamic port will allocate a `HostPort` within the selected `MIN_PORT`
    ///   and `MAX_PORT` range passed to the controller at installation time.
    /// - When `Static` portPolicy is specified, `HostPort` is required, to
    ///   specify the port that game clients will connect to
    #[serde(default)]
    pub port_policy: PortPolicy,
    /// The name of the container on which to open the port. Defaults to the
    /// game server container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// The port that is being opened on the specified container's process
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_port: Option<u16>,
    /// The port exposed on the host for clients to connect to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_port: Option<u16>,
    /// Protocol is the network protocol being used. Defaults to UDP. TCP and TCPUDP are other options.
    #[serde(default)]
    pub protocol: Protocol,
}

/// The status for a [`GameServer`] resource.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GameServerStatus {
    /// The current state of a [`GameServer`].
    pub state: GameServerState,
    pub ports: Option<Vec<GameServerStatusPort>>,
    pub address: String,
    #[serde(deserialize_with = "deserialize_null_default")]
    pub addresses: Vec<NodeAddress>,
    pub node_name: String,
    pub reserved_until: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum GameServerState {
    /// A dynamically allocating [`GameServer`] is being created, an open port needs
    /// to be allocated
    PortAllocation,
    /// The Pod for the [`GameServer`] is being created.
    Creating,
    /// The Pods for the [`GameServer`] are being created but are not yet Scheduled
    Starting,
    /// We have determined that the Pod has been scheduled in the cluster --
    /// basically, we have a `NodeName`
    Scheduled,
    /// The [`GameServer`] has declared that it is ready
    RequestReady,
    /// The [`GameServer`] is ready to take connections from game clients.
    Ready,
    /// The [`GameServer`] has shutdown and everything needs to be deleted from the cluster
    Shutdown,
    /// Something has gone wrong with the [`GameServer`] and it cannot be resolved
    Error,
    /// The [`GameServer`] has failed its health checks
    Unhealthy,
    /// The [`GameServer`] is reserved and therefore can be allocated but not removed
    Reserved,
    /// The [`GameServer`] has been allocated to a session
    Allocated,
}

/// The port that was allocated to a [`GameServer`].
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct GameServerStatusPort {
    pub name: String,
    pub port: u16,
}

/// Parameters for the Agones SDK Server sidecar container
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkServer {
    /// The log level for SDK server (sidecar) logs. Defaults to [`SdkServerLogLevel::Info`]
    #[serde(default)]
    pub log_level: SdkServerLogLevel,
    /// The port on which the SDK Server binds the gRPC server to accept incoming connections
    #[serde(default = "default_sdk_grpc_port")]
    pub grpc_port: u16,
    /// The port on which the SDK Server binds the HTTP gRPC gateway server to accept incoming connections
    #[serde(default = "default_sdk_http_port")]
    pub http_port: u16,
}

fn default_sdk_grpc_port() -> u16 {
    9357
}
fn default_sdk_http_port() -> u16 {
    9358
}

impl Default for SdkServer {
    fn default() -> Self {
        Self {
            log_level: Default::default(),
            grpc_port: default_sdk_grpc_port(),
            http_port: default_sdk_http_port(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum SdkServerLogLevel {
    /// Output all messages except for debug messages.
    Info,
    /// Output all messages including debug messages.
    Debug,
    /// Only output error messages.
    Error,
}

impl Default for SdkServerLogLevel {
    fn default() -> Self {
        Self::Info
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum PortPolicy {
    /// The user defines the host port to be used in the configuration.
    Static,
    /// The system will choose an open port for the [`GameServer`] in question.
    Dynamic,
    /// Dynamically sets the container port to the same value as the dynamically
    /// selected host port. This will mean that users will need to lookup what
    /// port has been opened through the server side SDK.
    Passthrough,
    None,
}

impl Default for PortPolicy {
    fn default() -> Self {
        Self::Dynamic
    }
}

/// The strategy that a [`Fleet`] & [`GameServer`]s will use when scheduling
/// [`GameServer`]s' Pods across a cluster. In future versions, this will also
/// impact Fleet scale down, and Pod Scheduling.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum SchedulingStrategy {
    /// Prioritise allocating [`GameServer`]s on Nodes with the most Allocated, and
    /// then Ready [`GameServer`]s to bin pack as many Allocated [`GameServer`]s on a
    /// single node.  This is most useful for dynamic Kubernetes clusters - such
    /// as on Cloud Providers.
    Packed,
    /// Prioritise allocating [`GameServer`]s on Nodes with the least Allocated, and
    /// then Ready [`GameServer`]s to distribute Allocated [`GameServer`]s across many
    /// nodes. This is most useful for statically sized Kubernetes clusters -
    /// such as on physical hardware.
    Distributed,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum Protocol {
    #[serde(rename = "UDP")]
    Udp,
    #[serde(rename = "TCP")]
    Tcp,
    #[serde(rename = "TCPUDP")]
    UdpTcp,
}

impl Default for Protocol {
    fn default() -> Self {
        Self::Udp
    }
}

#[derive(Clone, Debug, JsonSchema)]
pub struct Fleet {
    #[schemars(skip)]
    pub metadata: ::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    pub spec: FleetSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<FleetStatus>,
}

impl Fleet {
    /// Spec based constructor for derived custom resource
    pub fn new(name: &str, spec: FleetSpec) -> Self {
        Self {
            metadata: ::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct FleetInner {
    #[schemars(skip)]
    metadata: ObjectMeta,
    spec: FleetSpec,
    status: Option<FleetStatus>,
}

impl<'de> serde::Deserialize<'de> for Fleet {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(de).unwrap();

        serde_json::from_value::<FleetInner>(value.clone())
            .map_err(|error| {
                tracing::trace!(%error, %value, "fleet failed");
                Error::custom(error)
            })
            .map(
                |FleetInner {
                     metadata,
                     spec,
                     status,
                 }| Self {
                    metadata,
                    spec,
                    status,
                },
            )
    }
}

impl serde::Serialize for Fleet {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut obj = ser.serialize_struct("Fleet", 5)?;
        obj.serialize_field("apiVersion", &Fleet::api_version(&()))?;
        obj.serialize_field("kind", &Fleet::kind(&()))?;
        obj.serialize_field("metadata", &self.metadata)?;
        obj.serialize_field("spec", &self.spec)?;
        obj.serialize_field("status", &self.status)?;
        obj.end()
    }
}

impl ::kube::core::Resource for Fleet {
    type DynamicType = ();
    type Scope = ::kube::core::NamespaceResourceScope;
    fn group(_: &()) -> std::borrow::Cow<'_, str> {
        "agones.dev".into()
    }
    fn kind(_: &()) -> std::borrow::Cow<'_, str> {
        "Fleet".into()
    }
    fn version(_: &()) -> std::borrow::Cow<'_, str> {
        "v1".into()
    }
    fn api_version(_: &()) -> std::borrow::Cow<'_, str> {
        "agones.dev/v1".into()
    }
    fn plural(_: &()) -> std::borrow::Cow<'_, str> {
        "fleets".into()
    }
    fn meta(&self) -> &::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        &mut self.metadata
    }
}

impl ::kube::core::crd::v1::CustomResourceExt for Fleet {
    fn crd() -> ::k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition{
        let columns: Vec<
            ::k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceColumnDefinition,
        > = ::serde_json::from_str("[  ]").expect("valid printer column json");
        let scale: Option<
            ::k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceSubresourceScale,
        > = None;
        let categories: Vec<String> = ::serde_json::from_str("[]").expect("valid categories");
        let shorts: Vec<String> = ::serde_json::from_str("[]").expect("valid shortnames");
        let subres = if true {
            if scale.is_some() {
                ::serde_json::Value::Object({
                    let mut object = ::serde_json::Map::new();
                    object.insert(
                        ("status").into(),
                        ::serde_json::Value::Object(::serde_json::Map::new()),
                    );
                    object.insert(("scale").into(), ::serde_json::to_value(&scale).unwrap());
                    object
                })
            } else {
                ::serde_json::Value::Object({
                    let mut object = ::serde_json::Map::new();
                    object.insert(
                        ("status").into(),
                        ::serde_json::Value::Object(::serde_json::Map::new()),
                    );
                    object
                })
            }
        } else {
            ::serde_json::Value::Object(::serde_json::Map::new())
        };
        let r#gen = ::schemars::r#gen::SchemaSettings::openapi3()
            .with(|s| {
                s.inline_subschemas = true;
                s.meta_schema = None;
            })
            .with_visitor(kube_core::schema::StructuralSchemaRewriter)
            .into_generator();
        let schema = r#gen.into_root_schema_for::<Self>();
        let jsondata = ::serde_json::Value::Object({
            let mut object = ::serde_json::Map::new();
            object.insert(
                ("metadata").into(),
                ::serde_json::Value::Object({
                    let mut object = ::serde_json::Map::new();
                    object.insert(
                        ("name").into(),
                        ::serde_json::to_value("fleets.agones.dev").unwrap(),
                    );
                    object
                }),
            );
            object.insert(
                ("spec").into(),
                ::serde_json::Value::Object({
                    let mut object = ::serde_json::Map::new();
                    object.insert(
                        ("group").into(),
                        ::serde_json::to_value("agones.dev").unwrap(),
                    );
                    object.insert(
                        ("scope").into(),
                        ::serde_json::to_value("Namespaced").unwrap(),
                    );
                    object.insert(
                        ("names").into(),
                        ::serde_json::Value::Object({
                            let mut object = ::serde_json::Map::new();
                            object.insert(
                                ("categories").into(),
                                ::serde_json::to_value(categories).unwrap(),
                            );
                            object.insert(
                                ("plural").into(),
                                ::serde_json::to_value("fleets").unwrap(),
                            );
                            object.insert(
                                ("singular").into(),
                                ::serde_json::to_value("fleet").unwrap(),
                            );
                            object
                                .insert(("kind").into(), ::serde_json::to_value("Fleet").unwrap());
                            object.insert(
                                ("shortNames").into(),
                                ::serde_json::to_value(shorts).unwrap(),
                            );
                            object
                        }),
                    );
                    object.insert(
                        ("versions").into(),
                        ::serde_json::Value::Array(<[_]>::into_vec(Box::new([
                            ::serde_json::Value::Object({
                                let mut object = ::serde_json::Map::new();
                                object
                                    .insert(("name").into(), ::serde_json::to_value("v1").unwrap());
                                object.insert(("served").into(), ::serde_json::Value::Bool(true));
                                object.insert(("storage").into(), ::serde_json::Value::Bool(true));
                                object.insert(
                                    ("schema").into(),
                                    ::serde_json::Value::Object({
                                        let mut object = ::serde_json::Map::new();
                                        object.insert(
                                            ("openAPIV3Schema").into(),
                                            ::serde_json::to_value(&schema).unwrap(),
                                        );
                                        object
                                    }),
                                );
                                object.insert(
                                    ("additionalPrinterColumns").into(),
                                    ::serde_json::to_value(columns).unwrap(),
                                );
                                object.insert(
                                    ("subresources").into(),
                                    ::serde_json::to_value(subres).unwrap(),
                                );
                                object
                            }),
                        ]))),
                    );
                    object
                }),
            );
            object
        });
        ::serde_json::from_value(jsondata).expect("valid custom resource from #[kube(attrs..)]")
    }
    fn crd_name() -> &'static str {
        "fleets.agones.dev"
    }
    fn api_resource() -> ::kube::core::dynamic::ApiResource {
        ::kube::core::dynamic::ApiResource::erase::<Self>(&())
    }
    fn shortnames() -> &'static [&'static str] {
        &[]
    }
}

impl ::kube::core::object::HasSpec for Fleet {
    type Spec = FleetSpec;
    fn spec(&self) -> &FleetSpec {
        &self.spec
    }
    fn spec_mut(&mut self) -> &mut FleetSpec {
        &mut self.spec
    }
}

impl ::kube::core::object::HasStatus for Fleet {
    type Status = FleetStatus;
    fn status(&self) -> Option<&FleetStatus> {
        self.status.as_ref()
    }
    fn status_mut(&mut self) -> &mut Option<FleetStatus> {
        &mut self.status
    }
}

/// The spec for a [`Fleet`]. More info: <https://agones.dev/site/docs/reference/agones_crd_api_reference/#agones.dev/v1.Fleet>
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct FleetSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<FleetScheduling>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<FleetStrategy>,
    /// Template for a [`GameServer`] resource.
    pub template: GameServerTemplateSpec,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum FleetScheduling {
    Packed,
    Distributed,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct FleetStrategy {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "rollingUpdate"
    )]
    pub rolling_update: Option<FleetStrategyRollingUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<FleetStrategyType>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct FleetStrategyRollingUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "maxSurge")]
    pub max_surge: Option<IntOrString>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "maxUnavailable"
    )]
    pub max_unavailable: Option<IntOrString>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum FleetStrategyType {
    Recreate,
    RollingUpdate,
}

/// Spec for a [`GameServer`] resource.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct GameServerTemplateSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,
    /// The spec for a [`GameServer`] resource. More info: <https://agones.dev/site/docs/reference/agones_crd_api_reference/#agones.dev/v1.GameServer>
    pub spec: GameServerSpec,
}

/// The status of a Fleet. More info: <https://agones.dev/site/docs/reference/agones_crd_api_reference/#agones.dev/v1.Fleet>
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct FleetStatus {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allocatedReplicas"
    )]
    pub allocated_replicas: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "readyReplicas"
    )]
    pub ready_replicas: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "reservedReplicas"
    )]
    pub reserved_replicas: Option<i64>,
}
