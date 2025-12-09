/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    collections::BTreeMap,
    env,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{
            ConfigMap, Container, EnvVar, Event, HTTPGetAction, Namespace, Pod, PodSpec,
            PodTemplateSpec, Probe, ResourceRequirements, ServiceAccount, VolumeMount,
        },
        core::v1::{ContainerPort, Node},
        rbac::{
            v1::PolicyRule,
            v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{LabelSelector, ObjectMeta},
        util::intstr::IntOrString,
    },
    chrono,
};
use kube::{
    Api, Resource, ResourceExt,
    api::{DeleteParams, ListParams, LogParams, PostParams},
    runtime::wait::{Condition, await_condition},
};
use tokio::{sync::OnceCell, time::timeout};
use tracing::debug;

use quilkin::providers::k8s::agones::{
    Fleet, FleetSpec, GameServer, GameServerPort, GameServerSpec, GameServerState,
    GameServerTemplateSpec,
};

mod pod;
mod provider;
mod relay;
mod sidecar;

pub static CLIENT: OnceCell<Client> = OnceCell::const_new();
pub const IMAGE_TAG: &str = "IMAGE_TAG";
pub const PREV_IMAGE_TAG: &str = "PREV_IMAGE_TAG";
const DELETE_DELAY_SECONDS: &str = "DELETE_DELAY_SECONDS";
/// A simple udp server that returns packets that are sent to it.
/// See: <https://github.com/googleforgames/agones/tree/main/examples/simple-game-server>
/// for more details.
pub const GAMESERVER_IMAGE: &str =
    "us-docker.pkg.dev/agones-images/examples/simple-game-server:0.16";

/// The dynamic metadata key for routing tokens
pub const TOKEN_KEY: &str = "quilkin.dev/tokens";

#[derive(Clone)]
pub struct Client {
    /// The Kubernetes client
    pub kubernetes: kube::Client,
    /// The namespace the tests will happen in
    pub namespace: String,
    /// The name and tag of the Quilkin image being tested
    pub quilkin_image: String,
    pub prev_quilkin_image: String,
}

impl Client {
    /// Thread safe way to create a Clients across multiple tests.
    /// Executes the setup required:
    /// * Creates a test namespace for this test
    /// * Removes previous test namespaces
    /// * Retrieves the `IMAGE_TAG` to test from env vars, and panics if it if not available.
    pub async fn new() -> Client {
        let _provider = rustls::crypto::ring::default_provider().install_default();
        let mut client = CLIENT
            .get_or_init(|| async {
                let client = kube::Client::try_default()
                    .await
                    .expect("Kubernetes client to be created");

                Client {
                    kubernetes: client.clone(),
                    namespace: setup_namespace(client).await,
                    quilkin_image: env::var(IMAGE_TAG).expect(IMAGE_TAG),
                    prev_quilkin_image: env::var(PREV_IMAGE_TAG).expect(PREV_IMAGE_TAG),
                }
            })
            .await
            .clone();

        // create a new client on each invocation, as the client can close
        // at the end of each test.
        client.kubernetes = kube::Client::try_default()
            .await
            .expect("Kubernetes client to be created");
        client
    }

    /// Returns a typed API client for this client in this test namespace.
    pub fn namespaced_api<K: Resource<Scope = kube::core::NamespaceResourceScope>>(&self) -> Api<K>
    where
        <K as Resource>::DynamicType: Default,
    {
        Api::namespaced(self.kubernetes.clone(), self.namespace.as_str())
    }
}

/// Deletes old quilkin test namespaces, and then create
/// a new namespace based on EPOCH time, and return its string value.
#[allow(dead_code)]
async fn setup_namespace(client: kube::Client) -> String {
    let namespaces: Api<Namespace> = Api::all(client.clone());

    let lp = ListParams::default().labels("owner=quilkin-test");
    let nss = namespaces.list(&lp).await.unwrap();
    let dp = DeleteParams::default();

    let delay = env::var(DELETE_DELAY_SECONDS)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(chrono::Duration::seconds);

    for ns in nss {
        let name = ns.name_unchecked();

        let delete = delay
            .and_then(|duration| {
                let expiry = ns.creation_timestamp()?.0 + duration;
                Some(chrono::Utc::now() > expiry)
            })
            .unwrap_or(true);
        if delete && let Err(err) = namespaces.delete(name.as_str(), &dp).await {
            println!("Failure attempting to deleted namespace: {:?}, {err}", name);
        }
    }

    let name = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();

    let metadata = ObjectMeta {
        name: Some(name),
        labels: Some(BTreeMap::from([("owner".into(), "quilkin-test".into())])),
        ..Default::default()
    };
    let test_namespace = Namespace {
        metadata,
        spec: None,
        status: None,
    };

    let pp = PostParams::default();
    namespaces
        .create(&pp, &test_namespace)
        .await
        .expect("namespace to be created");

    add_agones_service_account(client, test_namespace.name_unchecked()).await;

    test_namespace.name_unchecked()
}

async fn add_agones_service_account(client: kube::Client, namespace: String) {
    let service_accounts: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace.as_str());
    let role_bindings: Api<RoleBinding> = Api::namespaced(client, namespace.as_str());
    let pp = PostParams::default();
    let labels = BTreeMap::from([("app".to_string(), "agones".to_string())]);

    let service_account = ServiceAccount {
        metadata: ObjectMeta {
            name: Some("agones-sdk".into()),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        ..Default::default()
    };

    let service_account = service_accounts
        .create(&pp, &service_account)
        .await
        .unwrap();

    let role_binding = RoleBinding {
        metadata: ObjectMeta {
            name: Some("agones-sdk-access".into()),
            namespace: Some(namespace.clone()),
            labels: Some(labels),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: "agones-sdk".into(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: service_account.name_unchecked(),
            namespace: Some(namespace),
            api_group: None,
        }]),
    };

    role_bindings.create(&pp, &role_binding).await.unwrap();
}

/// Creates a Service account and related RBAC objects to enable a process to query Agones
/// and [`ConfigMap`] resources within a cluster
pub async fn create_agones_rbac_read_account(
    client: &Client,
    service_accounts: Api<ServiceAccount>,
    cluster_roles: Api<ClusterRole>,
    role_bindings: Api<RoleBinding>,
) -> String {
    let pp = PostParams::default();
    let rbac_name = "quilkin-agones";

    // check if sevice account already exists, otherwise create it.
    if service_accounts.get(rbac_name).await.is_ok() {
        return rbac_name.into();
    }

    // create all the rbac rules

    let rbac_meta = ObjectMeta {
        name: Some(rbac_name.into()),
        ..Default::default()
    };
    let service_account = ServiceAccount {
        metadata: rbac_meta.clone(),
        ..Default::default()
    };
    service_accounts
        .create(&pp, &service_account)
        .await
        .unwrap();

    // Delete the cluster role if it already exists, since it's cluster wide.
    match cluster_roles
        .delete(rbac_name, &DeleteParams::default())
        .await
    {
        Ok(_) => {}
        Err(err) => println!("Cluster role not found: {err}"),
    };
    let cluster_role = ClusterRole {
        metadata: rbac_meta.clone(),
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec!["agones.dev".into()]),
                resources: Some(vec!["gameservers".into()]),
                verbs: ["get", "list", "watch"].map(String::from).to_vec(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["configmaps".into()]),
                verbs: ["get", "list", "watch"].map(String::from).to_vec(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    cluster_roles.create(&pp, &cluster_role).await.unwrap();

    let binding = RoleBinding {
        metadata: rbac_meta,
        subjects: Some(vec![Subject {
            kind: "User".into(),
            name: format!("system:serviceaccount:{}:{rbac_name}", client.namespace),
            api_group: Some("rbac.authorization.k8s.io".into()),
            ..Default::default()
        }]),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: rbac_name.into(),
        },
    };
    role_bindings.create(&pp, &binding).await.unwrap();
    rbac_name.into()
}

/// Create a Deployment with a singular Quilkin proxy, and return its address.
/// The `name` variable is used as role={name} for label lookup.
pub async fn quilkin_proxy_deployment(
    client: &Client,
    deployments: Api<Deployment>,
    name: String,
    host_port: u16,
    management_server: String,
    current: bool,
) -> SocketAddr {
    let pp = PostParams::default();
    let mut container = quilkin_container(
        client,
        Some(vec![
            "--service.udp".into(),
            "--service.qcmp".into(),
            format!("--provider.xds.endpoints={management_server}"),
        ]),
        None,
        current,
    );

    // we'll use a host port, since spinning up a load balancer takes a long time.
    // we know that port 7777 is open because this is an Agones cluster and it has associated
    // firewall rules , and even if we conflict with a GameServer
    // the k8s scheduler will move us to another node.
    container.ports = Some(vec![ContainerPort {
        container_port: 7777,
        host_port: Some(host_port as i32),
        protocol: Some("UDP".into()),
        ..Default::default()
    }]);

    let labels = BTreeMap::from([("role".to_string(), name.clone())]);
    let deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(name),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_expressions: None,
                match_labels: Some(labels.clone()),
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let deployment = deployments.create(&pp, &deployment).await.unwrap();
    let name = deployment.name_unchecked();
    // should not be ready, since there are no endpoints, but let's wait 3 seconds, make sure it doesn't do something we don't expect
    let result = timeout(
        Duration::from_secs(3),
        await_condition(deployments.clone(), name.as_str(), is_deployment_ready()),
    )
    .await;
    assert!(result.is_err());

    // get the address to send data to
    let pods = client.namespaced_api::<Pod>();
    let list = pods
        .list(&ListParams {
            label_selector: Some(format!("role={name}")),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(1, list.items.len());

    let nodes: Api<Node> = Api::all(client.kubernetes.clone());
    let name = list.items[0]
        .spec
        .as_ref()
        .unwrap()
        .node_name
        .as_ref()
        .unwrap();
    let node = nodes.get(name.as_str()).await.unwrap();
    let external_ip = node
        .status
        .unwrap()
        .addresses
        .unwrap()
        .iter()
        .find(|addr| addr.type_ == "ExternalIP")
        .unwrap()
        .address
        .clone();

    SocketAddr::new(external_ip.parse().unwrap(), host_port)
}

/// Create a Fleet, and pick on it's [`GameServer`]s and add the token to it.
/// Returns the details of the [`GameServer`] that has been selected.
pub async fn create_tokenised_gameserver(
    fleets: Api<Fleet>,
    gameservers: Api<GameServer>,
    token: &str,
) -> GameServer {
    let pp = PostParams::default();

    // create a fleet so we can ensure that a packet is going to the GameServer we expect, and not
    // any other.
    let fleet = fleet();
    let fleet = fleets.create(&pp, &fleet).await.unwrap();
    let name = fleet.name_unchecked();
    timeout(
        Duration::from_secs(30),
        await_condition(fleets.clone(), name.as_str(), is_fleet_ready()),
    )
    .await
    .expect("Fleet should be ready")
    .unwrap();

    let lp = ListParams {
        label_selector: Some(format!("agones.dev/fleet={}", fleet.name_unchecked())),
        ..Default::default()
    };
    let list = gameservers.list(&lp).await.unwrap();

    let mut gs = list.items[0].clone();
    // add routing label to the GameServer
    assert_eq!(3, token.len());
    gs.metadata
        .annotations
        .get_or_insert(Default::default())
        .insert(
            TOKEN_KEY.into(),
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, token),
        );
    gameservers
        .replace(gs.name_unchecked().as_str(), &pp, &gs)
        .await
        .unwrap();
    gs
}

/// Returns a test [`GameServer`] with the UDP test binary that is used for
/// Agones e2e tests.
pub fn game_server() -> GameServer {
    let mut resources = BTreeMap::new();

    resources.insert("cpu".into(), Quantity("30m".into()));
    resources.insert("memory".into(), Quantity("32Mi".into()));
    let labels = BTreeMap::from([("role".to_string(), "gameserver".to_string())]);

    GameServer {
        metadata: ObjectMeta {
            generate_name: Some("gameserver-".into()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: GameServerSpec {
            ports: vec![GameServerPort {
                container_port: Some(7654),
                host_port: None,
                name: "udp-port".into(),
                port_policy: Default::default(),
                container: None,
                protocol: Default::default(),
            }],
            template: PodTemplateSpec {
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "game-server".into(),
                        image: Some(GAMESERVER_IMAGE.into()),
                        resources: Some(ResourceRequirements {
                            limits: Some(resources.clone()),
                            requests: Some(resources),
                            claims: None,
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        },
        status: None,
    }
}

/// Returns a Fleet of 3 replicas of the UDP testing [`GameServer`]
pub fn fleet() -> Fleet {
    let gs = game_server();
    Fleet {
        metadata: ObjectMeta {
            generate_name: Some("fleet-".into()),
            ..Default::default()
        },
        spec: FleetSpec {
            replicas: Some(3),
            template: GameServerTemplateSpec {
                metadata: None,
                spec: gs.spec,
            },
            ..Default::default()
        },
        status: None,
    }
}

/// Condition to wait for a [`GameServer`] to become Ready.
pub fn is_gameserver_ready() -> impl Condition<GameServer> {
    |obj: Option<&GameServer>| {
        obj.and_then(|gs| gs.status.clone())
            .is_some_and(|status| matches!(status.state, GameServerState::Ready))
    }
}

pub fn is_pod_ready() -> impl Condition<Pod> {
    |obj: Option<&Pod>| {
        if let Some(pod) = obj {
            return pod
                .status
                .as_ref()
                .and_then(|status| status.conditions.as_ref())
                .and_then(|conditions| {
                    conditions
                        .iter()
                        .find(|condition| condition.type_ == "Ready" && condition.status == "True")
                })
                .is_some();
        }
        false
    }
}

/// Condition to wait for a Deployment to have all the replicas it is expecting to be ready.
pub fn is_deployment_ready() -> impl Condition<Deployment> {
    |obj: Option<&Deployment>| {
        if let Some(deployment) = obj {
            let expected = deployment.spec.as_ref().unwrap().replicas.as_ref().unwrap();

            return deployment
                .status
                .as_ref()
                .and_then(|status| status.ready_replicas)
                .is_some_and(|replicas| &replicas == expected);
        }
        false
    }
}

/// Condition to wait for a Fleet to have all the replicas it is expecting to be ready.
pub fn is_fleet_ready() -> impl Condition<Fleet> {
    |obj: Option<&Fleet>| {
        if let Some(fleet) = obj {
            let expected = fleet.spec.replicas.as_ref().unwrap();

            return fleet
                .status
                .as_ref()
                .and_then(|status| status.ready_replicas)
                .is_some_and(|replicas| &replicas == expected);
        }
        false
    }
}

/// Returns a container for Quilkin, with an optional volume mount name
pub fn quilkin_container(
    client: &Client,
    args: Option<Vec<String>>,
    volume_mount: Option<String>,
    current: bool,
) -> Container {
    let image = if current {
        client.quilkin_image.clone()
    } else {
        client.prev_quilkin_image.clone()
    };

    let mut container = Container {
        name: "quilkin".into(),
        image: Some(image),
        args,
        env: Some(vec![EnvVar {
            name: "RUST_LOG".to_string(),
            value: Some("quilkin=trace".into()),
            value_from: None,
        }]),
        liveness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/live".into()),
                port: IntOrString::Int(8000),
                ..Default::default()
            }),
            initial_delay_seconds: Some(3),
            period_seconds: Some(2),
            ..Default::default()
        }),
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/ready".into()),
                port: IntOrString::Int(8000),
                ..Default::default()
            }),
            initial_delay_seconds: Some(3),
            period_seconds: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    };

    if let Some(name) = volume_mount {
        container.volume_mounts = Some(vec![VolumeMount {
            name,
            mount_path: "/etc/quilkin".into(),
            ..Default::default()
        }]);
    };

    container
}

/// Return a [`ConfigMap`] in the format that Quilkin expects it to be able to
/// consume the config yaml.
pub fn quilkin_config_map(config: &str) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            generate_name: Some("quilkin-config-".into()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "quilkin.yaml".to_string(),
            config.to_string(),
        )])),
        ..Default::default()
    }
}

/// Return a [`ConfigMap`] that has a standard testing Token Router configuration
pub async fn create_token_router_config(config_maps: &Api<ConfigMap>) -> ConfigMap {
    let pp = PostParams::default();

    let config = r#"
version: v1alpha1
filters:
  - name: quilkin.filters.capture.v1alpha1.Capture # Capture and remove the authentication token
    config:
      suffix:
          size: 3
          remove: true
  - name: quilkin.filters.token_router.v1alpha1.TokenRouter
"#;
    let mut config_map = quilkin_config_map(config);
    config_map
        .metadata
        .labels
        .get_or_insert(Default::default())
        .insert("quilkin.dev/configmap".into(), "true".into());

    config_maps.create(&pp, &config_map).await.unwrap()
}

/// Convenience function to return the address with the first port of [`GameServer`]
pub fn gameserver_address(gs: &GameServer) -> String {
    let status = gs.status.as_ref().unwrap();
    let address = format!(
        "{}:{}",
        status.address,
        status.ports.as_ref().unwrap()[0].port
    );
    address
}

// Output the events and logs for each pod that matches this label selector.
// Useful for determining why something is failing in CI without having to run a cluster.
// Requires quilkin::test::enable_log("agones=debug"); to enable debug logging within
// the test
pub async fn debug_pods(client: &Client, labels: String) {
    debug!(labels, "🪓 Debug output for Selector");
    let pods: Api<Pod> = client.namespaced_api();
    let events: Api<Event> = client.namespaced_api();

    let params = ListParams::default();
    let event_list = events.list(&params).await.unwrap();
    let pod_list = pods
        .list(&ListParams {
            label_selector: Some(labels),
            ..Default::default()
        })
        .await
        .unwrap();

    let params = LogParams::default();
    for pod in pod_list {
        let name = pod.name_unchecked();
        let pod_events: Vec<&Event> = event_list
            .iter()
            .filter(|item| {
                item.involved_object.kind == Some("Pod".into())
                    && item.involved_object.name == Some(name.clone())
            })
            .collect();
        debug!(pod = name, "🗓️  Pod Events");
        for event in pod_events {
            debug!(
                pod = name,
                type_ = event.type_,
                reason = event.reason,
                message = event.message,
                count = event.count
            );
        }

        debug!(pod = name, "📃 Pod Logs");
        let mut logs = pods
            .log_stream(name.as_str(), &params)
            .await
            .unwrap()
            .lines();

        while let Some(line) = logs.try_next().await.unwrap() {
            debug!(pod = name, line);
        }
    }
}
