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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            core::v1::{
                ConfigMap, PodSpec, PodTemplateSpec, Service, ServiceAccount, ServicePort,
                ServiceSpec,
            },
            rbac::v1::{ClusterRole, RoleBinding},
        },
        apimachinery::pkg::{
            apis::meta::v1::{LabelSelector, ObjectMeta},
            util::intstr::IntOrString,
        },
    };
    use kube::{
        Api, ResourceExt,
        api::{DeleteParams, PostParams},
        runtime::wait::await_condition,
    };
    use serial_test::serial;
    use tokio::time::timeout;

    use quilkin::{
        providers::k8s::agones::{Fleet, GameServer},
        test::TestHelper,
    };

    use crate::{
        Client, TOKEN_KEY, create_agones_rbac_read_account, create_token_router_config,
        create_tokenised_gameserver, debug_pods, is_deployment_ready, quilkin_container,
        quilkin_proxy_deployment,
    };

    const PROXY_DEPLOYMENT: &str = "quilkin-xds-proxies";
    const SLOW: Duration = Duration::from_secs(60);

    #[tokio::test]
    #[serial]
    /// Test for Agones Provider integration. Since this will look at all `GameServers` in the namespace
    /// for this test, we should only run Agones integration test in a serial manner, since they
    /// could easily collide with each other.
    async fn agones_token_router() {
        let client = Client::new().await;

        let deployments: Api<Deployment> = client.namespaced_api();
        let fleets: Api<Fleet> = client.namespaced_api();
        let gameservers: Api<GameServer> = client.namespaced_api();
        let config_maps: Api<ConfigMap> = client.namespaced_api();

        let pp = PostParams::default();
        let dp = DeleteParams::default();

        let config_map = create_token_router_config(&config_maps).await;

        agones_control_plane(&client, deployments.clone()).await;
        let proxy_address = quilkin_proxy_deployment(
            &client,
            deployments.clone(),
            PROXY_DEPLOYMENT.into(),
            7005,
            "http://quilkin-manage-agones:7800".into(),
            true,
        )
        .await;

        let token = "456"; // NDU2
        let gs = create_tokenised_gameserver(fleets, gameservers.clone(), token).await;
        let gs_address = crate::gameserver_address(&gs);
        // and allocate it such that we have an endpoint.
        // let's allocate this specific game server
        let mut t = TestHelper::default();
        let (mut rx, socket) = t.open_socket_and_recv_multiple_packets().await;
        socket.send_to(b"ALLOCATE", gs_address).await.unwrap();

        let response = timeout(SLOW, rx.recv())
            .await
            .expect("should receive packet from GameServer")
            .unwrap();
        assert_eq!("ACK: ALLOCATE\n", response);

        // Proxy Deployment should be ready, since there is now an endpoint
        if timeout(
            SLOW,
            await_condition(deployments.clone(), PROXY_DEPLOYMENT, is_deployment_ready()),
        )
        .await
        .is_err()
        {
            debug_pods(&client, format!("role={PROXY_DEPLOYMENT}")).await;
            panic!("Quilkin proxy deployment should be ready");
        }

        // keep trying to send the packet to the proxy until it works, since distributed systems are eventually consistent.
        let mut response: String = "not-found".into();
        for i in 0..30 {
            println!("Connection Attempt: {i}");

            // returns the nae of the GameServer. This proves we are routing the the allocated
            // GameServer with the correct token attached.
            socket
                .send_to(format!("GAMESERVER{token}").as_bytes(), proxy_address)
                .await
                .unwrap();

            let result = timeout(Duration::from_secs(1), rx.recv()).await;
            if let Ok(Some(value)) = result {
                response = value;
                break;
            }
        }
        assert_eq!(format!("NAME: {}\n", gs.name_unchecked()), response);

        // let's remove the token from the gameserver, which should remove access.
        let mut gs = gameservers.get(gs.name_unchecked().as_str()).await.unwrap();
        let name = gs.name_unchecked();
        gs.metadata
            .annotations
            .as_mut()
            .map(|annotations| annotations.remove(TOKEN_KEY).unwrap());
        gameservers.replace(name.as_str(), &pp, &gs).await.unwrap();
        // now we should send a packet, and not get a response.
        let mut failed = false;
        for i in 0..30 {
            println!("Disconnection Attempt: {i}");
            socket
                .send_to(format!("GAMESERVER{token}").as_bytes(), proxy_address)
                .await
                .unwrap();

            let result = timeout(Duration::from_secs(1), rx.recv()).await;
            if result.is_err() {
                failed = true;
                break;
            }
        }
        if !failed {
            debug_pods(&client, format!("role={PROXY_DEPLOYMENT}")).await;
            debug_pods(&client, "role=xds".into()).await;
        }
        assert!(failed, "Packet should have failed");

        // cleanup
        config_maps
            .delete(&config_map.name_unchecked(), &dp)
            .await
            .unwrap();
    }

    /// Creates Quilkin xDS management instance that is in the mode to watch Agones `GameServers`
    /// in this test namespace
    async fn agones_control_plane(client: &Client, deployments: Api<Deployment>) {
        let services: Api<Service> = client.namespaced_api();
        let service_accounts: Api<ServiceAccount> = client.namespaced_api();
        let cluster_roles: Api<ClusterRole> = Api::all(client.kubernetes.clone());
        let role_bindings: Api<RoleBinding> = client.namespaced_api();
        let pp = PostParams::default();

        let rbac_name =
            create_agones_rbac_read_account(client, service_accounts, cluster_roles, role_bindings)
                .await;

        // Setup the xDS Agones provider server
        let args = [
            "--service.xds",
            "--service.mds",
            "--provider.k8s",
            "--provider.k8s.namespace",
            client.namespace.as_str(),
            "--provider.k8s.agones",
            "--provider.k8s.agones.namespace",
            client.namespace.as_str(),
        ]
        .map(String::from)
        .to_vec();
        let labels = BTreeMap::from([("role".to_string(), "xds".to_string())]);
        let deployment = Deployment {
            metadata: ObjectMeta {
                name: Some("quilkin-manage-agones".into()),
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
                        containers: vec![quilkin_container(client, Some(args), None, true)],
                        service_account_name: Some(rbac_name),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        let deployment = deployments.create(&pp, &deployment).await.unwrap();

        let service = Service {
            metadata: ObjectMeta {
                name: Some("quilkin-manage-agones".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(labels),
                ports: Some(vec![ServicePort {
                    protocol: Some("TCP".into()),
                    port: 7800,
                    target_port: Some(IntOrString::Int(7800)),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        services.create(&pp, &service).await.unwrap();

        // make sure the deployment and service are ready
        let name = deployment.name_unchecked();
        let result = timeout(
            Duration::from_secs(30),
            await_condition(deployments.clone(), name.as_str(), is_deployment_ready()),
        )
        .await;

        if let Ok(result) = result {
            result.unwrap();
        } else {
            debug_pods(client, "role=xds".into()).await;
            panic!("xDS provider deployment should be ready");
        }
    }
}
