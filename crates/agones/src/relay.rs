/*
 * Copyright 2023 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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

    const SLOW: Duration = Duration::from_secs(30);

    #[tokio::test]
    #[serial]
    /// Test for Agones Provider integration. Since this will look at all `GameServers` in the namespace
    /// for this test, we should only run Agones integration test in a serial manner, since they
    /// could easily collide with each other.
    async fn agones_token_router() {
        quilkin::test::enable_log("agones=debug");

        run_test(true, true, true, 0).await;
        //run_test(true, true, false, 1).await;
        //run_test(true, false, true, 2).await;
        //run_test(false, true, true, 3).await;
    }

    async fn run_test(proxy: bool, relay: bool, agent: bool, id: u8) {
        println!("running agones_token_router {id}");

        let client = Client::new().await;
        let config_maps: Api<ConfigMap> = client.namespaced_api();
        let deployments: Api<Deployment> = client.namespaced_api();
        let fleets: Api<Fleet> = client.namespaced_api();
        let gameservers: Api<GameServer> = client.namespaced_api();

        let pp = PostParams::default();
        let dp = DeleteParams::default();

        let config_map = create_token_router_config(&config_maps).await;
        let (relay_name, agent_names) =
            agones_agent_deployment(&client, deployments.clone(), relay, agent, 1, id).await;

        let relay_proxy_name = format!("quilkin-relay-proxy-{id}");
        let proxy_address = quilkin_proxy_deployment(
            &client,
            deployments.clone(),
            relay_proxy_name.clone(),
            7005,
            format!("http://{relay_name}:7800"),
            proxy,
        )
        .await;

        let token = "789";
        let gs = create_tokenised_gameserver(fleets, gameservers.clone(), token).await;
        let gs_address = crate::gameserver_address(&gs);

        let mut t = TestHelper::default();
        let (mut rx, socket) = t.open_socket_and_recv_multiple_packets().await;
        socket.send_to(b"ALLOCATE", gs_address).await.unwrap();

        let response = timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("should receive packet from GameServer")
            .unwrap();
        assert_eq!("ACK: ALLOCATE\n", response);

        // Proxy Deployment should be ready, since there is now an endpoint
        if timeout(
            SLOW,
            await_condition(
                deployments.clone(),
                &relay_proxy_name,
                is_deployment_ready(),
            ),
        )
        .await
        .is_err()
        {
            debug_pods(&client, format!("role={relay_proxy_name}")).await;
            debug_pods(&client, "role=relay".into()).await;
            debug_pods(&client, "role=agent".into()).await;
            panic!("Quilkin proxy deployment should be ready");
        }

        // keep trying to send the packet to the proxy until it works, since distributed systems are eventually consistent.
        let mut response: String = "not-found".into();
        for i in 0..30 {
            println!("Connection Attempt: {i}");

            // returns the name of the GameServer. This proves we are routing the allocated
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

        if format!("NAME: {}\n", gs.name_unchecked()) != response {
            debug_pods(&client, format!("role={relay_proxy_name}")).await;
            debug_pods(&client, "role=relay".into()).await;
            debug_pods(&client, "role=agent".into()).await;
            panic!("failed send packets to {}", gs.name_unchecked());
        }

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
            debug_pods(&client, format!("role={relay_proxy_name}")).await;
            debug_pods(&client, "role=relay".into()).await;
            debug_pods(&client, "role=agent".into()).await;
        }
        assert!(failed, "Packet should have failed");

        println!("deleting resources...");
        use either::Either;
        let cm_name = config_map.name_unchecked();
        // cleanup
        match config_maps
            .delete(&cm_name, &dp)
            .await
            .expect("failed to delete config map")
        {
            Either::Left(_) => {
                timeout(
                    SLOW,
                    await_condition(
                        deployments.clone(),
                        &relay_proxy_name,
                        kube::runtime::conditions::is_deleted(&cm_name),
                    ),
                )
                .await
                .expect("failed to delete config map within timeout")
                .expect("failed to delete config map");
                println!("...config map deleted");
            }
            Either::Right(_) => {
                println!("config map deleted");
            }
        }

        async fn delete_deployment(dp: &Api<Deployment>, name: &str) -> Result<(), kube::Error> {
            async fn inner(dp: &Api<Deployment>, name: &str) -> Result<(), kube::Error> {
                if let Either::Left(d) = dp.delete(name, &DeleteParams::default()).await? {
                    await_condition(
                        dp.clone(),
                        name,
                        kube::runtime::conditions::is_deleted(&d.uid().unwrap()),
                    )
                    .await
                    .map_err(|err| kube::Error::Service(Box::new(err)))?;
                }

                Ok(())
            }

            timeout(SLOW, inner(dp, name)).await.map_err(|_err| {
                kube::Error::Api(kube::error::ErrorResponse {
                    message: format!("failed to delete deployment {name} within {SLOW:?}"),
                    status: String::new(),
                    reason: String::new(),
                    code: 408,
                })
            })??;
            println!("deployment {name} deleted");
            Ok(())
        }

        async fn delete_agents(
            dp: &Api<Deployment>,
            agents: Vec<String>,
        ) -> Result<(), kube::Error> {
            for agent in agents {
                delete_deployment(dp, &agent).await?;
            }

            Ok(())
        }

        tokio::try_join!(
            delete_deployment(&deployments, &relay_proxy_name),
            delete_agents(&deployments, agent_names),
            delete_deployment(&deployments, &relay_name),
        )
        .expect("failed to delete deployment(s) within timeout");
    }

    /// Deploys the Agent and Relay Server Deployments and Services
    async fn agones_agent_deployment(
        client: &Client,
        deployments: Api<Deployment>,
        relay: bool,
        agent: bool,
        agent_count: u8,
        id: u8,
    ) -> (String, Vec<String>) {
        let service_accounts: Api<ServiceAccount> = client.namespaced_api();
        let cluster_roles: Api<ClusterRole> = Api::all(client.kubernetes.clone());
        let role_bindings: Api<RoleBinding> = client.namespaced_api();
        let services: Api<Service> = client.namespaced_api();

        let pp = PostParams::default();

        let rbac_name =
            create_agones_rbac_read_account(client, service_accounts, cluster_roles, role_bindings)
                .await;

        let relay_name = format!("quilkin-relay-agones-{id}");

        // Setup the relay
        let args = [
            "--service.xds",
            "--service.mds",
            "--provider.k8s",
            "--provider.k8s.namespace",
            client.namespace.as_str(),
        ]
        .map(String::from)
        .to_vec();
        let labels = BTreeMap::from([("role".to_string(), "relay".to_string())]);
        let deployment = Deployment {
            metadata: ObjectMeta {
                name: Some(relay_name.clone()),
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
                        containers: vec![quilkin_container(client, Some(args), None, relay)],
                        service_account_name: Some(rbac_name.clone()),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let relay_deployment = deployments.create(&pp, &deployment).await.unwrap();

        // relay service
        let service = Service {
            metadata: ObjectMeta {
                name: Some(relay_name.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(labels),
                ports: Some(vec![
                    ServicePort {
                        name: Some("ads".into()),
                        protocol: Some("TCP".into()),
                        port: 7800,
                        target_port: Some(IntOrString::Int(7800)),
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("cpds".into()),
                        protocol: Some("TCP".into()),
                        port: 7900,
                        target_port: Some(IntOrString::Int(7900)),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        services.create(&pp, &service).await.unwrap();

        let name = relay_deployment.name_unchecked();
        let result = timeout(
            SLOW,
            await_condition(deployments.clone(), &name, is_deployment_ready()),
        )
        .await;
        if result.is_err() {
            debug_pods(client, "role=relay".into()).await;
            debug_pods(client, "role=agent".into()).await;
            debug_pods(client, "role=proxy".into()).await;

            panic!("Relay Deployment should be ready");
        }
        result.unwrap().expect("Should have a relay deployment");

        let mut agent_names = Vec::with_capacity(agent_count as _);
        for i in 0..agent_count {
            let agent_name = format!("quilkin-agones-agent-{id}-{i}");
            let icao = format!("XXX{}", "ABCD".get(i as usize..(i + 1) as usize).unwrap());

            // agent deployment
            let args = [
                "--provider.mds.endpoints",
                &format!("http://{relay_name}:7900"),
                "--locality.icao",
                &icao,
                "--provider.k8s",
                "--provider.k8s.namespace",
                client.namespace.as_str(),
                "--provider.k8s.agones",
                "--provider.k8s.agones.namespace",
                client.namespace.as_str(),
            ]
            .map(String::from)
            .to_vec();
            let labels = BTreeMap::from([("role".to_string(), "agent".to_string())]);
            let deployment = Deployment {
                metadata: ObjectMeta {
                    name: Some(agent_name.clone()),
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
                            containers: vec![quilkin_container(client, Some(args), None, agent)],
                            service_account_name: Some(rbac_name.clone()),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
                ..Default::default()
            };
            let agent_deployment = deployments.create(&pp, &deployment).await.unwrap();
            let name = agent_deployment.name_unchecked();
            let result = timeout(
                SLOW,
                await_condition(deployments.clone(), name.as_str(), is_deployment_ready()),
            )
            .await;
            if result.is_err() {
                debug_pods(client, "role=agent".into()).await;
                panic!("Agent Deployment should be ready");
            }
            result.unwrap().expect("Should have an agent deployment");
            agent_names.push(agent_name);
        }

        (relay_name, agent_names)
    }
}
