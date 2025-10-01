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
    use k8s_openapi::{
        api::core::v1::{Pod, PodSpec},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use kube::{Api, ResourceExt, api::PostParams, runtime::wait::await_condition};
    use std::time::Duration;
    use tokio::time::timeout;

    use crate::{Client, is_pod_ready, quilkin_container};

    #[tokio::test]
    async fn create_quilkin_pod() {
        let client = Client::new().await;

        let pods: Api<Pod> = client.namespaced_api();
        let cmds = [
            "--service.udp",
            "--provider.static.endpoints",
            "127.0.0.1:9999",
        ]
        .map(String::from)
        .to_vec();
        let pod = Pod {
            metadata: ObjectMeta {
                generate_name: Some("quilkin-".into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![quilkin_container(&client, Some(cmds), None, true)],
                ..Default::default()
            }),
            status: None,
        };

        // create the pod
        let pp = PostParams::default();
        let pod = pods.create(&pp, &pod).await.unwrap();

        // now wait for it be become ready.
        let name = pod.name_unchecked();
        let running = await_condition(pods, name.as_str(), is_pod_ready());
        timeout(Duration::from_secs(30), running)
            .await
            .expect("Pod should be running")
            .unwrap();
    }
}
