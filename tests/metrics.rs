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

use std::net::SocketAddr;

use quilkin::{
    net::endpoint::Endpoint,
    test::{AddressType, TestHelper},
};

#[tokio::test]
#[cfg_attr(target_os = "macos", ignore)]
async fn metrics_server() {
    let mut t = TestHelper::default();

    // create an echo server as an endpoint.
    let echo = t.run_echo_server(AddressType::Random).await;
    let metrics_port = quilkin::test::available_addr(AddressType::Random)
        .await
        .port();

    // create server configuration
    let server_config = TestHelper::new_config();
    server_config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| clusters.insert_default([Endpoint::new(echo.clone())].into()));
    let server_port = t
        .run_server(
            server_config,
            None,
            Some(Some((std::net::Ipv4Addr::UNSPECIFIED, metrics_port).into())),
        )
        .await;

    // create a local client
    let client_config = TestHelper::new_config();
    client_config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| {
            clusters.insert_default(
                [Endpoint::new(
                    (std::net::Ipv6Addr::LOCALHOST, server_port).into(),
                )]
                .into(),
            );
        });
    let client_port = t.run_server(client_config, None, None).await;

    // let's send the packet
    let (mut recv_chan, socket) = t.open_socket_and_recv_multiple_packets().await;

    // game_client
    let local_addr = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, client_port));
    tracing::info!(address = %local_addr, "Sending hello");
    socket.send_to(b"hello", &local_addr).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_millis(100), recv_chan.recv())
        .await
        .unwrap()
        .unwrap();

    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http::<http_body_util::Empty<bytes::Bytes>>();
    use http_body_util::BodyExt;
    let resp = client
        .get(
            format!("http://localhost:{metrics_port}/metrics")
                .parse()
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();

    let response = String::from_utf8(resp.to_vec()).unwrap();
    let read_regex = regex::Regex::new(r#"quilkin_packets_total\{.*event="read".*\} 2"#).unwrap();
    let write_regex = regex::Regex::new(r#"quilkin_packets_total\{.*event="write".*\} 2"#).unwrap();
    assert!(read_regex.is_match(&response));
    assert!(write_regex.is_match(&response));
}
