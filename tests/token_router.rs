/*
 * Copyright 2020 Google LLC
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

use std::net::{Ipv4Addr, SocketAddr};

use tokio::time::{timeout, Duration};

use quilkin::{
    config::Filter,
    endpoint::Endpoint,
    filters::{Capture, StaticFilter, TokenRouter},
    metadata::MetadataView,
    test_utils::TestHelper,
};

/// This test covers both token_router and capture filters,
/// since they work in concert together.
#[tokio::test]
async fn token_router() {
    let mut t = TestHelper::default();
    let echo = t.run_echo_server().await;

    let capture_yaml = "
suffix:
    size: 3
    remove: true
";
    let endpoint_metadata = "
quilkin.dev:
    tokens:
        - YWJj # abc
        ";
    let server_port = 12348;
    let server_config = quilkin::Config::builder()
        .port(server_port)
        .filters(vec![
            Filter {
                name: Capture::factory().name().into(),
                config: serde_yaml::from_str(capture_yaml).unwrap(),
            },
            Filter {
                name: TokenRouter::factory().name().into(),
                config: None,
            },
        ])
        .endpoints(vec![Endpoint::with_metadata(
            echo,
            serde_yaml::from_str::<MetadataView<_>>(endpoint_metadata).unwrap(),
        )])
        .build()
        .unwrap();
    t.run_server_with_config(server_config);

    // valid packet
    let (mut recv_chan, socket) = t.open_socket_and_recv_multiple_packets().await;

    let local_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, server_port));
    let msg = b"helloabc";
    socket.send_to(msg, &local_addr).await.unwrap();

    assert_eq!(
        "hello",
        timeout(Duration::from_secs(5), recv_chan.recv())
            .await
            .expect("should have received a packet")
            .unwrap()
    );

    // send an invalid packet
    let msg = b"helloxyz";
    socket.send_to(msg, &local_addr).await.unwrap();

    let result = timeout(Duration::from_secs(3), recv_chan.recv()).await;
    assert!(result.is_err(), "should not have received a packet");
}
