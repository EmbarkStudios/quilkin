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

use std::net::Ipv4Addr;

use tokio::time::{timeout, Duration};

use quilkin::{
    config::Filter,
    endpoint::Endpoint,
    filters::{ConcatenateBytes, StaticFilter},
    test_utils::TestHelper,
};

#[tokio::test]
async fn concatenate_bytes() {
    let mut t = TestHelper::default();
    let yaml = "
on_read: APPEND
bytes: YWJj #abc
";
    let echo = t.run_echo_server().await;

    let server_port = 12346;
    let server_proxy = quilkin::cli::Proxy {
        port: server_port,
        ..<_>::default()
    };
    let server_config = std::sync::Arc::new(quilkin::Config::default());
    server_config
        .clusters
        .modify(|clusters| clusters.insert_default(vec![Endpoint::new(echo.clone())]));
    server_config.filters.store(
        quilkin::filters::FilterChain::try_from(vec![Filter {
            name: ConcatenateBytes::factory().name().into(),
            label: None,
            config: serde_yaml::from_str(yaml).unwrap(),
        }])
        .map(std::sync::Arc::new)
        .unwrap(),
    );
    t.run_server(server_config, server_proxy, None);

    // let's send the packet
    let (mut recv_chan, socket) = t.open_socket_and_recv_multiple_packets().await;

    let local_addr = (Ipv4Addr::LOCALHOST, server_port);
    socket.send_to(b"hello", &local_addr).await.unwrap();

    assert_eq!(
        "helloabc",
        timeout(Duration::from_secs(5), recv_chan.recv())
            .await
            .expect("should have received a packet")
            .unwrap()
    );
}
