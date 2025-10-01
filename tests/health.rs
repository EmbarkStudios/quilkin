/*
 * Copyright 2021 Google LLC
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
use std::panic;

use hyper::Uri;

use quilkin::{net::endpoint::Endpoint, test::TestHelper};

const LIVE_ADDRESS: &str = "http://localhost:9093/live";

#[tokio::test]
async fn health_server() {
    let mut t = TestHelper::default();

    // create server configuration
    let server_config = TestHelper::new_config();
    server_config
        .dyn_cfg
        .clusters()
        .unwrap()
        .modify(|clusters| {
            clusters.insert_default(["127.0.0.1:0".parse::<Endpoint>().unwrap()].into());
        });
    t.run_server(
        server_config,
        None,
        Some(Some((std::net::Ipv6Addr::UNSPECIFIED, 9093).into())),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build_http::<http_body_util::Empty<bytes::Bytes>>();
    use http_body_util::BodyExt;
    let resp = client
        .get(Uri::from_static(LIVE_ADDRESS))
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();

    assert_eq!("ok", String::from_utf8(resp).unwrap());

    let _unused = panic::catch_unwind(|| {
        panic!("oh no!");
    });

    let resp = client.get(Uri::from_static(LIVE_ADDRESS)).await.unwrap();
    assert!(resp.status().is_server_error(), "Should be unhealthy");
}
