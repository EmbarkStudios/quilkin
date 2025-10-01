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

#![cfg(target_os = "linux")]

use qt::xdp_util;
use quilkin::filters;

#[tokio::test]
async fn echo() {
    let server_one = "10.1.1.100:9001".parse().unwrap();
    let server_two = "[::f390]:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        filters::FilterChain::testing([]),
        xdp_util::endpoints(&[(server_one, &[]), (server_two, &[])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(3, proc_state, cfg_state);

    let client = simple_loop
        .make_client_packet("1.2.3.4".parse().unwrap(), 23, b"multi-no-filter")
        .unwrap();

    let packets = simple_loop.process_multi::<4>(client);

    let mut servers = vec![server_one, server_two];

    let packet = packets[0].as_ref().expect("expected a first packet");
    let pos = servers
        .iter()
        .position(|sa| sa == &packet.udp_headers.destination_address())
        .expect("failed to find server");
    servers.remove(pos);
    let packet = packets[1].as_ref().expect("expected a second packet");
    let pos = servers
        .iter()
        .position(|sa| sa == &packet.udp_headers.destination_address())
        .expect("failed to find server");
    servers.remove(pos);

    assert!(servers.is_empty());
    assert!(packets[2].is_none() && packets[3].is_none());
}
