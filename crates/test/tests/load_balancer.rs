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
async fn load_balancer_round_robin() {
    let servers: &mut [(std::net::SocketAddr, u32)] = &mut [
        ("1.2.3.4:9001".parse().unwrap(), 0),
        ("[::1111]:1900".parse().unwrap(), 0),
        ("1.2.3.4:1900".parse().unwrap(), 0),
        ("[::f3f3]:1900".parse().unwrap(), 0),
    ];

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            LoadBalancer => filters::load_balancer::Config {
                policy: filters::load_balancer::Policy::RoundRobin,
            },
        ]),
        servers
            .iter()
            .map(|(addr, _)| quilkin::net::Endpoint::new((*addr).into()))
            .collect(),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    const SENDS: u32 = 32;

    let client = "4.4.4.4".parse().unwrap();

    for _ in 0..SENDS {
        let tp = simple_loop.make_client_packet(client, 3333, b"lb").unwrap();
        let sp = simple_loop.process(tp).unwrap();

        let dest = sp.udp_headers.destination_address();

        let count = servers
            .iter_mut()
            .find_map(|(addr, count)| (addr == &dest).then_some(count))
            .expect("failed to find an expected server");
        *count += 1;
    }

    let expected = SENDS / servers.len() as u32;
    for (addr, count) in servers {
        assert_eq!(
            *count, expected,
            "{addr} didn't have the expected number of packets"
        );
    }
}
