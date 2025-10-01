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
async fn local_rate_limit_filter() {
    let server_addr = "10.1.1.100:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            LocalRateLimit => filters::local_rate_limit::Config {
               max_packets: 2,
               period: 1,
            }
        ]),
        xdp_util::endpoints(&[(server_addr, &[])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    for (i, &rated) in [false, false, true, true].iter().enumerate() {
        let packet = simple_loop
            .make_client_packet("1.1.1.1".parse().unwrap(), 20, &i.to_ne_bytes())
            .unwrap();
        let packet_to_server = simple_loop.process(packet);

        if rated {
            assert!(packet_to_server.is_none());
        } else {
            let pts = packet_to_server.unwrap();
            assert_eq!(pts.udp_headers.destination_address(), server_addr);
            assert_eq!(pts.payload(), &i.to_ne_bytes());
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    for (i, &rated) in [false, false, true, true].iter().enumerate() {
        let packet = simple_loop
            .make_client_packet("1.1.1.1".parse().unwrap(), 20, &(i + 22).to_ne_bytes())
            .unwrap();
        let packet_to_server = simple_loop.process(packet);

        if rated {
            assert!(packet_to_server.is_none());
        } else {
            let pts = packet_to_server.unwrap();
            assert_eq!(pts.udp_headers.destination_address(), server_addr);
            assert_eq!(pts.payload(), &(i + 22).to_ne_bytes());
        }
    }
}
