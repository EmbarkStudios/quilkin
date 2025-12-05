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
#![cfg(target_os = "linux")]

use qt::xdp_util;
use quilkin::filters;

// This test covers the scenario in https://github.com/EmbarkStudios/quilkin/issues/988
// to make sure there are no issues with overlapping streams between clients.
#[tokio::test]
async fn multiple_clients() {
    let server_addr = "10.1.1.100:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                size: 3,
                remove: true,
            }),
            TokenRouter => None,
        ]),
        xdp_util::endpoints(&[(server_addr, &[b"abc"])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    let mut payload = [0u8; 7];
    payload[4..].copy_from_slice(b"abc");

    for i in 0..4500u32 {
        let expected = i.to_ne_bytes();
        payload[..4].copy_from_slice(&expected);

        let packet = simple_loop
            .echo(std::net::Ipv4Addr::from_bits(i).into(), 8990, &payload)
            .unwrap();
        assert_eq!(packet.payload(), &expected);
    }
}
