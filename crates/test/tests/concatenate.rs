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
async fn concatenate() {
    let server_addr = "10.1.1.100:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            Concatenate => filters::concatenate::Config {
                on_read: filters::concatenate::Strategy::Append,
                on_write: filters::concatenate::Strategy::DoNothing,
                bytes: b"abc".to_vec(),
            },
        ]),
        xdp_util::endpoints(&[(server_addr, &[])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    let packet = simple_loop
        .make_client_packet("1.1.1.1".parse().unwrap(), 20, b"hello")
        .unwrap();
    let proxied = simple_loop.process(packet).unwrap();
    assert_eq!(proxied.payload(), b"helloabc");
    assert_eq!(server_addr, proxied.udp_headers.destination_address());
}
