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
async fn test_filter() {
    let server_addr = "[fefe::0490]:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            TestFilter => None,
        ]),
        xdp_util::endpoints(&[(server_addr, &[])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    let client_addr = "109.108.107.106".parse().unwrap();

    const ORIGINAL: &[u8] = b"testing testing, 123";
    let packet = simple_loop.echo(client_addr, 4532, ORIGINAL).unwrap();

    let payload = packet.payload();
    assert_eq!(&payload[..ORIGINAL.len()], ORIGINAL);

    let read = format!(":odr:{client_addr}:4532");
    assert_eq!(
        &payload[ORIGINAL.len()..ORIGINAL.len() + read.len()],
        read.as_bytes()
    );
    let write = format!(":our:{server_addr}:{client_addr}:4532");
    assert_eq!(&payload[ORIGINAL.len() + read.len()..], write.as_bytes());
}

#[tokio::test]
async fn debug_filter() {
    let server_addr = "[fefe::0490]:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            Debug => Some(filters::debug::Config {
                id: Some("client".to_owned()),
            }),
        ]),
        xdp_util::endpoints(&[(server_addr, &[])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    let packet = simple_loop
        .echo("12.13.14.15".parse().unwrap(), 2222, b"packet data")
        .unwrap();
    assert_eq!(packet.payload(), b"packet data");
}
