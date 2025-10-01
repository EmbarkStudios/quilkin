/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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

struct Rule {
    allow: bool,
    ip: std::net::IpAddr,
    prefix: u8,
    port: u16,
}

struct Config {
    read: Rule,
    write: Rule,
}

fn exec(test: &'static str, config: Config) {
    use filters::firewall as fw;

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            Firewall => fw::Config {
                on_read: vec![fw::Rule {
                    action: config.read.allow.into(),
                    sources: vec![fw::Cidr(
                        fw::IpNetwork::new(config.read.ip, config.read.prefix).unwrap(),
                    )],
                    ports: vec![config.read.port.into()],
                }],
                on_write: vec![fw::Rule {
                    action: config.write.allow.into(),
                    sources: vec![fw::Cidr(
                        fw::IpNetwork::new(config.write.ip, config.write.prefix).unwrap(),
                    )],
                    ports: vec![config.write.port.into()],
                }],
            },
        ]),
        xdp_util::endpoints(&[((config.write.ip, config.write.port).into(), &[])]),
    ));

    const PAYLOAD: &[u8] = b"hello firewall";

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);
    let packet = simple_loop
        .make_client_packet(config.read.ip, config.read.port, PAYLOAD)
        .unwrap();
    let proxied = simple_loop.process(packet);

    let (dest_port, src_port) = {
        let Some(proxied) = proxied else {
            assert!(
                !config.read.allow,
                "`{test}` expected the firewall to pass the client packet"
            );
            return;
        };
        assert_eq!(proxied.payload(), PAYLOAD);
        (
            proxied.udp_headers.udp.source.host(),
            proxied.udp_headers.udp.destination.host(),
        )
    };

    let packet = simple_loop
        .make_server_packet(config.write.ip, src_port, dest_port, PAYLOAD)
        .unwrap();

    let Some(proxied) = simple_loop.process(packet) else {
        assert!(
            !config.write.allow,
            "`{test}` expected the firewall to pass the server packet"
        );
        return;
    };

    assert_eq!(proxied.payload(), PAYLOAD);
    assert_eq!(
        proxied.udp_headers.destination_address(),
        (config.read.ip, config.read.port).into()
    );
}

#[tokio::test]
async fn ipv4_firewall_allow() {
    exec(
        qt::func_name!(),
        Config {
            read: Rule {
                allow: true,
                ip: "127.0.0.1".parse().unwrap(),
                prefix: 32,
                port: 99,
            },
            write: Rule {
                allow: true,
                ip: "127.0.0.0".parse().unwrap(),
                prefix: 24,
                port: 1111,
            },
        },
    );
}

#[tokio::test]
async fn ipv6_firewall_allow() {
    exec(
        qt::func_name!(),
        Config {
            read: Rule {
                allow: true,
                ip: "::1".parse().unwrap(),
                prefix: 128,
                port: 2000,
            },
            write: Rule {
                allow: true,
                ip: "::1".parse().unwrap(),
                prefix: 64,
                port: 40000,
            },
        },
    );
}

#[tokio::test]
async fn ipv4_firewall_read_deny() {
    exec(
        qt::func_name!(),
        Config {
            read: Rule {
                allow: false,
                ip: "127.0.0.1".parse().unwrap(),
                prefix: 32,
                port: 2000,
            },
            write: Rule {
                allow: true,
                ip: "::1".parse().unwrap(),
                prefix: 64,
                port: 40000,
            },
        },
    );
}

#[tokio::test]
async fn ipv6_firewall_read_deny() {
    exec(
        qt::func_name!(),
        Config {
            read: Rule {
                allow: false,
                ip: "::1".parse().unwrap(),
                prefix: 128,
                port: 2000,
            },
            write: Rule {
                allow: true,
                ip: "::1".parse().unwrap(),
                prefix: 64,
                port: 40000,
            },
        },
    );
}

#[tokio::test]
async fn ipv4_firewall_write_deny() {
    exec(
        qt::func_name!(),
        Config {
            read: Rule {
                allow: true,
                ip: "::1".parse().unwrap(),
                prefix: 32,
                port: 2000,
            },
            write: Rule {
                allow: false,
                ip: "127.0.0.0".parse().unwrap(),
                prefix: 24,
                port: 40000,
            },
        },
    );
}

#[tokio::test]
async fn ipv6_firewall_write_deny() {
    exec(
        qt::func_name!(),
        Config {
            read: Rule {
                allow: true,
                ip: "::1".parse().unwrap(),
                prefix: 32,
                port: 2000,
            },
            write: Rule {
                allow: false,
                ip: "::fe90".parse().unwrap(),
                prefix: 64,
                port: 40000,
            },
        },
    );
}
