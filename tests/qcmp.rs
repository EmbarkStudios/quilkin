/*
 * Copyright 2023 Google LLC
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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::time::Duration;

use quilkin::codec::qcmp::Protocol;

#[tokio::test]
#[cfg_attr(target_os = "macos", ignore)]
async fn proxy_ping() {
    let shutdown_handler = quilkin::signal::spawn_handler();
    let stx = shutdown_handler.shutdown_tx();

    let providers = quilkin::Providers::default();

    let svc = quilkin::Service::builder().qcmp().qcmp_port(0);

    let config = quilkin::Config::new_rc(
        None,
        quilkin_types::IcaoCode::new_testing([b'X'; 4]),
        &providers,
        &svc,
        tokio_util::sync::CancellationToken::new(),
    );

    let (task, ports) = svc
        .spawn_services(&config, shutdown_handler, None)
        .await
        .unwrap();

    ping(ports.qcmp.expect("didn't spawn QCMP")).await;
    stx.send(()).unwrap();
    task.await.unwrap().1.unwrap();
}

async fn ping(port: u16) {
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
        .await
        .unwrap();
    let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let ping = Protocol::ping();

    let mut ping_packet = quilkin::codec::qcmp::QcmpPacket::default();
    socket
        .send_to(ping.encode(&mut ping_packet), &local_addr)
        .await
        .unwrap();
    let mut buf = [0; quilkin::codec::qcmp::MAX_QCMP_PACKET_LEN];
    let (size, _) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let recv_time = quilkin::time::UtcTimestamp::now();
    let reply = Protocol::parse(&buf[..size]).unwrap().unwrap();

    assert_eq!(ping.nonce(), reply.nonce());
    const MAX: std::time::Duration = std::time::Duration::from_millis(50);

    // If it takes longer than 50 milliseconds locally, it's likely that there
    // is bug.
    let delay = reply.round_trip_delay(recv_time).unwrap();
    assert!(
        MAX > delay.duration(),
        "Delay {:?} greater than {MAX:?}",
        delay.duration(),
    );
}
