use etherparse as ep;
use std::{io, net, os::fd::AsRawFd};

use quilkin_xdp::l2_cache::types::RawSocket;

fn ip_cmd_ok(cmd: &str) {
    let mut c = std::process::Command::new("ip");
    c.args(cmd.split(' '));
    assert!(c.status().unwrap().success(), "failed to run `ip {cmd}`");
}

fn ip_cmd(cmd: &str) {
    let mut c = std::process::Command::new("ip");
    c.args(cmd.split(' '));
    c.status().unwrap();
}

#[test]
fn sends_raw() {
    ip_cmd("netns del raw-sender");
    ip_cmd_ok("netns add raw-sender");
    ip_cmd("netns del raw-echo");
    ip_cmd_ok("netns add raw-echo");

    ip_cmd_ok("link add raw-tx netns raw-sender type veth peer name raw-rx netns raw-echo");

    let tx_ip = net::Ipv4Addr::new(192, 168, 88, 45);
    let tx_ip6 = "fe80::feed".parse::<net::Ipv6Addr>().unwrap();
    let rx_ip = net::Ipv4Addr::new(192, 168, 88, 46);
    let rx_ip6 = "fe80::deef".parse::<net::Ipv6Addr>().unwrap();
    ip_cmd_ok(&format!("-n raw-sender addr add {tx_ip}/24 dev raw-tx"));
    ip_cmd_ok(&format!("-6 -n raw-sender addr add {tx_ip6}/64 dev raw-tx"));
    ip_cmd_ok(&format!("-n raw-echo addr add {rx_ip}/24 dev raw-rx"));
    ip_cmd_ok(&format!("-6 -n raw-echo addr add {rx_ip6}/64 dev raw-rx"));

    ip_cmd_ok("-n raw-sender link set raw-tx up");
    ip_cmd_ok("-n raw-echo link set raw-rx up");

    let tx_ip6 = {
        let mut c = std::process::Command::new("ip");
        c.args([
            "-6",
            "-n",
            "raw-sender",
            "-brief",
            "addr",
            "show",
            "scope",
            "link",
        ]);
        let s = String::from_utf8(c.output().unwrap().stdout).unwrap();
        let pingee = s.split(' ').filter(|s| !s.is_empty()).nth(2).unwrap();
        pingee
            .strip_suffix("/64")
            .unwrap()
            .parse::<net::Ipv6Addr>()
            .unwrap()
    };

    let rx_ip6 = {
        let mut c = std::process::Command::new("ip");
        c.args([
            "-6", "-n", "raw-echo", "-brief", "addr", "show", "scope", "link",
        ]);
        let s = String::from_utf8(c.output().unwrap().stdout).unwrap();
        let pingee = s.split(' ').filter(|s| !s.is_empty()).nth(2).unwrap();
        pingee
            .strip_suffix("/64")
            .unwrap()
            .parse::<net::Ipv6Addr>()
            .unwrap()
    };

    let src_port = 8888;
    let dst_port = 9999;

    std::thread::scope(|s| {
        s.spawn(|| {
            let f = std::fs::File::open("/var/run/netns/raw-sender").unwrap();
            unsafe {
                libc::setns(f.as_raw_fd(), libc::CLONE_NEWNET);
            };

            let raw = RawSocket::new().unwrap();
            raw.in4.set_nonblocking(false).unwrap();
            raw.in6.set_nonblocking(false).unwrap();

            let mut send = Vec::new();

            for i in 1..64u8 {
                let payload = vec![i; i as usize];
                //                 ep::PacketBuilder::ipv4(tx_ip.octets(), rx_ip.octets(), 64)
                //                     .udp(src_port, dst_port)
                //                     .write_to_vec(&mut send, &payload)
                //                     .unwrap();
                //
                //                 raw.in4.send_to(&send, (rx_ip, dst_port)).unwrap();
                //                 send.clear();

                ep::PacketBuilder::ipv6(tx_ip6.octets(), rx_ip6.octets(), 64)
                    .udp(src_port, dst_port)
                    .write_to_vec(&mut send, &payload)
                    .unwrap();

                raw.in6.send_to(&send, (rx_ip6, dst_port)).unwrap();
                send.clear();
            }

            ep::PacketBuilder::ipv4(tx_ip.octets(), rx_ip.octets(), 64)
                .udp(src_port, dst_port)
                .write_to_vec(&mut send, &[])
                .unwrap();

            raw.in4.send_to(&send, (rx_ip, dst_port)).unwrap();
            send.clear();
        });
        s.spawn(|| {
            let f = std::fs::File::open("/var/run/netns/raw-echo").unwrap();
            unsafe {
                libc::setns(f.as_raw_fd(), libc::CLONE_NEWNET);
            };

            let echo = std::net::UdpSocket::bind((std::net::Ipv6Addr::UNSPECIFIED, dst_port))
                .expect("failed to bind echo socket");
            let mut buf = [0; 128];
            loop {
                let (read, addr) = dbg!(echo.recv_from(&mut buf).unwrap());

                assert!(buf[..read].iter().all(|b| *b == read as u8));

                if read == 0 {
                    break;
                }

                echo.send_to(&buf[..read], addr).unwrap();
            }
        });
    });
}
