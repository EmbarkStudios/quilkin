use std::{io, net, os::fd::AsRawFd};

use quilkin_xdp::l2_cache::icmp::*;

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
fn pings() {
    ip_cmd("netns del pinger");
    ip_cmd_ok("netns add pinger");
    ip_cmd("netns del pingee");
    ip_cmd_ok("netns add pingee");

    ip_cmd_ok("link add vtest netns pinger type veth peer name end netns pingee");

    ip_cmd_ok("-n pinger addr add 192.168.88.45/24 dev vtest");
    ip_cmd_ok("-6 -n pinger addr add fe80::feed/64 dev vtest");
    ip_cmd_ok("-n pingee addr add 192.168.88.46/24 dev end");
    ip_cmd_ok("-6 -n pingee addr add fe80::deef/64 dev end");

    ip_cmd_ok("-n pinger link set vtest up");
    ip_cmd_ok("-n pingee link set end up");

    let pingeev6 = {
        let mut c = std::process::Command::new("ip");
        c.args([
            "-6", "-n", "pingee", "-brief", "addr", "show", "scope", "link",
        ]);
        let s = String::from_utf8(c.output().unwrap().stdout).unwrap();
        let pingee = s.split(' ').filter(|s| !s.is_empty()).nth(2).unwrap();
        pingee.strip_suffix("/64").unwrap().to_owned()
    };

    let f = std::fs::File::open("/var/run/netns/pinger").unwrap();
    unsafe {
        libc::setns(f.as_raw_fd(), libc::CLONE_NEWNET);
    };

    let sock_4 = IcmpSocket::create_socket(None).unwrap();

    let mut req = [0; 8];
    let mut res = [0; 128];

    make_echo_request(44, &mut req, true);

    sock_4
        .send_to(&req, (std::net::Ipv4Addr::new(192, 168, 88, 46), 0))
        .unwrap();

    let (len, src) = sock_4.recv_from(&mut res).unwrap();

    let seq = read_echo_reply(&res[..len], true).unwrap();
    assert_eq!(seq, 44, "bad sequence from {src}");

    make_echo_request(66, &mut req, false);

    let addr = net::SocketAddrV6::new(
        pingeev6.parse::<net::Ipv6Addr>().unwrap(),
        0,
        0,
        2, /* lo will be 1, the veth peer will be 2 */
    );

    let sock_6 = IcmpSocket::create_socket(Some(2)).unwrap();

    // exceptionally irritating, but ipv6 sendto will almost always fail with addrnotavailable for some period, so
    // just send until we can actually succeed
    loop {
        match sock_6.send_to(&req, addr) {
            Ok(_) => break,
            Err(err) => {
                if err.kind() == io::ErrorKind::AddrNotAvailable {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }

                panic!("failed ipv6 sendto: {err}");
            }
        }
    }

    // since we're a raw icmpv6 socket our first successful request initiated a neighbor solicitation that will
    // also be sent to our socket, so skip them until we get the echo reply
    let (len, src) = loop {
        let (len, src) = sock_6.recv_from(&mut res).unwrap();

        if len >= std::mem::size_of::<IcmpHeader>() {
            unsafe {
                if res.as_ptr().cast::<IcmpHeader>().read_unaligned().kind == ECHO_REPLYV6 {
                    break (len, src);
                }
            }
        }
    };

    let seq = read_echo_reply(&res[..len], false).unwrap();
    assert_eq!(seq, 66, "bad sequence from {src}");
}
