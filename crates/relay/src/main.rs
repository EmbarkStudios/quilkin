fn main() {
    let name = "client";
    let mut file = unsafe {
        let mut ifr: libc::ifreq = std::mem::zeroed();
        ifr.ifr_ifru.ifru_flags = (libc::IFF_TAP | libc::IFF_NO_PI | libc::IFF_MULTI_QUEUE) as _;
        name.as_bytes()
            .as_ptr()
            .cast::<i8>()
            .copy_to_nonoverlapping(ifr.ifr_name.as_mut_ptr(), name.len());

        let fd = libc::open(
            c"/dev/net/tun".as_ptr().cast(),
            libc::O_RDWR | libc::O_CLOEXEC,
        );
        assert!(
            fd >= 0,
            "failed to open /dev/net/tun {}",
            std::io::Error::last_os_error()
        );
        if libc::ioctl(fd, libc::TUNSETIFF, &mut ifr) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            panic!("failed to open {name}: {err}");
        }

        use std::os::fd::FromRawFd;
        std::fs::File::from_raw_fd(fd)
    };

    let mut buf = [0; 1024];
    loop {
        use std::io::Read;
        let read = file.read(&mut buf).unwrap();

        println!("read {read} bytes");

        if read == 0 {
            break;
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(10000));
    //     let [one, two] = std::env::args()
    //         .skip(1)
    //         .take(2)
    //         .map(|addr| std::net::UdpSocket::bind(addr).unwrap())
    //         .collect::<Vec<_>>()
    //         .try_into()
    //         .unwrap();
    //
    //     let [outside, inside] = std::env::args()
    //         .skip(3)
    //         .take(2)
    //         .map(|addr| addr.parse::<std::net::SocketAddr>().unwrap())
    //         .collect::<Vec<_>>()
    //         .try_into()
    //         .unwrap();
    //
    //     std::thread::scope(|s| {
    //         s.spawn(|| {
    //             let mut buf = [0; 1024];
    //
    //             loop {
    //                 let read = one.recv(&mut buf).unwrap();
    //                 two.send_to(&buf[..read], inside).unwrap();
    //             }
    //         });
    //         s.spawn(|| {
    //             let mut buf = [0; 1024];
    //
    //             loop {
    //                 let read = two.recv(&mut buf).unwrap();
    //                 one.send_to(&buf[..read], outside).unwrap();
    //             }
    //         });
    //     })
}
