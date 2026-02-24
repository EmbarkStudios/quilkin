use socket2::{Domain, Socket, TcpKeepalive, Type};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

/// Creates and binds a TCP socket with some sane defaults for an HTTP server
pub fn default_socket(socket_addr: impl Into<SocketAddr>) -> std::io::Result<Socket> {
    let socket_addr: SocketAddr = socket_addr.into();

    let domain = if socket_addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, None)?;

    if domain == Domain::IPV6 {
        // Allow IPv4 traffic via IPv4-mapped IPv6 address
        socket.set_only_v6(false)?;
    }

    // https://github.com/tokio-rs/mio/blob/66ac9fab79bf191218488c4f35c99d13935b7e12/src/net/tcp/listener.rs#L81
    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;

    socket.bind(&socket_addr.into())?;

    // Ensure that broken connections eventually get culled
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_mins(5))
        .with_retries(5)
        .with_interval(Duration::from_secs(90));
    socket.set_tcp_keepalive(&keepalive)?;

    // https://github.com/tokio-rs/mio/blob/66ac9fab79bf191218488c4f35c99d13935b7e12/src/sys/mod.rs#L84
    let backlog = if cfg!(target_os = "windows") {
        128
    } else {
        -1 // linux, apple, bsd
    };
    socket.listen(backlog)?;

    tracing::debug!(?socket_addr, "tcp socket configured");
    Ok(socket)
}

/// Creates a non-blocking `TcpListener` with some sane defaults for the underlying socket
pub fn default_nonblocking_listener(
    socket_addr: impl Into<SocketAddr>,
) -> std::io::Result<TcpListener> {
    let listener: TcpListener = default_socket(socket_addr)?.into();
    listener.set_nonblocking(true)?;

    tracing::debug!(?listener, "tcp listener configured");
    Ok(listener)
}
