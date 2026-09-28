/// Hooks shutdown signals (e.g. SIGTERM, SIGINT) and will attempt to gracefully shutdown the all async tasks that are
/// children of the provided token
pub fn hook(token: quilkin_graceful::RootToken) {
    crate::metrics::shutdown_initiated().set(false as _);

    #[cfg(target_os = "linux")]
    let mut sig_term_fut =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    std::thread::Builder::new()
        .name("signal-handler".into())
        .spawn(move || {
            // SAFETY: syscalls
            #[cfg(target_os = "linux")]
            unsafe {
                let mut block = std::mem::zeroed();
                libc::sigemptyset(&mut block);
                libc::sigaddset(&mut block, libc::SIGTERM);
                libc::sigaddset(&mut block, libc::SIGINT);
                libc::sigprocmask(libc::SIG_UNBLOCK, &block, std::ptr::null_mut());
            }

            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build_local(Default::default())
                .unwrap()
                .block_on(async move {
                    #[cfg(target_os = "linux")]
                    let sig_term = sig_term_fut.recv();
                    #[cfg(not(target_os = "linux"))]
                    let sig_term = std::future::pending();

                    let signal = tokio::select! {
                        _ = tokio::signal::ctrl_c() => "SIGINT",
                        _ = sig_term => "SIGTERM",
                    };

                    crate::metrics::shutdown_initiated().set(true as _);
                    tracing::info!(%signal, "shutting down from signal");

                    // Cancel the token, initiating the graceful shutdown process
                    token.cancel();
                });
        })
        .expect("failed to spawn signal handler");
}
