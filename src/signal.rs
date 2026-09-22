pub use quilkin_graceful::{ChildToken, RootToken, TaskTracker};

pub struct ShutdownHandler {
    token: RootToken,
    tracker: TaskTracker,
    services:
        std::collections::BTreeMap<&'static str, tokio::sync::oneshot::Receiver<eyre::Result<()>>>,
}

impl ShutdownHandler {
    pub fn with_token(token: RootToken) -> Self {
        Self {
            token,
            tracker: TaskTracker::new(),
            services: Default::default(),
        }
    }

    pub fn new() -> Self {
        Self::with_token(RootToken::new())
    }

    /// Hooks shutdown signals (e.g. SIGTERM, SIGINT) and will attempt to gracefully shutdown the various async tasks
    /// registered
    pub fn hook() -> Self {
        crate::metrics::shutdown_initiated().set(false as _);

        let token = RootToken::new();
        let tok = token.clone();

        #[cfg(target_os = "linux")]
        let mut sig_term_fut =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

        std::thread::Builder::new()
            .name("signal-handler".into())
            .spawn(move || {
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
                        tok.cancel();
                    });
            })
            .expect("failed to spawn signal handler");

        Self::with_token(token)
    }

    #[inline]
    pub fn push_async(
        &mut self,
        svc: &'static str,
        task: impl Future<Output = eyre::Result<()>> + Send + 'static,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.services.insert(svc, rx).is_some() {
            panic!("service '{svc}' already registered");
        }

        self.tracker.spawn(async move {
            if tx.send(task.await).is_err() {
                tracing::warn!(service = svc, "failed to send result of service");
            }
        });
    }

    #[inline]
    pub fn push_sync(
        &mut self,
        svc: &'static str,
        wait: impl FnOnce() -> eyre::Result<()> + Send + 'static,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.services.insert(svc, rx).is_some() {
            panic!("service '{svc}' already registered");
        }

        let token = self.token.clone();
        self.tracker.spawn(async move {
            token.cancelled().await;

            let res = tokio::task::block_in_place(|| wait());

            if tx.send(res).is_err() {
                tracing::warn!(service = svc, "failed to send result of service");
            }
        });
    }

    #[inline]
    pub fn root(&self) -> RootToken {
        self.token.clone()
    }

    #[inline]
    pub fn child(&self) -> ChildToken {
        self.token.child()
    }

    #[inline]
    pub async fn wait_signal(self) -> Vec<(&'static str, eyre::Result<()>)> {
        let mut results = Vec::with_capacity(self.services.len());
        self.await_all(&mut results).await;
        results
    }

    #[inline]
    pub async fn shutdown(self) -> Vec<(&'static str, eyre::Result<()>)> {
        self.token.cancel();

        let mut results = Vec::with_capacity(self.services.len());
        self.await_all(&mut results).await;
        results
    }

    pub async fn await_any_then_shutdown(mut self) -> Vec<(&'static str, eyre::Result<()>)> {
        let (which, res) = {
            let mut completions = std::pin::pin!(&mut self.services);
            let mut srx = std::pin::pin!(self.token.cancelled());

            std::future::poll_fn(move |cx| {
                use std::task::Poll;

                if srx.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(("", Ok(())));
                }

                for (key, value) in completions.as_mut().iter_mut() {
                    if let Poll::Ready(res) = std::pin::pin!(value).as_mut().poll(cx) {
                        return Poll::Ready((*key, res.unwrap_or(Ok(()))));
                    }
                }

                Poll::Pending
            })
            .await
        };

        let mut results = Vec::with_capacity(self.services.len());

        // One of the tasks exited prematurely, signal the rest to begin shutting down
        self.token.cancel();

        if !which.is_empty() {
            results.push((which, res));
        }

        self.await_all(&mut results).await;
        results
    }

    async fn await_all(mut self, results: &mut Vec<(&'static str, eyre::Result<()>)>) {
        let start = tokio::time::Instant::now();
        let mut report = tokio::time::Instant::now();
        let mut sleep = std::time::Duration::from_millis(10);

        loop {
            self.services.retain(|k, v| match v.try_recv() {
                Ok(res) => {
                    results.push((*k, res));
                    false
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => true,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    results.push((
                        *k,
                        Err(eyre::format_err!("task exited without providing result")),
                    ));
                    false
                }
            });

            if self.services.is_empty() {
                tracing::info!(elapsed = ?start.elapsed(), count = results.len(), "services all finished");
                break;
            }

            if report.elapsed() > std::time::Duration::from_secs(5) {
                report = tokio::time::Instant::now();
                tracing::debug!(tasks = ?self.services.keys().collect::<Vec<_>>(), "tasks still running");
            }

            tokio::time::sleep(sleep).await;
            sleep = std::cmp::min(
                sleep + std::time::Duration::from_millis(10),
                std::time::Duration::from_millis(100),
            );
        }

        // This should return immediately since we've already gotten results from all of the tasks that were tracked
        self.tracker.wait().await;
    }
}
