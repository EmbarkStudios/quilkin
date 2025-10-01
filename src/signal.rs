/// Receiver for a shutdown event.
pub type ShutdownRx = tokio::sync::watch::Receiver<()>;
pub type ShutdownTx = tokio::sync::watch::Sender<()>;

/// Creates a new handler for shutdown signal (e.g. SIGTERM, SIGINT), and
/// returns a receiver channel that will receive an event when a shutdown has
/// been requested.
pub fn spawn_handler() -> ShutdownHandler {
    let (tx, rx) = channel();
    crate::metrics::shutdown_initiated().set(false as _);

    #[cfg(target_os = "linux")]
    let mut sig_term_fut =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    let shutdown_tx = tx.clone();
    tokio::spawn(async move {
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
        // Don't unwrap in order to ensure that we execute
        // any subsequent shutdown tasks.
        let _ = shutdown_tx.send(());
    });

    ShutdownHandler::new(tx, rx)
}

pub fn channel() -> (ShutdownTx, ShutdownRx) {
    tokio::sync::watch::channel(())
}

/// Adapter method to create a `CancellationToken` that will be cancelled when the `ShutdownRx`
/// watch channel is changed.
///
/// Spawns a tokio task so avoid calling more than once, clone the token instead.
pub fn cancellation_token(mut rx: ShutdownRx) -> tokio_util::sync::CancellationToken {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let task_token = shutdown_token.clone();
    tokio::spawn(async move {
        let _ = rx.changed().await;
        task_token.cancel();
    });
    shutdown_token
}

pub struct ShutdownHandler {
    tx: ShutdownTx,
    rx: ShutdownRx,
    services:
        std::collections::BTreeMap<&'static str, tokio::sync::oneshot::Receiver<eyre::Result<()>>>,
}

impl ShutdownHandler {
    pub fn new(tx: ShutdownTx, rx: ShutdownRx) -> Self {
        Self {
            tx,
            rx,
            services: Default::default(),
        }
    }

    #[inline]
    pub fn push(&mut self, svc: &'static str) -> tokio::sync::oneshot::Sender<eyre::Result<()>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.services.insert(svc, rx).is_some() {
            panic!("service '{svc}' already registered");
        }
        tx
    }

    #[inline]
    pub fn shutdown_rx(&self) -> ShutdownRx {
        self.rx.clone()
    }

    #[inline]
    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.tx.clone()
    }

    #[inline]
    pub async fn wait_signal(
        mut self,
    ) -> (
        ShutdownTx,
        ShutdownRx,
        Vec<(&'static str, eyre::Result<()>)>,
    ) {
        let _ = self.rx.changed().await;
        let mut results = Vec::with_capacity(self.services.len());
        let (t, r) = self.await_all(&mut results).await;
        (t, r, results)
    }

    #[inline]
    pub async fn shutdown(
        self,
    ) -> (
        ShutdownTx,
        ShutdownRx,
        Vec<(&'static str, eyre::Result<()>)>,
    ) {
        let _ = self.tx.send(());
        let mut results = Vec::with_capacity(self.services.len());
        let (t, r) = self.await_all(&mut results).await;
        (t, r, results)
    }

    pub async fn await_any_then_shutdown(
        mut self,
    ) -> (
        ShutdownTx,
        ShutdownRx,
        Vec<(&'static str, eyre::Result<()>)>,
    ) {
        let (which, res) = {
            let mut completions = std::pin::pin!(&mut self.services);
            let mut srx = std::pin::pin!(self.rx.changed());
            std::future::poll_fn(move |cx| {
                use std::task::Poll;

                if srx.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(("", Ok(())));
                }

                for (key, value) in completions.as_mut().iter_mut() {
                    if let Poll::Ready(res) = std::pin::pin!(value).as_mut().poll(cx) {
                        return Poll::Ready((key, res.unwrap_or(Ok(()))));
                    }
                }

                Poll::Pending
            })
            .await
        };

        let mut results = Vec::with_capacity(self.services.len());

        // If the future completed due to a task exiting, signal shutdown to ensure
        // all the other tasks know to exit
        if !which.is_empty() {
            let _ = self.tx.send(());
            results.push((which, res));
        }

        let (t, r) = self.await_all(&mut results).await;
        (t, r, results)
    }

    async fn await_all(
        mut self,
        results: &mut Vec<(&'static str, eyre::Result<()>)>,
    ) -> (ShutdownTx, ShutdownRx) {
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

        (self.tx, self.rx)
    }
}
