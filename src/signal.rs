/// Receiver for a shutdown event.
pub type ShutdownRx = quilkin_system::lifecycle::ShutdownRx;
pub type ShutdownTx = quilkin_system::lifecycle::ShutdownTx;

pub fn channel() -> (ShutdownTx, ShutdownRx) {
    tokio::sync::watch::channel(())
}

pub struct ShutdownHandler {
    lifecycle: quilkin_system::lifecycle::Lifecycle,
    services:
        std::collections::BTreeMap<&'static str, tokio::sync::oneshot::Receiver<eyre::Result<()>>>,
}

impl ShutdownHandler {
    pub fn new() -> Self {
        Self {
            lifecycle: Default::default(),
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
        self.lifecycle.shutdown_rx()
    }

    #[inline]
    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.lifecycle.shutdown_tx()
    }

    #[inline]
    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.lifecycle.shutdown_token()
    }

    #[inline]
    pub fn shutdown_future(&self) -> impl Future<Output = ()> + use<> {
        self.lifecycle.shutdown_future()
    }

    #[inline]
    pub fn lifecycle(&self) -> quilkin_system::lifecycle::Lifecycle {
        self.lifecycle.clone()
    }

    // #[inline]
    // pub async fn wait_signal(
    //     mut self,
    // ) -> (
    //     ShutdownTx,
    //     ShutdownRx,
    //     Vec<(&'static str, eyre::Result<()>)>,
    // ) {
    //     let _ = self.rx.changed().await;
    //     let mut results = Vec::with_capacity(self.services.len());
    //     let (t, r) = self.await_all(&mut results).await;
    //     (t, r, results)
    // }
    //
    // #[inline]
    // pub async fn shutdown(
    //     self,
    // ) -> (
    //     ShutdownTx,
    //     ShutdownRx,
    //     Vec<(&'static str, eyre::Result<()>)>,
    // ) {
    //     let _ = self.tx.send(());
    //     let mut results = Vec::with_capacity(self.services.len());
    //     let (t, r) = self.await_all(&mut results).await;
    //     (t, r, results)
    // }

    pub async fn await_any_then_shutdown(mut self) -> Vec<(&'static str, eyre::Result<()>)> {
        let (which, res) = {
            let mut rx = self.shutdown_rx();
            let mut completions = std::pin::pin!(&mut self.services);
            let mut srx = std::pin::pin!(rx.changed());
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
            let _ = self.shutdown_tx().send(());
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
    }
}
