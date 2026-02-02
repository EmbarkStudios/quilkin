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
    pub fn lifecycle(&self) -> &quilkin_system::lifecycle::Lifecycle {
        &self.lifecycle
    }

    #[inline]
    pub fn lifecycle_owned(&self) -> quilkin_system::lifecycle::Lifecycle {
        self.lifecycle.clone()
    }

    pub async fn await_any_then_shutdown(mut self) -> Vec<(&'static str, eyre::Result<()>)> {
        let (which, res) = {
            let mut rx = self.lifecycle.shutdown_rx();
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
            let _ = self.lifecycle.shutdown_tx().send(());
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
