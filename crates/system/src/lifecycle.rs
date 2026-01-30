/// Receiver for a shutdown event.
pub type ShutdownRx = tokio::sync::watch::Receiver<()>;
pub type ShutdownTx = tokio::sync::watch::Sender<()>;

pub mod metrics {
    use std::sync::LazyLock;

    use prometheus_client::{metrics::gauge::Gauge, registry::Registry};

    struct LifecycleMetrics {
        shutdown_initiated: Gauge,
    }

    fn get_lifecycle_metrics() -> &'static LifecycleMetrics {
        static LIFECYCLE_METRICS: LazyLock<LifecycleMetrics> = LazyLock::new(|| LifecycleMetrics {
            shutdown_initiated: <_>::default(),
        });
        &LIFECYCLE_METRICS
    }

    pub fn register_metrics(registry: &mut Registry) {
        let lifecycle_metrics = get_lifecycle_metrics();
        registry.register(
            "shutdown_initiated",
            "The application is shutting down",
            lifecycle_metrics.shutdown_initiated.clone(),
        );
    }

    pub(super) fn shutdown_initiated() {
        get_lifecycle_metrics().shutdown_initiated.set(1);
    }
}

/// Creates a new handler for shutdown signal (e.g. SIGTERM, SIGINT), which will trigger a shutdown
/// via `shutdown_tx` when a signal has been received.
pub fn spawn_signal_handler(shutdown_tx: ShutdownTx) {
    #[cfg(target_os = "linux")]
    let mut sig_term_fut =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    tokio::spawn(async move {
        #[cfg(target_os = "linux")]
        let sig_term = sig_term_fut.recv();
        #[cfg(not(target_os = "linux"))]
        let sig_term = std::future::pending();

        let signal = tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sig_term => "SIGTERM",
        };
        tracing::info!(%signal, "shutting down from signal");
        // Don't unwrap in order to ensure that we execute
        // any subsequent shutdown tasks.
        let _ = shutdown_tx.send(());
    });
}

#[derive(Clone)]
pub struct Lifecycle {
    tx: ShutdownTx,
    rx: ShutdownRx,
    token: tokio_util::sync::CancellationToken,
    // services:
    //     std::collections::BTreeMap<&'static str, tokio::sync::oneshot::Receiver<eyre::Result<()>>>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl Lifecycle {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(());
        let token = tokio_util::sync::CancellationToken::new();
        tokio::spawn({
            let mut rx = rx.clone();
            let token = token.clone();
            async move {
                let _ = rx.changed().await;
                metrics::shutdown_initiated();
                token.cancel();
            }
        });
        Self { tx, rx, token }
    }

    /// Returns a `ShutdownTx` which can be used to trigger an application shutdown
    #[inline]
    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.tx.clone()
    }

    /// Returns a `ShutdownRx` that will change whenever a shutdown has been triggered
    #[inline]
    pub fn shutdown_rx(&self) -> ShutdownRx {
        self.rx.clone()
    }

    /// Returns a `CancellationToken` that will be canceled when shutdown is triggered
    ///
    /// Cancelling this token will not trigger a shutdown, use `shutdown_tx()` instead
    #[inline]
    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.token.child_token()
    }

    /// Returns a future that finishes when a shutdown has been triggered
    #[inline]
    pub fn shutdown_future(&self) -> impl Future<Output = ()> + use<> {
        let mut rx = self.rx.clone();
        async move {
            let _ = rx.changed().await;
        }
    }
}
