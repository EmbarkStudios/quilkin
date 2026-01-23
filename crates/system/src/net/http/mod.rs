use core::pin::pin;
use futures::FutureExt as _;
use tokio::sync::watch;

pub mod metrics;

/// Serves an axum http service.
///
/// Heavily inspired by and very similar to `axum::serve().with_graceful_shutdown()`, but with added
/// `http_connections` metric to track how many open connections there are for this service.
pub async fn serve<L, F>(
    service: impl Into<String>,
    mut listener: L,
    router: axum::Router,
    shutdown_signal: F,
) -> std::io::Result<()>
where
    L: axum::serve::Listener,
    F: Future<Output = ()> + Send + 'static,
{
    let service: String = service.into();

    // watch channel used to initiate graceful shutdown of connections and stop accepting new ones
    let (signal_tx, signal_rx) = watch::channel(());
    tokio::spawn(async move {
        shutdown_signal.await;
        tracing::trace!("received graceful shutdown signal. Telling tasks to shutdown");
        drop(signal_rx);
    });

    // watch channel used to ensure accepted requests are fullfilled before we shut down fully
    let (close_tx, close_rx) = watch::channel(());

    loop {
        let (socket, _remote_addr) = tokio::select! {
            conn = listener.accept() => conn,
            _ = signal_tx.closed() => {
                tracing::trace!("signal received, not accepting new connections");
                break;
            }
        };

        handle_connection::<L>(&service, &signal_tx, &close_rx, socket, &router).await;
    }

    drop(close_rx);

    // ensure all requests currently being processed are allowed to finish before we return
    tracing::trace!(
        "waiting for {} task(s) to finish",
        close_tx.receiver_count()
    );
    close_tx.closed().await;
    Ok(())
}

async fn handle_connection<L: axum::serve::Listener>(
    service: &str,
    signal_tx: &watch::Sender<()>,
    close_rx: &watch::Receiver<()>,
    socket: <L as axum::serve::Listener>::Io,
    router: &axum::Router,
) {
    let socket = hyper_util::rt::TokioIo::new(socket);

    let service = service.to_owned();
    let signal_tx = signal_tx.clone();
    let close_rx = close_rx.clone();
    let hyper_service = hyper_util::service::TowerToHyperService::new(router.clone());

    tokio::spawn(async move {
        let conn_labels = metrics::ConnectionLabels { service };
        let _conn_guard = metrics::connection_guard(&conn_labels);

        let mut builder =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
        // CONNECT protocol needed for HTTP/2 websockets
        builder.http2().enable_connect_protocol();

        let mut conn = pin!(builder.serve_connection_with_upgrades(socket, hyper_service));
        let mut signal_closed = pin!(signal_tx.closed().fuse());

        loop {
            tokio::select! {
                result = conn.as_mut() => {
                    if let Err(error) = result {
                        tracing::trace!(%error, "connection closed");
                    }
                    break;
                }
                _ = &mut signal_closed => {
                    tracing::trace!("signal received in task, starting graceful shutdown");
                    conn.as_mut().graceful_shutdown();
                }
            }
        }
        // signal that all requests on this connection have finished
        drop(close_rx);
    });
}
