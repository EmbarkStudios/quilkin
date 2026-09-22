use core::pin::pin;

pub mod metrics;

/// Serves an axum http service.
///
/// Heavily inspired by and very similar to `axum::serve().with_graceful_shutdown()`, but with added
/// `http_connections` metric to track how many open connections there are for this service.
pub async fn serve<L>(
    service: &'static str,
    mut listener: L,
    router: axum::Router,
    mut spawner: quilkin_graceful::GracefulSpawner,
    shutdown_timeout: std::time::Duration,
) -> std::io::Result<()>
where
    L: axum::serve::Listener,
{
    let shutdown = spawner.token();

    {
        let handle = spawner.handle();
        loop {
            let (socket, _remote_addr) = tokio::select! {
                conn = listener.accept() => conn,
                _ = shutdown.cancelled() => {
                    tracing::trace!("signal received, not accepting new connections");
                    break;
                }
            };

            handle_connection::<L>(service, socket, &router, &handle).await;
        }
    }

    if !spawner.wait(shutdown_timeout).await {
        tracing::warn!(
            service,
            ?shutdown_timeout,
            "timed out waiting for all tasks to finish"
        );
    }

    Ok(())
}

async fn handle_connection<L: axum::serve::Listener>(
    service: &str,
    socket: <L as axum::serve::Listener>::Io,
    router: &axum::Router,
    spawner: &quilkin_graceful::Spawner<'_>,
) {
    let socket = hyper_util::rt::TokioIo::new(socket);

    let service = service.to_owned();
    let shutdown = spawner.token();
    let hyper_service = hyper_util::service::TowerToHyperService::new(router.clone());

    spawner.spawn(async move {
        let conn_labels = metrics::ConnectionLabels { service };
        let _conn_guard = metrics::connection_guard(&conn_labels);

        let mut builder =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
        // CONNECT protocol needed for HTTP/2 websockets
        builder.http2().enable_connect_protocol();

        let mut conn = pin!(builder.serve_connection_with_upgrades(socket, hyper_service));

        loop {
            tokio::select! {
                result = conn.as_mut() => {
                    if let Err(error) = result {
                        tracing::trace!(%error, "connection closed");
                    }
                    break;
                }
                _ = shutdown.cancelled() => {
                    tracing::trace!("signal received in task, starting graceful shutdown");
                    conn.as_mut().graceful_shutdown();
                }
            }
        }
    });
}
