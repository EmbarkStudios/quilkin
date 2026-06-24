use super::GossipContext;
use corro_types::broadcast;
use speedy::Readable as _;
use std::{net::SocketAddr, time::Duration};
use tokio_util::codec;
use tracing::Instrument as _;
use tripwire::{Outcome, PreemptibleFutureExt as _, TimeoutFutureExt as _, Tripwire};

/// Spawn a tree of tasks that handles incoming gossip server connections, streams, and their respective payloads.
pub fn spawn_gossipserver_unencrypted(
    ctx: GossipContext,
    mut tripwire: Tripwire,
    gossip_server_endpoint: SocketAddr,
) -> std::io::Result<()> {
    let mut cfg = quinn_plaintext::server_config();
    let mut tcfg = quinn::TransportConfig::default();

    // Note this is the same as the current default quinn idle timeout, but we specify it here again in case the default
    // changes
    tcfg.max_idle_timeout(Some(
        Duration::from_secs(30).try_into().expect("unreachable"),
    ));
    cfg.transport_config(std::sync::Arc::new(tcfg));

    let endpoint = quinn::Endpoint::server(cfg, gossip_server_endpoint)?;

    spawn::spawn_counted(async move {
        loop {
            let incoming = match endpoint.accept().preemptible(&mut tripwire).await {
                Outcome::Completed(Some(incoming)) => incoming,
                Outcome::Completed(None) => return,
                Outcome::Preempted(_) => break,
            };

            spawn_incoming_connection_handlers(ctx.clone(), tripwire.clone(), incoming);
        }

        // graceful shutdown
        let idle = endpoint.wait_idle().with_timeout(Duration::from_secs(5));
        tokio::pin!(idle);

        loop {
            tokio::select! {
                _ = &mut idle => {
                    break;
                }
                incoming = endpoint.accept() => {
                    // Refuse any new connections while we are gracefully shutting down
                    if let Some(inc) = incoming {
                        inc.refuse();
                    }
                }
            }
        }

        endpoint.close(0u32.into(), b"shutting down");
    });

    Ok(())
}

/// Spawn a task which handles all state and interactions for a given
/// incoming connection.
///
/// This function spawns many futures!
fn spawn_incoming_connection_handlers(
    ctx: GossipContext,
    mut tripwire: Tripwire,
    connecting: quinn::Incoming,
) {
    let remote_addr = connecting.remote_address();

    tokio::spawn(
        async move {
            tracing::trace!("got incoming gossip connection");

            let conn = match connecting.await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::error!(%error, "gossip handshake failed");
                    ctx.metrics.server_handshakes_failed_inc(&error);
                    return;
                }
            };

            tracing::trace!("accepted a gossip connection");
            ctx.metrics.server_conns_inc();

            loop {
                tokio::select! {
                    // Datagrams are used to communicate foca (SWIM) state
                    datagram = conn.read_datagram() => {
                        process_foca_datagram(&ctx, datagram).await;
                    }
                    uni = conn.accept_uni() => {
                        process_broadcast_stream(ctx.clone(), uni, remote_addr);
                    }
                    bi = conn.accept_bi() => {
                        process_sync_stream(ctx.clone(), bi, remote_addr);
                    }
                    _ = &mut tripwire => {
                        tracing::debug!("connection cancelled");
                        return;
                    }
                }
            }
        }
        .instrument(tracing::trace_span!("gossip_connection", %remote_addr)),
    );
}

#[inline]
async fn process_foca_datagram(
    ctx: &GossipContext,
    datagram: Result<bytes::Bytes, quinn::ConnectionError>,
) {
    match datagram {
        Ok(bytes) => {
            ctx.metrics.server_datagrams_inc(&bytes);

            drop(ctx.foca_tx.send(broadcast::FocaInput::Data(bytes)).await);
        }
        Err(error) => {
            ctx.metrics.server_datagrams_failed_inc(&error);
        }
    }
}

/// Spawn a task that accepts unidirectional broadcast streams, then spawns another task for each incoming stream to handle.
fn process_broadcast_stream(
    ctx: GossipContext,
    recv: Result<quinn::RecvStream, quinn::ConnectionError>,
    remote_addr: std::net::SocketAddr,
) {
    let recv = match recv {
        Ok(r) => r,
        Err(error) => {
            ctx.metrics
                .server_streams_failed_inc(&error, quinn::Dir::Uni);
            return;
        }
    };

    tokio::spawn(async move {
        let stream_metrics = super::StreamMetrics::uni(ctx.metrics);

        let mut framed = codec::FramedRead::new(
            recv,
            codec::LengthDelimitedCodec::builder()
                .max_frame_length(100 * 1_024 * 1_024)
                .new_codec(),
        );

        let mut changes = Vec::with_capacity(1024);

        loop {
            let Some(next) = tokio_stream::StreamExt::next(&mut framed).await else {
                break;
            };

            let next = match next {
                Ok(n) => n,
                Err(error) => {
                    tracing::error!(%error, "failed to decode broadcast stream");
                    continue;
                }
            };

            stream_metrics.incoming(&next, super::transport::TrafficClass::Broadcast);

            match broadcast::UniPayload::read_from_buffer(&next) {
                Ok(broadcast::UniPayload::V1 {
                    data: broadcast::UniPayloadV1::Broadcast(broadcast::BroadcastV1::Change(change)),
                    cluster_id,
                }) => {
                    if cluster_id == ctx.cluster_id {
                        changes.push((change, broadcast::ChangeSource::Broadcast));
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "failed to decode broadcast payload");
                }
            }
        }

        if ctx.changes_tx.send(super::Change::Bulk(changes)).await.is_err() {
            tracing::error!("failed to send broadcast changes, receiver dropped");
        }
    }.instrument(tracing::trace_span!("gossip broadcast stream", %remote_addr)));
}

fn process_sync_stream(
    ctx: GossipContext,
    stream: Result<(quinn::SendStream, quinn::RecvStream), quinn::ConnectionError>,
    remote_addr: std::net::SocketAddr,
) {
    let (send, recv) = match stream {
        Ok(r) => r,
        Err(error) => {
            ctx.metrics
                .server_streams_failed_inc(&error, quinn::Dir::Bi);
            return;
        }
    };

    tokio::spawn(
        async move {
            let stream_metrics = super::StreamMetrics::bi(ctx.metrics);

            let mut framed = codec::FramedRead::new(
                recv,
                codec::LengthDelimitedCodec::builder()
                    .max_frame_length(100 * 1_024 * 1_024)
                    .new_codec(),
            );

            loop {
                let frame = match tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio_stream::StreamExt::next(&mut framed),
                )
                .await
                {
                    Err(_) => {
                        tracing::warn!("timed out receiving sync frame");
                        return;
                    }
                    Ok(None) => {
                        return;
                    }
                    Ok(Some(Ok(p))) => p,
                    Ok(Some(Err(error))) => {
                        tracing::error!(%error, "failed to read frame from sync stream");
                        continue;
                    }
                };

                stream_metrics.incoming(&frame, super::transport::TrafficClass::Sync);

                match broadcast::BiPayload::read_from_buffer(&frame) {
                    Ok(broadcast::BiPayload::V1 {
                        data:
                            broadcast::BiPayloadV1::SyncStart {
                                actor_id,
                                trace_ctx,
                            },
                        cluster_id,
                    }) => {
                        let span = tracing::trace_span!("sync", local_actor = %ctx.actor_id, remote_actor = %actor_id);
                        if let Err(error) = super::sync::serve_sync(ctx, stream_metrics, actor_id, cluster_id, trace_ctx, framed, send).instrument(span).await {
                            tracing::warn!(%error, "failed to complete sync");
                        }

                        break;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to decode sync payload");
                    }
                }
            }
        }
        .instrument(tracing::trace_span!("gossip sync stream", %remote_addr)),
    );
}

/// Spawn a single task that accepts chunks from a receiver and updates cluster member round-trip-times in the agent state.
pub fn spawn_rtt_handler(
    members: super::Members,
    rtt_rx: tokio::sync::mpsc::Receiver<(SocketAddr, Duration)>,
    mut tripwire: Tripwire,
) {
    use tokio_stream::StreamExt;

    spawn::spawn_counted(async move {
        let stream = tokio_stream::wrappers::ReceiverStream::new(rtt_rx);
        // we can handle a lot of them I think...
        let chunker = stream.chunks_timeout(1024, Duration::from_secs(1));
        tokio::pin!(chunker);

        loop {
            tokio::select! {
                biased;
                _ = &mut tripwire => {
                    break;
                }
                chunks = chunker.next() => {
                    if let Some(chunks) = chunks {
                        let mut members = members.0.write();
                        for (addr, rtt) in chunks {
                            members.add_rtt(addr, rtt);
                            //histogram!("corro.transport.rtt.v2.seconds").record(rtt.as_secs_f64());
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    });
}
