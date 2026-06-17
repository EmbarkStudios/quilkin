use super::{GossipContext, StreamMetrics, metrics::Direction};
use bytes::{BufMut, BytesMut};
use corro_types::{
    actor::{self, ActorId},
    agent,
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq, CrsqlSeqRange},
    bookie::Bookie,
    broadcast as bx, change as cx,
    sync::{self, SyncMessage, SyncMessageV1, SyncNeedV1, SyncRequestV1},
};
use quinn::SendStream;
use speedy::Writable;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::codec::{self, Encoder, LengthDelimitedCodec};
use tracing::Instrument;

#[derive(Debug, thiserror::Error)]
pub enum SyncSendError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("expected a column, there were none")]
    NoMoreColumns,
    #[error("expected column '{0}', got none")]
    MissingColumn(&'static str),
    #[error("expected column '{0}', got '{1}'")]
    WrongColumn(&'static str, String),
    #[error("unexpected column '{0}'")]
    UnexpectedColumn(String),
    #[error("unknown resource state")]
    UnknownResourceState,
    #[error("unknown resource type")]
    UnknownResourceType,
    #[error("wrong ip byte len: {0}")]
    WrongIpByteLength(usize),
    #[error("could not encode message: {0}")]
    Encode(#[from] sync::SyncMessageEncodeError),
    #[error("sync send channel is closed")]
    ChannelClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncRecvError {
    #[error("could not decode message: {0}")]
    Decoded(#[from] sync::SyncMessageDecodeError),
    #[error(transparent)]
    Change(#[from] agent::ChangeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("expected sync state message, received something else")]
    ExpectedSyncState,
    #[error("unexpected end of stream")]
    UnexpectedEndOfStream,
    #[error("expected sync clock message, received something else")]
    ExpectedClockMessage,
    #[error("timed out waiting for sync message")]
    TimedOut(#[from] tokio::time::error::Elapsed),
    #[error("changes channel is closed")]
    ChangesChannelClosed,
    #[error("requests channel is closed")]
    RequestsChannelClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum BiPayloadEncodeError {
    #[error(transparent)]
    Encode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum BiPayloadSendError {
    #[error("could not encode payload: {0}")]
    Encode(#[from] BiPayloadEncodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error(transparent)]
    Datagram(#[from] quinn::SendDatagramError),
    #[error(transparent)]
    SendStreamWrite(#[from] quinn::WriteError),
    #[error(transparent)]
    TimedOut(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Stopped(#[from] quinn::StoppedError),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Send(#[from] SyncSendError),
    #[error(transparent)]
    BiPayloadSend(#[from] BiPayloadSendError),
    #[error(transparent)]
    Recv(#[from] SyncRecvError),
    #[error(transparent)]
    Rejection(#[from] sync::SyncRejectionV1),
    #[error(transparent)]
    Transport(#[from] TransportError),
}

async fn read_sync_msg(
    read: &mut codec::FramedRead<quinn::RecvStream, codec::LengthDelimitedCodec>,
    stream_metrics: &super::StreamMetrics,
) -> Result<Option<SyncMessageV1>, SyncRecvError> {
    use tokio_stream::StreamExt;

    match read.next().await {
        Some(buf_res) => match buf_res {
            Ok(mut buf) => {
                stream_metrics.incoming(&buf, crate::gossip::transport::TrafficClass::Sync);

                match SyncMessage::from_buf(&mut buf) {
                    Ok(SyncMessage::V1(sm)) => Ok(Some(sm)),
                    Err(e) => Err(SyncRecvError::from(e)),
                }
            }
            Err(e) => Err(SyncRecvError::from(e)),
        },
        None => Ok(None),
    }
}

struct Writer<'m> {
    codec: LengthDelimitedCodec,
    encode: BytesMut,
    send: BytesMut,
    metrics: &'m StreamMetrics,
}

impl<'m> Writer<'m> {
    #[inline]
    fn new(metrics: &'m StreamMetrics) -> Self {
        Self {
            codec: LengthDelimitedCodec::builder()
                .max_frame_length(100 * 1_024 * 1_024)
                .new_codec(),
            encode: BytesMut::new(),
            send: BytesMut::new(),
            metrics,
        }
    }

    #[inline]
    fn encode(&mut self, msg: SyncMessageV1) -> Result<(), SyncSendError> {
        SyncMessage::V1(msg)
            .write_to_stream((&mut self.encode).writer())
            .map_err(sync::SyncMessageEncodeError::from)?;

        let data = self.encode.split().freeze();
        self.codec.encode(data, &mut self.send)?;
        Ok(())
    }

    #[inline]
    async fn send_if_non_empty(&mut self, send: &mut SendStream) -> Result<(), SyncSendError> {
        if !self.send.is_empty() {
            self.send(send).await?;
        }

        Ok(())
    }

    #[inline]
    async fn send(&mut self, send: &mut SendStream) -> Result<(), SyncSendError> {
        let len = self.send.len();
        send.write_chunk(self.send.split().freeze()).await?;

        self.metrics
            .outgoing(len as _, super::transport::TrafficClass::Sync);

        Ok(())
    }

    #[inline]
    async fn encode_and_send(
        &mut self,
        msg: SyncMessageV1,
        send: &mut SendStream,
    ) -> Result<(), SyncSendError> {
        self.encode(msg)?;
        self.send(send).await?;
        Ok(())
    }
}

pub(super) async fn serve_sync(
    ctx: GossipContext,
    stream_metrics: super::StreamMetrics,
    remote_actor_id: ActorId,
    remote_cluster_id: actor::ClusterId,
    _trace_ctx: sync::SyncTraceContextV1,
    mut recv: codec::FramedRead<quinn::RecvStream, codec::LengthDelimitedCodec>,
    mut send: SendStream,
) -> Result<usize, SyncError> {
    tracing::debug!("received sync request");

    let mut writer = Writer::new(&stream_metrics);

    // Reject sync requests from different clusters. This is how corro-agent does it, but it would be simpler to just
    // close the stream with a specific error code IMO
    if remote_cluster_id != ctx.cluster_id {
        writer
            .encode_and_send(
                SyncMessageV1::Rejection(sync::SyncRejectionV1::DifferentCluster),
                &mut send,
            )
            .await?;
        return Ok(0);
    }

    // The first message after a sync request should always be the remote actor's clock
    match read_sync_msg(&mut recv, &stream_metrics).await? {
        Some(SyncMessageV1::Clock(ts)) => {
            if let Err(error) = ctx.clock.update_with_timestamp(remote_actor_id, ts) {
                tracing::warn!(error, %remote_actor_id, "could not update clock from remote actor");
            }
        }
        Some(_) => return Err(SyncRecvError::ExpectedClockMessage.into()),
        None => return Err(SyncRecvError::UnexpectedEndOfStream.into()),
    }

    let Ok(_permit) = ctx.sync_permits.try_acquire() else {
        writer
            .encode_and_send(
                SyncMessageV1::Rejection(sync::SyncRejectionV1::MaxConcurrencyReached),
                &mut send,
            )
            .await?;
        return Ok(0);
    };

    let sync_state = generate_sync(&ctx).await;

    // first, send the current sync state
    writer
        .encode_and_send(SyncMessageV1::State(sync_state), &mut send)
        .await?;

    // then the current clock's timestamp
    writer
        .encode_and_send(SyncMessageV1::Clock(ctx.clock.new_timestamp()), &mut send)
        .await?;

    // ensure we flush here so the data gets there fast. clock needs to be fresh!
    send.flush().await.map_err(SyncSendError::from)?;

    let (tx_need, rx_need) = mpsc::channel(1024);
    let (tx, mut rx) = mpsc::channel::<SyncMessageV1>(256);

    tokio::spawn({
        let pool = ctx.pool.clone();
        let bookie = ctx.bookie.clone();

        async move {
            if let Err(error) = process_sync(pool, bookie, tx, rx_need)
                .instrument(tracing::trace_span!("process_sync", %remote_actor_id))
                .await
            {
                tracing::error!(%error, "failed to process sync request");
            }
        }
    });

    let stream_metrics = std::pin::pin!(&stream_metrics);

    let (send_res, recv_res) = tokio::join!(
        async move {
            let mut count = 0;
            let mut check_buf = tokio::time::interval(Duration::from_secs(1));
            let mut stopped = false;

            loop {
                tokio::select! {
                    biased;
                    stopped_res = send.stopped() => {
                        match stopped_res {
                            Ok(Some(code)) => {
                                tracing::debug!(%code, "send stream was stopped by peer");
                            },
                            Ok(None) => {
                                tracing::debug!("send stream was stopped by us");
                            }
                            Err(error) => {
                                tracing::warn!(%error, "error waiting for stop from stream");
                            }
                        }
                        stopped = true;
                        break;
                    },
                    maybe_msg = rx.recv() => {
                        let Some(msg) = maybe_msg else {
                            break;
                        };

                        if let SyncMessageV1::Changeset(change) = &msg {
                            count += change.len();
                        }

                        writer.encode(msg)?;

                        if writer.send.len() >= 16 * 1024 {
                            writer.send(&mut send).await?;
                        }
                    },
                    _ = check_buf.tick() => {
                        writer.send_if_non_empty(&mut send).await?;
                    }
                }
            }

            if !stopped {
                writer.send_if_non_empty(&mut send).await?;

                if let Err(error) = send.finish() {
                    tracing::warn!(%error, "could not properly finish gossip send stream");
                } else if let Err(error) = send.stopped().await {
                    tracing::warn!(%error, "could not properly wait for gossip send stream to stop");
                }
            }

            tracing::debug!(actor_id = %ctx.actor_id, count, "done writing sync messages");

            ctx.metrics.sync_changes_inc(count as _, Direction::Out);

            Ok::<_, SyncError>(count)
        }.instrument(tracing::info_span!("process_versions_to_send", %remote_actor_id)),
        async move {
            let mut count = 0;

            loop {
                let msg = match read_sync_msg(&mut recv, &stream_metrics).await {
                    Ok(None) => {
                        break;
                    }
                    Err(error) => {
                        tracing::error!(%error, "sync recv error");
                        break;
                    }
                    Ok(Some(msg)) => msg,
                };

                match msg {
                    SyncMessageV1::Request(req) => {
                        count += req
                            .iter()
                            .map(|(_, needs)| {
                                needs.iter().map(|need| need.count()).sum::<usize>()
                            })
                            .sum::<usize>();
                        tx_need
                            .send(req)
                            .await
                            .map_err(|_e| SyncRecvError::RequestsChannelClosed)?;
                    }
                    SyncMessageV1::Changeset(_) => {
                        tracing::warn!("received sync changeset message unexpectedly, ignoring");
                    }
                    SyncMessageV1::State(_) => {
                        tracing::warn!("received sync state message unexpectedly, ignoring");
                    }
                    SyncMessageV1::Clock(_) => {
                        tracing::warn!("received sync clock message more than once, ignoring");
                    }
                    SyncMessageV1::Rejection(rejection) => {
                        return Err(rejection.into())
                    }
                }
            }

            tracing::debug!(local_actor_id = %ctx.actor_id, "done reading sync messages");

            ctx.metrics.sync_requests_inc(count as u64, Direction::In);

            Ok(count)
        }.instrument(tracing::info_span!("process_version_requests", %remote_actor_id))
    );

    if let Err(error) = send_res {
        tracing::error!(%remote_actor_id, %error, "could not complete serving sync due to a send side error");
    }

    recv_res
}

// Generates a `SyncMessage` to tell another node what versions we're missing
async fn generate_sync(ctx: &GossipContext) -> sync::SyncStateV1 {
    let mut state = sync::SyncStateV1 {
        actor_id: ctx.actor_id,
        ..Default::default()
    };

    let bookie = &ctx.bookie;
    let guard = bookie.owned_guard();

    for (&actor_id, booked) in bookie.iter(&guard) {
        let bookedr = booked.read();

        let last_version = match bookedr.last() {
            None => continue,
            Some(v) => v,
        };

        let need = bookedr.needed().iter().cloned().collect::<Vec<_>>();

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            for (v, partial) in bookedr
                .partials
                .iter()
                // don't set partial if it is effectively complete
                .filter(|(_, partial)| !partial.is_complete())
            {
                state.partial_need.entry(actor_id).or_default().insert(
                    *v,
                    partial
                        .seqs
                        .gaps(&(CrsqlSeq(0)..=partial.last_seq))
                        .collect(),
                );
            }
        }
        state.heads.insert(actor_id, last_version);
    }

    state
}

async fn process_sync(
    pool: corro_types::agent::SplitPool,
    bookie: Bookie,
    sender: mpsc::Sender<SyncMessageV1>,
    recv: mpsc::Receiver<SyncRequestV1>,
) -> eyre::Result<()> {
    use futures::TryStreamExt;

    let chunked_reqs = tokio_stream::wrappers::ReceiverStream::new(recv)
        .chunks_timeout(10, Duration::from_millis(500));
    tokio::pin!(chunked_reqs);

    let (job_tx, job_rx) = mpsc::unbounded_channel();

    let mut buf = futures::StreamExt::buffer_unordered(
        tokio_stream::wrappers::UnboundedReceiverStream::<
            std::pin::Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>,
        >::new(job_rx),
        6,
    );
    loop {
        let reqs = tokio::select! {
            Some(reqs) = chunked_reqs.next() => {
                reqs
            },
            Some(res) = buf.next() => {
                res?;
                continue;
            },
            else => {
                break;
            }
        };

        use itertools::Itertools;

        let agg = reqs
            .into_iter()
            .flatten()
            .chunk_by(|req| req.0)
            .into_iter()
            .map(|(actor_id, reqs)| (actor_id, reqs.flat_map(|(_, reqs)| reqs).collect()))
            .collect::<Vec<(ActorId, Vec<SyncNeedV1>)>>();

        for (actor_id, needs) in agg {
            let Some(booked) = bookie.get(&actor_id) else {
                continue;
            };

            let booked_read = booked.read();

            for need in needs {
                match &need {
                    SyncNeedV1::Full { versions } => {
                        if versions.clone().all(|v| {
                            booked_read.needed().contains(&v)
                                || booked_read.last().is_some_and(|max| v > max)
                        }) {
                            continue;
                        }
                    }
                    SyncNeedV1::Partial { version, .. } => {
                        if booked_read.needed().contains(version)
                            || booked_read.last().is_some_and(|max| *version > max)
                        {
                            continue;
                        }
                    }
                    SyncNeedV1::Empty { .. } => {
                        tracing::debug!("received empty need");
                    }
                }

                let pool = pool.clone();
                let sender = sender.clone();

                let fut = Box::pin(async move {
                    let mut conn = pool.read().await?;

                    tokio::task::block_in_place(|| {
                        handle_need(&mut conn, actor_id, need, &sender)
                    })?;

                    Ok(())
                });

                if job_tx.send(fut).is_err() {
                    eyre::bail!("could not send into job channel");
                }
            }
        }
    }

    tracing::debug!("gossip sync server loop finished");

    drop(job_tx);

    let _: () = buf.try_collect().await?;

    tracing::debug!("finished processing gossip sync state");

    Ok(())
}

fn send_change_chunks<I: Iterator<Item = rusqlite::Result<cx::Change>>>(
    tx: &mpsc::Sender<SyncMessageV1>,
    mut chunked: cx::ChunkedChanges<I>,
    actor_id: ActorId,
    version: CrsqlDbVersion,
    last_seq: CrsqlSeq,
    ts: bx::Timestamp,
) -> eyre::Result<()> {
    const ADAPT_CHUNK_SIZE_THRESHOLD: Duration = Duration::from_millis(500);
    const MIN_CHANGES_BYTES_PER_MESSAGE: usize = 1024;
    const TIMEOUT: Duration = Duration::from_secs(5);

    let mut max_buf_size = chunked.max_buf_size();
    loop {
        eyre::ensure!(!tx.is_closed(), "sync message sender channel is closed");

        match chunked.next() {
            Some(Ok((changes, seqs))) => {
                if changes.is_empty() && seqs.start() == CrsqlSeq(0) && seqs.end() == last_seq {
                    tracing::warn!(%actor_id, %version, "got an empty changes we should've had");
                    return Ok(());
                }

                let start = std::time::Instant::now();

                tx.blocking_send(SyncMessageV1::Changeset(bx::ChangeV1 {
                    actor_id,
                    changeset: bx::Changeset::FullV2 {
                        actor_id,
                        version,
                        changes,
                        seqs,
                        last_seq,
                        ts,
                    },
                }))?;

                let elapsed = start.elapsed();

                eyre::ensure!(elapsed <= TIMEOUT, "time out: peer is too slow");

                if elapsed <= ADAPT_CHUNK_SIZE_THRESHOLD {
                    continue;
                }

                eyre::ensure!(
                    max_buf_size > MIN_CHANGES_BYTES_PER_MESSAGE,
                    "time out: peer is too slow even after reducing throughput"
                );

                max_buf_size /= 2;
                chunked.set_max_buf_size(max_buf_size);
            }
            Some(Err(error)) => {
                tracing::error!(%actor_id, %version, %error, "could not process changes to send via sync");
                break;
            }
            None => {
                break;
            }
        }
    }

    Ok(())
}

fn handle_need(
    conn: &mut rusqlite::Connection,
    actor_id: ActorId,
    need: SyncNeedV1,
    sender: &mpsc::Sender<SyncMessageV1>,
) -> eyre::Result<()> {
    use bx::Timestamp;
    use corro_types::change::row_to_change;
    use rangemap::RangeInclusiveSet as Ris;
    use rusqlite::named_params;

    const MAX_CHANGES_BYTES_PER_MESSAGE: usize = 8 * 1024;

    let mut empties = Ris::<CrsqlDbVersion>::new();

    // this is a read transaction!
    let tx = conn.transaction()?;
    let timeout = Some(Duration::from_secs(60));
    let tx = sqlite_pool::InterruptibleTransaction::new(tx, timeout, "handle_need");

    let mut prepped = tx.prepare_cached(
        "
        SELECT db_version, MAX(seq) AS last_seq, MAX(ts) AS ts
            FROM crsql_changes
            WHERE site_id = :actor_id
              AND db_version BETWEEN :start AND :end
            GROUP BY db_version
            ORDER BY db_version DESC, seq ASC
    ",
    )?;

    match need {
        SyncNeedV1::Full { versions } => {
            let mut rows = prepped.query(named_params! {
                ":actor_id": actor_id,
                ":start": versions.start(),
                ":end": versions.end(),
            })?;

            let mut unprocessed = Ris::new();
            unprocessed.insert(versions.into());

            loop {
                let row = match rows.next()? {
                    Some(row) => row,
                    None => {
                        break;
                    }
                };

                let version: CrsqlDbVersion = row.get(0)?;

                unprocessed.remove(version..=version);

                let last_seq: CrsqlSeq = row.get(1)?;
                let ts: Timestamp = row.get(2)?;

                let mut prepped = tx.prepare_cached(
                    r#"
                        SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl, ts
                            FROM crsql_changes
                            WHERE site_id = :actor_id
                              AND db_version = :version
                            ORDER BY seq ASC
                    "#,
                )?;

                let rows = prepped.query_map(
                    rusqlite::named_params! {
                        ":actor_id": actor_id,
                        ":version": version
                    },
                    row_to_change,
                )?;

                send_change_chunks(
                    sender,
                    cx::ChunkedChanges::new(
                        rows,
                        CrsqlSeq(0),
                        last_seq,
                        MAX_CHANGES_BYTES_PER_MESSAGE,
                    ),
                    actor_id,
                    version,
                    last_seq,
                    ts,
                )?;
            }

            // now process the last unprocessed in case we have partials
            for versions in unprocessed {
                for version in CrsqlDbVersionRange::from(versions) {
                    let (in_gaps, buffered): (bool, bool) = tx
                        .prepare_cached(
                            "
                        SELECT
                            EXISTS(
                                SELECT 1
                                FROM __corro_bookkeeping_gaps
                                    WHERE actor_id = :actor_id
                                      AND :version BETWEEN start AND end
                            ) AS in_gaps,
                            EXISTS(
                                SELECT 1
                                FROM __corro_buffered_changes
                                    WHERE site_id = :actor_id
                                      AND db_version = :version
                            ) AS buffered",
                        )?
                        .query_row(
                            named_params! {
                                ":actor_id": actor_id,
                                ":version": version
                            },
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )?;

                    if !buffered {
                        if !in_gaps {
                            // this is an empty!
                            empties.insert(version..=version);
                        }
                        continue;
                    }

                    let seqs = tx
                        .prepare_cached("
                        SELECT start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping WHERE site_id = :actor_id AND db_version = :db_version
                        ")?.query_map(named_params!{
                            ":actor_id": actor_id,
                            ":db_version": version
                        },|row| Ok((CrsqlSeqRange::new(row.get(0)?, row.get(1)?), row.get(2)?, row.get(3)?)))?.collect::<rusqlite::Result<Vec<(CrsqlSeqRange, CrsqlSeq, Timestamp)>>>()?;

                    for (range_needed, last_seq, ts) in seqs {
                        let mut prepped = tx.prepare_cached(
                            r#"
                                SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                                    FROM __corro_buffered_changes
                                    WHERE site_id = :actor_id
                                        AND db_version = :db_version
                                        AND seq BETWEEN :start_seq AND :end_seq
                                    ORDER BY seq ASC
                            "#,
                        )?;

                        let start_seq = range_needed.start();
                        let end_seq = range_needed.end();

                        let rows = prepped.query_map(
                            named_params! {
                                ":actor_id": actor_id,
                                ":db_version": version,
                                ":start_seq": start_seq,
                                ":end_seq": end_seq
                            },
                            row_to_change,
                        )?;

                        send_change_chunks(
                            sender,
                            cx::ChunkedChanges::new(
                                rows,
                                start_seq,
                                end_seq,
                                MAX_CHANGES_BYTES_PER_MESSAGE,
                            ),
                            actor_id,
                            version,
                            last_seq,
                            ts,
                        )?;
                    }
                }
            }
        }
        SyncNeedV1::Partial { version, seqs } => {
            let mut rows = prepped.query(named_params! {
                ":actor_id": actor_id,
                ":start": version,
                ":end": version,
            })?;

            if let Some(row) = rows.next()? {
                let version: CrsqlDbVersion = row.get(0)?;
                let last_seq: CrsqlSeq = row.get(1)?;
                let ts: bx::Timestamp = row.get(2)?;

                for range_needed in seqs {
                    let mut prepped = tx.prepare_cached(
                        r#"
                            SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                                FROM crsql_changes
                                WHERE site_id = :actor_id
                                  AND db_version = :version
                                  AND seq BETWEEN :start AND :end
                                ORDER BY seq ASC
                        "#,
                    )?;

                    let rows = prepped.query_map(
                        named_params! {
                            ":actor_id": actor_id,
                            ":version": version,
                            ":start": range_needed.start(),
                            ":end": range_needed.end(),
                        },
                        row_to_change,
                    )?;

                    send_change_chunks(
                        sender,
                        cx::ChunkedChanges::new(
                            rows,
                            range_needed.start(),
                            range_needed.end(),
                            MAX_CHANGES_BYTES_PER_MESSAGE,
                        ),
                        actor_id,
                        version,
                        last_seq,
                        ts,
                    )?;
                }
            } else {
                let (in_gaps, buffered): (bool, bool) = tx
                    .prepare_cached(
                        "
                        SELECT
                            EXISTS(
                                SELECT 1
                                FROM __corro_bookkeeping_gaps
                                    WHERE actor_id = :actor_id
                                      AND :version BETWEEN start AND end
                            ) AS in_gaps,
                            EXISTS(
                                SELECT 1
                                FROM __corro_buffered_changes
                                    WHERE site_id = :actor_id
                                      AND db_version = :version
                            ) AS buffered",
                    )?
                    .query_row(
                        named_params! {
                            ":actor_id": actor_id,
                            ":version": version
                        },
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;

                if !buffered && !in_gaps {
                    // this is an empty!
                    empties.insert(version..=version);
                }

                if buffered {
                    for seqs_range in seqs {
                        let seqs = tx
                            .prepare_cached(
                                "
                                SELECT start_seq, end_seq, last_seq, ts
                                    FROM __corro_seq_bookkeeping
                                    WHERE
                                        site_id = :actor_id AND
                                        db_version = :db_version AND
                                        (
                                            -- [:start]---[start_seq]---[:end]
                                            ( start_seq BETWEEN :start AND :end ) OR

                                            -- [start_seq]---[:start]---[:end]---[end_seq]
                                            ( start_seq <= :start AND end_seq >= :end ) OR

                                            -- [:start]---[start_seq]---[:end]---[end_seq]
                                            ( start_seq <= :end AND end_seq >= :end ) OR

                                            -- [:start]---[end_seq]---[:end]
                                            ( end_seq BETWEEN :start AND :end )
                                        )
                                ",
                            )?
                            .query_map(
                                named_params! {
                                    ":actor_id": actor_id,
                                    ":db_version": version,
                                    ":start": seqs_range.start(),
                                    ":end": seqs_range.end(),
                                },
                                |row| {
                                    Ok((
                                        CrsqlSeqRange::new(row.get(0)?, row.get(1)?),
                                        row.get(2)?,
                                        row.get(3)?,
                                    ))
                                },
                            )?
                            .collect::<rusqlite::Result<Vec<(CrsqlSeqRange, CrsqlSeq, Timestamp)>>>(
                            )?;

                        for (range_needed, last_seq, ts) in seqs {
                            let mut prepped = tx.prepare_cached(
                                r#"
                                    SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                                        FROM __corro_buffered_changes
                                        WHERE site_id = :actor_id
                                            AND db_version = :version
                                            AND seq BETWEEN :start_seq AND :end_seq
                                        ORDER BY seq ASC
                                "#,
                            )?;

                            // scope query to only the sequences we have
                            let start_seq = std::cmp::max(range_needed.start(), seqs_range.start());
                            let end_seq = std::cmp::min(range_needed.end(), seqs_range.end());

                            let rows = prepped.query_map(
                                named_params! {
                                    ":actor_id": actor_id,
                                    ":version": version,
                                    ":start_seq": start_seq,
                                    ":end_seq": end_seq
                                },
                                row_to_change,
                            )?;

                            send_change_chunks(
                                sender,
                                cx::ChunkedChanges::new(
                                    rows,
                                    start_seq,
                                    end_seq,
                                    MAX_CHANGES_BYTES_PER_MESSAGE,
                                ),
                                actor_id,
                                version,
                                last_seq,
                                ts,
                            )?;
                        }
                    }
                }
            }
        }
        SyncNeedV1::Empty { .. } => {
            // NOTE: no more empties in the new reality
        }
    }

    if !empties.is_empty() {
        for versions in empties {
            sender.blocking_send(SyncMessageV1::Changeset(bx::ChangeV1 {
                actor_id,
                changeset: bx::Changeset::Empty {
                    versions: versions.into(),
                    ts: None,
                },
            }))?;
        }
    }

    Ok(())
}
