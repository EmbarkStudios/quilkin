pub mod changes;
pub mod handler;
pub mod metrics;
pub mod swim;
pub mod sync;
pub mod transport;

use corro_types::broadcast as bx;
use metrics::GossipMetrics;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub type ChangeAndSource = (bx::ChangeV1, bx::ChangeSource);

pub enum Change {
    Bulk(Vec<ChangeAndSource>),
    Single(ChangeAndSource),
}

impl Change {
    fn len(&self) -> usize {
        match self {
            Self::Bulk(b) => b.len(),
            Self::Single(_) => 1,
        }
    }
}

pub enum ChangeIter {
    Bulk(std::iter::Rev<std::vec::IntoIter<ChangeAndSource>>),
    Single(std::iter::Once<ChangeAndSource>),
}

impl Iterator for ChangeIter {
    type Item = ChangeAndSource;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Bulk(v) => v.next(),
            Self::Single(s) => s.next(),
        }
    }
}

impl IntoIterator for Change {
    type Item = ChangeAndSource;
    type IntoIter = ChangeIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Bulk(v) => ChangeIter::Bulk(v.into_iter().rev()),
            Self::Single(change) => ChangeIter::Single(std::iter::once(change)),
        }
    }
}

#[derive(Clone)]
pub struct GossipContext {
    pub metrics: &'static GossipMetrics,
    pub foca_tx: Sender<bx::FocaInput>,
    pub changes_tx: Sender<Change>,
    pub bookie: corro_types::bookie::Bookie,
    pub cluster_id: corro_types::actor::ClusterId,
    pub actor_id: corro_types::actor::ActorId,
    pub clock: crate::Clock,
    pub sync_permits: Arc<tokio::sync::Semaphore>,
    pub pool: corro_types::agent::SplitPool,
}

struct StreamMetrics {
    m: &'static GossipMetrics,
    dir: quinn::Dir,
}

impl StreamMetrics {
    #[inline]
    fn uni(m: &'static GossipMetrics) -> Self {
        tracing::trace!("accepted unidirectional gossip stream");
        m.streams_inc(quinn::Dir::Uni);

        Self {
            m,
            dir: quinn::Dir::Uni,
        }
    }

    #[inline]
    fn bi(m: &'static GossipMetrics) -> Self {
        tracing::trace!("accepted bidirectional gossip stream");
        m.streams_inc(quinn::Dir::Bi);

        Self {
            m,
            dir: quinn::Dir::Bi,
        }
    }

    #[inline]
    fn incoming(&self, bytes: &bytes::BytesMut) {
        self.m
            .stream_bytes_inc(bytes.len() as _, self.dir, metrics::Direction::In);
    }

    #[inline]
    fn outgoing(&self, chunk_len: u64) {
        self.m
            .stream_bytes_inc(chunk_len, self.dir, metrics::Direction::Out);
    }
}

impl Drop for StreamMetrics {
    #[inline]
    fn drop(&mut self) {
        self.m.streams_dec(self.dir);
    }
}

#[inline]
fn fatal_db_issue(issue: &'static str, stx: &tokio::sync::watch::Sender<()>) {
    tracing::error!(issue, "fatal DB issue encountered, attempting shutdown");
    let _dont_care = stx.send(());
}
