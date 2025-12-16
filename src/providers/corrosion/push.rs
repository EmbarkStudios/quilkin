use super::*;

pub(super) fn corrosion_mutate(
    state: State,
    endpoints: CorrosionAddrs,
    hc: HealthCheck,
) -> Option<Mutator> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    Some(Mutator { tx })
}

enum Mutation {}

/// Mutator that keeps track of state, publishing changes to a remote corrosion database
#[derive(Clone)]
pub struct Mutator {
    tx: tokio::sync::mpsc::UnboundedSender<Mutation>,
}
