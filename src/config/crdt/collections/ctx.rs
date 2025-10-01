use std::fmt::Debug;

use super::{Cmrdt, Dot, VClock};

/// `ReadCtx`'s are used to extract data from CRDT's while maintaining some causal history.
/// You should store `ReadCtx`'s close to where mutation is exposed to the user.
#[derive(Debug, PartialEq, Eq)]
pub struct ReadCtx<V, A: Ord> {
    /// clock used to derive an [`AddCtx`]
    pub add_clock: VClock<A>,

    /// clock used to derive an [`RmCtx`]
    pub rm_clock: VClock<A>,

    /// the data read from the CRDT
    pub val: V,
}

/// [`AddCtx`] is used for mutations that add new information to a CRDT
#[derive(Debug)]
pub struct AddCtx<A: Ord> {
    /// The adding vclock context
    pub clock: VClock<A>,

    /// The Actor and the Actor's version at the time of the add
    pub dot: Dot<A>,
}

/// [`RmCtx`] is used for mutations that remove information from a CRDT
#[derive(Debug, Clone)]
pub struct RmCtx<A: Ord> {
    /// The removing vclock context
    pub clock: VClock<A>,
}

impl<V, A: Ord + Clone + Debug> ReadCtx<V, A> {
    /// Derives an [`AddCtx`] for a given actor from a [`Self`]
    #[inline]
    pub fn derive_add_ctx(self, actor: A) -> AddCtx<A> {
        let mut clock = self.add_clock;
        let dot = clock.inc(actor);
        clock.apply(dot.clone());
        AddCtx { clock, dot }
    }

    /// Derives a [`RmCtx`] from a [`Self`]
    #[inline]
    pub fn derive_rm_ctx(self) -> RmCtx<A> {
        RmCtx {
            clock: self.rm_clock,
        }
    }

    /// Splits this [`ReadCtx`] into its data and an empty [`ReadCtx`]
    #[inline]
    pub fn split(self) -> (V, ReadCtx<(), A>) {
        (
            self.val,
            ReadCtx {
                add_clock: self.add_clock,
                rm_clock: self.rm_clock,
                val: (),
            },
        )
    }
}
