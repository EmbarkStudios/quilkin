/// Observed-Remove Set With Out Tombstones (ORSWOT), ported directly from `riak_dt`.
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    hash::Hash,
    mem,
};

//use bincode::{Decode, Encode};
use gxhash::{HashMap, HashSet};

use super::{
    Cmrdt, Cvrdt, Dot, ResetRemove, VClock,
    ctx::{AddCtx, ReadCtx, RmCtx},
    traits::ContentEqual,
};

/// `Orswot` is an add-biased or-set without tombstones ported from
/// the `riak_dt` CRDT library.
#[derive(Debug, Clone, PartialEq, Eq /*Encode, Decode*/)]
pub struct Orswot<M: Hash + Eq, A: Ord + Hash> {
    pub(crate) clock: VClock<A>,
    pub(crate) entries: HashMap<M, VClock<A>>,
    pub(crate) deferred: HashMap<VClock<A>, HashSet<M>>,
}

/// Op's define an edit to an Orswot, Op's must be replayed in the exact order
/// they were produced to guarantee convergence.
///
/// Op's are idempotent, that is, applying an Op twice will not have an effect
#[derive(Clone, PartialEq, Eq, Hash /*Encode, Decode*/)]
pub enum Op<M, A: Ord> {
    /// Add members to the set
    Add {
        /// witnessing dot
        dot: Dot<A>,
        /// Members to add
        members: Vec<M>,
    },
    /// Remove member from the set
    Rm {
        /// witnessing clock
        clock: VClock<A>,
        /// Members to remove
        members: Vec<M>,
    },
}

impl<M: Hash + Eq, A: Ord + Hash> Default for Orswot<M, A> {
    fn default() -> Self {
        Self {
            clock: Default::default(),
            entries: HashMap::with_hasher(gxhash::GxBuildHasher::with_seed(0)),
            deferred: HashMap::with_hasher(gxhash::GxBuildHasher::with_seed(0)),
        }
    }
}

impl<M: Hash + Clone + Eq, A: Ord + Hash + Clone + Debug> Cmrdt for Orswot<M, A> {
    type Op = Op<M, A>;
    type Validation = <VClock<A> as Cmrdt>::Validation;

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        match op {
            Op::Add { dot, .. } => self.clock.validate_op(dot),
            Op::Rm { .. } => Ok(()),
        }
    }

    fn apply(&mut self, op: Self::Op) {
        match op {
            Op::Add { dot, members } => {
                if self.clock.get(&dot.actor) >= dot.counter {
                    // we've already seen this op
                    return;
                }

                for member in members {
                    let member_vclock = self.entries.entry(member).or_default();
                    member_vclock.apply(dot.clone());
                }

                self.clock.apply(dot);
                self.apply_deferred();
            }
            Op::Rm { clock, members } => {
                self.apply_rm(members.into_iter().collect(), clock);
            }
        }
    }
}

/// The variations that an ORSWOT may fail validation.
#[derive(Debug, PartialEq, Eq)]
pub enum Validation<M, A> {
    /// We've detected that two different members were inserted with the same dot.
    /// This can break associativity.
    DoubleSpentDot {
        /// The dot that was double spent
        dot: Dot<A>,
        /// Our member inserted with this dot
        our_member: M,
        /// Their member inserted with this dot
        their_member: M,
    },
}

impl<M: Debug, A: Debug> Display for Validation<M, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

impl<M: Debug, A: Debug> std::error::Error for Validation<M, A> {}

impl<M: Hash + Eq + Clone + Debug, A: Ord + Hash + Clone + Debug> Cvrdt for Orswot<M, A> {
    type Validation = Validation<M, A>;

    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        for (member, clock) in self.entries.iter() {
            for (other_member, other_clock) in other.entries.iter() {
                for Dot { actor, counter } in clock.iter() {
                    if other_member != member && other_clock.get(actor) == counter {
                        return Err(Validation::DoubleSpentDot {
                            dot: Dot::new(actor.clone(), counter),
                            our_member: member.clone(),
                            their_member: other_member.clone(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Merge combines another `Orswot` with this one.
    fn merge(&mut self, other: Self) {
        let entries = mem::replace(
            &mut self.entries,
            HashMap::with_hasher(gxhash::GxBuildHasher::with_seed(0)),
        );
        self.entries
            .extend(entries.into_iter().filter_map(|(entry, mut clock)| {
                if !other.entries.contains_key(&entry) {
                    // other doesn't contain this entry because it:
                    //  1. has seen it and dropped it
                    //  2. hasn't seen it
                    if other.clock >= clock {
                        // other has seen this entry and dropped it
                        None
                    } else {
                        // the other map has not seen this version of this
                        // entry, so add it. But first, we have to remove any
                        // information that may have been known at some point
                        // by the other map about this key and was removed.
                        clock.reset_remove(&other.clock);
                        Some((entry, clock))
                    }
                } else {
                    Some((entry, clock))
                }
            }));

        for (entry, mut clock) in other.entries {
            if let Some(our_clock) = self.entries.get_mut(&entry) {
                // SUBTLE: this entry is present in both orswots, BUT that doesn't mean we
                // shouldn't drop it!
                // Perfectly possible that an item in both sets should be dropped
                let mut common = VClock::intersection(&clock, our_clock);
                common.merge(clock.clone_without(&self.clock));
                common.merge(our_clock.clone_without(&other.clock));
                if common.is_empty() {
                    // both maps had seen each others entry and removed them
                    self.entries.remove(&entry).unwrap();
                } else {
                    // we should not drop, as there is information still tracked in
                    // the common clock.
                    *our_clock = common;
                }
            } else {
                // we don't have this entry, is it because we:
                //  1. have seen it and dropped it
                //  2. have not seen it
                if self.clock >= clock {
                    // We've seen this entry and dropped it, we won't add it back
                } else {
                    // We have not seen this version of this entry, so we add it.
                    // but first, we have to remove the information on this entry
                    // that we have seen and deleted
                    clock.reset_remove(&self.clock);
                    self.entries.insert(entry, clock);
                }
            }
        }

        // merge deferred removals
        for (rm_clock, members) in other.deferred {
            self.apply_rm(members, rm_clock);
        }

        self.clock.merge(other.clock);

        self.apply_deferred();
    }
}

impl<M: Hash + Clone + Eq, A: Ord + Hash> ResetRemove<A> for Orswot<M, A> {
    fn reset_remove(&mut self, clock: &VClock<A>) {
        self.clock.reset_remove(clock);

        let entries = mem::replace(
            &mut self.entries,
            HashMap::with_hasher(gxhash::GxBuildHasher::with_seed(0)),
        );
        self.entries
            .extend(entries.into_iter().filter_map(|(val, mut val_clock)| {
                val_clock.reset_remove(clock);
                if val_clock.is_empty() {
                    None
                } else {
                    Some((val, val_clock))
                }
            }));

        let deferred = mem::replace(
            &mut self.deferred,
            HashMap::with_hasher(gxhash::GxBuildHasher::with_seed(0)),
        );
        self.deferred
            .extend(deferred.into_iter().filter_map(|(mut vclock, deferred)| {
                vclock.reset_remove(clock);
                if vclock.is_empty() {
                    None
                } else {
                    Some((vclock, deferred))
                }
            }));
    }
}

#[allow(clippy::unused_self)]
impl<M: Hash + Clone + Eq, A: Ord + Hash + Clone> Orswot<M, A> {
    /// Returns a new `Orswot` instance.
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a single element.
    #[inline]
    pub fn add(&self, member: M, ctx: AddCtx<A>) -> Op<M, A> {
        Op::Add {
            dot: ctx.dot,
            members: std::iter::once(member).collect(),
        }
    }

    /// Add multiple elements.
    #[inline]
    pub fn add_all<I: IntoIterator<Item = M>>(&self, members: I, ctx: AddCtx<A>) -> Op<M, A> {
        Op::Add {
            dot: ctx.dot,
            members: members.into_iter().collect(),
        }
    }

    /// Remove a member with a witnessing ctx.
    #[inline]
    pub fn rm(&self, member: M, ctx: RmCtx<A>) -> Op<M, A> {
        Op::Rm {
            clock: ctx.clock,
            members: std::iter::once(member).collect(),
        }
    }

    /// Remove members with a witnessing ctx.
    #[inline]
    pub fn rm_all<I: IntoIterator<Item = M>>(&self, members: I, ctx: RmCtx<A>) -> Op<M, A> {
        Op::Rm {
            clock: ctx.clock,
            members: members.into_iter().collect(),
        }
    }

    /// Remove members using a witnessing clock.
    fn apply_rm(&mut self, members: HashSet<M>, clock: VClock<A>) {
        for member in members.iter() {
            if let Some(member_clock) = self.entries.get_mut(member) {
                member_clock.reset_remove(&clock);
                if member_clock.is_empty() {
                    self.entries.remove(member);
                }
            }
        }

        match clock.partial_cmp(&self.clock) {
            None | Some(Ordering::Greater) => {
                if let Some(existing_deferred) = self.deferred.get_mut(&clock) {
                    existing_deferred.extend(members);
                } else {
                    self.deferred.insert(clock, members);
                }
            }
            _ => { /* we've already seen this remove */ }
        }
    }

    /// Check if the set contains a member
    #[inline]
    pub fn contains(&self, member: &M) -> bool {
        self.entries.contains_key(member)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Gets an iterator over the entries of the `Map`.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &M> {
        self.entries.keys()
    }

    /// Retrieve the current members.
    #[inline]
    pub fn read(&self) -> ReadCtx<HashSet<M>, A> {
        ReadCtx {
            add_clock: self.clock.clone(),
            rm_clock: self.clock.clone(),
            val: self.entries.keys().cloned().collect(),
        }
    }

    /// Retrieve the current read context
    #[inline]
    pub fn read_ctx(&self) -> ReadCtx<(), A> {
        ReadCtx {
            add_clock: self.clock.clone(),
            rm_clock: self.clock.clone(),
            val: (),
        }
    }

    #[inline]
    pub fn rm_ctx(&self) -> RmCtx<A> {
        RmCtx {
            clock: self.clock.clone(),
        }
    }

    #[inline]
    pub(crate) fn apply_deferred(&mut self) {
        let deferred = mem::replace(
            &mut self.deferred,
            HashMap::with_hasher(gxhash::GxBuildHasher::with_seed(0)),
        );
        for (clock, entries) in deferred {
            self.apply_rm(entries, clock);
        }
    }
}

impl<M: Hash + Eq, A: Hash + Ord + Clone + Debug> Orswot<M, A> {
    #[inline]
    pub fn add_ctx(&self, actor: A) -> AddCtx<A> {
        let mut clock = self.clock.clone();
        let dot = clock.inc(actor);
        clock.apply(dot.clone());
        AddCtx { dot, clock }
    }
}

impl<M, A> ContentEqual for Orswot<M, A>
where
    M: Ord + Hash + Clone + Eq + Debug,
    A: Debug + Ord + Hash + Clone,
{
    #[inline]
    fn content_equal(&self, other: &Self) -> bool {
        if self.entries.len() != other.entries.len() {
            return false;
        }

        let s = self
            .entries
            .keys()
            .collect::<std::collections::BTreeSet<_>>();
        let o = other
            .entries
            .keys()
            .collect::<std::collections::BTreeSet<_>>();

        for (a, b) in s.into_iter().zip(o.into_iter()) {
            if a != b {
                return false;
            }
        }

        true
    }
}

impl<M: Ord + Hash + Clone + Eq + Debug, A: Debug + Ord + Hash + Clone> Orswot<M, A> {
    #[inline]
    pub fn content_equal_assert(&self, other: &Self) {
        assert_eq!(self.entries.len(), other.entries.len());

        let s = self
            .entries
            .keys()
            .collect::<std::collections::BTreeSet<_>>();
        let o = other
            .entries
            .keys()
            .collect::<std::collections::BTreeSet<_>>();

        for (a, b) in s.into_iter().zip(o.into_iter()) {
            assert_eq!(a, b);
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn merge_ops(
        &self,
        other: std::collections::BTreeSet<M>,
        actor: A,
    ) -> (Option<(VClock<A>, Vec<M>)>, Option<(Dot<A>, Vec<M>)>) {
        let mut rm = Vec::new();
        for key in self.entries.keys() {
            if !other.contains(key) {
                rm.push(key.clone());
            }
        }

        let rm_op = (!rm.is_empty()).then(|| (self.clock.clone(), rm));

        let mut add = Vec::new();
        for v in other {
            if !self.entries.contains_key(&v) {
                add.push(v);
            }
        }

        let add_op = (!add.is_empty()).then(|| (self.clock.inc(actor), add));

        (rm_op, add_op)
    }
}

impl<M: Debug, A: Ord + Hash + Debug> Debug for Op<M, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Add { dot, members } => write!(f, "Add({dot:?}, {members:?})"),
            Op::Rm { clock, members } => write!(f, "Rm({clock:?}, {members:?})"),
        }
    }
}

impl<M: Hash + Eq, A: Hash + Ord> IntoIterator for Orswot<M, A> {
    type Item = M;
    type IntoIter = std::collections::hash_map::IntoKeys<M, VClock<A>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_keys()
    }
}

impl<M, A> serde::Serialize for Orswot<M, A>
where
    M: serde::Serialize + Hash + Eq,
    A: Hash + Ord,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.entries.len()))?;

        for k in self.entries.keys() {
            seq.serialize_element(k)?;
        }

        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // a bug found with rust quickcheck where deferred operations
    // are not carried over after a merge.
    // symptoms:
    //  if nothing is added, it works
    //  if removed elem is added first, it only misses one
    //  if non-related elem is added, it misses both
    fn ensure_deferred_merges() {
        let mut a = Orswot::new();
        let mut b = Orswot::new();

        b.apply(b.add("element 1", b.read().derive_add_ctx("A")));

        // remove with a future context
        b.apply(b.rm(
            "element 1",
            RmCtx {
                clock: Dot::new("A", 4).into(),
            },
        ));

        a.apply(a.add("element 4", a.read().derive_add_ctx("B")));

        // remove with a future context
        b.apply(b.rm(
            "element 9",
            RmCtx {
                clock: Dot::new("C", 4).into(),
            },
        ));

        let mut merged = Orswot::new();
        merged.merge(a);
        merged.merge(b);
        merged.merge(Orswot::new());
        assert_eq!(merged.deferred.len(), 2);
    }

    // a bug found with rust quickcheck where deferred removals
    // were not properly preserved across merges.
    #[test]
    fn preserve_deferred_across_merges() {
        let mut a = Orswot::new();
        let mut b = a.clone();
        let mut c = a.clone();

        // add element 5 from witness 1
        a.apply(a.add(5, a.read().derive_add_ctx("A")));

        // on another clock, remove 5 with an advanced clock for witnesses A and B
        let mut vc = VClock::new();
        vc.apply(Dot::new("A", 3));
        vc.apply(Dot::new("B", 8));

        // remove from b (has not yet seen add for 5) with advanced ctx
        b.apply(b.rm(5, RmCtx { clock: vc }));
        assert_eq!(b.deferred.len(), 1);

        // ensure that the deferred elements survive across a merge
        c.merge(b);
        assert_eq!(c.deferred.len(), 1);

        // after merging the set with deferred elements with the set that contains
        // an inferior member, ensure that the member is no longer visible and
        // the deferred set still contains this info
        a.merge(c);
        assert!(a.read().val.is_empty());
    }

    // port from riak_dt
    // Bug found by EQC, not dropping dots in merge when an element is
    // present in both Sets leads to removed items remaining after merge.
    #[test]
    fn test_present_but_removed() {
        let mut a = Orswot::new();
        let mut b = Orswot::new();

        a.apply(a.add(0, a.read().derive_add_ctx("A")));

        // Replicate it to C so A has 0->{a, 1}
        let c = a.clone();

        a.apply(a.rm(0, a.contains(&0).derive_rm_ctx()));
        assert_eq!(a.deferred.len(), 0);

        b.apply(b.add(0, b.read().derive_add_ctx("B")));

        // Replicate B to A, so now A has a 0
        // the one with a Dot of {b,1} and clock
        // of [{a, 1}, {b, 1}]
        a.merge(b.clone());

        b.apply(b.rm(0, b.contains(&0).derive_rm_ctx()));

        // Both C and A have a '0', but when they merge, there should be
        // no '0' as C's has been removed by A and A's has been removed by
        // C.
        a.merge(b);
        a.merge(c);
        assert!(a.read().val.is_empty());
    }
}
