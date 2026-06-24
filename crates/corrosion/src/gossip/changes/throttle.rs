use std::time::{Duration, Instant};

type Change = (
    corro_types::actor::ActorId,
    corro_types::base::CrsqlDbVersion,
);

#[derive(Clone)]
struct ThrottleEntry {
    blocked_until: Instant,
    throttle_count: u32,
}

/// Per-key exponential throttle: each [`Self:.throttle`] increases the wait from
/// `throttle_min` by powers of two until `throttle_max`. [`Self::retries`] count the number of
/// times the key has been throttled).
pub(super) struct ThrottleMap {
    inner: std::collections::HashMap<Change, ThrottleEntry>,
    throttle_min: Duration,
    throttle_max: Duration,

    max_pow: u32,
}

#[inline]
fn exponential_increase(pow: u32, throttle_min: Duration, throttle_max: Duration) -> Duration {
    let mult = 1u32.checked_shl(pow).unwrap_or(u32::MAX);
    (throttle_min * mult).min(throttle_max)
}

#[inline]
fn max_pow(throttle_min: Duration, throttle_max: Duration) -> u32 {
    if throttle_min > throttle_max {
        return 0;
    }
    let ratio = if throttle_min.is_zero() {
        throttle_max.as_nanos()
    } else {
        throttle_max.as_nanos() / throttle_min.as_nanos()
    };
    ratio.ilog2()
}

impl ThrottleMap {
    pub(super) fn new(throttle_min: Duration, throttle_max: Duration) -> Self {
        Self {
            inner: Default::default(),
            throttle_min,
            throttle_max,
            max_pow: max_pow(throttle_min, throttle_max),
        }
    }

    #[inline]
    pub(super) fn is_throttled(&self, key: &Change) -> Option<Instant> {
        if let Some(e) = self.inner.get(key)
            && Instant::now() < e.blocked_until
        {
            return Some(e.blocked_until);
        }

        None
    }

    #[inline]
    pub fn throttle_count(&self, key: &Change) -> u64 {
        self.inner
            .get(key)
            .map(|e| e.throttle_count as u64)
            .unwrap_or(0)
    }

    #[inline]
    pub fn throttle(&mut self, key: Change) {
        let now = Instant::now();
        let entry = self.inner.entry(key).or_insert(ThrottleEntry {
            blocked_until: now,
            throttle_count: 0,
        });

        // we calculate max pow so we can clamp things and not worry about overflow
        let pow = entry.throttle_count.min(self.max_pow);
        let wait = exponential_increase(pow, self.throttle_min, self.throttle_max);

        entry.blocked_until = now + wait;
        entry.throttle_count = entry.throttle_count.saturating_add(1);
    }

    // #[inline]
    // pub fn clear_expired(&mut self) {
    //     let now = Instant::now();
    //     self.inner.retain(|_, entry| entry.blocked_until > now);
    // }

    #[inline]
    pub fn remove(&mut self, key: &Change) {
        self.inner.remove(key);
    }

    // #[inline]
    // pub fn contains(&self, key: &Change) -> bool {
    //     self.inner.contains_key(key)
    // }
}
