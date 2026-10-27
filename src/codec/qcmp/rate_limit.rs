/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! A per source budget for the QCMP responder.

use std::{net::IpAddr, num::NonZeroU32};

use governor::{Quota, clock, state::keyed::DefaultKeyedStateStore};

/// Acquires between sweeps of the keyed state
///
/// Keys are only added, never removed, until a sweep drops the ones that have
/// returned to a full budget. Sources are unauthenticated and spoofable, so
/// without this the state grows with every address ever seen rather than with
/// the addresses currently being answered.
const SWEEP_EVERY: u32 = 1024;

type Keyed<C> = governor::RateLimiter<
    IpAddr,
    DefaultKeyedStateStore<IpAddr>,
    C,
    governor::middleware::NoOpMiddleware<<C as clock::Clock>::Instant>,
>;

/// How many replies a source may be sent per second
///
/// A [GCRA][gcra] budget per address, so a source that spends its allowance
/// regains it steadily rather than all at once on a window boundary, which
/// would let twice the budget through either side of one.
///
/// [gcra]: https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm
pub struct RateLimiter<C: clock::Clock = clock::DefaultClock> {
    /// `None` lifts the limit entirely
    inner: Option<Keyed<C>>,
    until_sweep: u32,
}

impl RateLimiter<clock::DefaultClock> {
    /// A limiter allowing each source `per_second` replies, or one that allows
    /// everything through when `per_second` is 0
    pub fn new(per_second: u32) -> Self {
        Self::with_clock(per_second, clock::DefaultClock::default())
    }
}

impl<C: clock::Clock> RateLimiter<C> {
    pub fn with_clock(per_second: u32, clock: C) -> Self {
        Self {
            inner: NonZeroU32::new(per_second).map(|per_second| {
                Keyed::new(
                    Quota::per_second(per_second),
                    DefaultKeyedStateStore::default(),
                    clock,
                )
            }),
            until_sweep: SWEEP_EVERY,
        }
    }

    /// Whether `source` may be sent a reply, spending a unit of its budget when
    /// it may
    pub fn acquire(&mut self, source: IpAddr) -> bool {
        let Some(inner) = &self.inner else {
            return true;
        };

        self.until_sweep -= 1;
        if self.until_sweep == 0 {
            self.until_sweep = SWEEP_EVERY;
            inner.retain_recent();
        }

        inner.check_key(&source).is_ok()
    }

    /// The addresses currently holding state, which a sweep bounds to those
    /// still spending a budget
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.inner.as_ref().map_or(0, |inner| inner.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const A: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(5, 5, 5, 5));
    const B: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(9, 8, 7, 6));

    fn limiter(
        per_second: u32,
    ) -> (
        RateLimiter<clock::FakeRelativeClock>,
        clock::FakeRelativeClock,
    ) {
        let clock = clock::FakeRelativeClock::default();
        (RateLimiter::with_clock(per_second, clock.clone()), clock)
    }

    #[test]
    fn spends_a_budget_then_regains_it() {
        let (mut limiter, clock) = limiter(4);

        assert!((0..4).all(|_| limiter.acquire(A)));
        assert!(!limiter.acquire(A));

        // A quarter of the budget per second is back a quarter of a second later
        clock.advance(Duration::from_millis(250));
        assert!(limiter.acquire(A));
        assert!(!limiter.acquire(A));

        clock.advance(Duration::from_secs(1));
        assert!((0..4).all(|_| limiter.acquire(A)));
    }

    #[test]
    fn budgets_are_per_source() {
        let (mut limiter, _clock) = limiter(1);

        assert!(limiter.acquire(A));
        assert!(!limiter.acquire(A));
        assert!(limiter.acquire(B));
    }

    #[test]
    fn a_zero_limit_allows_everything() {
        let (mut limiter, _clock) = limiter(0);
        assert!((0..10_000).all(|_| limiter.acquire(A)));
    }

    /// A flood of spoofed sources must not grow the state without bound
    #[test]
    fn sweeps_sources_that_regained_their_budget() {
        let (mut limiter, clock) = limiter(1);

        let flood = |limiter: &mut RateLimiter<_>, range: std::ops::Range<u32>| {
            for i in range {
                limiter.acquire(IpAddr::V4(std::net::Ipv4Addr::from_bits(i)));
            }
        };

        flood(&mut limiter, 0..SWEEP_EVERY * 4);
        let before = limiter.tracked();
        assert!(
            before > 0,
            "the flood should be tracked while it is spending"
        );

        // Every address from the flood is back to a full budget, so the next
        // sweep has nothing left to keep
        clock.advance(Duration::from_secs(60));
        flood(&mut limiter, SWEEP_EVERY * 4..SWEEP_EVERY * 5);

        assert!(
            limiter.tracked() < before,
            "state grew from {before} to {} across a sweep",
            limiter.tracked()
        );
    }
}
