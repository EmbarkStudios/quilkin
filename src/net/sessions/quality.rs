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

//! Per-session connection quality, and the aggregation that turns it into
//! bounded metrics.
//!
//! A session's jitter and its client's ASN are per-player facts, so neither can
//! be a metric label: concurrent sessions and the ~9700 ASNs seen in a day of
//! traffic both blow up cardinality. Instead every session registers a
//! [`SessionQuality`] here, the I/O paths stamp packet arrivals onto it, and
//! [`spawn_aggregator`] periodically folds the whole registry into a jitter
//! histogram plus per-ASN aggregates whose series count is bounded by
//! configuration rather than by traffic.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use once_cell::sync::Lazy;

use crate::{metrics, net::maxmind_db::IpNetEntry};

/// Divisor of the RFC 3550 interarrival jitter estimator, which weights the
/// estimate towards recent packet pairs.
const JITTER_GAIN: i64 = 16;

/// Interarrival gap beyond which a packet pair is treated as the stream
/// restarting rather than as jitter.
///
/// RFC 3550 assumes a continuous media stream. Game traffic pauses — loading
/// screens, alt-tabs, backgrounded mobile clients — and feeding a multi-second
/// gap to the estimator spikes it by gap/16, which against a 30 ms threshold
/// reads as a badly degraded player rather than one who stopped sending.
const MAX_INTERARRIVAL_NANOS: i64 = 1_000_000_000;

/// Monotonic nanoseconds since the first call.
///
/// Interarrival must not be measured against the wall clock: an NTP step
/// backwards yields a negative delta, and the estimator turns that into a jitter
/// spike the size of the step.
#[inline]
fn monotonic_nanos() -> i64 {
    static START: Lazy<std::time::Instant> = Lazy::new(std::time::Instant::now);

    // i64 nanos covers 292 years of uptime
    START.elapsed().as_nanos() as i64
}

/// Packets a session needs within an aggregation interval for its jitter
/// estimate to be worth recording. Two packets give one interarrival delta and
/// no variation to compare it against.
const MIN_PACKETS_FOR_JITTER: u64 = 3;

/// Label value used for sessions whose client IP resolved to no ASN, either
/// because no maxmind database is loaded or because the address isn't in it.
const UNKNOWN_ASN: &str = "unknown";

/// Label value carrying the sessions of every ASN outside the exported top N,
/// so per-ASN shares still sum to the session total.
const REMAINDER_ASN: &str = "other";

/// Interarrival jitter of one session, and the ASN of the client it belongs to.
///
/// Updated from the I/O paths on every downstream packet and read by the
/// aggregator, so all access is via relaxed atomics. Packets for one session
/// normally arrive on a single worker, but nothing guarantees it; a concurrent
/// update perturbs the estimate for one packet pair and is not worth locking
/// the hot path to prevent.
pub struct SessionQuality {
    /// ASN of the client, `None` when it couldn't be resolved.
    asn: Option<u32>,
    /// Arrival of the most recent downstream packet in unix nanos, 0 before the
    /// first one.
    last_arrival: AtomicI64,
    /// Interarrival delta of the previous packet pair, in nanos.
    last_delta: AtomicI64,
    /// RFC 3550 interarrival jitter estimate, in nanos.
    jitter: AtomicI64,
    /// Downstream packets seen since the last aggregation.
    packets: AtomicU64,
}

impl SessionQuality {
    fn new(asn: Option<u32>) -> Self {
        Self {
            asn,
            last_arrival: AtomicI64::new(0),
            last_delta: AtomicI64::new(0),
            jitter: AtomicI64::new(0),
            packets: AtomicU64::new(0),
        }
    }

    /// Records the arrival of a downstream packet, updating the session's jitter
    /// estimate.
    #[inline]
    pub fn record_arrival(&self) {
        self.record_arrival_at(monotonic_nanos());
    }

    /// [`Self::record_arrival`] against a caller supplied monotonic reading, so
    /// the estimator can be exercised without the clock.
    #[inline]
    fn record_arrival_at(&self, now: i64) {
        self.packets.fetch_add(1, Relaxed);

        let previous_arrival = self.last_arrival.swap(now, Relaxed);
        if previous_arrival == 0 {
            return;
        }

        // A non-positive delta means two threads interleaved on this session, and
        // an oversized one means the stream paused. Neither is jitter, and both
        // would inflate the estimate, so the pair is dropped and the next one
        // measures from here.
        let delta = now - previous_arrival;
        if delta <= 0 || delta > MAX_INTERARRIVAL_NANOS {
            self.last_delta.store(0, Relaxed);
            return;
        }

        let previous_delta = self.last_delta.swap(delta, Relaxed);
        if previous_delta == 0 {
            return;
        }

        // J += (|D(i-1, i)| - J) / 16, per RFC 3550 A.8. Both operands are
        // non-negative and bounded by MAX_INTERARRIVAL_NANOS, so the estimate
        // cannot go negative and overflow the unsigned conversion at sampling.
        let deviation = (delta - previous_delta).abs();
        let jitter = self.jitter.load(Relaxed);
        self.jitter
            .store(jitter + (deviation - jitter) / JITTER_GAIN, Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn packets_since_last_sample(&self) -> u64 {
        self.packets.load(Relaxed)
    }

    /// Jitter estimate in nanos, and the packets seen since the previous call,
    /// which this resets.
    fn take_sample(&self) -> (i64, u64) {
        (self.jitter.load(Relaxed), self.packets.swap(0, Relaxed))
    }
}

/// Every live session's quality state, keyed by a registration id.
///
/// Sessions insert and remove themselves once each, so the cost lands on
/// session churn rather than on the packet path.
static REGISTRY: Lazy<dashmap::DashMap<u64, Arc<SessionQuality>>> = Lazy::new(<_>::default);

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// Keeps a session's [`SessionQuality`] in the registry for as long as the
/// session lives.
pub struct SessionQualityHandle {
    id: u64,
    quality: Arc<SessionQuality>,
}

impl SessionQualityHandle {
    /// Registers quality tracking for a session whose client resolved to `asn`.
    pub fn register(asn: Option<&IpNetEntry>) -> Self {
        // ASNs are 32-bit, the wider maxmind field is truncated rather than
        // dropped so a malformed database can't lose the whole session
        Self::register_for_asn(asn.map(|entry| entry.id as u32))
    }

    fn register_for_asn(asn: Option<u32>) -> Self {
        let id = NEXT_ID.fetch_add(1, Relaxed);
        let quality = Arc::new(SessionQuality::new(asn));
        REGISTRY.insert(id, quality.clone());

        Self { id, quality }
    }
}

impl std::ops::Deref for SessionQualityHandle {
    type Target = SessionQuality;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.quality
    }
}

impl Drop for SessionQualityHandle {
    fn drop(&mut self) {
        drop(REGISTRY.remove(&self.id));
    }
}

/// Tunables for [`spawn_aggregator`].
#[derive(Clone, Copy, Debug)]
pub struct AggregationConfig {
    /// How often the registry is folded into metrics.
    pub interval: Duration,
    /// Fraction of sessions whose jitter is recorded into the histogram each
    /// interval, in `0.0..=1.0`. Lowering it trades resolution for time spent in
    /// the aggregation.
    pub sample_fraction: f64,
    /// Number of client ASNs to report, largest first, with the rest folded into
    /// a remainder bucket. 0 disables per-ASN reporting.
    pub top_asns: usize,
    /// Jitter at or above which a session counts as degraded.
    pub jitter_threshold: Duration,
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            sample_fraction: 1.0,
            top_asns: 32,
            jitter_threshold: Duration::from_millis(30),
        }
    }
}

impl AggregationConfig {
    /// Rejects values that would silently produce meaningless metrics.
    pub fn validate(&self) -> eyre::Result<()> {
        if self.interval.is_zero() {
            eyre::bail!("session metrics interval must be at least a second");
        }

        if !(0.0..=1.0).contains(&self.sample_fraction) {
            eyre::bail!(
                "session metrics sample fraction must be between 0 and 1, got {}",
                self.sample_fraction
            );
        }

        if self.jitter_threshold.is_zero() {
            eyre::bail!("session metrics jitter threshold must be non-zero");
        }

        Ok(())
    }
}

/// The `reason` label value for the only quality signal the proxy can derive.
///
/// Loss and RTT on the client's leg need either packet sequence numbers or a
/// client-side timestamp, and the proxy has neither.
const REASON_JITTER: &str = "jitter";

/// What one interval measured for a single ASN.
#[derive(Default)]
struct AsnSample {
    /// Every session of this ASN, whether or not it carried traffic.
    sessions: usize,
    /// Sessions that carried enough packets to judge. A session gone quiet says
    /// nothing about its client's connection.
    judged: usize,
    /// Judged sessions at or above the jitter threshold.
    degraded: usize,
}

impl AsnSample {
    fn merge(&mut self, other: &Self) {
        self.sessions += other.sessions;
        self.judged += other.judged;
        self.degraded += other.degraded;
    }
}

/// One session's contribution, copied out of the registry before any metric is
/// touched.
struct Sample {
    asn: Option<u32>,
    jitter_nanos: i64,
    packets: u64,
}

/// Folds the session registry into metrics, holding the state that spans
/// intervals.
///
/// Whether an individual session is having a bad time is a judgement the proxy
/// can make, since it holds that session's packet timing. Whether an *ISP* is
/// having a bad time is not: a pod carries around a hundred concurrent sessions
/// spread over thousands of ASNs, so no single proxy sees enough of any one ASN
/// to threshold on. This exports the numerator and denominator per ASN and leaves
/// that decision to whatever sums them across the fleet.
struct Aggregator {
    config: AggregationConfig,
    /// ASN label values currently exported, so an ASN dropping out of the
    /// reported set has its series removed rather than left at a stale value.
    exported_asns: HashSet<String>,
    /// ASN label values currently exported as having degraded sessions.
    exported_degraded: HashSet<String>,
    /// `quilkin_packet_jitter` observation counts per direction as of the
    /// previous interval, used to spot a gauge that has gone stale.
    last_jitter_observations: [u64; 2],
    /// Reused across intervals so folding the registry doesn't allocate
    /// proportionally to the session count every time.
    samples: Vec<Sample>,
}

impl Aggregator {
    fn new(config: AggregationConfig) -> Self {
        Self {
            config,
            exported_asns: HashSet::new(),
            exported_degraded: HashSet::new(),
            last_jitter_observations: [0, 0],
            samples: Vec::new(),
        }
    }

    fn tick(&mut self) {
        // Copied out first: iterating the registry holds a shard lock, and session
        // registration needs that same lock on the packet path, so no metric work
        // happens while it's held
        self.samples.clear();
        self.samples.extend(REGISTRY.iter().map(|entry| {
            let (jitter_nanos, packets) = entry.value().take_sample();
            Sample {
                asn: entry.value().asn,
                jitter_nanos,
                packets,
            }
        }));

        let jitter_threshold = self.config.jitter_threshold.as_nanos() as i64;
        let mut per_asn: HashMap<Option<u32>, AsnSample> = HashMap::new();

        for sample in &self.samples {
            let entry = per_asn.entry(sample.asn).or_default();
            entry.sessions += 1;

            if sample.packets < MIN_PACKETS_FOR_JITTER {
                continue;
            }

            entry.judged += 1;

            if sample.jitter_nanos >= jitter_threshold {
                entry.degraded += 1;
            }

            if sampled(self.config.sample_fraction) {
                // Non-negative and bounded by construction, see `record_arrival`
                metrics::session_jitter_seconds()
                    .observe(Duration::from_nanos(sample.jitter_nanos as u64).as_secs_f64());
            }
        }

        self.update_asn_metrics(&per_asn);
        self.prune_stale_jitter_gauges();
    }

    /// Exports session and degraded-session counts for the largest `top_asns`
    /// ASNs, with everything else folded into a remainder bucket so both sum to
    /// the proxy's totals.
    fn update_asn_metrics(&mut self, per_asn: &HashMap<Option<u32>, AsnSample>) {
        let mut buckets: Vec<(String, AsnSample)> = Vec::new();

        if self.config.top_asns > 0 {
            let mut ranked: Vec<(String, &AsnSample)> = per_asn
                .iter()
                .map(|(asn, sample)| (asn_label(*asn), sample))
                .collect();
            // Ties broken by label so a pod's reported set doesn't churn between
            // equally sized ASNs from one interval to the next
            ranked.sort_unstable_by(|(a_label, a), (b_label, b)| {
                b.sessions
                    .cmp(&a.sessions)
                    .then_with(|| a_label.cmp(b_label))
            });

            let remainder = ranked.split_off(self.config.top_asns.min(ranked.len()));
            buckets.extend(
                ranked
                    .into_iter()
                    .map(|(label, sample)| (label, AsnSample { ..*sample })),
            );

            if !remainder.is_empty() {
                let mut folded = AsnSample::default();
                for (_, sample) in remainder {
                    folded.merge(sample);
                }
                buckets.push((REMAINDER_ASN.to_owned(), folded));
            }
        }

        let mut active = HashSet::with_capacity(buckets.len());
        let mut degraded = HashSet::new();
        let mut degraded_observations = 0;

        for (label, sample) in buckets {
            metrics::sessions_active_by_asn(&label).set(sample.sessions as i64);

            // Only exported while non-zero, so a healthy fleet publishes nothing
            // here and the series count follows the number of ISPs in trouble
            if sample.degraded > 0 {
                metrics::client_sessions_degraded(&label, REASON_JITTER)
                    .set(sample.degraded as i64);
                degraded_observations += sample.degraded;
                degraded.insert(label.clone());
            }

            active.insert(label);
        }

        for stale in self.exported_asns.difference(&active) {
            metrics::remove_sessions_active_by_asn(stale);
        }
        for stale in self.exported_degraded.difference(&degraded) {
            metrics::remove_client_sessions_degraded(stale, REASON_JITTER);
        }

        self.exported_asns = active;
        self.exported_degraded = degraded;

        if degraded_observations > 0 {
            metrics::client_sessions_degraded_total(REASON_JITTER)
                .inc_by(degraded_observations as u64);
        }
    }

    /// `quilkin_packet_jitter` is a gauge set per packet, so a proxy that stops
    /// receiving keeps publishing the last value it saw indefinitely. Drop the
    /// series when nothing observed it in the interval instead.
    fn prune_stale_jitter_gauges(&mut self) {
        for (index, direction) in [metrics::READ, metrics::WRITE].into_iter().enumerate() {
            let observations = metrics::packet_jitter_observations(direction);
            if observations == self.last_jitter_observations[index] {
                metrics::remove_packet_jitter(direction);
            }
            self.last_jitter_observations[index] = observations;
        }
    }
}

#[inline]
fn sampled(fraction: f64) -> bool {
    fraction >= 1.0 || rand::random::<f64>() < fraction
}

fn asn_label(asn: Option<u32>) -> String {
    match asn {
        Some(asn) => asn.to_string(),
        None => UNKNOWN_ASN.to_owned(),
    }
}

/// Spawns the task that periodically folds session quality into metrics.
///
/// The registry and the metrics it writes are process-wide, so a second
/// aggregator would fight the first over the same series. Repeat calls are
/// refused rather than silently adopting the first caller's configuration.
pub fn spawn_aggregator(
    config: AggregationConfig,
    shutdown: &mut crate::signal::ShutdownHandler,
) -> eyre::Result<()> {
    static SPAWNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    config.validate()?;

    if SPAWNED.swap(true, Relaxed) {
        tracing::warn!("session metrics aggregation is already running, ignoring configuration");
        return Ok(());
    }

    // Registered up front so the series exists for any proxy serving UDP, rather
    // than appearing only once a session has enough packets to sample
    let _ = metrics::session_jitter_seconds();

    let mut aggregator = Aggregator::new(config);
    let finished = shutdown.push("session_metrics");
    let mut srx = shutdown.shutdown_rx();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => aggregator.tick(),
                _ = srx.changed() => break,
            }
        }

        // So a scrape during drain doesn't see a value from before shutdown
        aggregator.tick();
        drop(finished.send(Ok(())));
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quality(asn: Option<u32>) -> SessionQuality {
        SessionQuality {
            asn,
            last_arrival: AtomicI64::new(0),
            last_delta: AtomicI64::new(0),
            jitter: AtomicI64::new(0),
            packets: AtomicU64::new(0),
        }
    }

    /// Feeds a sequence of interarrival gaps in milliseconds.
    fn arrivals(q: &SessionQuality, gaps_ms: &[i64]) {
        let mut now = 1;
        for gap in gaps_ms {
            now += gap * 1_000_000;
            q.record_arrival_at(now);
        }
    }

    #[test]
    fn steady_arrivals_have_no_jitter() {
        let q = quality(None);
        arrivals(&q, &[20; 10]);

        let (jitter, packets) = q.take_sample();
        assert_eq!(jitter, 0);
        assert_eq!(packets, 10);
        // The counter is a per-interval window
        assert_eq!(q.take_sample().1, 0);
    }

    #[test]
    fn varying_arrivals_accumulate_jitter() {
        let q = quality(Some(1));
        arrivals(&q, &[20, 60, 20, 70, 15, 80, 20, 65, 25, 75]);

        assert!(q.take_sample().0 > 0);
    }

    #[test]
    fn a_pause_in_the_stream_is_not_jitter() {
        let q = quality(None);
        // Steady, then the player alt-tabs for 30s, then steady again
        arrivals(&q, &[20; 20]);
        arrivals(&q, &[30_000]);
        arrivals(&q, &[20; 20]);

        // Fed to the estimator the gap would read as ~1.9s of jitter, which
        // against a 30ms threshold is a badly degraded player rather than one who
        // stopped sending
        assert_eq!(q.take_sample().0, 0);
    }

    #[test]
    fn a_clock_going_backwards_is_not_jitter() {
        let q = quality(None);
        arrivals(&q, &[20; 10]);

        // Two workers interleaving on one session, or a wall clock being stepped
        q.record_arrival_at(1);
        arrivals(&q, &[20; 10]);

        assert_eq!(q.take_sample().0, 0);
    }

    #[test]
    fn jitter_never_goes_negative() {
        let q = quality(None);
        // Alternating extremes, then settling: the estimate must stay in range for
        // the unsigned conversion at sampling to be sound
        arrivals(&q, &[1, 999, 1, 999, 1, 999]);
        arrivals(&q, &[20; 50]);

        let (jitter, _) = q.take_sample();
        assert!((0..=MAX_INTERARRIVAL_NANOS).contains(&jitter), "{jitter}");
    }

    #[test]
    fn handle_registration_is_scoped_to_the_session() {
        let before = REGISTRY.len();
        let handle = SessionQualityHandle::register(None);
        assert_eq!(REGISTRY.len(), before + 1);

        handle.record_arrival();
        drop(handle);
        assert_eq!(REGISTRY.len(), before);
    }

    /// `(asn, sessions, judged, degraded)`
    fn samples(entries: &[(Option<u32>, usize, usize, usize)]) -> HashMap<Option<u32>, AsnSample> {
        entries
            .iter()
            .map(|(asn, sessions, judged, degraded)| {
                (
                    *asn,
                    AsnSample {
                        sessions: *sessions,
                        judged: *judged,
                        degraded: *degraded,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn top_asns_are_capped_and_the_rest_land_in_the_remainder() {
        let mut aggregator = Aggregator::new(AggregationConfig {
            top_asns: 2,
            ..<_>::default()
        });

        aggregator.update_asn_metrics(&samples(&[
            (Some(1), 10, 10, 0),
            (Some(2), 5, 5, 0),
            (Some(3), 3, 3, 0),
            (Some(4), 1, 1, 0),
        ]));

        assert_eq!(
            aggregator.exported_asns,
            ["1", "2", REMAINDER_ASN]
                .into_iter()
                .map(String::from)
                .collect()
        );
        assert_eq!(metrics::sessions_active_by_asn("1").get(), 10);
        // Everything below the top 2 reconciles against the session total
        assert_eq!(metrics::sessions_active_by_asn(REMAINDER_ASN).get(), 4);
    }

    #[test]
    fn degraded_sessions_are_reported_against_a_denominator() {
        let mut aggregator = Aggregator::new(AggregationConfig {
            top_asns: 4,
            ..<_>::default()
        });

        // Six of this ASN's twenty judged sessions are having a bad time. Whether
        // 30% is bad enough to act on is not a decision one pod can make, so both
        // numbers are exported and neither is thresholded here.
        aggregator.update_asn_metrics(&samples(&[(Some(5), 25, 20, 6)]));

        assert_eq!(metrics::sessions_active_by_asn("5").get(), 25);
        assert_eq!(
            metrics::client_sessions_degraded("5", REASON_JITTER).get(),
            6
        );
        assert!(aggregator.exported_degraded.contains("5"));
    }

    #[test]
    fn a_healthy_asn_exports_no_degraded_series() {
        let mut aggregator = Aggregator::new(AggregationConfig {
            top_asns: 4,
            ..<_>::default()
        });

        aggregator.update_asn_metrics(&samples(&[(Some(6), 30, 30, 2)]));
        assert!(aggregator.exported_degraded.contains("6"));

        // Recovered, so the series goes away rather than sitting at a stale count
        aggregator.update_asn_metrics(&samples(&[(Some(6), 30, 30, 0)]));
        assert!(aggregator.exported_degraded.is_empty());
        assert!(aggregator.exported_asns.contains("6"));
    }

    #[test]
    fn quiet_sessions_are_counted_but_not_judged() {
        let mut aggregator = Aggregator::new(AggregationConfig {
            top_asns: 1,
            ..<_>::default()
        });

        // Sixty sessions of which twenty carried traffic: the session gauge counts
        // them all, so it reconciles with `quilkin_session_active`
        aggregator.update_asn_metrics(&samples(&[(Some(7), 60, 20, 15)]));

        assert_eq!(metrics::sessions_active_by_asn("7").get(), 60);
        assert_eq!(
            metrics::client_sessions_degraded("7", REASON_JITTER).get(),
            15
        );
    }

    #[test]
    fn per_asn_reporting_can_be_turned_off() {
        let mut aggregator = Aggregator::new(AggregationConfig {
            top_asns: 0,
            ..<_>::default()
        });

        aggregator.update_asn_metrics(&samples(&[(Some(8), 10, 10, 4)]));

        assert!(aggregator.exported_asns.is_empty());
        assert!(aggregator.exported_degraded.is_empty());
    }

    #[test]
    fn a_tick_folds_registered_sessions_into_the_asn_gauges() {
        let mut aggregator = Aggregator::new(AggregationConfig {
            top_asns: 4,
            jitter_threshold: Duration::from_millis(30),
            ..<_>::default()
        });

        let steady = SessionQualityHandle::register_for_asn(Some(424242));
        let jittery = SessionQualityHandle::register_for_asn(Some(424242));
        let quiet = SessionQualityHandle::register_for_asn(Some(424242));

        arrivals(&steady, &[20; 30]);
        arrivals(&jittery, &[5, 90, 5, 90, 5, 90, 5, 90, 5, 90]);
        // Below MIN_PACKETS_FOR_JITTER, so counted but not judged
        jittery.record_arrival();
        quiet.record_arrival();

        aggregator.tick();

        assert_eq!(metrics::sessions_active_by_asn("424242").get(), 3);
        assert_eq!(
            metrics::client_sessions_degraded("424242", REASON_JITTER).get(),
            1
        );

        drop((steady, jittery, quiet));
        aggregator.tick();

        // Series withdrawn once the sessions are gone
        assert!(!aggregator.exported_asns.contains("424242"));
        assert!(!aggregator.exported_degraded.contains("424242"));
    }

    #[test]
    fn configuration_is_rejected_rather_than_clamped() {
        assert!(AggregationConfig::default().validate().is_ok());

        for invalid in [
            AggregationConfig {
                sample_fraction: 5.0,
                ..<_>::default()
            },
            AggregationConfig {
                sample_fraction: -1.0,
                ..<_>::default()
            },
            AggregationConfig {
                interval: Duration::ZERO,
                ..<_>::default()
            },
            AggregationConfig {
                jitter_threshold: Duration::ZERO,
                ..<_>::default()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
    }
}
