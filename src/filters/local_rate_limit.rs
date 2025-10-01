/*
 * Copyright 2020 Google LLC
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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    collections::ttl::{Entry, TtlMap},
    filters::prelude::*,
    net::endpoint::EndpointAddress,
};

use crate::generated::quilkin::filters::local_rate_limit::v1alpha1 as proto;

// TODO: we should make these values configurable and transparent to the filter.
/// The default session timeout.
pub const SESSION_TIMEOUT_SECONDS: Duration = Duration::from_secs(60);

/// The default interval to check for expired sessions.
const SESSION_EXPIRY_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Bucket stores two atomics.
/// - A counter that tracks how many packets we've processed within a time window.
/// - A timestamp that stores the time we last reset the counter. It tracks
///   the start of the time window.
///
/// This allows us to have a simpler implementation for calculating token
/// exhaustion without needing a write lock in the common case. The downside
/// however is that since we're relying on two independent atomics, there is
/// in theory, a chance that we could allow a few packets through (i.e in-between
/// checking the counter and the timestamp). However, in practice this would be
/// quite rare and the number of such packets that do get through will likely be
/// insignificant (worse case scenario is ~N-1 stray packets where N is the
/// number of packet handling workers).
#[derive(Debug)]
struct Bucket {
    counter: Arc<AtomicUsize>,
    window_start_time_secs: Arc<AtomicU64>,
}

/// A filter that implements rate limiting on packets based on the token-bucket
/// algorithm.
///
/// Packets that violate the rate limit are dropped. It only applies rate
/// limiting on packets received from a downstream connection (processed
/// through [`Self::read`]).
///
/// Packets coming from upstream endpoints flow through the filter untouched.
pub struct LocalRateLimit {
    /// Tracks rate limiting state per source address.
    state: TtlMap<EndpointAddress, Bucket>,
    /// Filter configuration.
    config: Config,
}

impl LocalRateLimit {
    /// new returns a new [`Self`]. It spawns a future in the background
    /// that periodically refills the rate limiter's tokens.
    fn new(config: Config) -> Result<Self, CreationError> {
        if config.period < 1 {
            return Err(CreationError::FieldInvalid {
                field: "period".into(),
                reason: "value must be at least 1 second".into(),
            });
        }

        Ok(LocalRateLimit {
            state: TtlMap::new(SESSION_TIMEOUT_SECONDS, SESSION_EXPIRY_POLL_INTERVAL),
            config,
        })
    }

    pub fn testing(config: Config) -> Self {
        Self::new(config).unwrap()
    }

    /// This is called on behalf of every packet that is eligible
    /// for rate limiting.
    ///
    /// It returns whether there exists a token for the corresponding address in
    /// the current period - determining whether or not the packet should be
    /// forwarded or dropped.
    fn acquire_token(&self, address: &EndpointAddress) -> bool {
        if self.config.max_packets == 0 {
            return false;
        }

        if let Some(bucket) = self.state.get(address) {
            let prev_count = bucket.value.counter.fetch_add(1, Ordering::Relaxed);

            let now_secs = self.state.now_relative_secs();
            let window_start_secs = bucket.value.window_start_time_secs.load(Ordering::Relaxed);

            let elapsed_secs = now_secs - window_start_secs;
            let start_new_window = elapsed_secs > self.config.period as u64;

            // Check if allowing this packet will put us over the maximum.
            if prev_count >= self.config.max_packets {
                // If so, then we can only allow the packet if the current time
                // window has ended.
                if !start_new_window {
                    return false;
                }
            }

            if start_new_window {
                // Current time window has ended, so we can reset the counter and
                // start a new time window instead.
                bucket.value.counter.store(1, Ordering::Relaxed);
                bucket
                    .value
                    .window_start_time_secs
                    .store(now_secs, Ordering::Relaxed);
            }

            return true;
        }

        match self.state.entry(address.clone()) {
            Entry::Occupied(entry) => {
                // It is possible that some other task has added the item since we
                // checked for it. If so, only increment the counter - no need to
                // update the window start time since the window has just started.
                let bucket = entry.get();
                bucket.value.counter.fetch_add(1, Ordering::Relaxed);
            }
            Entry::Vacant(entry) => {
                // New entry, set both the time stamp and
                let now_secs = self.state.now_relative_secs();
                entry.insert(Bucket {
                    counter: Arc::new(AtomicUsize::new(1)),
                    window_start_time_secs: Arc::new(AtomicU64::new(now_secs)),
                });
            }
        };

        true
    }
}

impl Filter for LocalRateLimit {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        if self.acquire_token(&ctx.source) {
            Ok(())
        } else {
            Err(FilterError::RateLimitExceeded)
        }
    }
}

impl StaticFilter for LocalRateLimit {
    const NAME: &'static str = "quilkin.filters.local_rate_limit.v1alpha1.LocalRateLimit";
    type Configuration = Config;
    type BinaryConfiguration = proto::LocalRateLimit;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Self::new(Self::ensure_config_exists(config)?)
    }
}

/// Config represents a [self]'s configuration.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, schemars::JsonSchema)]
pub struct Config {
    /// The maximum number of packets allowed to be forwarded by the rate
    /// limiter in a given duration.
    pub max_packets: usize,
    /// The duration in seconds during which `max_packets` applies. If none is provided, it
    /// defaults to one second.
    pub period: u32,
}

/// default value for [`Config::period`]
fn default_period() -> u32 {
    1
}

impl From<Config> for proto::LocalRateLimit {
    fn from(config: Config) -> Self {
        Self {
            max_packets: config.max_packets as u64,
            period: Some(config.period),
        }
    }
}

impl TryFrom<proto::LocalRateLimit> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::LocalRateLimit) -> Result<Self, Self::Error> {
        Ok(Self {
            max_packets: p.max_packets as usize,
            period: p.period.unwrap_or_else(default_period),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryFrom, net::Ipv4Addr, time::Duration};

    use tokio::time;

    use super::*;
    use crate::{
        config::ConfigType,
        test::{alloc_buffer, assert_write_no_change},
    };

    fn rate_limiter(config: Config) -> LocalRateLimit {
        LocalRateLimit::new(config).unwrap()
    }

    fn address_pair() -> (EndpointAddress, EndpointAddress) {
        (
            (Ipv4Addr::LOCALHOST, 8080).into(),
            (Ipv4Addr::LOCALHOST, 8081).into(),
        )
    }

    /// Send a packet to the filter and assert whether or not it was processed.
    fn read(r: &LocalRateLimit, address: &EndpointAddress, should_succeed: bool) {
        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [crate::net::endpoint::Endpoint::new(
                (Ipv4Addr::LOCALHOST, 8089).into(),
            )]
            .into(),
        );

        let mut dest = Vec::new();
        let mut context =
            ReadContext::new(&endpoints, address.clone(), alloc_buffer([9]), &mut dest);
        let result = r.read(&mut context);

        if should_succeed {
            result.unwrap();
            assert_eq!(&*context.contents, [9]);
        } else {
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn config_minimum_period() {
        let factory = LocalRateLimit::factory();
        let config = "
max_packets: 10
period: 0
";
        let err = factory
            .create_filter(CreateFilterArgs {
                config: Some(ConfigType::Static(serde_yaml::from_str(config).unwrap())),
            })
            .err()
            .unwrap();
        assert!(format!("{err:?}").contains("value must be at least 1 second"));
    }

    #[test]
    fn convert_proto_config() {
        let test_cases = vec![
            (
                "should succeed when all valid values are provided",
                proto::LocalRateLimit {
                    max_packets: 10,
                    period: Some(2),
                },
                Some(Config {
                    max_packets: 10,
                    period: 2,
                }),
            ),
            (
                "should use correct default values",
                proto::LocalRateLimit {
                    max_packets: 10,
                    period: None,
                },
                Some(Config {
                    max_packets: 10,
                    period: 1,
                }),
            ),
        ];
        for (name, proto_config, expected) in test_cases {
            let result = Config::try_from(proto_config);
            assert_eq!(
                result.is_err(),
                expected.is_none(),
                "{}: error expectation does not match",
                name
            );
            if let Some(expected) = expected {
                assert_eq!(expected, result.unwrap(), "{}", name);
            }
        }
    }

    #[tokio::test]
    async fn initially_available_tokens() {
        // Test that we always start with the max number of tokens available.
        let r = rate_limiter(Config {
            max_packets: 3,
            period: 1,
        });

        let (address, _) = address_pair();

        read(&r, &address, true);
        read(&r, &address, true);
        read(&r, &address, true);
        read(&r, &address, false);
    }

    #[tokio::test]
    async fn filter_with_no_available_tokens() {
        let r = rate_limiter(Config {
            max_packets: 0,
            period: 1,
        });

        let (address, _) = address_pair();

        // Check that other routes are not affected.
        assert_write_no_change(&r);

        // Check that we're rate limited.
        read(&r, &address, false);
    }

    #[tokio::test]
    async fn rate_limit_reads_for_multiple_sources() {
        time::pause();

        let r = rate_limiter(Config {
            max_packets: 2,
            period: 1,
        });

        let (address1, address2) = address_pair();

        // Read until we exhaust tokens for both addresses.
        read(&r, &address1, true);
        read(&r, &address2, true);
        read(&r, &address1, true);
        read(&r, &address2, true);

        // Check that we've exhausted their tokens.
        read(&r, &address1, false);
        read(&r, &address2, false);
        read(&r, &address1, false);
        read(&r, &address2, false);

        // Advance time to refill tokens.
        time::advance(Duration::from_secs(2)).await;

        // Check that we are able to process packets again.
        read(&r, &address1, true);
        read(&r, &address2, true);
        read(&r, &address1, true);

        // Advance time to to the end of the current window.
        time::advance(Duration::from_secs(1)).await;

        // Only the second address should have tokens left.
        read(&r, &address1, false);
        read(&r, &address2, true);

        // Check that other routes are not affected.
        assert_write_no_change(&r);
    }

    #[tokio::test]
    async fn max_token_refills_is_never_exceeded_for_partially_filled_buckets() {
        // Check that if a token bucket isn't being used up, continuous
        // refills do not exceed the maximum number of tokens.
        time::pause();

        let r = rate_limiter(Config {
            max_packets: 2,
            period: 1,
        });

        let (address, _) = address_pair();

        // Acquire 1 token.
        read(&r, &address, true);

        // Advance to some time in the future after multiple token refills.
        time::advance(Duration::from_secs(10)).await;

        // Check that we still have the 2 tokens within a window.
        read(&r, &address, true);
        read(&r, &address, true);
        read(&r, &address, false);

        // Check that other routes are not affected.
        assert_write_no_change(&r);
    }
}
