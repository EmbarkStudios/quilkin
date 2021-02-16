/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

use std::convert::{TryFrom, TryInto};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::{channel, Sender};
use tokio::time::{self, Instant};

use metrics::Metrics;

use crate::extensions::filter_registry::{CreateFilterArgs, ReadContext, ReadResponse};
use crate::extensions::filters::ConvertProtoConfigError;
use crate::extensions::{Error, Filter, FilterFactory};
use proto::quilkin::extensions::filters::local_rate_limit::v1alpha1::LocalRateLimit as ProtoConfig;

mod metrics;
mod proto;

/// Config represents a RateLimitFilter's configuration.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Config {
    /// max_packets is the maximum number of packets allowed
    /// to be forwarded by the rate limiter in a given duration.
    max_packets: usize,
    /// period is the duration during which max_packets applies.
    /// If none is provided, it defaults to 1 second.
    #[serde(with = "humantime_serde", default = "default_period")]
    period: Duration,
}

/// default value for [`Config::period`]
fn default_period() -> Duration {
    Duration::from_secs(1)
}
impl TryFrom<ProtoConfig> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: ProtoConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            max_packets: p.max_packets as usize,
            period: p
                .period
                .map(|period| {
                    period.try_into().map_err(|err| {
                        ConvertProtoConfigError::new(
                            format!("invalid duration: {:?}", err),
                            Some("period".into()),
                        )
                    })
                })
                .transpose()?
                .unwrap_or_else(default_period),
        })
    }
}

/// Creates instances of RateLimitFilter.
#[derive(Default)]
pub struct RateLimitFilterFactory;

/// A filter that implements rate limiting on packets based on
/// the token-bucket algorithm.
/// Packets that violate the rate limit are dropped.
/// It only applies rate limiting on packets that are destined for the
/// proxy's endpoints. All other packets flow through the filter untouched.
struct RateLimitFilter {
    /// available_tokens is how many tokens are left in the bucket any
    /// any given moment.
    available_tokens: Arc<AtomicUsize>,
    /// metrics reporter for this filter.
    metrics: Metrics,
    /// shutdown_tx signals the spawned token refill future to exit.
    shutdown_tx: Option<Sender<()>>,
}

impl FilterFactory for RateLimitFilterFactory {
    fn name(&self) -> String {
        "quilkin.extensions.filters.local_rate_limit.v1alpha1.LocalRateLimit".into()
    }

    fn create_filter(&self, args: CreateFilterArgs) -> Result<Box<dyn Filter>, Error> {
        let config: Config = self
            .require_config(args.config)?
            .deserialize::<Config, ProtoConfig>(self.name().as_str())?;

        if config.period.lt(&Duration::from_millis(100)) {
            Err(Error::FieldInvalid {
                field: "period".into(),
                reason: "value must be at least 100ms".into(),
            })
        } else {
            Ok(Box::new(RateLimitFilter::new(
                config,
                Metrics::new(&args.metrics_registry)?,
            )))
        }
    }
}

impl RateLimitFilter {
    /// new returns a new RateLimitFilter. It spawns a future in the background
    /// that periodically refills the rate limiter's tokens.
    fn new(config: Config, metrics: Metrics) -> Self {
        let (shutdown_tx, mut shutdown_rx) = channel();

        let tokens = Arc::new(AtomicUsize::new(config.max_packets));

        let max_tokens = config.max_packets;
        let period = config.period;
        let available_tokens = tokens.clone();
        let _ = tokio::spawn(async move {
            let mut interval = time::interval_at(Instant::now() + period, period);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Refill tokens.
                        let mut refilled = false;
                        while !refilled {
                            let remaining_tokens = available_tokens.load(Ordering::Relaxed);

                            refilled = available_tokens.compare_and_swap(
                                remaining_tokens,
                                max_tokens,
                                Ordering::Relaxed) == remaining_tokens;
                        }
                    },
                    _ = &mut shutdown_rx => {
                        return;
                    }
                }
            }
        });

        RateLimitFilter {
            available_tokens: tokens,
            metrics,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// acquire_token is called on behalf of every packet that is eligible
    /// for rate limiting. It returns whether there exists a token in the current
    /// period - determining whether or not the packet should be forwarded or dropped.
    fn acquire_token(&self) -> Option<()> {
        loop {
            let remaining_tokens = self.available_tokens.load(Ordering::Relaxed);

            if remaining_tokens == 0 {
                return None;
            }

            if self.available_tokens.compare_and_swap(
                remaining_tokens,
                remaining_tokens - 1,
                Ordering::Relaxed,
            ) == remaining_tokens
            {
                return Some(());
            }
        }
    }
}

impl Drop for RateLimitFilter {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            shutdown_tx.send(()).ok();
        }
    }
}

impl Filter for RateLimitFilter {
    fn read(&self, ctx: ReadContext) -> Option<ReadResponse> {
        self.acquire_token().map(|()| ctx.into()).or_else(|| {
            self.metrics.packets_dropped_total.inc();
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::time::Duration;

    use prometheus::Registry;
    use tokio::time;

    use super::ProtoConfig;
    use crate::cluster::Endpoint;
    use crate::config::Endpoints;
    use crate::extensions::filter_registry::ReadContext;
    use crate::extensions::filters::local_rate_limit::metrics::Metrics;
    use crate::extensions::filters::local_rate_limit::{Config, RateLimitFilter};
    use crate::extensions::Filter;
    use crate::test_utils::assert_write_no_change;

    fn rate_limiter(config: Config) -> RateLimitFilter {
        RateLimitFilter::new(config, Metrics::new(&Registry::default()).unwrap())
    }

    #[test]
    fn convert_proto_config() {
        let test_cases = vec![
            (
                "should succeed when all valid values are provided",
                ProtoConfig {
                    max_packets: 10,
                    period: Some(Duration::from_secs(2).into()),
                },
                Some(Config {
                    max_packets: 10,
                    period: Duration::from_secs(2),
                }),
            ),
            (
                "should use correct default values",
                ProtoConfig {
                    max_packets: 10,
                    period: None,
                },
                Some(Config {
                    max_packets: 10,
                    period: Duration::from_secs(1),
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
            period: Duration::from_millis(100),
        });

        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), None);
    }

    #[tokio::test]
    async fn token_exhaustion_and_refill() {
        let r = rate_limiter(Config {
            max_packets: 2,
            period: Duration::from_millis(100),
        });

        // Exhaust tokens
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), None);

        // Wait for refill
        let mut num_iterations = 0;
        let first_token = loop {
            let token = r.acquire_token();
            if token.is_some() {
                break token;
            }
            num_iterations += 1;
            if num_iterations >= 1000 {
                unreachable!("timed-out waiting for token refill");
            }
            time::sleep(Duration::from_millis(10)).await;
        };

        // Exhaust tokens again.
        assert_eq!(first_token, Some(()));
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), None);
    }

    #[tokio::test]
    async fn token_refill_maximum() {
        // Test that we never refill more than the max_tokens specified.

        let r = rate_limiter(Config {
            max_packets: 3,
            period: Duration::from_millis(30),
        });

        // Use up some of the tokens.
        assert_eq!(r.acquire_token(), Some(()));

        // Wait for refill
        time::sleep(Duration::from_millis(110)).await;

        // Refill should not go over max token limit.
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), Some(()));
        assert_eq!(r.acquire_token(), None);
    }

    #[tokio::test]
    async fn filter_with_no_available_tokens() {
        let r = rate_limiter(Config {
            max_packets: 0,
            period: Duration::from_millis(100),
        });

        // Check that other routes are not affected.
        assert_write_no_change(&r);

        // Check that we're rate limited.
        assert!(r
            .read(ReadContext::new(
                Endpoints::new(vec![Endpoint::from_address(
                    "127.0.0.1:8080".parse().unwrap(),
                )])
                .unwrap()
                .into(),
                "127.0.0.1:8080".parse().unwrap(),
                vec![9],
            ))
            .is_none(),);
    }

    #[tokio::test]
    async fn filter_with_available_tokens() {
        let r = rate_limiter(Config {
            max_packets: 1,
            period: Duration::from_millis(100),
        });

        let result = r
            .read(ReadContext::new(
                Endpoints::new(vec![Endpoint::from_address(
                    "127.0.0.1:8080".parse().unwrap(),
                )])
                .unwrap()
                .into(),
                "127.0.0.1:8080".parse().unwrap(),
                vec![9],
            ))
            .unwrap();
        assert_eq!(result.contents, vec![9]);
        // We should be out of tokens now.
        assert_eq!(None, r.acquire_token());

        // Check that other routes are not affected.
        assert_write_no_change(&r);
    }
}
