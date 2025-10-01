/*
 * Copyright 2023 Google LLC
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
use std::net::SocketAddr;

use crate::time::DurationNanos;

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Qcmp {
    Ping(Ping),
}

/// Pings a endpoint for a `amount` of attempts, printing the resulting
/// median, average, and number of successful attempts.
#[derive(clap::Args, Clone, Debug)]
pub struct Ping {
    /// The quilkin endpoint to ping
    pub endpoint: SocketAddr,
    /// The number of pings to send to the endpoint (default: 5).
    #[clap(short, long, default_value_t = 5)]
    pub amount: usize,
    /// Ping at a set interval instead of immediately after the last response
    #[clap(short, long)]
    pub interval: Option<crate::cli::Duration>,
    /// The timeout threshold when waiting for a ping response
    #[clap(short, long, default_value = "1s")]
    pub timeout: crate::cli::Duration,
}

impl Ping {
    pub async fn run(&self) -> crate::Result<()> {
        tracing::info!("starting ping task");

        let mut results = Vec::new();

        let qcmp_transceiver = std::sync::Arc::new(crate::codec::qcmp::QcmpTransceiver::new()?);
        let mut ticker = self.interval.map(|d| tokio::time::interval(d.0));

        for _ in 0..self.amount {
            if let Some(ticker) = ticker.as_mut() {
                let _ = ticker.tick().await;
            }

            let (recv_time, reply) = match qcmp_transceiver
                .ping(self.endpoint, std::time::Duration::from_secs(5))
                .await
            {
                Ok((recv_time, reply)) => (recv_time, reply),
                Err(error) => {
                    tracing::error!(endpoint=%self.endpoint, ?error, "ping failed");
                    continue;
                }
            };

            let delay = reply.round_trip_delay(recv_time).unwrap();
            tracing::info!(delay_millis=%format!("{:.2}", delay.duration().as_secs_f64() * 1000.0), "successful ping");
            results.push(delay);
        }

        match median(&mut results) {
            Some(median) => {
                let median = median.duration();
                let average = std::time::Duration::from_nanos(
                    (results.iter().map(|dn| dn.nanos() as i128).sum::<i128>()
                        / results.len() as i128) as u64,
                );
                tracing::info!(
                    median_millis=%format!("{:.2}", median.as_secs_f64() * 1000.0),
                    average_millis=%format!("{:.2}", average.as_secs_f64() * 1000.0),
                    attempts=%self.amount,
                    successful_attempts=%results.len(),
                    "final results"
                );
            }
            None => {
                eyre::bail!("no successful results");
            }
        }

        Ok(())
    }
}

fn median(numbers: &mut [DurationNanos]) -> Option<DurationNanos> {
    let len = numbers.len();
    if len == 0 {
        return None;
    }

    // Sort the slice
    numbers.sort();

    if len % 2 == 1 {
        // Odd number of elements: Return the middle one.
        Some(numbers[len / 2])
    } else {
        // Even number of elements: Return the average of the two middle ones.
        let mid1 = numbers[(len - 1) / 2];
        let mid2 = numbers[len / 2];
        Some(DurationNanos::from_nanos((mid1.nanos() + mid2.nanos()) / 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dn(nanos: i64) -> DurationNanos {
        DurationNanos::from_nanos(nanos)
    }

    #[test]
    fn empty() {
        assert_eq!(median(&mut []), None);
    }

    #[test]
    fn single() {
        let dn = dn(42);
        assert_eq!(median(&mut [dn]), Some(dn));
    }

    #[test]
    fn odd() {
        assert_eq!(median(&mut [dn(3), dn(1), dn(2)]), Some(dn(2)));
    }

    #[test]
    fn even() {
        assert_eq!(median(&mut [dn(4), dn(3), dn(1), dn(2)]), Some(dn(2)));
    }
}
