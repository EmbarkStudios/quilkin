/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use prometheus::IntCounter;

use crate::{
    filters::{StaticFilter, metrics},
    metrics::Direction,
};

fn packets_matched_total(direction: Direction) -> IntCounter {
    metrics::counter(
        super::Match::NAME,
        "packets_matched_total",
        "Total number of packets where the dynamic metadata matches a branch value",
        direction,
    )
}

fn packets_fallthrough_total(direction: Direction) -> IntCounter {
    metrics::counter(
        super::Match::NAME,
        "packets_fallthrough_total",
        "Total number of packets that are processed by the fallthrough configuration",
        direction,
    )
}

/// Register and manage metrics for this filter
pub struct Metrics {
    pub packets_matched_total: IntCounter,
    pub packets_fallthrough_total: IntCounter,
}

impl Metrics {
    pub(super) fn new() -> Self {
        Metrics {
            packets_matched_total: packets_matched_total(Direction::Read),
            packets_fallthrough_total: packets_fallthrough_total(Direction::Read),
        }
    }
}
