/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
use prometheus::core::{AtomicI64, GenericCounter};
use prometheus::Result as MetricsResult;
use prometheus::{IntCounter, Registry};

use crate::metrics::{filter_opts, CollectorExt};

/// Register and manage metrics for this filter
pub(super) struct Metrics {
    pub(super) packets_dropped_total: GenericCounter<AtomicI64>,
}

impl Metrics {
    pub(super) fn new(registry: &Registry) -> MetricsResult<Self> {
        Ok(Metrics {
            packets_dropped_total: IntCounter::with_opts(filter_opts(
                "packets_dropped",
                "CaptureBytes",
                "Total number of packets dropped due capture size being larger than the received packet",
            ))?
            .register(registry)?,
        })
    }
}
