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

use once_cell::sync::Lazy;
use prometheus::{HistogramVec, IntCounterVec};

use crate::metrics::{Direction, registry};

pub(crate) fn counter(
    id: &str,
    label: &str,
    help: &str,
    direction: Direction,
) -> prometheus::IntCounter {
    static COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
        prometheus::register_int_counter_vec_with_registry! {
            prometheus::opts! {
                "quilkin_filter_int_counter",
                "generic filter counter, see help label for more specific info",
            },
            &["id", "label", "help", Direction::LABEL],
            registry(),
        }
        .unwrap()
    });

    COUNTERS.with_label_values(&[id, label, help, direction.label()])
}

pub(crate) fn histogram(
    id: &str,
    label: &str,
    help: &str,
    direction: Direction,
    metadata: &[&str],
) -> prometheus::Histogram {
    debug_assert!(
        metadata.len() <= 1,
        "shared metadata exceeds current label cardinality"
    );
    static HISTOGRAMS: Lazy<HistogramVec> = Lazy::new(|| {
        prometheus::register_histogram_vec_with_registry! {
            prometheus::histogram_opts! {
                "filter_histogram",
                "generic filter histogram, see help label for more specific info",
            },
            &["id", "label", "help", Direction::LABEL, "shared_metadata_1"],
            registry(),
        }
        .unwrap()
    });

    HISTOGRAMS.with_label_values(&[
        id,
        label,
        help,
        direction.label(),
        metadata.first().copied().unwrap_or_default(),
    ])
}
