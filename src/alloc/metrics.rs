/*
 * Copyright 2024 Google LLC
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

cfg_if::cfg_if! {
    if #[cfg(feature = "jemalloc")] {
        pub fn spawn_heap_stats_updates(_period: std::time::Duration, _srx: crate::signal::ShutdownRx) {}
    } else if #[cfg(feature = "heap-stats")] {
        /// Spawns a task to periodically update the heap stat metrics from our tracking allocator
        pub fn spawn_heap_stats_updates(period: std::time::Duration, mut srx: crate::signal::ShutdownRx) {
            use crate::metrics::registry;
            use once_cell::sync::Lazy;
            use prometheus::{IntCounterVec, IntGaugeVec};

            static BYTES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
                prometheus::register_int_counter_vec_with_registry! {
                    prometheus::opts! {
                        "allocation_bytes_total",
                        "total number of allocated bytes",
                    },
                    &[],
                    registry(),
                }
                .unwrap()
            });

            static ALLOCS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
                prometheus::register_int_counter_vec_with_registry! {
                    prometheus::opts! {
                        "allocation_total",
                        "total number of allocations",
                    },
                    &[],
                    registry(),
                }
                .unwrap()
            });

            static EXTANT_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
                prometheus::register_int_gauge_vec_with_registry! {
                    prometheus::opts! {
                        "extant_allocation_size",
                        "current total of extant allocation bytes",
                    },
                    &[],
                    registry(),
                }
                .unwrap()
            });

            static EXTANT_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
                prometheus::register_int_gauge_vec_with_registry! {
                    prometheus::opts! {
                        "extant_allocation_count",
                        "current number of extant allocations",
                    },
                    &[],
                    registry(),
                }
                .unwrap()
            });

            tokio::task::spawn(async move {
                let mut bytes_total = 0u64;
                let mut alloc_total = 0u64;

                loop {
                    if tokio::time::timeout(period, srx.changed()).await.is_err() {
                        let stats = super::tracking::Allocator::stats();

                        BYTES_TOTAL
                            .with_label_values::<&str>(&[])
                            .inc_by(stats.cumul_alloc_size - bytes_total);
                        bytes_total = stats.cumul_alloc_size;
                        ALLOCS_TOTAL
                            .with_label_values::<&str>(&[])
                            .inc_by(stats.cumul_alloc_count - alloc_total);
                        alloc_total = stats.cumul_alloc_count;

                        if let Ok(val) = stats.current_allocated_size().try_into() {
                            EXTANT_SIZE.with_label_values::<&str>(&[]).set(val);
                        }
                        if let Ok(val) = stats.current_allocation_count().try_into() {
                            EXTANT_COUNT.with_label_values::<&str>(&[]).set(val);
                        }
                    } else {
                        tracing::trace!("exiting heap-stats task");
                        break;
                    }
                }
            });
        }
    } else if #[cfg(feature = "mimalloc")] {
        pub fn spawn_heap_stats_updates(_period: std::time::Duration, _srx: crate::signal::ShutdownRx) {}
    } else {
        pub fn spawn_heap_stats_updates(_period: std::time::Duration, _srx: crate::signal::ShutdownRx) {}
    }
}
