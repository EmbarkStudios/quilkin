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
    if #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))] {
        use tikv_jemalloc_ctl::stats;

        struct JemallocCollector {
            epoch: tikv_jemalloc_ctl::epoch_mib,
            active: stats::active_mib,
            allocated: stats::allocated_mib,
            mapped: stats::mapped_mib,
            metadata: stats::metadata_mib,
            resident: stats::resident_mib,
            retained: stats::retained_mib,
        }

        impl std::fmt::Debug for JemallocCollector {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JemallocCollector {}")
            }
        }

        impl JemallocCollector {
            fn new() -> crate::Result<Self> {
                Ok(Self {
                    epoch: tikv_jemalloc_ctl::epoch::mib()?,
                    active: stats::active::mib()?,
                    allocated: stats::allocated::mib()?,
                    mapped: stats::mapped::mib()?,
                    metadata: stats::metadata::mib()?,
                    resident: stats::resident::mib()?,
                    retained: stats::retained::mib()?,
                })
            }
        }

        impl prometheus_client::collector::Collector for JemallocCollector {
            fn encode(&self, mut encoder: prometheus_client::encoding::DescriptorEncoder<'_>) -> std::fmt::Result {
                use prometheus_client::{registry::Unit, encoding::EncodeMetric, metrics::gauge::ConstGauge};
                // many statistics are cached and only updated when the epoch is advanced.
                if let Err(error) = self.epoch.advance() {
                    tracing::warn!(?error, "failed to advance epoch");
                }

                if let Ok(active) = self
                    .active
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    let active_bytes = ConstGauge::new(active as u64);
                    let active_bytes_metric = encoder.encode_descriptor(
                        "jemalloc_stats_active",
                        "Total number of bytes in active pages allocated by the application.",
                        Some(&Unit::Bytes),
                        active_bytes.metric_type(),
                    )?;
                    active_bytes.encode(active_bytes_metric)?;
                }

                if let Ok(allocated) = self
                    .allocated
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    let allocated_bytes = ConstGauge::new(allocated as u64);
                    let allocated_bytes_metric = encoder.encode_descriptor(
                        "jemalloc_stats_allocated",
                        "Total number of bytes allocated by the application.",
                        Some(&Unit::Bytes),
                        allocated_bytes.metric_type(),
                    )?;
                    allocated_bytes.encode(allocated_bytes_metric)?;
                }

                if let Ok(mapped) = self
                    .mapped
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    let mapped_bytes = ConstGauge::new(mapped as u64);
                    let mapped_bytes_metric = encoder.encode_descriptor(
                        "jemalloc_stats_mapped",
                        "Total number of bytes in active extents mapped by the allocator.",
                        Some(&Unit::Bytes),
                        mapped_bytes.metric_type(),
                    )?;
                    mapped_bytes.encode(mapped_bytes_metric)?;
                }

                if let Ok(metadata) = self
                    .metadata
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    let metadata_bytes = ConstGauge::new(metadata as u64);
                    let metadata_bytes_metric = encoder.encode_descriptor(
                        "jemalloc_stats_metadata",
                        "Total number of bytes dedicated to jemalloc metadata.",
                        Some(&Unit::Bytes),
                        metadata_bytes.metric_type(),
                    )?;
                    metadata_bytes.encode(metadata_bytes_metric)?;
                }

                if let Ok(resident) = self
                    .resident
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    let resident_bytes = ConstGauge::new(resident as u64);
                    let resident_bytes_metric = encoder.encode_descriptor(
                        "jemalloc_stats_resident",
                        "Total number of bytes in physically resident data pages mapped by the allocator.",
                        Some(&Unit::Bytes),
                        resident_bytes.metric_type(),
                    )?;
                    resident_bytes.encode(resident_bytes_metric)?;
                }

                if let Ok(retained) = self
                    .retained
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    let retained_bytes = ConstGauge::new(retained as u64);
                    let retained_bytes_metric = encoder.encode_descriptor(
                        "jemalloc_stats_retained",
                        "Total number of bytes in virtual memory mappings that were retained rather than being returned to the operating system via e.g. munmap(2).",
                        Some(&Unit::Bytes),
                        retained_bytes.metric_type(),
                    )?;
                    retained_bytes.encode(retained_bytes_metric)?;
                }

                Ok(())
            }
        }

        pub fn spawn_heap_stats_updates(_period: std::time::Duration, _srx: crate::signal::ShutdownRx) {
            match JemallocCollector::new() {
                Ok(collector) => {
                    crate::metrics::with_mut_registry(|mut registry| {
                        registry.register_collector(Box::new(collector));
                    });
                },
                Err(error) => tracing::error!(?error, "failed to create JemallocCollector"),
            }
        }
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
