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
        use prometheus::core::{Collector, Desc};
        use tikv_jemalloc_ctl::stats;

        struct JemallocCollector {
            epoch: tikv_jemalloc_ctl::epoch_mib,
            active_desc: Desc,
            active: stats::active_mib,
            allocated_desc: Desc,
            allocated: stats::allocated_mib,
            mapped_desc: Desc,
            mapped: stats::mapped_mib,
            metadata_desc: Desc,
            metadata: stats::metadata_mib,
            resident_desc: Desc,
            resident: stats::resident_mib,
            retained_desc: Desc,
            retained: stats::retained_mib,
        }

        impl JemallocCollector {
            fn new() -> crate::Result<Self> {
                Ok(Self {
                    epoch: tikv_jemalloc_ctl::epoch::mib()?,
                    active_desc: Desc::new(
                        "jemalloc_stats_active_bytes".to_string(),
                        "Total number of bytes in active pages allocated by the application.".to_string(),
                        Vec::new(),
                        std::collections::HashMap::new(),
                    )?,
                    active: stats::active::mib()?,
                    allocated_desc: Desc::new(
                        "jemalloc_stats_allocated_bytes".to_string(),
                        "Total number of bytes allocated by the application.".to_string(),
                        Vec::new(),
                        std::collections::HashMap::new(),
                    )?,
                    allocated: stats::allocated::mib()?,
                    mapped_desc: Desc::new(
                        "jemalloc_stats_mapped_bytes".to_string(),
                        "Total number of bytes in active extents mapped by the allocator.".to_string(),
                        Vec::new(),
                        std::collections::HashMap::new(),
                    )?,
                    mapped: stats::mapped::mib()?,
                    metadata_desc: Desc::new(
                        "jemalloc_stats_metadata_bytes".to_string(),
                        "Total number of bytes dedicated to jemalloc metadata.".to_string(),
                        Vec::new(),
                        std::collections::HashMap::new(),
                    )?,
                    metadata: stats::metadata::mib()?,
                    resident_desc: Desc::new(
                        "jemalloc_stats_resident_bytes".to_string(),
                        "Total number of bytes in physically resident data pages mapped by the allocator.".to_string(),
                        Vec::new(),
                        std::collections::HashMap::new(),
                    )?,
                    resident: stats::resident::mib()?,
                    retained_desc: Desc::new(
                        "jemalloc_stats_retained_bytes".to_string(),
                        "Total number of bytes in virtual memory mappings that were retained rather than being returned to the operating system via e.g. munmap(2).".to_string(),
                        Vec::new(),
                        std::collections::HashMap::new(),
                    )?,
                    retained: stats::retained::mib()?,
                })
            }
        }

        fn bytes_gauge_into_metric_family(
            bytes: usize,
            desc: &prometheus::core::Desc,
        ) -> prometheus::proto::MetricFamily {
            let mut gauge = prometheus::proto::Gauge::default();
            gauge.set_value(bytes as f64);
            let mut mf = prometheus::proto::MetricFamily::default();
            mf.set_name(desc.fq_name.clone());
            mf.set_help(desc.help.clone());
            mf.set_field_type(prometheus::proto::MetricType::GAUGE);
            mf.set_metric(vec![prometheus::proto::Metric::from_gauge(gauge)]);
            mf
        }

        impl Collector for JemallocCollector {
            fn desc(&self) -> Vec<&Desc> {
                vec![
                    &self.active_desc,
                    &self.allocated_desc,
                    &self.mapped_desc,
                    &self.metadata_desc,
                    &self.resident_desc,
                    &self.retained_desc,
                ]
            }

            fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
                // many statistics are cached and only updated when the epoch is advanced.
                if let Err(error) = self.epoch.advance() {
                    tracing::warn!(?error, "failed to advance epoch");
                }

                let mut results = Vec::with_capacity(6);

                if let Ok(active) = self
                    .active
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    results.push(bytes_gauge_into_metric_family(active, &self.active_desc));
                }

                if let Ok(allocated) = self
                    .allocated
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    results.push(bytes_gauge_into_metric_family(
                        allocated,
                        &self.allocated_desc,
                    ));
                }

                if let Ok(mapped) = self
                    .mapped
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    results.push(bytes_gauge_into_metric_family(mapped, &self.mapped_desc));
                }

                if let Ok(metadata) = self
                    .metadata
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    results.push(bytes_gauge_into_metric_family(
                        metadata,
                        &self.metadata_desc,
                    ));
                }

                if let Ok(resident) = self
                    .resident
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    results.push(bytes_gauge_into_metric_family(
                        resident,
                        &self.resident_desc,
                    ));
                }

                if let Ok(retained) = self
                    .retained
                    .read()
                    .inspect_err(|error| tracing::warn!(?error, "failed to collect jemalloc metric"))
                {
                    results.push(bytes_gauge_into_metric_family(
                        retained,
                        &self.retained_desc,
                    ));
                }

                results
            }
        }

        pub fn spawn_heap_stats_updates(_period: std::time::Duration, _srx: crate::signal::ShutdownRx) {
            match JemallocCollector::new() {
                Ok(collector) => {
                    if let Err(error) = crate::metrics::registry().register(Box::new(collector)) {
                        tracing::error!(?error, "failed to register JemallocCollector");
                    }
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
