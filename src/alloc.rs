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

mod metrics;
pub use metrics::spawn_heap_stats_updates;

cfg_if::cfg_if! {
    if #[cfg(feature = "jemalloc")] {
        #[cfg(not(target_env = "msvc"))]
        #[global_allocator]
        static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

        #[allow(non_upper_case_globals)]
        #[unsafe(export_name = "malloc_conf")]
        pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
    } else if #[cfg(feature = "heap-stats")] {
        mod tracking;
    } else if #[cfg(feature = "mimalloc")] {
        #[global_allocator]
        static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
    } else {
    }
}
