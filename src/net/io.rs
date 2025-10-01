/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/// Allows creation of spans only when `debug_assertions` are enabled, to avoid
/// hitting the cap of 4096 threads that is unconfigurable in
/// `tracing_subscriber` -> `sharded_slab` for span ids
macro_rules! uring_span {
    ($span:expr_2021) => {{
        cfg_if::cfg_if! {
            if #[cfg(debug_assertions)] {
                Some($span)
            } else {
                Option::<tracing::Span>::None
            }
        }
    }};
}

use std::sync::Arc;

use crate::Config;

#[cfg(target_os = "linux")]
pub mod completion;
pub mod nic;
#[cfg(not(target_os = "linux"))]
pub mod poll;

/// Represents the required arguments to run a worker task that
/// processes packets received downstream.
pub struct Listener {
    /// ID of the worker.
    pub worker_id: usize,
    pub port: u16,
    pub config: Arc<Config>,
    pub sessions: Arc<crate::net::sessions::SessionPool>,
    pub buffer_pool: Arc<crate::collections::BufferPool>,
}
