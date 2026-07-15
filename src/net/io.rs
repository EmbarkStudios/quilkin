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
        cfg_select! {
            debug_assertions => {
                Some($span)
            }
            _ => {
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
pub mod poll;

/// Runtime-selected UDP I/O backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum UdpBackend {
    /// Auto-select: kernel (XDP) -> completion (io-uring) -> poll (epoll).
    #[default]
    Auto,
    /// Tokio epoll — available on all platforms.
    Poll,
    /// io-uring completion I/O (Linux only).
    #[cfg(target_os = "linux")]
    Completion,
    /// XDP kernel-bypass (Linux only).
    #[cfg(target_os = "linux")]
    Kernel,
}

impl UdpBackend {
    /// Resolve `Auto` to the best concrete backend. Never returns `Auto`.
    pub fn resolve(self) -> Self {
        match self {
            Self::Auto => Self::probe(),
            other => other,
        }
    }

    #[cfg(target_os = "linux")]
    fn probe() -> Self {
        if nic::xdp::is_available() {
            Self::Kernel
        } else {
            Self::probe_user_space()
        }
    }

    /// Probe for the best user-space backend (io-uring or epoll), skipping XDP.
    ///
    /// Use this when a socket-based listener is required regardless of XDP availability.
    #[cfg(target_os = "linux")]
    pub fn probe_user_space() -> Self {
        if io_uring::IoUring::new(2).is_ok() {
            Self::Completion
        } else {
            Self::Poll
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn probe() -> Self {
        Self::Poll
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Poll => "poll",
            #[cfg(target_os = "linux")]
            Self::Completion => "completion",
            #[cfg(target_os = "linux")]
            Self::Kernel => "kernel",
        }
    }
}

impl std::fmt::Display for UdpBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Represents the required arguments to run a worker task that
/// processes packets received downstream.
pub struct Listener {
    /// ID of the worker.
    pub worker_id: usize,
    pub port: u16,
    pub config: Arc<Config>,
    pub sessions: Arc<crate::net::sessions::SessionPool>,
    pub backend: UdpBackend,
}

impl Listener {
    pub fn spawn_io_loop(
        self,
        queue: crate::net::PacketQueue,
        fc: crate::config::filter::CachedFilterChain,
    ) -> eyre::Result<()> {
        match self.backend {
            UdpBackend::Poll => poll::tokio::spawn_listener(self, queue, fc),
            #[cfg(target_os = "linux")]
            UdpBackend::Completion => completion::io_uring::spawn_listener(self, queue, fc),
            #[cfg(target_os = "linux")]
            UdpBackend::Kernel => {
                unreachable!("XDP runs via spawn_xdp and never reaches the queue-based listener")
            }
            UdpBackend::Auto => unreachable!("UdpBackend must be resolved before spawning"),
        }
    }
}
