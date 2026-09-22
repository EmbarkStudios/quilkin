/*
 * Copyright 2021 Google LLC
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

use std::{
    panic,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};

#[derive(Clone)]
pub struct Health {
    healthy: Arc<AtomicBool>,
    shutdown: quilkin_graceful::RootToken,
}

impl Health {
    /// Creates a [`Self`] that installs a panic hook, marking the instance as unhealthy and initiating shutdown if
    /// a panic is triggered
    pub fn new(shutdown: quilkin_graceful::RootToken) -> Self {
        let health = Self {
            healthy: Arc::new(AtomicBool::new(true)),
            shutdown,
        };

        let healthy = health.healthy.clone();
        let shutdown = health.shutdown.clone();
        let default_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            tracing::error!(%panic_info, "Panic has occurred. Moving to Unhealthy");
            healthy.swap(false, Relaxed);
            shutdown.cancel();
            default_hook(panic_info);
        }));

        health
    }

    /// returns a HTTP 200 response if the proxy is healthy.
    pub fn check_liveness(&self) -> bool {
        self.healthy.load(Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_hook() {
        let token = quilkin_graceful::root();
        let health = Health::new(token.clone());

        assert!(health.check_liveness());

        let _unused = std::panic::catch_unwind(|| {
            panic!("oh no!");
        });

        assert!(!health.check_liveness());
        assert!(token.is_cancelled());
    }
}
