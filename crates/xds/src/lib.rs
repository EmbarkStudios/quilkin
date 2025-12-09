/*
 * Copyright 2020 Google LLC
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

pub mod client;
pub mod config;
pub mod locality;
pub mod metrics;
pub mod net;
pub mod server;

pub use client::{Client, delta_subscribe};

pub use generated::envoy::{
    config::core::v3::{self as core, socket_address},
    config::listener::v3 as listener,
    service::discovery::v3 as discovery,
};
pub use generated::quilkin::config::v1alpha1 as proto;
pub use quilkin_proto::generated;

pub type Result<T, E = eyre::Error> = std::result::Result<T, E>;

const HTTP2_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);
const HTTP2_KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Checks if the status is a "broken pipe" eg. the other end of the connection
/// was terminated.. THIS IS NOT AN ERROR
fn is_broken_pipe(err_status: &tonic::Status) -> bool {
    use std::error::Error as _;
    let mut error = err_status.source();

    'lup: while let Some(err) = error {
        let io = 'block: {
            if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
                break 'block io_err;
            }

            // h2::Error do not expose std::io::Error with `source()`
            // https://github.com/hyperium/h2/pull/462
            if let Some(h2_err) = err.downcast_ref::<h2::Error>()
                && let Some(io_err) = h2_err.get_io()
            {
                break 'block io_err;
            }

            error = err.source();
            continue 'lup;
        };

        return io.kind() == std::io::ErrorKind::BrokenPipe;
    }

    false
}

pub type ShutdownSignal = tokio::sync::watch::Receiver<()>;
