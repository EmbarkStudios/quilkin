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

#[allow(clippy::exit)]
fn main() {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut block = std::mem::zeroed();
        libc::sigemptyset(&mut block);
        libc::sigaddset(&mut block, libc::SIGTERM);
        libc::sigaddset(&mut block, libc::SIGINT);
        libc::sigprocmask(libc::SIG_BLOCK, &block, std::ptr::null_mut());
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("tokio-main-{id}")
        })
        .build()
        .unwrap()
        .block_on(async {
            // Unwrap is safe here as it will only fail if called more than once.
            stable_eyre::install().unwrap();
            rustls::crypto::ring::default_provider()
                .install_default()
                .unwrap();

            match <quilkin::Cli as clap::Parser>::parse().drive().await {
                Ok(()) => {
                    std::process::exit(0);
                }
                Err(error) => {
                    tracing::error!(?error, "fatal error");

                    std::process::exit(-1)
                }
            }
        });
}
