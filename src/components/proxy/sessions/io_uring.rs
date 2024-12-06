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

use std::sync::Arc;

static SESSION_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl super::SessionPool {
    pub(super) fn spawn_session(
        self: Arc<Self>,
        raw_socket: socket2::Socket,
        port: u16,
        downstream_receiver: tokio::sync::mpsc::Receiver<crate::components::proxy::SendPacket>,
    ) -> Result<std::sync::mpsc::Receiver<()>, crate::components::proxy::PipelineError> {
        use crate::components::proxy::io_uring_shared;

        let pool = self;
        let id = SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _thread_span = uring_span!(tracing::debug_span!("session", id).or_current());

        let io_loop = io_uring_shared::IoUringLoop::new(
            2000,
            crate::net::DualStackLocalSocket::from_raw(raw_socket),
        )?;
        let buffer_pool = pool.buffer_pool.clone();
        let shutdown = pool.shutdown_rx.clone();

        io_loop.spawn(
            format!("session-{id}"),
            io_uring_shared::PacketProcessorCtx::SessionPool { pool, port },
            io_uring_shared::PacketReceiver::SessionPool(downstream_receiver),
            buffer_pool,
            shutdown,
        )
    }
}
