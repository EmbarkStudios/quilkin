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

//! Useful filters for common operations.

pub use capture_bytes::CaptureBytesFactory;
pub use compress::CompressFactory;
pub use concatenate_bytes::ConcatBytesFactory;
pub use debug::DebugFactory;
pub use load_balancer::LoadBalancerFilterFactory;
pub use local_rate_limit::RateLimitFilterFactory;
pub use token_router::TokenRouterFactory;

mod capture_bytes;
mod compress;
mod concatenate_bytes;
mod debug;
mod load_balancer;
mod local_rate_limit;
mod token_router;

pub const CAPTURED_BYTES: &str = "quilkin.dev/captured_bytes";
