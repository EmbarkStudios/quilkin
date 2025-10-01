// SPDX-FileCopyrightText: 2020 Tyler Neely, David Rusu
// SPDX-License-Identifier: Apache-2.0

//! The code in this module is copied from <https://github.com/rust-crdt/rust-crdt>
//! and has some modifications

mod ctx;
pub mod dot;
pub mod map;
pub mod orswot;
mod traits;
pub mod vclock;

pub use dot::{Dot, DotRange};
pub use map::Map;
pub use orswot::Orswot;
pub use traits::{Cmrdt, ContentEqual, Cvrdt, ResetRemove};
pub use vclock::VClock;
