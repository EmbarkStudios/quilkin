//! Implementation for a persistent connection between a client (agent) and
//! server (relay).

pub mod client;
mod error;
pub mod executor;
pub mod proto;
pub mod server;

pub use corro_api_types::{ExecResponse, ExecResult};
pub use error::ErrorCode;
