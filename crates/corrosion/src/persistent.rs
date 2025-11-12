//! Implementation for a persistent connection between a client (agent) and
//! server (relay).

pub mod client;
mod error;
pub mod executor;
pub mod proto;
pub mod server;

use bytes::{BufMut, BytesMut};
pub use corro_api_types::{ExecResponse, ExecResult};
use quilkin_types::{Endpoint, IcaoCode, TokenSet};
use serde::{Deserialize, Serialize};
