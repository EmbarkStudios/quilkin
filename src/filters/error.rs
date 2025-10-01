/*
 * Copyright 2021 Google LLC
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

use prometheus::Error as MetricsError;

use crate::filters;
use std::fmt;

#[cfg(doc)]
use filters::{Filter, FilterFactory};

/// All possible errors that can be returned from [`Filter`] implementations
#[derive(Debug)]
pub enum FilterError {
    NoValueCaptured,
    TokenRouter(filters::token_router::RouterError),
    Io(std::io::Error),
    FirewallDenied,
    MatchNoMetadata,
    Dropped,
    RateLimitExceeded,
    Custom(&'static str),
}

impl FilterError {
    pub fn discriminant(&self) -> &'static str {
        match self {
            Self::NoValueCaptured => "filter::capture::no value captured",
            Self::TokenRouter(tr) => tr.discriminant(),
            Self::Io(..) => "filter::io",
            Self::FirewallDenied => "filter::firewall::denied",
            Self::MatchNoMetadata => "filter::match::no metadata",
            Self::Dropped => "filter::drop::dropped",
            Self::RateLimitExceeded => "filter::rate_limit::dropped",
            Self::Custom(custom) => custom,
        }
    }
}

impl std::error::Error for FilterError {}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoValueCaptured => f.write_str("no value captured"),
            Self::TokenRouter(tr) => write!(f, "{tr}"),
            Self::Io(io) => write!(f, "{io}"),
            Self::FirewallDenied => f.write_str("packet denied by firewall"),
            Self::MatchNoMetadata => f.write_str("expected metadata key for match not present"),
            Self::Dropped => f.write_str("dropped"),
            Self::RateLimitExceeded => f.write_str("rate limit exceeded"),
            Self::Custom(custom) => f.write_str(custom),
        }
    }
}

impl From<std::io::Error> for FilterError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl Eq for FilterError {}

impl PartialEq for FilterError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::FirewallDenied, Self::FirewallDenied)
            | (Self::MatchNoMetadata, Self::MatchNoMetadata)
            | (Self::Dropped, Self::Dropped)
            | (Self::RateLimitExceeded, Self::RateLimitExceeded)
            | (Self::NoValueCaptured, Self::NoValueCaptured) => true,
            (Self::TokenRouter(tra), Self::TokenRouter(trb)) => tra.eq(trb),
            (Self::Io(ia), Self::Io(ib)) => ia.kind().eq(&ib.kind()),
            (Self::Custom(a), Self::Custom(b)) => a == b,
            _ => false,
        }
    }
}

use std::hash::Hash;

impl Hash for FilterError {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let disc = std::mem::discriminant(self);
        Hash::hash(&disc, state);

        match self {
            Self::TokenRouter(re) => Hash::hash(&re, state),
            Self::Io(io) => Hash::hash(&io.kind(), state),
            Self::Custom(ce) => state.write(ce.as_bytes()),
            Self::NoValueCaptured
            | Self::FirewallDenied
            | Self::MatchNoMetadata
            | Self::Dropped
            | Self::RateLimitExceeded => {}
        }
    }
}

/// An error that occurred when attempting to create a [`Filter`] from
/// a [`FilterFactory`].
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum CreationError {
    #[error("filter `{}` not found", .0)]
    NotFound(String),
    #[error("Expected <{}> message, received <{}> ", expected, actual)]
    MismatchedTypes { expected: String, actual: String },
    #[error("filter `{}` requires configuration, but none provided", .0)]
    MissingConfig(&'static str),
    #[error("field `{}` is invalid, reason: {}", field, reason)]
    FieldInvalid { field: String, reason: String },
    #[error("Deserialization failed: {}", .0)]
    DeserializeFailed(String),
    #[error("Failed to initialize metrics: {}", .0)]
    InitializeMetricsFailed(String),
    #[error("Protobuf error: {}", .0)]
    ConvertProtoConfig(ConvertProtoConfigError),
    #[error("Infallible! This should never occur")]
    Infallible,
}

impl From<std::convert::Infallible> for CreationError {
    fn from(_: std::convert::Infallible) -> Self {
        Self::Infallible
    }
}

impl From<MetricsError> for CreationError {
    fn from(error: MetricsError) -> Self {
        Self::InitializeMetricsFailed(error.to_string())
    }
}

impl From<serde_yaml::Error> for CreationError {
    fn from(error: serde_yaml::Error) -> Self {
        Self::DeserializeFailed(error.to_string())
    }
}

impl From<serde_json::Error> for CreationError {
    fn from(error: serde_json::Error) -> Self {
        Self::DeserializeFailed(error.to_string())
    }
}

impl From<prost::EncodeError> for CreationError {
    fn from(error: prost::EncodeError) -> Self {
        Self::ConvertProtoConfig(ConvertProtoConfigError::new(error, None))
    }
}

impl From<prost::DecodeError> for CreationError {
    fn from(error: prost::DecodeError) -> Self {
        Self::ConvertProtoConfig(ConvertProtoConfigError::new(error, None))
    }
}

impl From<ConvertProtoConfigError> for CreationError {
    fn from(error: ConvertProtoConfigError) -> Self {
        Self::ConvertProtoConfig(error)
    }
}

/// An error representing failure to convert a filter's protobuf configuration
/// to its static representation.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "{}failed to convert protobuf config: {}",
    self.field.as_ref().map(|f| format!("Field `{f}`")).unwrap_or_default(),
    reason
)]
pub struct ConvertProtoConfigError {
    /// Reason for the failure.
    reason: String,
    /// Set if the failure is specific to a single field in the config.
    field: Option<String>,
}

impl ConvertProtoConfigError {
    pub fn new(reason: impl std::fmt::Display, field: Option<String>) -> Self {
        Self {
            reason: reason.to_string(),
            field,
        }
    }

    pub fn missing_field(field: &'static str) -> Self {
        Self {
            reason: format!("`{field}` is required but not found"),
            field: Some(field.into()),
        }
    }
}
