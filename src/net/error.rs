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

use std::{fmt, hash::Hash};

#[derive(Debug)]
pub enum PipelineError {
    NoUpstreamEndpoints,
    Filter(crate::filters::FilterError),
    Session(super::sessions::SessionError),
    Io(std::io::Error),
    DisallowedSourceIP(std::net::IpAddr),
}

impl PipelineError {
    /// We only want to mark potential I/O errors as errors, as they
    /// can indicate something wrong with the system, error variants
    /// from packets being bad aren't errors from quilkin's perspective.
    pub(crate) fn inc_system_errors_total(
        &self,
        direction: crate::metrics::Direction,
        asn_info: &crate::metrics::AsnInfo<'_>,
    ) {
        if matches!(
            self,
            PipelineError::Io(_) | PipelineError::Filter(crate::filters::FilterError::Io(_))
        ) {
            crate::metrics::errors_total(direction, &self.to_string(), asn_info).inc();
        }
    }

    pub fn discriminant(&self) -> &'static str {
        match self {
            Self::NoUpstreamEndpoints => "no upstream endpoints",
            Self::Filter(fe) => fe.discriminant(),
            Self::Session(_) => "session",
            Self::Io(_) => "io",
            Self::DisallowedSourceIP(_) => "disallowed source ip",
        }
    }
}

impl std::error::Error for PipelineError {}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUpstreamEndpoints => f.write_str("No upstream endpoints available"),
            Self::Filter(fe) => write!(f, "filter {fe}"),
            Self::Session(session) => write!(f, "session error: {session}"),
            Self::Io(io) => write!(f, "OS level error: {io}"),
            Self::DisallowedSourceIP(ip) => write!(f, "disallowed ip: {ip}"),
        }
    }
}

impl From<std::io::Error> for PipelineError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<super::sessions::SessionError> for PipelineError {
    fn from(value: super::sessions::SessionError) -> Self {
        Self::Session(value)
    }
}

impl PartialEq for PipelineError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NoUpstreamEndpoints, Self::NoUpstreamEndpoints) => true,
            (Self::Filter(fa), Self::Filter(fb)) => fa.eq(fb),
            (Self::Session(sa), Self::Session(sb)) => sa.eq(sb),
            (Self::Io(ia), Self::Io(ib)) => ia.kind().eq(&ib.kind()),
            (Self::DisallowedSourceIP(ia), Self::DisallowedSourceIP(ib)) => ia.eq(ib),
            _ => false,
        }
    }
}

impl Eq for PipelineError {}

impl Hash for PipelineError {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let disc = std::mem::discriminant(self);
        Hash::hash(&disc, state);

        match self {
            Self::Filter(fe) => Hash::hash(&fe, state),
            Self::Session(se) => Hash::hash(&se, state),
            Self::Io(io) => Hash::hash(&io.kind(), state),
            Self::NoUpstreamEndpoints => {}
            Self::DisallowedSourceIP(ip) => Hash::hash(&ip, state),
        }
    }
}
