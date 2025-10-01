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

//! Types representing where the data is the sent.

pub(crate) mod address;
pub mod metadata;

use crate::net::cluster::proto;
use eyre::ContextCompat;
use serde::{Deserialize, Serialize};

pub use self::{
    address::{AddressKind, EndpointAddress},
    metadata::DynamicMetadata,
};

pub use quilkin_xds::locality::Locality;

pub type EndpointMetadata = metadata::MetadataView<Metadata>;

/// A destination endpoint with any associated metadata.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Eq, schemars::JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    #[schemars(with = "String")]
    pub address: EndpointAddress,
    #[serde(default)]
    pub metadata: EndpointMetadata,
}

impl Endpoint {
    /// Creates a new [`Endpoint`] with no metadata.
    pub fn new(address: EndpointAddress) -> Self {
        Self {
            address,
            ..<_>::default()
        }
    }

    /// Creates a new [`Endpoint`] with the specified `metadata`.
    pub fn with_metadata(address: EndpointAddress, metadata: impl Into<EndpointMetadata>) -> Self {
        Self {
            address,
            metadata: metadata.into(),
            ..<_>::default()
        }
    }

    #[inline]
    pub fn from_proto(proto: proto::Endpoint) -> eyre::Result<Self> {
        let host: AddressKind = if let Some(host) = proto.host2 {
            match host.inner.context("should be unreachable")? {
                proto::host::Inner::Name(name) => AddressKind::Name(name),
                proto::host::Inner::Ipv4(v4) => {
                    AddressKind::Ip(std::net::Ipv4Addr::from(v4).into())
                }
                proto::host::Inner::Ipv6(v6) => AddressKind::Ip(
                    std::net::Ipv6Addr::from(((v6.first as u128) << 64) | v6.second as u128).into(),
                ),
            }
        } else {
            proto.host.parse()?
        };

        Ok(Self {
            address: (host, proto.port as u16).into(),
            metadata: proto
                .metadata
                .map(TryFrom::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    #[inline]
    pub fn into_proto(self) -> proto::Endpoint {
        let host = match self.address.host {
            AddressKind::Name(name) => proto::host::Inner::Name(name),
            AddressKind::Ip(ip) => match ip {
                std::net::IpAddr::V4(v4) => {
                    proto::host::Inner::Ipv4(u32::from_be_bytes(v4.octets()))
                }
                std::net::IpAddr::V6(v6) => {
                    let ip = u128::from_be_bytes(v6.octets());

                    let first = ((ip >> 64) & 0xffffffffffffffff) as u64;
                    let second = (ip & 0xffffffffffffffff) as u64;

                    proto::host::Inner::Ipv6(proto::Ipv6 { first, second })
                }
            },
        };

        proto::Endpoint {
            host: String::new(),
            port: self.address.port.into(),
            metadata: Some(self.metadata.into()),
            host2: Some(proto::Host { inner: Some(host) }),
        }
    }
}

impl Default for Endpoint {
    fn default() -> Self {
        Self {
            address: EndpointAddress::UNSPECIFIED,
            metadata: <_>::default(),
        }
    }
}

impl std::str::FromStr for Endpoint {
    type Err = <EndpointAddress as std::str::FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            address: s.parse()?,
            ..Self::default()
        })
    }
}

impl From<Endpoint> for proto::Endpoint {
    fn from(endpoint: Endpoint) -> Self {
        Self {
            host: endpoint.address.host.to_string(),
            port: endpoint.address.port.into(),
            metadata: Some(endpoint.metadata.into()),
            host2: None,
        }
    }
}

impl TryFrom<proto::Endpoint> for Endpoint {
    type Error = eyre::Error;

    fn try_from(endpoint: proto::Endpoint) -> Result<Self, Self::Error> {
        let host: address::AddressKind = endpoint.host.parse()?;
        if endpoint.port > u16::MAX as u32 {
            return Err(eyre::eyre!("invalid endpoint port"));
        }

        Ok(Self {
            address: (host, endpoint.port as u16).into(),
            metadata: endpoint
                .metadata
                .map(TryFrom::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl std::cmp::PartialEq<EndpointAddress> for Endpoint {
    fn eq(&self, rhs: &EndpointAddress) -> bool {
        self.address == *rhs
    }
}

impl<T: Into<EndpointAddress>> From<T> for Endpoint {
    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}

impl Ord for Endpoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.address.cmp(&other.address)
    }
}

impl PartialOrd for Endpoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Endpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.metadata.known.tokens.hash(state);
    }
}

/// Metadata specific to endpoints.
#[derive(
    Default, Debug, Deserialize, Serialize, PartialEq, Clone, PartialOrd, Eq, schemars::JsonSchema,
)]
pub struct Metadata {
    pub tokens: quilkin_types::TokenSet,
}

impl From<Metadata> for crate::net::endpoint::metadata::MetadataView<Metadata> {
    fn from(metadata: Metadata) -> Self {
        Self {
            known: metadata,
            ..<_>::default()
        }
    }
}

impl From<Metadata> for prost_types::Struct {
    fn from(metadata: Metadata) -> Self {
        let tokens = prost_types::Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: metadata
                        .tokens
                        .into_iter()
                        .map(crate::codec::base64::encode)
                        .map(prost_types::value::Kind::StringValue)
                        .map(|k| prost_types::Value { kind: Some(k) })
                        .collect(),
                },
            )),
        };

        Self {
            fields: <_>::from([("tokens".into(), tokens)]),
        }
    }
}

impl std::convert::TryFrom<prost_types::Struct> for Metadata {
    type Error = MetadataError;

    fn try_from(mut value: prost_types::Struct) -> Result<Self, Self::Error> {
        use prost_types::value::Kind;
        const TOKENS: &str = "tokens";

        let tokens =
            if let Some(kind) = value.fields.remove(TOKENS).and_then(|v| v.kind) {
                match kind {
                    Kind::ListValue(list) => list
                        .values
                        .into_iter()
                        .filter_map(|v| v.kind)
                        .map(|kind| {
                            if let Kind::StringValue(string) = kind {
                                crate::codec::base64::decode(string)
                                    .map_err(MetadataError::InvalidBase64)
                            } else {
                                Err(MetadataError::InvalidType {
                                    key: "quilkin.dev.tokens",
                                    expected: "base64 string",
                                })
                            }
                        })
                        .collect::<Result<_, _>>()?,
                    Kind::StringValue(string) => <_>::from([crate::codec::base64::decode(string)
                        .map_err(MetadataError::InvalidBase64)?]),
                    _ => return Err(MetadataError::MissingKey(TOKENS)),
                }
            } else {
                <_>::default()
            };

        Ok(Self { tokens })
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum MetadataError {
    #[error("Invalid bas64 encoded token: `{0}`.")]
    InvalidBase64(base64::DecodeError),
    #[error("Missing required key `{0}`.")]
    MissingKey(&'static str),
    #[error("Invalid type ({expected}) given for `{key}`.")]
    InvalidType {
        key: &'static str,
        expected: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_metadata() {
        let metadata = Metadata {
            tokens: vec!["Man".into()].into_iter().collect(),
        };

        assert_eq!(
            serde_json::to_value(EndpointMetadata::from(metadata)).unwrap(),
            serde_json::json!({
                crate::net::endpoint::metadata::KEY: {
                    "tokens": ["TWFu"],
                }
            })
        );
    }

    #[test]
    fn parse_dns_endpoints() {
        let localhost = "address: localhost:80";
        serde_yaml::from_str::<Endpoint>(localhost).unwrap();
    }

    #[test]
    fn yaml_parse_invalid_endpoint_metadata() {
        let not_a_list = "
 quilkin.dev:
     tokens: OGdqM3YyaQ==
 ";
        let not_a_string_value = "
 quilkin.dev:
     tokens:
         - map:
           a: b
 ";
        let not_a_base64_string = "
 quilkin.dev:
     tokens:
         - OGdqM3YyaQ== #8gj3v2i
         - iix
 ";
        for yaml in &[not_a_list, not_a_string_value, not_a_base64_string] {
            serde_yaml::from_str::<EndpointMetadata>(yaml).unwrap_err();
        }
    }

    // Sanity check conversion between endpoint <-> proto works
    #[test]
    fn endpoint_proto_conversion() {
        let first = Endpoint::new(EndpointAddress {
            host: AddressKind::Ip(std::net::IpAddr::V6(std::net::Ipv6Addr::new(
                0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0xab, 0xcd,
            ))),
            port: 2001,
        });

        let expected = first.clone();
        let proto = first.into_proto();
        let actual = Endpoint::from_proto(proto).unwrap();

        assert_eq!(expected, actual);
    }
}
