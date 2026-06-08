/*
 * Copyright 2020 Google LLC
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

use serde::{Deserialize, Serialize};

use crate::{
    filters::{capture::CAPTURED_BYTES, prelude::*},
    net::endpoint::metadata,
};

use quilkin_xds::generated::quilkin::filters::token_router::v1alpha1 as proto;

/// Filter that only allows packets to be passed to Endpoints that have a matching
/// `connection_id` to the token stored in the Filter's dynamic metadata.
#[derive(Default)]
pub struct TokenRouter {
    config: Config,
}

impl TokenRouter {
    pub fn testing(config: Option<Config>) -> Self {
        Self {
            config: config.unwrap_or_default(),
        }
    }
}

impl StaticFilter for TokenRouter {
    const NAME: &'static str = "quilkin.filters.token_router.v1alpha1.TokenRouter";
    type Configuration = Config;
    type BinaryConfiguration = proto::TokenRouter;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Self {
            config: config.unwrap_or_default(),
        })
    }
}

impl Filter for TokenRouter {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        match ctx.metadata.get(&self.config.metadata_key) {
            Some(metadata::Value::Bytes(token)) => {
                let tok = crate::net::cluster::Token::new(token);

                ctx.endpoints.addresses_for_token(tok, ctx.destinations);

                if ctx.destinations.is_empty() {
                    Err(FilterError::TokenRouter(RouterError::NoEndpointMatch {
                        token: token.clone(),
                    }))
                } else {
                    Ok(())
                }
            }
            Some(_value) => unreachable!(
                "this means the capture filter has regressed, it only ever captures byte slices"
            ),
            None => Err(FilterError::TokenRouter(RouterError::NoTokenFound)),
        }
    }
}

pub struct HashedTokenRouter(TokenRouter);

impl StaticFilter for HashedTokenRouter {
    const NAME: &'static str = "quilkin.filters.token_router.v1alpha1.HashedTokenRouter";
    type Configuration = Config;
    type BinaryConfiguration = proto::TokenRouter;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Self(TokenRouter {
            config: config.unwrap_or_default(),
        }))
    }
}

impl Filter for HashedTokenRouter {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        self.0.read(ctx)
    }
}

pub enum RouterError {
    NoTokenFound,
    NoEndpointMatch { token: bytes::Bytes },
}

impl RouterError {
    #[inline]
    pub fn discriminant(&self) -> &'static str {
        match self {
            Self::NoEndpointMatch { .. } => "filter::token_router::no endpoint match",
            Self::NoTokenFound => "filter::token_router::no token found",
        }
    }
}

use std::fmt;

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEndpointMatch { token } => {
                write!(
                    f,
                    "no endpoint matched token `{}`",
                    base64::display::Base64Display::new(
                        token,
                        &base64::engine::general_purpose::STANDARD
                    )
                )
            }
            Self::NoTokenFound => f.write_str("routing token not captured"),
        }
    }
}

impl fmt::Debug for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEndpointMatch { .. } => f.write_str("no endpoint matched routing token"),
            Self::NoTokenFound => f.write_str("routing token not captured"),
        }
    }
}

impl Eq for RouterError {}

impl PartialEq for RouterError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NoTokenFound, Self::NoTokenFound) => true,
            (Self::NoEndpointMatch { token: a }, Self::NoEndpointMatch { token: b }) => a == b,
            _ => false,
        }
    }
}

impl std::hash::Hash for RouterError {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&std::mem::discriminant(self), state);

        match self {
            Self::NoEndpointMatch { token } => state.write(token),
            Self::NoTokenFound => {}
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    /// the key to use when retrieving the token from the Filter's dynamic metadata
    #[serde(rename = "metadataKey", default = "default_metadata_key")]
    pub metadata_key: metadata::Key,
}

/// Default value for [`Config::metadata_key`]
fn default_metadata_key() -> metadata::Key {
    metadata::Key::from_static(CAPTURED_BYTES)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            metadata_key: default_metadata_key(),
        }
    }
}

impl From<Config> for proto::TokenRouter {
    fn from(config: Config) -> Self {
        Self {
            metadata_key: Some(config.metadata_key.to_string()),
        }
    }
}

impl TryFrom<proto::TokenRouter> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::TokenRouter) -> Result<Self, Self::Error> {
        Ok(Self {
            metadata_key: p
                .metadata_key
                .map_or_else(default_metadata_key, metadata::Key::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        net::endpoint::{Endpoint, Metadata, metadata::Value},
        test::assert_write_no_change,
    };

    use super::*;

    const TOKEN_KEY: &str = "TOKEN";

    #[test]
    fn convert_proto_config() {
        let test_cases = vec![
            (
                "should succeed when all valid values are provided",
                proto::TokenRouter {
                    metadata_key: Some("foobar".into()),
                },
                Some(Config {
                    metadata_key: "foobar".into(),
                }),
            ),
            (
                "should use correct default values",
                proto::TokenRouter { metadata_key: None },
                Some(Config {
                    metadata_key: default_metadata_key(),
                }),
            ),
        ];
        for (name, proto_config, expected) in test_cases {
            let result = Config::try_from(proto_config);
            assert_eq!(
                result.is_err(),
                expected.is_none(),
                "{}: error expectation does not match",
                name
            );
            if let Some(expected) = expected {
                assert_eq!(expected, result.unwrap(), "{name}");
            }
        }
    }

    #[tokio::test]
    async fn factory_custom_tokens() {
        let filter = TokenRouter::from_config(
            Config {
                metadata_key: TOKEN_KEY.into(),
            }
            .into(),
        );
        let mut dest = Vec::new();
        with_ctx(&mut dest, |mut ctx| {
            ctx.metadata
                .insert(TOKEN_KEY.into(), Value::Bytes(b"123".to_vec().into()));
            assert_read(&filter, ctx);
        });
    }

    #[tokio::test]
    async fn factory_empty_config() {
        let filter = TokenRouter::from_config(None);
        let mut dest = Vec::new();
        with_ctx(&mut dest, |mut ctx| {
            ctx.metadata
                .insert(CAPTURED_BYTES.into(), Value::Bytes(b"123".to_vec().into()));
            assert_read(&filter, ctx);
        });
    }

    #[tokio::test]
    async fn downstream_receive() {
        // valid key
        let config = Config {
            metadata_key: CAPTURED_BYTES.into(),
        };
        let filter = TokenRouter::from_config(config.into());
        let mut dest = Vec::new();

        with_ctx(&mut dest, |mut ctx| {
            ctx.metadata
                .insert(CAPTURED_BYTES.into(), Value::Bytes(b"123".to_vec().into()));
            assert_read(&filter, ctx);
        });
        dest.clear();

        // invalid key
        with_ctx(&mut dest, |mut ctx| {
            ctx.metadata
                .insert(CAPTURED_BYTES.into(), Value::Bytes(b"567".to_vec().into()));

            assert!(filter.read(&mut ctx).is_err());
        });
        dest.clear();

        // no key
        with_ctx(&mut dest, |mut ctx| {
            assert!(filter.read(&mut ctx).is_err());
        });
    }

    #[tokio::test]
    async fn write() {
        let config = Config {
            metadata_key: CAPTURED_BYTES.into(),
        };
        let filter = TokenRouter::from_config(config.into());
        assert_write_no_change(&filter);
    }

    fn with_ctx(
        dest: &mut Vec<crate::net::EndpointAddress>,
        test: impl FnOnce(ReadContext<'_, bytes::BytesMut>),
    ) {
        let endpoint1 = Endpoint::with_metadata(
            "127.0.0.1:80".parse().unwrap(),
            Metadata {
                tokens: vec!["123".into()].into_iter().collect(),
            },
        );
        let endpoint2 = Endpoint::with_metadata(
            "127.0.0.1:90".parse().unwrap(),
            Metadata {
                tokens: vec!["456".into()].into_iter().collect(),
            },
        );

        let endpoints = crate::net::cluster::ClusterMap::default();
        endpoints.insert_default([endpoint1, endpoint2].into());
        test(ReadContext::new(
            &endpoints,
            "127.0.0.1:100".parse().unwrap(),
            crate::test::alloc_buffer(b"hello"),
            dest,
        ));
    }

    fn assert_read<F, P>(filter: &F, mut ctx: ReadContext<'_, P>)
    where
        F: Filter + ?Sized,
        P: PacketMut,
    {
        filter.read(&mut ctx).unwrap();
        assert_eq!(b"hello", ctx.contents.as_slice());
    }
}
