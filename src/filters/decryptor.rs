/*
 * Copyright 2024 Google LLC
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

use chacha20::cipher::*;
use serde::{Deserialize, Serialize};

use crate::{
    filters::{capture::CAPTURED_BYTES, prelude::*},
    net::endpoint::metadata,
};

use quilkin_xds::generated::quilkin::filters::decryptor::v1alpha1 as proto;

/// The default key under which the [`Decryptor`] filter reads the nonce.
/// - **Type** `Vec<u8>`
pub const NONCE_KEY: &str = "quilkin.dev/nonce";

/// Filter that only allows packets to be passed to Endpoints that have a matching
/// connection_id to the token stored in the Filter's dynamic metadata.
pub struct Decryptor {
    config: Config,
}

impl Decryptor {
    fn decode_chacha20(&self, nonce: [u8; 12], data: &mut [u8]) {
        let mut cipher = chacha20::ChaCha20::new(&self.config.key.into(), &nonce.into());
        cipher.apply_keystream(data);
    }

    fn apply_mode(&self, data: &[u8], ctx: &mut ReadContext) -> Result<(), FilterError> {
        match self.config.mode {
            Mode::Destination => match data.len() {
                6 => {
                    let ip: [u8; 4] = data[..4].try_into().unwrap();
                    let port = u16::from_be_bytes(<[u8; 2]>::try_from(&data[4..]).unwrap());

                    ctx.destinations = vec![(ip, port).into()];
                    Ok(())
                }
                18 => todo!(),
                _ => Err(FilterError::Custom(
                    "Invalid decoded data length, must be `6` or `8` bytes.",
                )),
            },
        }
    }
}

impl StaticFilter for Decryptor {
    const NAME: &'static str = "quilkin.filters.token_router.v1alpha1.Decryptor";
    type Configuration = Config;
    type BinaryConfiguration = proto::Decryptor;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Self {
            config: Self::ensure_config_exists(config)?,
        })
    }
}

impl Filter for Decryptor {
    fn read(&self, ctx: &mut ReadContext) -> Result<(), FilterError> {
        match (
            ctx.metadata.get(&self.config.data_key),
            ctx.metadata.get(&self.config.nonce_key),
        ) {
            (Some(metadata::Value::Bytes(data)), Some(metadata::Value::Bytes(nonce))) => {
                let nonce = <[u8; 12]>::try_from(&**nonce)
                    .map_err(|_| FilterError::Custom("Expected 12 byte nonce"))?;
                let mut data = Vec::from(&**data);

                self.decode_chacha20(nonce, &mut data);
                self.apply_mode(&data, ctx)
            }
            (Some(metadata::Value::Bytes(_)), Some(_)) => {
                Err(FilterError::Custom("expected `bytes` value in nonce key"))
            }
            (Some(_), Some(metadata::Value::Bytes(_))) => {
                Err(FilterError::Custom("expected `bytes` value in data key"))
            }
            (Some(_), Some(_)) => Err(FilterError::Custom(
                "expected `bytes` value in data and nonce key",
            )),
            (Some(_), None) => Err(FilterError::Custom("Nonce key is missing")),
            (None, Some(_)) => Err(FilterError::Custom("Data key is missing")),
            (None, None) => Err(FilterError::Custom("Nonce and data key is missing")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, schemars::JsonSchema)]
pub struct Config {
    #[serde(deserialize_with = "deserialize", serialize_with = "serialize")]
    #[schemars(with = "String")]
    pub key: [u8; 32],
    /// the key to use when retrieving the data from the Filter's dynamic metadata
    #[serde(rename = "metadataKey", default = "default_data_key")]
    pub data_key: metadata::Key,
    #[serde(rename = "metadataKey", default = "default_nonce_key")]
    pub nonce_key: metadata::Key,
    pub mode: Mode,
}

/// Default value for [`Config::data_key`]
fn default_data_key() -> metadata::Key {
    metadata::Key::from_static(CAPTURED_BYTES)
}

/// Default value for [`Config::nonce_key`]
fn default_nonce_key() -> metadata::Key {
    metadata::Key::from_static(NONCE_KEY)
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, schemars::JsonSchema)]
pub enum Mode {
    /// Value is expected to be a IP:port pair to be used for setting the destination.
    Destination,
}

impl From<Mode> for proto::decryptor::Mode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Destination => Self::Destination,
        }
    }
}

impl From<proto::decryptor::Mode> for Mode {
    fn from(mode: proto::decryptor::Mode) -> Self {
        match mode {
            proto::decryptor::Mode::Destination => Self::Destination,
        }
    }
}

impl TryFrom<i32> for Mode {
    type Error = ConvertProtoConfigError;
    fn try_from(mode: i32) -> Result<Self, Self::Error> {
        match mode {
            0 => Ok(Self::Destination),
            _ => Err(ConvertProtoConfigError::missing_field("mode")),
        }
    }
}

fn deserialize<'de, D>(de: D) -> Result<[u8; 32], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let string = String::deserialize(de)?;

    crate::codec::base64::decode(string)
        .map_err(serde::de::Error::custom)?
        .try_into()
        .map_err(|_| serde::de::Error::custom("invalid key, expected 32 bytes"))
}

fn serialize<S>(value: &[u8; 32], ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    crate::codec::base64::encode(value).serialize(ser)
}

impl From<Config> for proto::Decryptor {
    fn from(config: Config) -> Self {
        Self {
            key: config.key.into(),
            mode: proto::decryptor::Mode::from(config.mode).into(),
            data_key: Some(config.data_key.to_string()),
            nonce_key: Some(config.nonce_key.to_string()),
        }
    }
}

impl TryFrom<proto::Decryptor> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::Decryptor) -> Result<Self, Self::Error> {
        Ok(Self {
            key: p.key.try_into().map_err(|_| {
                ConvertProtoConfigError::new(
                    "invalid key, expected 32 bytes",
                    Some("private_key".into()),
                )
            })?,
            mode: p.mode.try_into()?,
            data_key: p
                .data_key
                .map(metadata::Key::new)
                .unwrap_or_else(default_data_key),
            nonce_key: p
                .nonce_key
                .map(metadata::Key::new)
                .unwrap_or_else(default_nonce_key),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4() {
        let pool = std::sync::Arc::new(crate::pool::BufferPool::new(1, 5));

        let endpoints = crate::net::cluster::ClusterMap::default();
        let mut ctx = ReadContext::new(
            endpoints.into(),
            "0.0.0.0:0".parse().unwrap(),
            pool.alloc_slice(b"hello"),
        );

        let key = [0x42u8; 32];
        let nonce = [0x22u8; 12];
        let ip: [u8; 4] = [127, 0, 0, 1];
        let port: [u8; 2] = 8080u16.to_be_bytes();

        let mut data = Vec::new();
        data.extend(ip);
        data.extend(port);

        let mut cipher = chacha20::ChaCha20::new(&key.into(), &nonce.into());
        cipher.apply_keystream(&mut data);

        ctx.metadata.insert(
            NONCE_KEY.into(),
            bytes::Bytes::from(Vec::from(nonce)).into(),
        );
        ctx.metadata
            .insert(CAPTURED_BYTES.into(), bytes::Bytes::from(data).into());

        let config = Config {
            data_key: CAPTURED_BYTES.into(),
            nonce_key: NONCE_KEY.into(),
            key,
            mode: Mode::Destination,
        };
        let filter = Decryptor::from_config(config.into());

        filter.read(&mut ctx).unwrap();
        assert_eq!(
            std::net::SocketAddr::from(([127u8, 0, 0, 1], 8080u16)),
            ctx.destinations[0].to_socket_addr().unwrap()
        );
    }
}
