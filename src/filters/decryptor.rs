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

/// Filter that decrypts the destination IP and port from the packet
pub struct Decryptor {
    config: Config,
}

const DESTINATION_MIN: usize = 6;
const DESTINATION_MAX: usize = 18;

impl Decryptor {
    #[inline]
    fn decode_chacha20(&self, nonce: [u8; 12], data: &mut [u8]) {
        let mut cipher = chacha20::ChaCha20::new(&self.config.key.into(), &nonce.into());
        cipher.apply_keystream(data);
    }

    #[inline]
    fn decode_destination(data: &[u8]) -> std::net::SocketAddr {
        use std::net;

        let port = (data[data.len() - 2] as u16) << 8 | data[data.len() - 1] as u16;

        match data.len() {
            DESTINATION_MIN => net::SocketAddr::V4(net::SocketAddrV4::new(
                net::Ipv4Addr::from_octets(data[..4].try_into().unwrap()),
                port,
            )),
            DESTINATION_MAX => net::SocketAddr::V6(net::SocketAddrV6::new(
                net::Ipv6Addr::from_octets(data[..16].try_into().unwrap()),
                port,
                0,
                0,
            )),
            _ => unreachable!(),
        }
    }
}

impl StaticFilter for Decryptor {
    const NAME: &'static str = "quilkin.filters.decryptor.v1alpha1.Decryptor";
    type Configuration = Config;
    type BinaryConfiguration = proto::Decryptor;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Self {
            config: Self::ensure_config_exists(config)?,
        })
    }
}

impl Filter for Decryptor {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        match (
            ctx.metadata.get(&self.config.data_key),
            ctx.metadata.get(&self.config.nonce_key),
        ) {
            (Some(metadata::Value::Bytes(data)), Some(metadata::Value::Bytes(nonce))) => {
                let nonce = <[u8; 12]>::try_from(&**nonce)
                    .map_err(|_e| FilterError::Custom("Expected 12 byte nonce"))?;

                match self.config.mode {
                    Mode::Destination => {
                        // We can avoid a heap allocation since we know the maximum size of the encrypted payload
                        let mut edata = [0u8; DESTINATION_MAX];

                        let edata = match data.len() {
                            DESTINATION_MIN => {
                                edata[..DESTINATION_MIN].copy_from_slice(data);
                                &mut edata[..DESTINATION_MIN]
                            }
                            DESTINATION_MAX => {
                                edata[..DESTINATION_MAX].copy_from_slice(data);
                                &mut edata[..DESTINATION_MAX]
                            }
                            _ => {
                                return Err(FilterError::Custom(
                                    "Invalid decoded data length, must be `6` or `18` bytes.",
                                ));
                            }
                        };

                        self.decode_chacha20(nonce, edata);
                        ctx.destinations
                            .push(Self::decode_destination(edata).into());
                        Ok(())
                    }
                }
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
    #[serde(rename = "dataKey", default = "default_data_key")]
    pub data_key: metadata::Key,
    #[serde(rename = "nonceKey", default = "default_nonce_key")]
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
        .map_err(|_e| serde::de::Error::custom("invalid key, expected 32 bytes"))
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
            key: p.key.try_into().map_err(|_e| {
                ConvertProtoConfigError::new(
                    "invalid key, expected 32 bytes",
                    Some("private_key".into()),
                )
            })?,
            mode: p.mode.try_into()?,
            data_key: p.data_key.map_or_else(default_data_key, metadata::Key::new),
            nonce_key: p
                .nonce_key
                .map_or_else(default_nonce_key, metadata::Key::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::alloc_buffer;

    #[test]
    fn decryptor() {
        let endpoints = crate::net::cluster::ClusterMap::default();
        let mut dest = Vec::new();
        let mut ctx = ReadContext::new(
            &endpoints,
            "0.0.0.0:0".parse().unwrap(),
            alloc_buffer(b"hello"),
            &mut dest,
        );

        let key = [0x42u8; 32];
        let nonce = [0x22u8; 12];

        ctx.metadata.insert(
            NONCE_KEY.into(),
            bytes::Bytes::from(Vec::from(nonce)).into(),
        );

        let config = Config {
            data_key: CAPTURED_BYTES.into(),
            nonce_key: NONCE_KEY.into(),
            key,
            mode: Mode::Destination,
        };

        let filter = Decryptor::from_config(config.into());

        // ipv4
        {
            let expected = std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(127, 0, 0, 1), 8080);

            let mut data = Vec::new();
            data.extend(expected.ip().to_bits().to_be_bytes());
            data.extend(expected.port().to_be_bytes());

            let mut cipher = chacha20::ChaCha20::new(&key.into(), &nonce.into());
            cipher.apply_keystream(&mut data);

            ctx.metadata
                .insert(CAPTURED_BYTES.into(), bytes::Bytes::from(data).into());

            filter.read(&mut ctx).unwrap();
            assert_eq!(
                std::net::SocketAddr::from(expected),
                ctx.destinations.pop().unwrap().to_socket_addr().unwrap()
            );
        }

        // ipv6
        {
            let expected = std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::new(1, 2, 3, 4, 5, 6, 7, 8),
                45000,
                0,
                0,
            );

            let mut data = Vec::new();
            data.extend(expected.ip().octets());
            data.extend(expected.port().to_be_bytes());

            let mut cipher = chacha20::ChaCha20::new(&key.into(), &nonce.into());
            cipher.apply_keystream(&mut data);

            ctx.metadata
                .insert(CAPTURED_BYTES.into(), bytes::Bytes::from(data).into());

            filter.read(&mut ctx).unwrap();
            assert_eq!(
                std::net::SocketAddr::from(expected),
                ctx.destinations.pop().unwrap().to_socket_addr().unwrap()
            );
        }
    }
}
