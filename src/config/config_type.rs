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

use bytes::Bytes;

use crate::filters::CreationError;

/// The configuration of a [`Filter`][crate::filters::Filter] from either a
/// static or dynamic source.
#[derive(Clone, Debug, PartialEq, schemars::JsonSchema)]
pub enum ConfigType {
    /// Static configuration from YAML.
    #[schemars(with = "serde_json::Value")]
    Static(serde_json::Value),
    /// Dynamic configuration from Protobuf.
    #[schemars(skip)]
    Dynamic(prost_types::Any),
}

impl ConfigType {
    /// Deserializes takes two type arguments `Static` and `Dynamic` representing
    /// the types of a static and dynamic configuration respectively.
    ///
    /// If the configuration input is a [`ConfigType::Static`], then it is directly
    /// deserialized into the returned `Static` instance.
    ///
    /// Otherwise if the input is a [`ConfigType::Dynamic`] then the contained Protobuf
    /// data is decoded into a type `Dynamic` instance, after which the decoded
    /// value is converted into the returned `Static` instance.
    /// As a result `TryFrom` must be implemented from the `Dynamic` to the `Static`
    /// type.
    ///
    /// It returns both the deserialized, as well as, a JSON representation
    /// of the provided config.
    /// It returns an error if any of the serialization or deserialization steps fail.
    pub fn deserialize<TextConfiguration, BinaryConfiguration>(
        self,
        filter_name: &str,
    ) -> Result<(serde_json::Value, TextConfiguration), CreationError>
    where
        BinaryConfiguration: prost::Message + Default,
        TextConfiguration:
            serde::Serialize + for<'de> serde::Deserialize<'de> + TryFrom<BinaryConfiguration>,
        CreationError: From<<BinaryConfiguration as TryInto<TextConfiguration>>::Error>,
    {
        match self {
            ConfigType::Static(ref config) => serde_yaml::to_string(config)
                .and_then(|raw_config| {
                    serde_yaml::from_str::<TextConfiguration>(raw_config.as_str())
                })
                .map_err(|err| {
                    CreationError::DeserializeFailed(format!(
                        "filter `{filter_name}`: failed to YAML deserialize config: {err}",
                    ))
                })
                .and_then(|config| {
                    Self::get_json_config(filter_name, &config)
                        .map(|config_json| (config_json, config))
                }),
            ConfigType::Dynamic(config) => prost::Message::decode(Bytes::from(config.value))
                .map_err(|err| {
                    CreationError::DeserializeFailed(format!(
                        "filter `{filter_name}`: config decode error: {err}",
                    ))
                })
                .and_then(|config| TextConfiguration::try_from(config).map_err(From::from))
                .and_then(|config| {
                    Self::get_json_config(filter_name, &config)
                        .map(|config_json| (config_json, config))
                }),
        }
    }

    // Returns an equivalent json value for the passed in config.
    fn get_json_config<T>(filter_name: &str, config: &T) -> Result<serde_json::Value, CreationError>
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        serde_json::to_value(config).map_err(|err| {
            CreationError::DeserializeFailed(format!(
                "filter `{filter_name}`: failed to serialize config to json: {err}",
            ))
        })
    }
}

impl<'de> serde::Deserialize<'de> for ConfigType {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde_json::Value::deserialize(de).map(ConfigType::Static)
    }
}

impl serde::Serialize for ConfigType {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Static(value) => value.serialize(ser),
            Self::Dynamic(_) => Err(serde::ser::Error::custom(
                "Protobuf configs can't be serialized.",
            )),
        }
    }
}

impl From<serde_json::Value> for ConfigType {
    fn from(value: serde_json::Value) -> Self {
        Self::Static(value)
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigType;
    use serde::{Deserialize, Serialize};

    #[test]
    fn get_json_config() {
        #[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
        struct TestConfig {
            name: String,
            value: usize,
        }
        let expected_config = TestConfig {
            name: "bebop".into(),
            value: 98,
        };
        let config_json = ConfigType::get_json_config("my-filter", &expected_config).unwrap();

        assert_eq!(
            serde_json::json!({
                "name": "bebop",
                "value": 98,
            }),
            config_json
        );
    }
}
