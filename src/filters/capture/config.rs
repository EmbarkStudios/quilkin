/*
 * Copyright 2021 Google LLC All Rights Reserved.
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

use serde::{Deserialize, Serialize};

use super::{CAPTURED_BYTES, Prefix, Regex, Suffix, proto};
use crate::filters::ConvertProtoConfigError;

/// Strategy to apply for acquiring a set of bytes in the UDP packet
#[derive(Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
#[serde(tag = "kind")]
pub enum Strategy {
    /// Looks for the set of bytes at the beginning of the packet
    #[serde(rename = "PREFIX")]
    Prefix(Prefix),
    /// Look for the set of bytes at the end of the packet
    #[serde(rename = "SUFFIX")]
    Suffix(Suffix),
    /// Look for the set of bytes at the end of the packet
    #[serde(rename = "REGEX")]
    Regex(Regex),
}

impl Strategy {
    pub fn into_capture(self) -> Box<dyn super::CaptureStrategy + Send + Sync> {
        match self {
            Self::Prefix(value) => Box::from(value),
            Self::Suffix(value) => Box::from(value),
            Self::Regex(value) => Box::from(value),
        }
    }
}

impl From<Prefix> for Strategy {
    fn from(prefix: Prefix) -> Self {
        Self::Prefix(prefix)
    }
}

impl From<Suffix> for Strategy {
    fn from(suffix: Suffix) -> Self {
        Self::Suffix(suffix)
    }
}

impl From<Regex> for Strategy {
    fn from(regex: Regex) -> Self {
        Self::Regex(regex)
    }
}

#[derive(Debug, PartialEq, schemars::JsonSchema)]
pub struct Config {
    /// The key to use when storing the captured value in the filter context.
    /// If a match was found it is available
    /// under `{{metadata_key}}/is_present`.
    pub metadata_key: crate::net::endpoint::metadata::Key,
    /// The capture strategy.
    pub strategy: Strategy,
}

impl Config {
    pub fn with_strategy(strategy: impl Into<Strategy>) -> Self {
        Self {
            metadata_key: crate::net::endpoint::metadata::Key::from_static(CAPTURED_BYTES),
            strategy: strategy.into(),
        }
    }
}

impl Serialize for Config {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut s = serializer.serialize_struct("Config", 2)?;
        s.serialize_field("metadataKey", &self.metadata_key)?;
        match &self.strategy {
            Strategy::Prefix(value) => s.serialize_field("prefix", value)?,
            Strategy::Suffix(value) => s.serialize_field("suffix", value)?,
            Strategy::Regex(value) => s.serialize_field("regex", value)?,
        }

        s.end()
    }
}

impl<'de> serde::Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            #[serde(rename = "metadataKey")]
            MetadataKey,
            Prefix,
            Suffix,
            Regex,
        }

        struct ConfigVisitor;

        impl<'de> serde::de::Visitor<'de> for ConfigVisitor {
            type Value = Config;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("Capture config")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut metadata_key = None;
                let mut strategy = None;
                let strategy_exists_err = || {
                    Err(serde::de::Error::custom(
                        "Multiple strategies found, only one capture strategy is permitted",
                    ))
                };

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::MetadataKey => {
                            if metadata_key.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadataKey"));
                            }

                            metadata_key = Some(map.next_value()?);
                        }

                        Field::Prefix => {
                            if strategy.is_some() {
                                return (strategy_exists_err)();
                            }

                            strategy = Some(Strategy::Prefix(map.next_value()?));
                        }

                        Field::Suffix => {
                            if strategy.is_some() {
                                return (strategy_exists_err)();
                            }

                            strategy = Some(Strategy::Suffix(map.next_value()?));
                        }

                        Field::Regex => {
                            if strategy.is_some() {
                                return (strategy_exists_err)();
                            }

                            strategy = Some(Strategy::Regex(map.next_value()?));
                        }
                    }
                }

                let metadata_key = metadata_key.unwrap_or_else(|| {
                    crate::net::endpoint::metadata::Key::from_static(CAPTURED_BYTES)
                });
                let strategy = strategy.ok_or_else(|| {
                    serde::de::Error::custom(
                        "Capture strategy of `regex`, `suffix`, or `prefix` is required",
                    )
                })?;

                Ok(Config {
                    metadata_key,
                    strategy,
                })
            }
        }

        deserializer.deserialize_map(ConfigVisitor)
    }
}

impl From<Config> for proto::Capture {
    fn from(config: Config) -> Self {
        Self {
            metadata_key: Some(config.metadata_key.to_string()),
            strategy: Some(config.strategy.into()),
        }
    }
}

impl TryFrom<proto::Capture> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::Capture) -> Result<Self, Self::Error> {
        let strategy = p
            .strategy
            .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("strategy".into())))?;

        Ok(Self {
            metadata_key: p
                .metadata_key
                .map(crate::net::endpoint::metadata::Key::from)
                .ok_or_else(|| {
                    ConvertProtoConfigError::new("Missing", Some("metadata_key".into()))
                })?,
            strategy: strategy.try_into()?,
        })
    }
}

impl From<Strategy> for proto::capture::Strategy {
    fn from(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Prefix(prefix) => Self::Prefix(proto::capture::Prefix {
                size: prefix.size,
                remove: Some(prefix.remove),
            }),
            Strategy::Suffix(suffix) => Self::Suffix(proto::capture::Suffix {
                size: suffix.size,
                remove: Some(suffix.remove),
            }),
            Strategy::Regex(regex) => Self::Regex(proto::capture::Regex {
                regex: Some(regex.pattern.as_str().into()),
            }),
        }
    }
}

impl TryFrom<proto::capture::Strategy> for Strategy {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::capture::Strategy) -> Result<Self, Self::Error> {
        use proto::capture;

        Ok(match p {
            capture::Strategy::Prefix(prefix) => Self::Prefix(Prefix {
                size: prefix.size,
                remove: prefix.remove.unwrap_or_default(),
            }),
            capture::Strategy::Suffix(suffix) => Self::Suffix(Suffix {
                size: suffix.size,
                remove: suffix.remove.unwrap_or_default(),
            }),
            capture::Strategy::Regex(regex) => {
                let regex = regex.regex.ok_or_else(|| {
                    ConvertProtoConfigError::new("Missing", Some("Regex.regex".into()))
                })?;
                Self::Regex(Regex {
                    pattern: regex.parse().map_err(|error: regex::Error| {
                        ConvertProtoConfigError::new(error.to_string(), Some("Regex.regex".into()))
                    })?,
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_proto_config() {
        let test_cases = vec![(
            "should succeed when all valid values are provided",
            proto::Capture {
                strategy: Some(proto::capture::Strategy::Suffix(proto::capture::Suffix {
                    size: 42,
                    remove: Some(true),
                })),
                metadata_key: Some("foobar".into()),
            },
            Some(Config {
                metadata_key: "foobar".into(),
                strategy: Strategy::Suffix(Suffix {
                    size: 42,
                    remove: true,
                }),
            }),
        )];

        for (name, proto_config, expected) in test_cases {
            let result = Config::try_from(proto_config);
            assert_eq!(
                result.is_err(),
                expected.is_none(),
                "{}: error expectation does not match",
                name
            );
            if let Some(expected) = expected {
                assert_eq!(expected, result.unwrap(), "{}", name);
            }
        }
    }
}
