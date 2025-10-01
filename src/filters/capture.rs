/*
 * Copyright 2020 Google LLC
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

mod affix;
mod config;
mod regex;

use crate::generated::quilkin::filters::capture::v1alpha1 as proto;

use crate::{filters::prelude::*, net::endpoint::metadata};

/// The default key under which the [`Capture`] filter puts the
/// byte slices it extracts from each packet.
/// - **Type** `Vec<u8>`
pub const CAPTURED_BYTES: &str = "quilkin.dev/capture";

pub use self::{
    affix::{Prefix, Suffix},
    config::{Config, Strategy},
    regex::Regex,
};

/// Trait to implement different strategies for capturing packet data.
pub trait CaptureStrategy {
    /// Capture packet data from the contents, and optionally returns a value if
    /// anything was captured.
    fn capture(&self, contents: &[u8]) -> Option<(metadata::Value, isize)>;
}

pub struct Capture {
    capture: Box<dyn CaptureStrategy + Sync + Send>,
    metadata_key: metadata::Key,
    is_present_key: metadata::Key,
}

impl Capture {
    fn new(config: Config) -> Self {
        Self {
            capture: config.strategy.into_capture(),
            is_present_key: format!("{}/is_present", config.metadata_key).into(),
            metadata_key: config.metadata_key,
        }
    }

    pub fn testing(config: Config) -> Self {
        Self::new(config)
    }
}

impl Filter for Capture {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        let capture = self.capture.capture(ctx.contents.as_slice());
        ctx.metadata.insert(
            self.is_present_key,
            metadata::Value::Bool(capture.is_some()),
        );

        if let Some((value, remove)) = capture {
            tracing::trace!(key=%self.metadata_key, %value, "captured value");
            ctx.metadata.insert(self.metadata_key, value);

            if remove != 0 {
                if remove < 0 {
                    ctx.contents.remove_head(remove.unsigned_abs());
                } else {
                    ctx.contents.remove_tail(remove as _);
                }
            }

            Ok(())
        } else {
            tracing::trace!(key = %self.metadata_key, "No value captured");
            Err(FilterError::NoValueCaptured)
        }
    }
}

impl StaticFilter for Capture {
    const NAME: &'static str = "quilkin.filters.capture.v1alpha1.Capture";
    type Configuration = Config;
    type BinaryConfiguration = proto::Capture;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Capture::new(Self::ensure_config_exists(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::CAPTURED_BYTES;
    use crate::{
        net::endpoint::{Endpoint, metadata::Value},
        test::{alloc_buffer, assert_write_no_change},
    };

    use super::*;

    const TOKEN_KEY: &str = "TOKEN";

    #[tokio::test]
    async fn factory_valid_config_all() {
        let config = serde_json::json!({
            "metadataKey": TOKEN_KEY.to_string(),
            "suffix": {
                "size": 3_i64,
                "remove": true,
            }
        });
        let filter = Capture::from_config(Some(serde_json::from_value(config).unwrap()));
        assert_end_strategy(&filter, TOKEN_KEY.into(), true);
    }

    #[tokio::test]
    async fn factory_valid_config_defaults() {
        let config = serde_json::json!({
            "suffix": {
                "size": 3_i64,
            }
        });

        let filter = Capture::from_config(Some(serde_json::from_value(config).unwrap()));
        assert_end_strategy(&filter, CAPTURED_BYTES.into(), false);
    }

    #[test]
    fn invalid_config() {
        let config = serde_json::json!({
            "suffix": {
                "size": "WRONG",
            }
        });
        assert!(serde_json::from_value::<Config>(config).is_err());
    }

    #[tokio::test]
    async fn read() {
        let config = Config {
            metadata_key: TOKEN_KEY.into(),
            strategy: Strategy::Suffix(Suffix {
                size: 3,
                remove: true,
            }),
        };

        let filter = Capture::from_config(config.into());
        assert_end_strategy(&filter, TOKEN_KEY.into(), true);
    }

    #[tokio::test]
    async fn read_overflow_capture_size() {
        let config = Config {
            metadata_key: TOKEN_KEY.into(),
            strategy: Strategy::Suffix(Suffix {
                size: 99,
                remove: true,
            }),
        };
        let filter = Capture::from_config(config.into());
        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut dest = Vec::new();
        assert!(
            filter
                .read(&mut ReadContext::new(
                    &endpoints,
                    (std::net::Ipv4Addr::LOCALHOST, 80).into(),
                    alloc_buffer(b"abc"),
                    &mut dest,
                ))
                .is_err()
        );
    }

    #[tokio::test]
    async fn write() {
        let config = Config {
            strategy: Strategy::Suffix(Suffix {
                size: 0,
                remove: false,
            }),
            metadata_key: TOKEN_KEY.into(),
        };
        let filter = Capture::from_config(config.into());
        assert_write_no_change(&filter);
    }

    #[test]
    fn regex_capture() {
        let end = Regex {
            pattern: ::regex::bytes::Regex::new(".{3}$").unwrap(),
        };
        let contents = alloc_buffer(b"helloabc");
        let result = end.capture(&contents).unwrap().0;
        assert_eq!(Value::Bytes(b"abc".to_vec().into()), result);
        assert_eq!(b"helloabc", &*contents);
    }

    #[test]
    fn end_capture() {
        let mut end = Suffix {
            size: 3,
            remove: false,
        };
        let mut contents = alloc_buffer(b"helloabc");
        let (result, remove) = end.capture(&contents).unwrap();
        assert_eq!(Value::Bytes(b"abc".to_vec().into()), result);
        assert_eq!(remove, 0);
        assert_eq!(b"helloabc", &*contents);

        end.remove = true;

        let (result, remove) = end.capture(&contents).unwrap();
        assert_eq!(Value::Bytes(b"abc".to_vec().into()), result);
        assert_eq!(remove, 3);
        contents.remove_tail(remove as _);
        assert_eq!(b"hello", &*contents);
    }

    #[test]
    fn beginning_capture() {
        let mut beg = Prefix {
            size: 3,
            remove: false,
        };
        let mut contents = alloc_buffer(b"abchello");

        let (result, remove) = beg.capture(&contents).unwrap();
        assert_eq!(Value::Bytes(b"abc".to_vec().into()), result);
        assert_eq!(remove, 0);
        assert_eq!(b"abchello", &*contents);

        beg.remove = true;

        let (result, remove) = beg.capture(&contents).unwrap();
        assert_eq!(Value::Bytes(b"abc".to_vec().into()), result);
        assert_eq!(remove, -3);
        contents.remove_head(remove.unsigned_abs());
        assert_eq!(b"hello", &*contents);
    }

    fn assert_end_strategy<F>(filter: &F, key: metadata::Key, remove: bool)
    where
        F: Filter + ?Sized,
    {
        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut dest = Vec::new();
        let mut context = ReadContext::new(
            &endpoints,
            "127.0.0.1:80".parse().unwrap(),
            alloc_buffer(b"helloabc"),
            &mut dest,
        );

        filter.read(&mut context).unwrap();

        if remove {
            assert_eq!(b"hello", &*context.contents);
        } else {
            assert_eq!(b"helloabc", &*context.contents);
        }

        let token = context.metadata.get(&key).unwrap().as_bytes().unwrap();
        assert_eq!(b"abc", &**token);
    }
}
