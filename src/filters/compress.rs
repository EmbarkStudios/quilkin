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

mod compressor;
mod config;
mod metrics;

use crate::generated::quilkin::filters::compress::v1alpha1 as proto;

use crate::{filters::prelude::*, pool::BufferPool};

pub use compressor::Compressor;
use metrics::Metrics;
use std::sync::Arc;

pub use config::{Action, Config, Mode};

/// Filter for compressing and decompressing packet data
pub struct Compress {
    metrics: Metrics,
    // Keeping for now it would be needed for
    // https://github.com/googleforgames/quilkin/issues/637
    #[allow(unused)]
    compression_mode: Mode,
    on_read: Action,
    on_write: Action,
    pool: Arc<BufferPool>,
    compressor: Compressor,
}

impl Compress {
    fn new(config: Config, metrics: Metrics) -> Self {
        Self {
            metrics,
            compressor: config.mode.as_compressor(),
            compression_mode: config.mode,
            on_read: config.on_read,
            on_write: config.on_write,
            pool: Arc::new(BufferPool::new(num_cpus::get(), 64 * 1024)),
        }
    }
}

#[async_trait::async_trait]
impl Filter for Compress {
    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    async fn read(&self, ctx: &mut ReadContext) -> Result<(), FilterError> {
        let original_size = ctx.contents.len();

        match self.on_read {
            Action::Compress => {
                match self.compressor.encode(self.pool.clone(), &mut ctx.contents) {
                    Ok(()) => {
                        self.metrics
                            .read_decompressed_bytes_total
                            .inc_by(original_size as u64);
                        self.metrics
                            .read_compressed_bytes_total
                            .inc_by(ctx.contents.len() as u64);
                        Ok(())
                    }
                    Err(err) => Err(FilterError::new(err)),
                }
            }
            Action::Decompress => {
                match self.compressor.decode(self.pool.clone(), &mut ctx.contents) {
                    Ok(()) => {
                        self.metrics
                            .read_compressed_bytes_total
                            .inc_by(original_size as u64);
                        self.metrics
                            .read_decompressed_bytes_total
                            .inc_by(ctx.contents.len() as u64);
                        Ok(())
                    }
                    Err(err) => Err(FilterError::new(err)),
                }
            }
            Action::DoNothing => Ok(()),
        }
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(skip(self, ctx)))]
    async fn write(&self, ctx: &mut WriteContext) -> Result<(), FilterError> {
        let original_size = ctx.contents.len();
        match self.on_write {
            Action::Compress => {
                match self.compressor.encode(self.pool.clone(), &mut ctx.contents) {
                    Ok(()) => {
                        self.metrics
                            .write_decompressed_bytes_total
                            .inc_by(original_size as u64);
                        self.metrics
                            .write_compressed_bytes_total
                            .inc_by(ctx.contents.len() as u64);
                        Ok(())
                    }
                    Err(err) => Err(FilterError::new(err)),
                }
            }
            Action::Decompress => {
                match self.compressor.decode(self.pool.clone(), &mut ctx.contents) {
                    Ok(()) => {
                        self.metrics
                            .write_compressed_bytes_total
                            .inc_by(original_size as u64);
                        self.metrics
                            .write_decompressed_bytes_total
                            .inc_by(ctx.contents.len() as u64);
                        Ok(())
                    }

                    Err(err) => Err(FilterError::new(err)),
                }
            }
            Action::DoNothing => Ok(()),
        }
    }
}

impl StaticFilter for Compress {
    const NAME: &'static str = "quilkin.filters.compress.v1alpha1.Compress";
    type Configuration = Config;
    type BinaryConfiguration = proto::Compress;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Compress::new(
            Self::ensure_config_exists(config)?,
            Metrics::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        filters::compress::compressor::Compressor,
        net::endpoint::Endpoint,
        test::{alloc_buffer, BUFFER_POOL},
    };

    use super::*;

    #[tokio::test]
    async fn default_mode_factory() {
        let config = serde_json::json!({
            "on_read": "DECOMPRESS".to_string(),
            "on_write": "COMPRESS".to_string(),

        });
        let filter = Compress::from_config(Some(serde_json::from_value(config).unwrap()));
        assert_downstream(&filter).await;
    }

    #[tokio::test]
    async fn config_factory_snappy() {
        let config = serde_json::json!({
            "mode": "SNAPPY".to_string(),
            "on_read": "DECOMPRESS".to_string(),
            "on_write": "COMPRESS".to_string(),

        });
        let filter = Compress::from_config(Some(serde_json::from_value(config).unwrap()));
        assert_downstream(&filter).await;
    }

    #[tokio::test]
    async fn config_factory_lz4() {
        let config = serde_json::json!({
            "mode": "LZ4".to_string(),
            "on_read": "DECOMPRESS".to_string(),
            "on_write": "COMPRESS".to_string(),

        });
        let filter = Compress::from_config(Some(serde_json::from_value(config).unwrap()));
        assert_downstream(&filter).await;
    }

    #[tokio::test]
    async fn upstream() {
        let compress = Compress::new(
            Config {
                mode: Default::default(),
                on_read: Action::Compress,
                on_write: Action::Decompress,
            },
            Metrics::new(),
        );
        let expected = contents_fixture();

        // read compress
        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut read_context = ReadContext::new(
            endpoints.into(),
            "127.0.0.1:8080".parse().unwrap(),
            alloc_buffer(&expected),
        );
        compress
            .read(&mut read_context)
            .await
            .expect("should compress");

        assert_ne!(expected, &*read_context.contents);
        assert!(
            expected.len() > read_context.contents.len(),
            "Original: {}. Compressed: {}",
            expected.len(),
            read_context.contents.len()
        );

        // write decompress
        let mut write_context = WriteContext::new(
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
            read_context.contents,
        );

        compress
            .write(&mut write_context)
            .await
            .expect("should decompress");

        assert_eq!(expected, &*write_context.contents);
    }

    #[tokio::test]
    async fn failed_decompress() {
        let compression = Compress::new(
            Config {
                mode: Default::default(),
                on_read: Action::Compress,
                on_write: Action::Decompress,
            },
            Metrics::new(),
        );

        assert!(compression
            .write(&mut WriteContext::new(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:8081".parse().unwrap(),
                alloc_buffer(b"hello"),
            ))
            .await
            .is_err());

        let compression = Compress::new(
            Config {
                mode: Default::default(),
                on_read: Action::Decompress,
                on_write: Action::Compress,
            },
            Metrics::new(),
        );

        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        assert!(compression
            .read(&mut ReadContext::new(
                endpoints.into(),
                "127.0.0.1:8080".parse().unwrap(),
                alloc_buffer(b"hello"),
            ))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn do_nothing() {
        let compression = Compress::new(
            Config {
                mode: Default::default(),
                on_read: Action::default(),
                on_write: Action::default(),
            },
            Metrics::new(),
        );

        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut read_context = ReadContext::new(
            endpoints.into(),
            "127.0.0.1:8080".parse().unwrap(),
            alloc_buffer(b"hello"),
        );
        compression.read(&mut read_context).await.unwrap();
        assert_eq!(b"hello", &*read_context.contents);

        let mut write_context = WriteContext::new(
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
            alloc_buffer(b"hello"),
        );

        compression.write(&mut write_context).await.unwrap();

        assert_eq!(b"hello".to_vec(), &*write_context.contents)
    }

    fn roundtrip_compression(compressor: Compressor) {
        let expected = contents_fixture();
        let mut contents = alloc_buffer(&expected);

        compressor
            .encode(BUFFER_POOL.clone(), &mut contents)
            .expect("failed to compress");
        assert!(
            !contents.is_empty(),
            "compressed array should be greater than 0"
        );
        assert_ne!(
            expected, &*contents,
            "should not be equal, as one should be compressed"
        );
        assert!(
            expected.len() > contents.len(),
            "Original: {}. Compressed: {}",
            expected.len(),
            contents.len()
        ); // 45000 bytes uncompressed, 276 bytes compressed

        compressor
            .decode(BUFFER_POOL.clone(), &mut contents)
            .expect("failed to decompress");
        assert_eq!(
            expected, &*contents,
            "should be equal, as decompressed state should go back to normal"
        );
    }

    #[test]
    fn snappy() {
        roundtrip_compression(Mode::Snappy.into());
    }

    #[test]
    fn lz4() {
        roundtrip_compression(Mode::Lz4.into());
    }

    /// At small data packets, compression will add data, so let's give a bigger data packet!
    fn contents_fixture() -> Vec<u8> {
        "hello my name is mark and I like to do things"
            .repeat(100)
            .into_bytes()
    }

    /// assert compression work with decompress on read and compress on write
    /// Returns the original data packet, and the compressed version
    async fn assert_downstream<F>(filter: &F)
    where
        F: Filter + ?Sized,
    {
        let expected = contents_fixture();
        // write compress
        let mut write_context = WriteContext::new(
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
            alloc_buffer(&expected),
        );

        filter
            .write(&mut write_context)
            .await
            .expect("should compress");

        assert_ne!(expected, &*write_context.contents);
        assert!(
            expected.len() > write_context.contents.len(),
            "Original: {}. Compressed: {}",
            expected.len(),
            write_context.contents.len()
        );

        // read decompress
        let endpoints = crate::net::cluster::ClusterMap::new_default(
            [Endpoint::new("127.0.0.1:81".parse().unwrap())].into(),
        );
        let mut read_context = ReadContext::new(
            endpoints.into(),
            "127.0.0.1:8080".parse().unwrap(),
            write_context.contents,
        );

        filter
            .read(&mut read_context)
            .await
            .expect("should decompress");

        assert_eq!(expected, &*read_context.contents);
    }
}
