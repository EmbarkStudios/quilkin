/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use std::sync::Arc;

use bytes::Bytes;
use hyper_util::client::legacy;
use maxminddb::Reader;
use once_cell::sync::Lazy;

type Result<T, E = Error> = std::result::Result<T, E>;

static HTTP: Lazy<
    legacy::Client<
        hyper_rustls::HttpsConnector<legacy::connect::HttpConnector>,
        http_body_util::Empty<Bytes>,
    >,
> = Lazy::new(|| {
    legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build(),
    )
});
pub static CLIENT: Lazy<arc_swap::ArcSwapOption<MaxmindDb>> = Lazy::new(<_>::default);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(tag = "kind")]
pub enum Source {
    File { path: std::path::PathBuf },
    Url { url: url::Url },
}

impl std::str::FromStr for Source {
    type Err = eyre::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(url) = input.parse() {
            Ok(Self::Url { url })
        } else {
            // Clippy says this parse is guarenteed to succeed.
            Ok(Self::File {
                path: input.parse().unwrap(),
            })
        }
    }
}

#[derive(Debug)]
pub struct MaxmindDb {
    reader: Reader<Bytes>,
}

impl MaxmindDb {
    fn new(reader: Reader<Bytes>) -> Self {
        Self { reader }
    }

    pub fn instance() -> arc_swap::Guard<Option<Arc<MaxmindDb>>> {
        CLIENT.load()
    }

    pub fn lookup(ip: std::net::IpAddr) -> Option<IpNetEntry> {
        let ip = ip.to_canonical();
        let Some(mmdb) = crate::MaxmindDb::instance().clone() else {
            tracing::trace!("skipping mmdb telemetry, no maxmind database available");
            return None;
        };

        match mmdb.lookup(ip) {
            Ok(lookup_result) => match lookup_result.decode::<IpNetEntry>() {
                Ok(asn) => asn,
                Err(error) => {
                    tracing::warn!(%ip, %error, "failed to decode ip");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(%ip, %error, "ip not found in maxmind database");
                None
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn update(source: Source) -> Result<()> {
        let db = Self::from_source(source).await?;
        CLIENT.store(Some(Arc::new(db)));
        tracing::info!("maxmind database updated");
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn from_source(source: Source) -> Result<Self> {
        match source {
            Source::File { path } => Self::open(path).await,
            Source::Url { url } => Self::open_url(&url).await,
        }
    }

    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub async fn open<A: AsRef<std::path::Path>>(path: A) -> Result<Self> {
        let path = path.as_ref();
        tracing::info!(path=%path.display(), "trying to read local maxmind database");
        let bytes = Bytes::from(tokio::fs::read(path).await?);
        Reader::from_source(bytes)
            .map(Self::new)
            .map_err(From::from)
    }

    /// Reads a Maxmind DB from `url`, and if `cache` is `true`, then will use
    /// the cached result, retreiving a fresh copy otherwise.
    #[tracing::instrument(skip_all, fields(url = %url))]
    pub async fn open_url(url: &url::Url) -> Result<Self> {
        tracing::info!("requesting maxmind database from network");

        use http_body_util::BodyExt;
        let data = HTTP
            .get(url.as_str().try_into().unwrap())
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes();

        tracing::debug!("finished download");
        let reader = Reader::from_source(data)?;

        Ok(Self { reader })
    }
}

impl std::ops::Deref for MaxmindDb {
    type Target = Reader<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl std::ops::DerefMut for MaxmindDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

#[derive(Clone, serde::Deserialize)]
pub struct IpNetEntry {
    #[serde(default, rename = "as")]
    pub id: u64,
    #[serde(default)]
    pub as_cc: String,
    #[serde(default)]
    pub as_name: String,
    #[serde(default)]
    pub prefix_entity: String,
    #[serde(default)]
    pub prefix_name: String,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Clone)]
pub struct MetricsIpNetEntry {
    pub prefix: String,
    pub asn: Asn,
}

#[derive(Copy, Clone)]
pub struct Asn {
    /// This is a 32-bit number, but there are only ~90000 asn's worldwide
    asn: [u8; 10],
    asn_len: u8,
}

impl Asn {
    pub(crate) fn new(id: u64) -> Self {
        let mut asn = [0u8; 10];
        let asn_len = itoa(id, &mut asn);

        Self { asn, asn_len }
    }

    pub(crate) fn as_str(&self) -> &str {
        // SAFETY: the asn only has ASCII bytes
        unsafe { std::str::from_utf8_unchecked(&self.asn[..self.asn_len as usize]) }
    }
}

impl MetricsIpNetEntry {}

impl<'a> From<&'a IpNetEntry> for MetricsIpNetEntry {
    fn from(value: &'a IpNetEntry) -> Self {
        Self {
            prefix: value.prefix.clone(),
            asn: Asn::new(value.id),
        }
    }
}

impl From<IpNetEntry> for MetricsIpNetEntry {
    fn from(value: IpNetEntry) -> Self {
        Self {
            prefix: value.prefix,
            asn: Asn::new(value.id),
        }
    }
}

#[inline]
pub(crate) fn itoa(mut num: u64, asn: &mut [u8]) -> u8 {
    let mut index = 0;

    loop {
        let rem = (num % 10) as u8;
        asn[index] = rem + b'0';
        index += 1;
        num /= 10;

        if num == 0 {
            break;
        }
    }

    asn[..index].reverse();

    index as u8
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    MaxmindDb(#[from] maxminddb::MaxMindDbError),
    #[error(transparent)]
    Http(#[from] hyper::Error),
    #[error(transparent)]
    HttpClient(#[from] legacy::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod test {
    fn check(num: u64, exp: &str) {
        let mut asn = [0u8; 10];
        let len = super::itoa(num, &mut asn);

        // SAFETY: itoa only writes ASCII
        let asn_str = unsafe { std::str::from_utf8_unchecked(&asn[..len as _]) };

        assert_eq!(asn_str, exp);
    }

    #[test]
    fn itoa() {
        check(0, "0");
        check(1, "1");
        check(10, "10");
        check((u32::MAX >> 1) as _, &(u32::MAX >> 1).to_string());
        check((u32::MAX - 1) as _, &(u32::MAX - 1).to_string());
        check(u32::MAX as _, &u32::MAX.to_string());
    }
}
