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
    #[serde(default, rename = "as", deserialize_with = "deserialize_asn")]
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

/// mmdb encodes an integer as the narrowest type that holds it, so `as` arrives
/// at any width and `maxminddb`'s typed deserializers reject all but one.
fn deserialize_asn<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    use serde::de::{Error, Unexpected, Visitor};

    struct AsnVisitor;

    impl Visitor<'_> for AsnVisitor {
        type Value = u64;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an autonomous system number")
        }

        fn visit_u64<E: Error>(self, id: u64) -> Result<u64, E> {
            Ok(id)
        }

        fn visit_i64<E: Error>(self, id: i64) -> Result<u64, E> {
            id.try_into()
                .map_err(|_e| E::invalid_value(Unexpected::Signed(id), &self))
        }
    }

    deserializer.deserialize_any(AsnVisitor)
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

    /// Just enough of the mmdb encoding to build one record database.
    #[derive(Default)]
    struct Mmdb(Vec<u8>);

    impl Mmdb {
        const UINT16: u8 = 5;
        const UINT32: u8 = 6;
        const UINT64: u8 = 9;

        fn control(&mut self, kind: u8, size: usize) {
            assert!(size < 29, "only the short size form is implemented");
            // Types above 7 move into a second byte, biased by 7.
            if kind > 7 {
                self.0.extend_from_slice(&[size as u8, kind - 7]);
            } else {
                self.0.push((kind << 5) | size as u8);
            }
        }

        fn uint(&mut self, kind: u8, value: u64) -> &mut Self {
            let bytes = value.to_be_bytes();
            let significant = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len());
            self.control(kind, bytes.len() - significant);
            self.0.extend_from_slice(&bytes[significant..]);
            self
        }

        fn string(&mut self, value: &str) -> &mut Self {
            self.control(2, value.len());
            self.0.extend_from_slice(value.as_bytes());
            self
        }

        fn map(&mut self, entries: usize) -> &mut Self {
            self.control(7, entries);
            self
        }

        fn array(&mut self, items: usize) -> &mut Self {
            self.control(11, items);
            self
        }
    }

    /// Wraps `record` in a single node tree that resolves every address to it.
    fn database(record: &[u8]) -> Vec<u8> {
        const NODE_COUNT: u32 = 1;
        const RECORD_SIZE: u64 = 24;
        // The reader takes the data section offset as `pointer - node_count - 16`.
        const POINTER: u32 = NODE_COUNT + 16;

        let mut db = Vec::new();
        for _ in 0..2 {
            db.extend_from_slice(&POINTER.to_be_bytes()[1..]);
        }
        db.extend_from_slice(&[0; 16]);
        db.extend_from_slice(record);
        db.extend_from_slice(b"\xab\xcd\xefMaxMind.com");

        let mut metadata = Mmdb::default();
        metadata
            .map(9)
            .string("binary_format_major_version")
            .uint(Mmdb::UINT16, 2)
            .string("binary_format_minor_version")
            .uint(Mmdb::UINT16, 0)
            .string("build_epoch")
            .uint(Mmdb::UINT64, 0)
            .string("database_type")
            .string("quilkin-test")
            .string("description")
            .map(0)
            .string("ip_version")
            .uint(Mmdb::UINT16, 4)
            .string("languages")
            .array(0)
            .string("node_count")
            .uint(Mmdb::UINT32, NODE_COUNT as _)
            .string("record_size")
            .uint(Mmdb::UINT16, RECORD_SIZE);
        db.extend_from_slice(&metadata.0);

        db
    }

    /// mmdb stores an integer as the narrowest type that holds it, and
    /// `maxminddb` decodes only the type it finds, so `as` has to be read at
    /// whichever width the database that produced it happened to use.
    #[test]
    fn decodes_asn_at_every_stored_width() {
        for kind in [Mmdb::UINT16, Mmdb::UINT32, Mmdb::UINT64] {
            let mut record = Mmdb::default();
            record
                .map(2)
                .string("as")
                .uint(kind, 15169)
                .string("prefix")
                .string("8.8.8.0/24");

            let reader = super::Reader::from_source(database(&record.0)).unwrap();
            let entry = reader
                .lookup(std::net::IpAddr::from([8, 8, 8, 8]))
                .unwrap()
                .decode::<super::IpNetEntry>()
                .unwrap()
                .unwrap_or_else(|| panic!("no record found for type {kind}"));

            assert_eq!(entry.id, 15169, "type {kind}");
            assert_eq!(entry.prefix, "8.8.8.0/24", "type {kind}");
        }
    }
}
