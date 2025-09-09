pub use corro_api_types::{QueryEvent, SqliteValue};
use eyre::ContextCompat as _;
use quilkin_types::{AddressKind, IcaoCode, TokenSet};
use serde::{
    Deserialize,
    de::{self, SeqAccess},
};
use std::{collections::BTreeSet, fmt, str::FromStr};

pub trait FromSqlValue: Sized {
    fn from_sql(values: &[SqliteValue]) -> eyre::Result<Self>;
}

pub struct ServerRow {
    pub address: AddressKind,
    pub port: u16,
    pub icao: IcaoCode,
    pub locality: Option<String>,
    pub tokens: TokenSet,
}

pub fn deserialize_token_set(s: &str) -> eyre::Result<TokenSet> {
    let mut ts = BTreeSet::default();

    let mut tokens = data_encoding::BASE64_NOPAD.decode(s.as_bytes())?;

    if tokens.is_empty() {
        return Ok(TokenSet(ts));
    }

    if tokens[0] & 0x80u8 != 0 {
        let len = (tokens[0] & !0x80) as usize;
        for tok in tokens[1..].chunks_exact(len) {
            ts.insert(tok.to_vec());
        }
    } else if tokens[0] > 1 {
        let mut toks = &tokens[1..];
        for _ in 0..tokens[0] as usize {
            let len = toks[0] as usize;
            eyre::ensure!(
                len <= toks.len() - 1,
                "token length {len} is longer than remaining binary slice"
            );

            ts.insert(toks[1..1 + len].to_vec());
            toks = &toks[1 + len..];
        }
    } else {
        tokens.remove(0);
        ts.insert(tokens);
    }

    Ok(TokenSet(ts))
}

#[inline]
fn parse_address(addr: &str) -> eyre::Result<(AddressKind, u16)> {
    let (addr, port) = addr.rsplit_once(':').context("missing ':'")?;
    let port = port.parse()?;
    if let Some(ip) = addr.strip_prefix('|') {
        let ip = ip.parse()?;
        Ok((AddressKind::Ip(ip), port))
    } else {
        Ok((AddressKind::Name(addr.to_owned()), port))
    }
}

macro_rules! get_column {
    ($index:expr, $name:literal, $v:expr) => {
        $v.get($index)
            .context(concat!("missing column '", $name, "'"))?
            .as_str()
            .context(concat!("column '", $name, "' is not a string"))?
    };
}

macro_rules! get_json {
    ($name:literal, $conv:expr, $seq:expr) => {{
        let v = $seq
            .next_element()?
            .ok_or(de::Error::missing_field($name))?;
        $conv(v).map_err(de::Error::custom)?
    }};
}

impl FromSqlValue for ServerRow {
    fn from_sql(values: &[SqliteValue]) -> eyre::Result<Self> {
        let (address, port) = parse_address(get_column!(0, "endpoint", values))?;
        let icao = get_column!(1, "icao", values).parse()?;
        let locality = values.get(2).and_then(|s| s.as_str().map(String::from));
        let tokens = deserialize_token_set(get_column!(3, "tokens", values))?;

        Ok(Self {
            address,
            port,
            icao,
            locality,
            tokens,
        })
    }
}

impl<'de> Deserialize<'de> for ServerRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = ServerRow;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("array of server columns")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let (address, port) = get_json!("endpoint", parse_address, seq);
                let icao = get_json!("icao", IcaoCode::from_str, seq);
                let locality = seq
                    .next_element()?
                    .ok_or(de::Error::missing_field("locality"))?;
                let tokens = get_json!("tokens", deserialize_token_set, seq);

                // Ignore the rest of the elements, if we don't we'll leave
                // the deserializer with tokens that will cause an error
                while let Some(Ignore) = seq.next_element()? {}

                Ok(ServerRow {
                    address,
                    port,
                    icao,
                    locality,
                    tokens,
                })
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}

pub struct Ignore;

impl<'de> de::Visitor<'de> for Ignore {
    type Value = Self;

    fn expecting(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }

    #[inline]
    fn visit_bool<E>(self, _x: bool) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_i64<E>(self, _x: i64) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_i128<E>(self, _x: i128) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_u64<E>(self, _x: u64) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_u128<E>(self, _x: u128) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_f64<E>(self, _x: f64) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_str<E>(self, _s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Self)
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Self::deserialize(deserializer)
    }

    #[inline]
    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Self::deserialize(deserializer)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Self)
    }

    #[inline]
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        while let Some(Self) = seq.next_element()? {}
        Ok(Self)
    }

    #[inline]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        while let Some((Self, Self)) = map.next_entry()? {}
        Ok(Self)
    }

    #[inline]
    fn visit_bytes<E>(self, _bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Self)
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        use serde::de::VariantAccess;
        data.variant::<Self>()?.1.newtype_variant()
    }
}

impl<'de> Deserialize<'de> for Ignore {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_ignored_any(Self)
    }
}
