use crate::{
    api::{SqliteParam, Statement},
    utils,
};
use quilkin_types::{AddressKind, IcaoCode};

pub type Statements<const N: usize> = smallvec::SmallVec<[Statement; N]>;

const MAX_TOKENS: usize = u8::MAX as usize >> 1;

/// Converts a token set to a SQL parameter
///
/// Due to the limitations imposed on us via JSON (binary data is cumbersome) and SQLite (no arrays)
/// we base64 a custom encoding for token sets
#[inline]
pub fn tokens_to_param(tokens: &TokenSet) -> SqliteParam {
    if tokens.is_empty() {
        return SqliteParam::Null;
    }

    let mut blob = smallvec::SmallVec::<[u8; 512]>::new();

    // We could varint encode this instead, but for now just fail
    debug_assert!(
        tokens.len() <= MAX_TOKENS,
        "number of tokens ({}) is more than {MAX_TOKENS}",
        tokens.len()
    );

    let len_prefix = if tokens.len() > 1 {
        // If all the tokens have the same length, and that length is less than
        // MAX_TOKENS, we can skip length prefixing each token
        let len = tokens.first().unwrap().len();
        let same_len = tokens.iter().all(|tok| tok.len() == len);

        if same_len && len <= MAX_TOKENS {
            blob.push(0x80 | len as u8);
        } else {
            blob.push(tokens.len() as u8);
        }

        !same_len
    } else {
        blob.push(1);
        false
    };

    for tok in tokens {
        if len_prefix {
            debug_assert!(
                tok.len() <= u8::MAX as usize,
                "token length {} is more than {}",
                tok.len(),
                u8::MAX
            );

            blob.push(tok.len() as u8);
        }

        blob.extend_from_slice(&tok);
    }

    SqliteParam::Text(data_encoding::BASE64_NOPAD.encode(&blob).into())
}

#[inline]
pub fn icao_to_param(icao: IcaoCode) -> SqliteParam {
    SqliteParam::Text(icao.as_ref().into())
}

pub enum Address<'stack> {
    Hostname(&'stack str),
    Ip(std::net::IpAddr),
}

impl<'stack> Address<'stack> {
    #[inline]
    pub fn into_sql_param(self, port: u16) -> SqliteParam {
        use std::fmt::Write as _;

        let mut cs = compact_str::CompactString::default();

        match self {
            Self::Hostname(hn) => {
                cs.push_str(hn);
            }
            Self::Ip(ip) => {
                write!(&mut cs, "{ip}").unwrap();
            }
        }

        write!(&mut cs, ":{port}").unwrap();
        SqliteParam::Text(cs)
    }
}

pub struct Server<const N: usize>(smallvec::SmallVec<[Statement; N]>);

impl<const N: usize> Server<N> {
    /// Create a statement to insert a new server
    pub fn insert(
        &mut self,
        address: &AddressKind,
        port: u16,
        icao: IcaoCode,
        locality: Option<&str>,
        tokens: &utils::TokenSet,
    ) {
        let mut params = Vec::with_capacity(4);

        params.push(address.into_sql_param(port));
        params.push(utils::icao_to_param(icao));
        params.push(locality.map_or(SqliteParam::Null, |s| SqliteParam::Text(s.into())));
        params.push(utils::tokens_to_param(tokens));

        self.0.push(Statement::WithParams(
            "INSERT INTO servers (endpoint,icao,locality,tokens) VALUES (?,?,?,?)".into(),
            params,
        ));
    }

    /// Create a statement to remove the specified server
    #[inline]
    pub fn remove(&mut self, address: &AddressKind, port: u16) {
        self.0.push(Statement::WithParams(
            "DELETE FROM servers WHERE endpoint = ? LIMIT 1".into(),
            vec![address.into_sql_param(port)],
        ));
    }

    /// Create a statement to update one or more server columns
    pub fn update(&mut self, update: UpdateBuilder<'_>) {
        let mut query = String::with_capacity(128);
        query.push_str("UPDATE servers SET ");

        let mut params = Vec::with_capacity(update.params() + 1);

        if let Some(icao) = update.icao {
            query.push_str("icao = ?");
            params.push(SqliteParam::Text(icao.as_ref().into()));
        }

        if let Some(locality) = update.locality {
            if !params.is_empty() {
                query.push_str(", ");
            }

            query.push_str("locality = ?");
            params.push(SqliteParam::Text(locality.into()));
        }

        if let Some(ts) = update.tokens {
            if !params.is_empty() {
                query.push_str(", ");
            }

            query.push_str("tokens = ?");
            params.push(utils::tokens_to_param(ts));
        }

        query.push_str(" WHERE endpoint = ? LIMIT 1");
        params.push(update.addr.into_sql_param(update.port));

        self.0.push(Statement::WithParams(query, params));
    }
}

pub struct UpdateBuilder<'s> {
    addr: Address<'s>,
    port: u16,
    icao: Option<IcaoCode>,
    locality: Option<&'s str>,
    tokens: Option<&'s crate::utils::TokenSet>,
}

impl<'s> UpdateBuilder<'s> {
    #[inline]
    pub fn new(addr: Address<'s>, port: u16) -> Self {
        Self {
            addr,
            port,
            icao: None,
            locality: None,
            tokens: None,
        }
    }

    #[inline]
    pub fn update_icao(mut self, icao: IcaoCode) -> Self {
        self.icao = Some(icao);
        self
    }

    #[inline]
    pub fn update_locality(mut self, locality: &'s str) -> Self {
        self.locality = Some(locality);
        self
    }

    #[inline]
    pub fn update_tokens(mut self, ts: &'s utils::TokenSet) -> Self {
        self.tokens = Some(ts);
        self
    }

    #[inline]
    fn params(&self) -> usize {
        let mut count = 0;
        if self.icao.is_some() {
            count += 1
        }
        if self.locality.is_some() {
            count += 1
        }
        if self.tokens.is_some() {
            count += 1
        }
        count
    }
}

pub struct Datacenter;

pub struct Filter;
