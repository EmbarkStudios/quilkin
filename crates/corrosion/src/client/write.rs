use crate::api::{SqliteParam, Statement};
use quilkin_types::{AddressKind, IcaoCode, TokenSet};
use std::net::IpAddr;

pub trait ToSqlParam {
    fn to_sql(&self) -> SqliteParam;
}

pub type Statements<const N: usize> = smallvec::SmallVec<[Statement; N]>;

impl ToSqlParam for TokenSet {
    /// Converts a token set to a SQL parameter
    ///
    /// Due to the limitations imposed on us via JSON (binary data is cumbersome) and SQLite (no arrays)
    /// we base64 a custom encoding for token sets
    fn to_sql(&self) -> SqliteParam {
        const MAX_TOKENS: usize = u8::MAX as usize >> 1;
        let tokens = &self.0;
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
}

impl ToSqlParam for IcaoCode {
    fn to_sql(&self) -> SqliteParam {
        SqliteParam::Text(self.as_ref().into())
    }
}

impl<'stack> ToSqlParam for (&'stack AddressKind, u16) {
    fn to_sql(&self) -> SqliteParam {
        use std::fmt::Write as _;

        let mut cs = compact_str::CompactString::default();

        match self.0 {
            AddressKind::Name(hn) => {
                cs.push_str(hn);
            }
            AddressKind::Ip(ip) => {
                // Put a | which is invalid in both hostnames and IPs so that
                // when parsing we can easily distinguish IPs from hostnames
                write!(&mut cs, "|{ip}").unwrap();
            }
        }

        write!(&mut cs, ":{}", self.1).unwrap();
        SqliteParam::Text(cs)
    }
}

pub struct Server<'s, const N: usize>(pub &'s mut smallvec::SmallVec<[Statement; N]>);

impl<'s, const N: usize> Server<'s, N> {
    /// Create a statement to insert a new server
    #[inline]
    pub fn insert(
        &mut self,
        address: &AddressKind,
        port: u16,
        icao: IcaoCode,
        locality: Option<&str>,
        tokens: &TokenSet,
    ) {
        let mut params = Vec::with_capacity(4);

        params.push((address, port).to_sql());
        params.push(icao.to_sql());
        params.push(locality.map_or(SqliteParam::Null, |s| SqliteParam::Text(s.into())));
        params.push(tokens.to_sql());

        self.0.push(Statement::WithParams(
            "INSERT INTO servers (endpoint,icao,locality,tokens) VALUES (?,?,?,?)".into(),
            params,
        ));
    }

    /// Create a statement to remove the specified server
    #[inline]
    pub fn remove(&mut self, address: &AddressKind, port: u16) {
        self.0.push(Statement::WithParams(
            "DELETE FROM servers WHERE rowid = (SELECT MIN(rowid) FROM servers WHERE endpoint = ?)"
                .into(),
            vec![(address, port).to_sql()],
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
            params.push(ts.to_sql());
        }

        // We know we are only updating one row, so ideally we would just stick
        // LIMIT 1 at the end...unfortunately we can't. SQLite only supports LIMIT
        // on UPDATE queries when built with `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`
        // ...but that doesn't work https://github.com/rusqlite/rusqlite/issues/1111
        query.push_str(" WHERE rowid = (SELECT MIN(rowid) FROM servers WHERE endpoint = ?)");
        params.push((update.addr, update.port).to_sql());

        self.0.push(Statement::WithParams(query, params));
    }
}

pub struct UpdateBuilder<'s> {
    addr: &'s AddressKind,
    port: u16,
    icao: Option<IcaoCode>,
    locality: Option<&'s str>,
    tokens: Option<&'s TokenSet>,
}

impl<'s> UpdateBuilder<'s> {
    #[inline]
    pub fn new(addr: &'s AddressKind, port: u16) -> Self {
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
    pub fn update_tokens(mut self, ts: &'s TokenSet) -> Self {
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

impl ToSqlParam for IpAddr {
    fn to_sql(&self) -> SqliteParam {
        use std::fmt::Write as _;
        let mut cs = compact_str::CompactString::default();
        write!(&mut cs, "{self}").unwrap();
        SqliteParam::Text(cs)
    }
}

pub struct Datacenter<'s, const N: usize>(pub &'s mut smallvec::SmallVec<[Statement; N]>);

impl<'s, const N: usize> Datacenter<'s, N> {
    #[inline]
    pub fn insert(&mut self, ip: IpAddr, qcmp: u16, icao: IcaoCode) {
        let mut params = Vec::with_capacity(3);

        params.push(ip.to_sql());
        params.push(SqliteParam::Integer(qcmp as _));
        params.push(icao.to_sql());

        self.0.push(Statement::WithParams(
            "INSERT INTO dc (ip,port,icao) VALUES (?,?,?)".into(),
            params,
        ));
    }

    /// Create a statement to remove the specified server
    #[inline]
    pub fn remove(&mut self, ip: IpAddr) {
        self.0.push(Statement::WithParams(
            "DELETE FROM dc WHERE ip = ? LIMIT 1".into(),
            vec![ip.to_sql()],
        ));
    }

    /// Create a statement to update one or more server columns
    pub fn update(&mut self, ip: IpAddr, port: Option<u16>, icao: Option<IcaoCode>) {
        debug_assert!(port.is_some() || icao.is_some());

        let mut query = String::with_capacity(128);
        query.push_str("UPDATE dc SET ");

        let mut params = Vec::with_capacity(3);

        if let Some(port) = port {
            query.push_str("port = ?");
            params.push(SqliteParam::Integer(port as _));
        }

        if let Some(icao) = icao {
            if !params.is_empty() {
                query.push_str(", ");
            }
            query.push_str("icao = ?");
            params.push(icao.to_sql());
        }

        query.push_str(" WHERE endpoint = ? LIMIT 1");
        params.push(ip.to_sql());

        self.0.push(Statement::WithParams(query, params));
    }
}

pub struct Filter<'s, const N: usize>(pub &'s mut smallvec::SmallVec<[Statement; N]>);

impl<'s, const N: usize> Filter<'s, N> {
    #[inline]
    pub fn upsert(&mut self, filter: &str) {
        self.0.push(Statement::WithParams(
            "INSERT INTO filter (id,filter) VALUES (9999,?) ON CONFLICT(id) DO UPDATE SET filter=excluded.filter".into(),
            vec![SqliteParam::Text(filter.into())]
        ));
    }
}
