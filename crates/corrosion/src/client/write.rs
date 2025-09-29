//! Serialization of queries and transactions sent to a corrosion agent

use crate::{
    Peer,
    api::{SqliteParam, Statement},
};
use quilkin_types::{AddressKind, Endpoint, IcaoCode, TokenSet};

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

impl ToSqlParam for Endpoint {
    fn to_sql(&self) -> SqliteParam {
        SqliteParam::Text(to_compact_str(self))
    }
}

impl ToSqlParam for std::net::Ipv6Addr {
    fn to_sql(&self) -> SqliteParam {
        SqliteParam::Text(compact_str::format_compact!("{self}"))
    }
}

#[inline]
fn to_compact_str(ep: &Endpoint) -> compact_str::CompactString {
    use std::fmt::Write as _;

    let mut cs = compact_str::CompactString::default();

    match &ep.address {
        AddressKind::Name(hn) => {
            cs.push_str(hn);
        }
        AddressKind::Ip(ip) => {
            // Put a | which is invalid in both hostnames and IPs so that
            // when parsing we can easily distinguish IPs from hostnames
            write!(&mut cs, "|{ip}").unwrap();
        }
    }

    write!(&mut cs, ":{}", ep.port).unwrap();
    cs
}

pub struct Server<'s, const N: usize> {
    pub peer: Peer,
    pub statements: &'s mut smallvec::SmallVec<[Statement; N]>,
}

impl<'s, const N: usize> Server<'s, N> {
    #[inline]
    pub fn for_peer(peer: Peer, statements: &'s mut smallvec::SmallVec<[Statement; N]>) -> Self {
        Self { peer, statements }
    }

    /// Create a statement to insert a new server
    #[inline]
    pub fn upsert(&mut self, endpoint: &Endpoint, icao: IcaoCode, tokens: &TokenSet) {
        let mut params = Vec::with_capacity(4);

        params.push(endpoint.to_sql());
        params.push(icao.to_sql());
        params.push(tokens.to_sql());

        let peer_ip = self.peer.ip().to_string();

        self.statements.push(Statement::WithParams(
            format!("INSERT INTO servers (endpoint,icao,tokens,contributors,cont_update) VALUES (?,?,?,jsonb('{{\"{peer_ip}\":{{}}}}'),unixepoch('now'))
             ON CONFLICT(endpoint) DO UPDATE SET
                contributors = jsonb_patch(contributors,'{{\"{peer_ip}\":{{}}}}'),
                cont_update = unixepoch('now')
             WHERE excluded.icao = servers.icao"),
            params,
        ));

        let server = endpoint.address.to_string();

        self.statements.push(Statement::WithParams(
            format!(
                "INSERT INTO dc (ip,port,icao,servers) VALUES (?,?,?,jsonb('{{\"{server}\":{{}}}}'))
            ON CONFLICT(ip) DO UPDATE SET
                servers = jsonb_patch(servers,'{{\"{server}\":{{}}}}')
            WHERE excluded.icao = dc.icao"
            ),
            vec![peer_ip.into(), self.peer.port().into(), icao.to_sql()],
        ));
    }

    /// Create a statement to remove the specified server immediately
    ///
    /// Unlike [`Self::remove_deferred`], deletion will occur regardless of how
    /// many contributors there are to the server
    #[inline]
    pub fn remove_immediate(&mut self, endpoint: &Endpoint) {
        self.statements.push(Statement::WithParams(
            "DELETE FROM servers WHERE rowid = (SELECT MIN(rowid) FROM servers WHERE endpoint = ?)"
                .into(),
            vec![endpoint.to_sql()],
        ));

        let server = endpoint.address.to_string();

        self.statements.push(Statement::WithParams(
            format!("UPDATE dc SET servers = jsonb_patch(servers, '{{\"{server}\":null}}') WHERE rowid = (SELECT MIN(rowid) FROM dc WHERE ip = ?)"),
            vec![self.peer.ip().to_string().into()]
        ));
    }

    /// Create a statement to remove the peer as a contributor to the server
    ///
    /// This method won't immediately delete the server like [`Self::remove_immediately`]
    /// but will update it to remove the specified peer as a contributor. The server
    /// can be removed later if it no longer has contributors after a specified
    /// time period.
    #[inline]
    pub fn remove_deferred(&mut self, endpoint: &Endpoint) {
        let peer_ip = self.peer.ip().to_string();

        self.statements.push(Statement::WithParams(
            format!(
                "UPDATE servers SET
                contributors = jsonb_patch(contributors,'{{\"{peer_ip}\":null}}'),
                cont_update = unixepoch('now')
            WHERE rowid = (SELECT MIN(rowid) FROM servers WHERE endpoint = ?)"
            ),
            vec![endpoint.to_sql()],
        ));

        let server = to_compact_str(endpoint);

        self.statements.push(Statement::WithParams(
            format!("UPDATE dc SET servers = jsonb_patch(servers, '{{\"{server}\":null}}') WHERE rowid = (SELECT MIN(rowid) FROM dc WHERE ip = ?)"),
            vec![peer_ip.into()]
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
        params.push(update.ep.to_sql());

        self.statements.push(Statement::WithParams(query, params));
    }

    /// Create a statement to remove servers with no contributors whose last
    /// update was older
    ///
    /// Note that unlike the other methods, the peer for this does not matter
    #[inline]
    pub fn reap_old(&mut self, max_age: std::time::Duration) {
        self.statements.push(Statement::Simple(format!(
            "DELETE FROM servers WHERE length(contributors) <= 1 AND unixepoch('now') - cont_update > {}", max_age.as_secs()
        )));
    }
}

pub struct UpdateBuilder<'s> {
    ep: &'s Endpoint,
    icao: Option<IcaoCode>,
    tokens: Option<&'s TokenSet>,
}

impl<'s> UpdateBuilder<'s> {
    #[inline]
    pub fn new(ep: &'s Endpoint) -> Self {
        Self {
            ep,
            icao: None,
            tokens: None,
        }
    }

    #[inline]
    pub fn update_icao(mut self, icao: IcaoCode) -> Self {
        self.icao = Some(icao);
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
        if self.tokens.is_some() {
            count += 1
        }
        count
    }
}

impl ToSqlParam for Peer {
    fn to_sql(&self) -> SqliteParam {
        use std::fmt::Write as _;
        let mut cs = compact_str::CompactString::default();
        write!(&mut cs, "{}", self.ip()).unwrap();
        SqliteParam::Text(cs)
    }
}

pub struct Datacenter<'s, const N: usize>(pub &'s mut smallvec::SmallVec<[Statement; N]>);

impl<'s, const N: usize> Datacenter<'s, N> {
    #[inline]
    pub fn insert(&mut self, peer: Peer, qcmp: u16, icao: IcaoCode) {
        let mut params = Vec::with_capacity(3);

        params.push(peer.to_sql());
        params.push(SqliteParam::Integer(qcmp as _));
        params.push(icao.to_sql());

        self.0.push(Statement::WithParams(
            "INSERT INTO dc (ip,port,icao,servers) VALUES (?,?,?,jsonb('{}'))".into(),
            params,
        ));
    }

    /// Create a statement to remove the specified peer
    ///
    /// The peer is also removed as a contributor for all servers it still knows
    /// of, to be cleaned up later if the server has no contributors
    ///
    /// The time for the update can be specified, defaulting to `UtcDateTime::now`
    /// if not specified
    #[inline]
    pub fn remove(&mut self, peer: Peer, update_time: Option<time::UtcDateTime>) {
        let time = update_time.unwrap_or(time::UtcDateTime::now());

        self.0.push(Statement::Simple(format!(
            "WITH sj AS (SELECT server.key FROM dc JOIN json_each(dc.servers) AS server WHERE ip = '{0}' LIMIT 1)
            UPDATE servers SET
                contributors = jsonb_patch(s.contributors,'{{\"{0}\":null}}'),
                cont_update = {1}
            FROM servers s
            LEFT JOIN sj ON s.endpoint = sj.key", peer.ip(), time.unix_timestamp()
        )));

        self.0.push(Statement::WithParams(
            "DELETE FROM dc WHERE rowid = (SELECT MIN(rowid) FROM dc WHERE ip = ?)".into(),
            vec![peer.to_sql()],
        ));
    }

    /// Create a statement to update one or more datacenter columns
    pub fn update(&mut self, peer: Peer, port: Option<u16>, icao: Option<IcaoCode>) {
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

        query.push_str(" WHERE rowid = (SELECT MIN(rowid) FROM dc WHERE ip = ?)");
        params.push(peer.to_sql());

        self.0.push(Statement::WithParams(query, params));
    }
}

pub struct Filter<'s, const N: usize>(pub &'s mut smallvec::SmallVec<[Statement; N]>);

impl<'s, const N: usize> Filter<'s, N> {
    #[inline]
    pub fn upsert(&mut self, filter: &str) {
        self.0.push(Statement::WithParams(
            "INSERT INTO filter (id,filter) VALUES (9999,?) ON CONFLICT(id) DO UPDATE SET filter = excluded.filter".into(),
            vec![SqliteParam::Text(filter.into())]
        ));
    }
}
