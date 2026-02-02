use quilkin_types::{Endpoint, IcaoCode, TokenSet};

const HASH_SEED: i64 = 0xdeadbeef;

#[derive(PartialEq, Clone, Debug)]
struct ServerData {
    icao: IcaoCode,
    tokens: TokenSet,
}

#[inline]
fn hash_token(token: &[u8]) -> u64 {
    gxhash::gxhash64(token, HASH_SEED)
}

#[derive(Clone, Debug)]
pub struct Servers {
    /// The complete set of known server endpoints and their current state
    servers: dashmap::DashMap<Endpoint, ServerData, gxhash::GxBuildHasher>,
    /// A mapping of hashed tokens to one or more endpoints
    token_map: dashmap::DashMap<u64, Vec<Endpoint>>,
}

impl typemap_rev::TypeMapKey for Servers {
    type Value = Servers;
}

impl Servers {
    /// Upserts a server
    ///
    /// Either inserts a new server and any associated tokens, or updates the
    /// state of the server and associated tokens if it already exists
    pub fn upsert(&self, server_endpoint: Endpoint, icao: IcaoCode, tokens: TokenSet) {
        if let Some(mut current) = self.servers.get_mut(&server_endpoint) {
            current.icao = icao;

            // Remove tokens that are not in the new set
            for old in current.tokens.0.difference(&tokens.0) {
                let old = hash_token(old);
                self.remove_endpoint_token(old, &server_endpoint);
            }

            // Add any new tokens
            for new in tokens.0.difference(&current.tokens.0) {
                let new = hash_token(new);
                self.insert_endpoint_token(new, server_endpoint.clone());
            }

            current.tokens = tokens;
        } else {
            for tok in tokens.iter() {
                let new = hash_token(tok);
                self.insert_endpoint_token(new, server_endpoint.clone());
            }

            self.servers
                .insert(server_endpoint, ServerData { icao, tokens });
        }
    }

    /// Removes a server
    ///
    /// Also removes the server from the token map
    #[inline]
    pub fn remove(&self, server_endpoint: &Endpoint) -> Option<(IcaoCode, TokenSet)> {
        let (_, sd) = self.servers.remove(server_endpoint)?;

        for tok in &sd.tokens.0 {
            let tok = hash_token(tok);
            self.remove_endpoint_token(tok, server_endpoint);
        }

        Some((sd.icao, sd.tokens))
    }

    #[inline]
    fn remove_endpoint_token(&self, hash: u64, ep: &Endpoint) {
        // Note: keep this scoped! We remove the entry entirely if the token has
        // no more endpoints, but the remove is in the same scope we could deadlock
        {
            let Some(mut eps) = self.token_map.get_mut(&hash) else {
                return;
            };

            let Some(i) = eps.iter().position(|e| e == ep) else {
                tracing::debug!(endpoint = %ep, "failed to find expected endpoint for token");
                return;
            };

            if eps.len() != 1 {
                eps.swap_remove(i);
                return;
            }
        }

        self.token_map.remove(&hash);
    }

    #[inline]
    fn insert_endpoint_token(&self, hash: u64, ep: Endpoint) {
        let mut eps = self.token_map.entry(hash).or_default();

        // The calling code should protect against inserting the same endpoint for
        // a token
        #[cfg(debug_assertions)]
        {
            assert!(
                !eps.iter().any(|curr| curr == &ep),
                "attempted to insert the same endpoint {ep} for a token"
            );
        }

        eps.push(ep);
    }
}

#[cfg(test)]
impl PartialEq for Servers {
    fn eq(&self, rhs: &Self) -> bool {
        for a in self.servers.iter() {
            if !rhs
                .servers
                .get(a.key())
                .is_some_and(|b| a.value() == b.value())
            {
                return false;
            }
        }

        for b in rhs.servers.iter() {
            if !self
                .servers
                .get(b.key())
                .is_some_and(|a| b.value() == a.value())
            {
                return false;
            }
        }

        true
    }
}
