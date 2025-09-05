use crate::api::SqliteParam;
use quilkin_types::IcaoCode;

pub type TokenSet = std::collections::BTreeSet<Vec<u8>>;

pub struct TokenSetColumn(pub TokenSet);

impl<'de> serde::Deserialize<'de> for TokenSetColumn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Option::<std::borrow::Cow<'de, str>>::deserialize(deserializer)?;

        let mut ts = TokenSet::new();
        let Some(s) = s else {
            return Ok(Self(ts));
        };

        let mut tokens = data_encoding::BASE64_NOPAD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;

        if tokens.is_empty() {
            return Ok(Self(ts));
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
                if len > toks.len() - 1 {
                    return Err(serde::de::Error::invalid_length(
                        toks.len() - 1,
                        &"token length is longer than remaining binary slice",
                    ));
                }

                ts.insert(toks[1..1 + len].to_vec());
                toks = &toks[1 + len..];
            }
        } else {
            tokens.remove(0);
            ts.insert(tokens);
        }

        Ok(Self(ts))
    }
}
