pub type TokenSet = std::collections::BTreeSet<Vec<u8>>;

const MAX_TOKENS: usize = u8::MAX as usize >> 1;

#[inline]
pub fn serialize_tokens_param(tokens: &TokenSet) -> corro_api_types::SqliteParam {
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

    corro_api_types::SqliteParam::Text(data_encoding::BASE64_NOPAD.encode(&blob).into())
}

pub struct TokenSetColumn(pub TokenSet);

impl<'de> serde::Deserialize<'de> for TokenSetColumn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        let mut tokens = data_encoding::BASE64_NOPAD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;

        let mut ts = TokenSet::new();

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
