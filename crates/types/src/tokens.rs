use std::collections::BTreeSet;

#[derive(Default, Debug, PartialEq, Clone, PartialOrd, Eq, Hash, schemars::JsonSchema)]
pub struct TokenSet(BTreeSet<Vec<u8>>);

impl IntoIterator for TokenSet {
    type IntoIter = std::collections::btree_set::IntoIter<Vec<u8>>;
    type Item = Vec<u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<Vec<u8>> for TokenSet {
    fn from_iter<T: IntoIterator<Item = Vec<u8>>>(iter: T) -> Self {
        Self(BTreeSet::from_iter(iter))
    }
}

impl serde::Serialize for TokenSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.iter().map(|tok| data_encoding::BASE64.encode(tok)))
    }
}

impl<'de> serde::Deserialize<'de> for TokenSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TokenVisitor;

        impl<'de> serde::de::Visitor<'de> for TokenVisitor {
            type Value = TokenSet;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an array of base64 encoded tokens")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut set = BTreeSet::default();

                while let Some(token) = seq.next_element::<std::borrow::Cow<'_, str>>()? {
                    let decoded = data_encoding::BASE64
                        .decode(token.as_bytes())
                        .map_err(serde::de::Error::custom)?;

                    if !set.insert(decoded) {
                        return Err(serde::de::Error::custom(
                            "Found duplicate tokens in endpoint metadata.",
                        ));
                    }
                }

                Ok(TokenSet(set))
            }
        }

        deserializer.deserialize_seq(TokenVisitor)
    }
}
