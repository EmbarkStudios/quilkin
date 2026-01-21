use std::collections::BTreeSet;

#[derive(Default, Debug, PartialEq, Clone, PartialOrd, Eq, Hash, schemars::JsonSchema)]
pub struct TokenSet(pub BTreeSet<Vec<u8>>);

impl TokenSet {
    #[inline]
    pub fn iter(&self) -> std::collections::btree_set::Iter<'_, Vec<u8>> {
        self.0.iter()
    }
}

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

impl<const N: usize, const N2: usize> From<[[u8; N]; N2]> for TokenSet {
    fn from(value: [[u8; N]; N2]) -> Self {
        value.into_iter().map(|v| v.to_vec()).collect()
    }
}

impl<const N: usize> From<[Vec<u8>; N]> for TokenSet {
    fn from(value: [Vec<u8>; N]) -> Self {
        value.into_iter().collect()
    }
}

impl serde::Serialize for TokenSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_seq(self.0.iter().map(|tok| data_encoding::BASE64.encode(tok)))
        } else {
            serializer.collect_seq(self.0.iter())
        }
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

        if deserializer.is_human_readable() {
            deserializer.deserialize_seq(TokenVisitor)
        } else {
            Ok(Self(BTreeSet::<Vec<u8>>::deserialize(deserializer)?))
        }
    }
}
