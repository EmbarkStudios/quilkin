/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use once_cell::sync::Lazy;

use super::{DynamicMetadata, Value};

static INTERNER: Lazy<lasso::ThreadedRodeo> = Lazy::new(lasso::ThreadedRodeo::new);

/// A key in the metadata table.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Ord, schemars::JsonSchema)]
pub struct Key(#[schemars(with = "String")] lasso::Spur);

impl Key {
    pub fn new<A: AsRef<str>>(key: A) -> Self {
        Self(INTERNER.get_or_intern(key.as_ref()))
    }

    pub fn from_static(key: &'static str) -> Self {
        Self(INTERNER.get_or_intern_static(key))
    }

    pub fn from_raw(spur: lasso::Spur) -> Self {
        Self(spur)
    }
}

impl From<lasso::Spur> for Key {
    fn from(spur: lasso::Spur) -> Self {
        Self::from_raw(spur)
    }
}

impl From<String> for Key {
    fn from(string: String) -> Self {
        Self::new(string)
    }
}

impl From<&'_ str> for Key {
    fn from(string: &str) -> Self {
        Self::new(string)
    }
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        INTERNER.resolve(&self.0).fmt(f)
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        INTERNER.resolve(&self.0).fmt(f)
    }
}

impl<'de> serde::Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        std::borrow::Cow::<'de, str>::deserialize(deserializer).map(Self::new)
    }
}

impl serde::Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

/// A literal value or a reference to a value in a metadata map.
#[derive(
    Clone,
    Debug,
    PartialEq,
    PartialOrd,
    serde::Serialize,
    serde::Deserialize,
    Eq,
    Ord,
    schemars::JsonSchema,
)]
#[serde(untagged)]
pub enum Symbol {
    Reference(Reference),
    Literal(Value),
}

impl Symbol {
    pub fn as_literal(&self) -> Option<&Value> {
        match self {
            Self::Literal(value) => Some(value),
            Self::Reference(_) => None,
        }
    }

    pub fn as_reference(&self) -> Option<&Reference> {
        match self {
            Self::Literal(_) => None,
            Self::Reference(reference) => Some(reference),
        }
    }

    /// Resolves a symbol into a [`Value`], using `ctx` for any references,
    /// returning `None` if could not be found.
    pub fn resolve<'literal: 'metadata, 'metadata>(
        &'literal self,
        metadata: &'metadata DynamicMetadata,
    ) -> Option<&'metadata Value> {
        match self {
            Self::Literal(value) => Some(value),
            Self::Reference(reference) => {
                let v = metadata.get(&reference.key());
                if v.is_none() {
                    tracing::warn!(key = %self.as_reference().unwrap(), "couldn't resolve key");
                }
                v
            }
        }
    }

    /// Tries to [`Self::resolve`] the symbol to a `bytes::Bytes`, performing
    /// a conversion process on different [`Value`] if relevant. Returning
    /// `None` if it isn't supported currently.
    ///
    /// - [`Value::Bytes`] The value is copied as-is.
    /// - [`Value::String`] The value is interpreted as a base64 string.
    /// - [`Value::Number`] The value is an eight byte number encoded as big endian.
    pub fn resolve_to_bytes<'literal: 'metadata, 'metadata>(
        &'literal self,
        metadata: &'metadata DynamicMetadata,
    ) -> Option<bytes::Bytes> {
        match self.resolve(metadata) {
            Some(Value::Number(value)) => Some(Vec::from(value.to_be_bytes()).into()),
            Some(Value::Bytes(bytes)) => Some(bytes.clone()),
            Some(Value::String(string)) => Some(crate::codec::base64::decode(string).ok()?.into()),
            _ => None,
        }
    }
}

impl From<Value> for Symbol {
    fn from(value: Value) -> Self {
        Self::Literal(value)
    }
}

impl From<Reference> for Symbol {
    fn from(reference: Reference) -> Self {
        Self::Reference(reference)
    }
}

/// Reference to a metadata value.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, schemars::JsonSchema)]
#[schemars(transparent)]
pub struct Reference {
    key: Key,
}

impl Reference {
    pub fn new<A: AsRef<str>>(key: A) -> Self {
        Self {
            key: Key::new(key.as_ref()),
        }
    }

    pub fn key(self) -> Key {
        self.key
    }
}

impl std::fmt::Display for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.key)
    }
}

impl serde::Serialize for Reference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Reference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;

        if let Some(string) = string.strip_prefix('$') {
            Ok(Self::new(string))
        } else {
            Err(<D::Error as serde::de::Error>::custom(
                "references are required to start with `$`",
            ))
        }
    }
}
