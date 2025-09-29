use std::fmt;

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct IcaoCode([u8; 4]);

const VALID_RANGE: std::ops::RangeInclusive<u8> = b'A'..=b'Z';

impl IcaoCode {
    /// Creates a new Icao from raw bytes
    ///
    /// This is meant for testing, and asserts if any of the characters are not valid
    pub fn new_testing(code: [u8; 4]) -> Self {
        for c in code {
            assert!(VALID_RANGE.contains(&c));
        }

        Self(code)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<str> for IcaoCode {
    fn as_ref(&self) -> &str {
        // SAFETY: We don't allow this to be constructed with an invalid utf-8 string
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl Default for IcaoCode {
    fn default() -> Self {
        Self([b'X', b'X', b'X', b'X'])
    }
}

#[derive(Debug)]
pub enum IcaoError {
    InvalidLength { len: usize },
    InvalidCharacter { character: char, index: usize },
}

impl fmt::Display for IcaoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { len } => {
                write!(f, "expected a length of 4 but got a length of {len}")
            }
            Self::InvalidCharacter { character, index } => write!(
                f,
                "invalid character '{character}' was found at index {index}"
            ),
        }
    }
}

impl std::error::Error for IcaoError {
    fn description(&self) -> &str {
        match self {
            Self::InvalidLength { .. } => "the length of the code was invalid",
            Self::InvalidCharacter { .. } => "an invalid character was found",
        }
    }
}

impl<'s> TryFrom<&'s [u8]> for IcaoCode {
    type Error = IcaoError;

    fn try_from(value: &'s [u8]) -> Result<Self, Self::Error> {
        if value.len() != 4 {
            return Err(IcaoError::InvalidLength { len: value.len() });
        }

        for (index, c) in value.iter().enumerate() {
            if !VALID_RANGE.contains(c) {
                return Err(IcaoError::InvalidCharacter {
                    character: *c as char,
                    index,
                });
            }
        }

        let mut c = [0u8; 4];
        c.copy_from_slice(value);

        Ok(Self(c))
    }
}

impl std::str::FromStr for IcaoCode {
    type Err = IcaoError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        const VALID_RANGE: std::ops::RangeInclusive<char> = 'A'..='Z';
        let mut arr = [0; 4];

        if input.len() != 4 {
            return Err(IcaoError::InvalidLength { len: input.len() });
        }

        for (index, character) in input.chars().enumerate() {
            if !VALID_RANGE.contains(&character) {
                return Err(IcaoError::InvalidCharacter { character, index });
            }

            arr[index] = character as u8;
        }

        Ok(Self(arr))
    }
}

impl fmt::Display for IcaoCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl fmt::Debug for IcaoCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl serde::Serialize for IcaoCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> serde::Deserialize<'de> for IcaoCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IcaoVisitor;

        impl<'de> serde::de::Visitor<'de> for IcaoVisitor {
            type Value = IcaoCode;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 4-character, uppercase, alphabetical ASCII ICAO code")
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse().map_err(serde::de::Error::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse().map_err(serde::de::Error::custom)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse().map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_str(IcaoVisitor)
    }
}

impl schemars::JsonSchema for IcaoCode {
    fn schema_name() -> String {
        "IcaoCode".into()
    }

    fn is_referenceable() -> bool {
        false
    }

    fn json_schema(r#gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = r#gen.subschema_for::<String>();
        if let schemars::schema::Schema::Object(schema_object) = &mut schema {
            if schema_object.has_type(schemars::schema::InstanceType::String) {
                let validation = schema_object.string();
                validation.pattern = Some(r"^[A-Z]{4}$".to_string());
            }
        }
        schema
    }
}
