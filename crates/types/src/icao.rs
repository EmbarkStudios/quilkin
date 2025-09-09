use std::fmt;

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct IcaoCode([u8; 4]);

impl IcaoCode {
    /// Creates a new Icao from raw bytes
    ///
    /// This is meant for testing, and asserts if any of the characters are not valid
    pub fn new_testing(code: [u8; 4]) -> Self {
        const VALID_RANGE: std::ops::RangeInclusive<u8> = b'A'..=b'Z';

        for c in code {
            assert!(VALID_RANGE.contains(&c));
        }

        Self(code)
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

impl std::str::FromStr for IcaoCode {
    type Err = eyre::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        const VALID_RANGE: std::ops::RangeInclusive<char> = 'A'..='Z';
        let mut arr = [0; 4];
        let mut i = 0;

        for c in input.chars() {
            eyre::ensure!(i < 4, "ICAO code is too long");
            eyre::ensure!(
                VALID_RANGE.contains(&c),
                "ICAO code contained invalid character '{c}'"
            );
            arr[i] = c as u8;
            i += 1;
        }

        eyre::ensure!(i == 4, "ICAO code was not long enough");
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
