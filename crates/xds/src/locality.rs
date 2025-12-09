/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

type FixedBuffer = fixedstr::Flexstr<64>;
const SEP: char = ':';

/// The location of an `Endpoint`.
#[derive(Clone, Default, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct Locality {
    /// Internal buffer with the full string
    buffer: FixedBuffer,
    /// End offset of the geographic region portion
    region: usize,
    /// End offset of the zone within the region, if applicable
    zone: Option<NonZeroUsize>,
}

impl Locality {
    pub fn new(region: impl AsRef<str>, zone: impl AsRef<str>, sub_zone: impl AsRef<str>) -> Self {
        let mut buffer = FixedBuffer::new();
        buffer.push_str(region.as_ref());
        let region = buffer.len();

        let zone = zone.as_ref();
        let zone = if !zone.is_empty() {
            buffer.push_char(SEP);
            buffer.push_str(zone.as_ref());
            let zone_offset = buffer.len();

            let sub = sub_zone.as_ref();
            if !sub.is_empty() {
                buffer.push_char(SEP);
                buffer.push_str(sub.as_ref());
            }

            NonZeroUsize::new(zone_offset)
        } else {
            None
        };

        Self {
            buffer,
            region,
            zone,
        }
    }

    pub fn with_region(region: impl AsRef<str>) -> Self {
        let region = region.as_ref();
        Self {
            buffer: region.into(),
            region: region.len(),
            zone: None,
        }
    }

    #[inline]
    pub fn colon_separated_string(&self) -> String {
        self.buffer.as_str().to_owned()
    }

    #[inline]
    pub fn region(&self) -> &str {
        &self.buffer[..self.region]
    }

    #[inline]
    pub fn zone(&self) -> Option<&str> {
        self.zone.map(|z| &self.buffer[self.region + 1..z.get()])
    }

    #[inline]
    pub fn sub_zone(&self) -> Option<&str> {
        self.zone.and_then(|z| {
            let o = z.get() + 1;
            (o < self.buffer.len()).then(|| &self.buffer[o..])
        })
    }
}

impl std::fmt::Display for Locality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.buffer.as_str())
    }
}

impl std::str::FromStr for Locality {
    type Err = eyre::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() {
            return Err(eyre::eyre!("region not specified"));
        }

        let mut iter = input.split(':');

        let Some(region) = iter.next().filter(|r| !r.is_empty()) else {
            return Err(eyre::eyre!("region not specified"));
        };

        let region = region.len();

        let zone = iter.next().and_then(|z| {
            (!z.is_empty())
                .then_some(region + 1 + z.len())
                .and_then(NonZeroUsize::new)
        });
        let _subzone = iter.next();

        if let Some(invalid) = iter.next() {
            return Err(eyre::eyre!(
                "locality identifier '{input}' had more than 3 components, '{invalid}' is not a region, zone, or subzone"
            ));
        }

        Ok(Self {
            buffer: input.into(),
            region,
            zone,
        })
    }
}

impl From<crate::proto::Locality> for Locality {
    #[inline]
    fn from(value: crate::proto::Locality) -> Self {
        Self::new(value.region, value.zone, value.sub_zone)
    }
}

impl From<Locality> for crate::proto::Locality {
    #[inline]
    fn from(value: Locality) -> Self {
        Self {
            region: value.region().to_owned(),
            zone: value.zone().unwrap_or_default().to_owned(),
            sub_zone: value.sub_zone().unwrap_or_default().to_owned(),
        }
    }
}

impl Serialize for Locality {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.buffer)
    }
}

impl<'de> Deserialize<'de> for Locality {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LocalityVisitor;

        impl<'de> serde::de::Visitor<'de> for LocalityVisitor {
            type Value = Locality;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a Locality identifier")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse().map_err(|err| E::custom(err))
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(v)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_any(LocalityVisitor)
    }
}

impl schemars::JsonSchema for Locality {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        String::schema_name()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        String::schema_id()
    }

    fn json_schema(sg: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        String::json_schema(sg)
    }

    fn _schemars_private_non_optional_json_schema(
        sg: &mut schemars::generate::SchemaGenerator,
    ) -> schemars::Schema {
        String::_schemars_private_non_optional_json_schema(sg)
    }

    fn _schemars_private_is_option() -> bool {
        String::_schemars_private_is_option()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn locality() {
        let components = [
            ("region", None, None),
            ("region1", Some("zone"), None),
            ("region2", Some("zone1"), Some("subzone")),
        ];

        for comp in components {
            let string = {
                let mut s = String::new();
                s.push_str(comp.0);

                if let Some(z) = comp.1 {
                    s.push(SEP);
                    s.push_str(z);

                    if let Some(sz) = comp.2 {
                        s.push(SEP);
                        s.push_str(sz);
                    }
                }

                s
            };

            let parsed: Locality = string.parse().unwrap();
            assert_eq!(parsed.to_string(), string);

            assert_eq!(comp.0, parsed.region());
            assert_eq!(comp.1, parsed.zone());
            assert_eq!(comp.2, parsed.sub_zone());
        }
    }

    #[test]
    fn parse_fails_invalid() {
        assert!("".parse::<Locality>().is_err());
        assert!(":".parse::<Locality>().is_err());
        assert!("::".parse::<Locality>().is_err());
        assert!("region:zone:subzone:invalid".parse::<Locality>().is_err());

        assert!("region::".parse::<Locality>().unwrap().zone().is_none());
    }
}
