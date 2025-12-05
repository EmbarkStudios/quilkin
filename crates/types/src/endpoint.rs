use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

/// The kind of address, such as Domain Name or IP address. **Note** that
/// the `FromStr` implementation doesn't actually validate that the name is
/// resolvable. Use [`EndpointAddress`] for complete address validation.
#[derive(Debug, PartialEq, Clone, PartialOrd, Eq, Hash, Ord)]
pub enum AddressKind {
    Name(String),
    Ip(IpAddr),
}

impl From<IpAddr> for AddressKind {
    fn from(value: IpAddr) -> Self {
        Self::Ip(value)
    }
}

impl From<Ipv4Addr> for AddressKind {
    fn from(value: Ipv4Addr) -> Self {
        Self::Ip(value.into())
    }
}

impl From<Ipv6Addr> for AddressKind {
    fn from(value: Ipv6Addr) -> Self {
        Self::Ip(value.into())
    }
}

impl From<String> for AddressKind {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl<'s> From<&'s str> for AddressKind {
    fn from(value: &'s str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl std::str::FromStr for AddressKind {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // check for wrapping "[..]" in an ipv6 host
        let mut host = s;
        let len = host.len();
        if len > 2 && s.starts_with('[') && s.ends_with(']') {
            host = &host[1..len - 1];
        }

        Ok(host
            .parse()
            .map_or_else(|_err| Self::Name(s.to_owned()), Self::Ip))
    }
}

impl<'de> serde::Deserialize<'de> for AddressKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        use std::borrow::Cow;

        #[inline]
        fn deserialize_helper<E: serde::de::Error>(s: Cow<'_, str>) -> Result<AddressKind, E> {
            if let Some(ips) = s.strip_prefix('|') {
                let ip = ips.parse::<IpAddr>().map_err(serde::de::Error::custom)?;
                Ok(AddressKind::Ip(ip))
            } else {
                Ok(AddressKind::Name(s.into_owned()))
            }
        }

        impl serde::de::Visitor<'_> for Visitor {
            type Value = AddressKind;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("AddressKind string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                deserialize_helper(Cow::Borrowed(v))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                deserialize_helper(Cow::Owned(v))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl serde::Serialize for AddressKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Ip(ip) => {
                use std::fmt::Write as _;
                let mut cs = compact_str::CompactString::with_capacity(20);
                write!(&mut cs, "|{ip}").expect("unreachable");
                serializer.serialize_str(&cs)
            }
            Self::Name(n) => serializer.serialize_str(n),
        }
    }
}

impl fmt::Display for AddressKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => name.fmt(f),
            Self::Ip(ip) => ip.fmt(f),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct Endpoint {
    #[serde(rename = "a")]
    pub address: AddressKind,
    #[serde(rename = "p")]
    pub port: u16,
}

impl Endpoint {
    #[inline]
    pub fn new(address: AddressKind, port: u16) -> Self {
        Self { address, port }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.address, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_serialization() {
        let name = AddressKind::Name("some-dns-name".to_owned());
        let v4 = Ipv4Addr::new(99, 88, 111, 2).into();
        let v6 = Ipv6Addr::new(6666, 7777, 8888, 9999, 4444, 3333, 2222, 1111).into();

        #[track_caller]
        fn roundtrip(address: AddressKind, port: u16) {
            let expected = Endpoint { address, port };

            let s = serde_json::to_string(&expected).expect("failed to serialize");
            let actual: Endpoint = serde_json::from_str(&s).expect("failed to deserialize");

            assert_eq!(expected, actual);
        }

        roundtrip(name, 1);
        roundtrip(v4, 11111);
        roundtrip(v6, 8888);
    }

    #[test]
    fn parses() {
        let name = AddressKind::Name("some-dns-name".to_owned());
        let v4: AddressKind = Ipv4Addr::new(99, 88, 111, 2).into();
        let v6: AddressKind = Ipv6Addr::new(6666, 7777, 8888, 9999, 4444, 3333, 2222, 1111).into();

        assert_eq!(name, name.to_string().parse().unwrap());
        assert_eq!(v4, v4.to_string().parse().unwrap());
        assert_eq!(v6, v6.to_string().parse().unwrap());
        assert_eq!(v6, format!("[{v6}]").parse().unwrap());
    }
}
