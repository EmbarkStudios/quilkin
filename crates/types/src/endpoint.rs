use std::{fmt, net::IpAddr};

/// The kind of address, such as Domain Name or IP address. **Note** that
/// the `FromStr` implementation doesn't actually validate that the name is
/// resolvable. Use [`EndpointAddress`] for complete address validation.
#[derive(
    Debug, PartialEq, Clone, PartialOrd, Eq, Hash, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(untagged)]
pub enum AddressKind {
    Name(String),
    Ip(IpAddr),
}

impl std::str::FromStr for AddressKind {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // check for wrapping "[..]" in an ipv6 host
        let mut host = s.to_string();
        let len = host.len();
        if len > 2 && s.starts_with('[') && s.ends_with(']') {
            host = host[1..len - 1].to_string();
        }

        Ok(host
            .parse()
            .map_or_else(|_err| Self::Name(s.to_owned()), Self::Ip))
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
