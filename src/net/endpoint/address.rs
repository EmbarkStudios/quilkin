/*
 * Copyright 2021 Google LLC
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

use std::{
    convert::{TryFrom, TryInto},
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
};

use hickory_resolver::TokioResolver;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub use quilkin_types::AddressKind;

use crate::generated::envoy::config::core::v3::{
    SocketAddress as EnvoySocketAddress, address::Address as EnvoyAddress,
};

/// A valid socket address. This differs from `std::net::SocketAddr`, in that it
/// it supports parsing Domain Names in addition to IP addresses. Domain Names
/// are resolved when `ToSocketAddrs::to_socket_addrs` is called.
#[derive(Debug, PartialEq, Clone, PartialOrd, Ord, Eq, Hash)]
pub struct EndpointAddress {
    /// A valid name or IP address that resolves to a address.
    pub host: AddressKind,
    /// The port of the socket address, if present.
    pub port: u16,
}

impl EndpointAddress {
    pub const UNSPECIFIED: Self = Self {
        host: AddressKind::Ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        port: 0,
    };
    pub const LOCALHOST: Self = Self {
        host: AddressKind::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: 0,
    };
}

impl EndpointAddress {
    /// Returns the port for the endpoint address, or `0` if no port
    /// was specified.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the socket address for the endpoint, resolving any DNS entries
    /// if present.
    pub fn to_socket_addr(&self) -> std::io::Result<SocketAddr> {
        static DNS: Lazy<TokioResolver> =
            Lazy::new(|| TokioResolver::builder_tokio().unwrap().build());

        let ip = match &self.host {
            AddressKind::Ip(ip) => *ip,
            AddressKind::Name(name) => {
                static CACHE: Lazy<crate::collections::ttl::TtlMap<String, IpAddr>> =
                    Lazy::new(<_>::default);

                if let Some(ip) = CACHE.get(name) {
                    **ip
                } else {
                    let handle = tokio::runtime::Handle::current();
                    let set = handle
                        .block_on(DNS.lookup_ip(&**name))?
                        .iter()
                        .collect::<std::collections::HashSet<_>>();

                    let ip = set
                        .iter()
                        .find(|item| matches!(item, IpAddr::V6(_)))
                        .or_else(|| set.iter().find(|item| matches!(item, IpAddr::V4(_))))
                        .copied()
                        .ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::Other, "no ip address found")
                        })?;

                    CACHE.insert(name.clone(), ip);
                    ip
                }
            }
        };

        Ok(SocketAddr::from((ip, self.port)))
    }
}

/// Forwards the deserialisation to use [`std::net::ToSocketAddrs`] instead of
/// [`FromStr`] for validation which allows us to resolve DNS hostnames such as
/// `localhost` or container network names at parse-time.
impl FromStr for EndpointAddress {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = if input.starts_with("udp://") {
            url::Url::parse(input)?
        } else if input.contains("://") {
            return Err(ParseError::NonUdpScheme);
        } else {
            url::Url::parse(&format!("udp://{}", input))?
        };

        if !matches!(url.scheme(), "" | "udp") {
            return Err(ParseError::NonUdpScheme);
        }

        // Check if the URL has a path
        if !url.path().is_empty() && url.path() != "/" {
            return Err(ParseError::PathsNotAllowed);
        }

        let host = url
            .host_str()
            .map(String::from)
            .ok_or(ParseError::EmptyHost)?;
        // Infallible
        let host = host.parse::<AddressKind>().unwrap();
        let port = url.port().ok_or(ParseError::EmptyPort)?;

        Ok(Self { host, port })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("non UDP based URLs are not permitted")]
    NonUdpScheme,
    #[error("hostname is required")]
    EmptyHost,
    #[error("port is required")]
    EmptyPort,
    #[error("parser error: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("No paths allowed in URLs, it must include only the hostname and port")]
    PathsNotAllowed,
}

impl From<SocketAddr> for EndpointAddress {
    fn from(socket: SocketAddr) -> Self {
        Self {
            host: AddressKind::Ip(socket.ip()),
            port: socket.port(),
        }
    }
}

impl From<(IpAddr, u16)> for EndpointAddress {
    fn from((ip, port): (IpAddr, u16)) -> Self {
        Self {
            host: AddressKind::Ip(ip),
            port,
        }
    }
}

impl From<(Ipv4Addr, u16)> for EndpointAddress {
    fn from((ip, port): (Ipv4Addr, u16)) -> Self {
        Self {
            host: AddressKind::Ip(IpAddr::V4(ip)),
            port,
        }
    }
}

impl From<([u8; 4], u16)> for EndpointAddress {
    fn from((ip, port): ([u8; 4], u16)) -> Self {
        Self {
            host: AddressKind::Ip(IpAddr::V4(ip.into())),
            port,
        }
    }
}

impl From<(Ipv6Addr, u16)> for EndpointAddress {
    fn from((ip, port): (Ipv6Addr, u16)) -> Self {
        Self {
            host: AddressKind::Ip(IpAddr::V6(ip)),
            port,
        }
    }
}

impl From<(String, u16)> for EndpointAddress {
    fn from((ip, port): (String, u16)) -> Self {
        Self {
            host: ip.parse().unwrap_or(AddressKind::Name(ip)),
            port,
        }
    }
}

impl From<(AddressKind, u16)> for EndpointAddress {
    fn from((host, port): (AddressKind, u16)) -> Self {
        Self { host, port }
    }
}

impl From<EndpointAddress> for EnvoySocketAddress {
    fn from(address: EndpointAddress) -> Self {
        use crate::net::xds::socket_address::{PortSpecifier, Protocol};

        Self {
            protocol: Protocol::Udp as i32,
            address: address.host.to_string(),
            port_specifier: Some(PortSpecifier::PortValue(u32::from(address.port))),
            ..<_>::default()
        }
    }
}

impl TryFrom<EnvoySocketAddress> for EndpointAddress {
    type Error = eyre::Error;

    fn try_from(value: EnvoySocketAddress) -> Result<Self, Self::Error> {
        use crate::net::xds::socket_address::PortSpecifier;

        let address = Self {
            host: value.address.parse()?,
            port: match value.port_specifier {
                Some(PortSpecifier::PortValue(value)) => value.try_into()?,
                Some(PortSpecifier::NamedPort(_)) => {
                    return Err(eyre::eyre!("named ports are not supported"));
                }
                None => return Err(eyre::eyre!("ports are required")),
            },
        };

        Ok(address)
    }
}

impl From<EndpointAddress> for crate::net::xds::core::Address {
    fn from(address: EndpointAddress) -> Self {
        Self {
            address: Some(address.into()),
        }
    }
}

impl From<EndpointAddress> for EnvoyAddress {
    fn from(address: EndpointAddress) -> Self {
        Self::SocketAddress(address.into())
    }
}

impl TryFrom<EnvoyAddress> for EndpointAddress {
    type Error = eyre::Error;

    fn try_from(value: EnvoyAddress) -> Result<Self, Self::Error> {
        match value {
            EnvoyAddress::SocketAddress(address) => address.try_into(),
            _ => Err(eyre::eyre!("Unsupported Envoy address type.")),
        }
    }
}

impl TryFrom<crate::net::xds::core::Address> for EndpointAddress {
    type Error = eyre::Error;

    fn try_from(value: crate::net::xds::core::Address) -> Result<Self, Self::Error> {
        match value.address {
            Some(address) => Self::try_from(address),
            _ => Err(eyre::eyre!("No address found")),
        }
    }
}

impl TryFrom<crate::generated::envoy::config::endpoint::v3::Endpoint> for EndpointAddress {
    type Error = eyre::Error;

    fn try_from(
        value: crate::generated::envoy::config::endpoint::v3::Endpoint,
    ) -> Result<Self, Self::Error> {
        match value.address {
            Some(address) => Self::try_from(address),
            _ => Err(eyre::eyre!("Missing address in endpoint")),
        }
    }
}

impl fmt::Display for EndpointAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let AddressKind::Ip(IpAddr::V6(ip)) = self.host {
            write!(f, "[{}]:{}", ip, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl<'de> Deserialize<'de> for EndpointAddress {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Accept borrowed or owned strings.
        let string = <std::borrow::Cow<'de, str>>::deserialize(de)?;
        string.parse::<Self>().map_err(serde::de::Error::custom)
    }
}

impl Serialize for EndpointAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_from_string() {
        // ipv
        let endpoint = "127.0.12.1:4567".parse::<EndpointAddress>().unwrap();
        match endpoint.host {
            AddressKind::Name(_) => panic!("Shouldn't be a name"),
            AddressKind::Ip(ip) => assert_eq!("127.0.12.1", ip.to_string()),
        };
        assert_eq!(4567, endpoint.port);

        // ipv6
        let endpoint = "[2345:0425:2ca1:0000:0000:0567:5673:24b5]:25999"
            .parse::<EndpointAddress>()
            .unwrap();
        match endpoint.host {
            AddressKind::Name(_) => panic!("Shouldn't be a name"),
            AddressKind::Ip(ip) => {
                assert_eq!("2345:425:2ca1::567:5673:24b5", ip.to_string());
            }
        };
        assert_eq!(25999, endpoint.port);
    }

    #[test]
    fn address_kind_from_string() {
        let ak = "127.0.12.1".parse::<AddressKind>().unwrap();

        match ak {
            AddressKind::Name(_) => panic!("Shouldn't be a name"),
            AddressKind::Ip(ip) => assert_eq!("127.0.12.1", ip.to_string()),
        }

        // ipv6
        let ak = "[2345:0425:2ca1:0000:0000:0567:5673:24b5]"
            .parse::<AddressKind>()
            .unwrap();
        match ak {
            AddressKind::Name(_) => panic!("Shouldn't be a name"),
            AddressKind::Ip(ip) => {
                assert_eq!("2345:425:2ca1::567:5673:24b5", ip.to_string());
            }
        };

        let ak = "2345:0425:2ca1:0000:0000:0567:5673:24b5"
            .parse::<AddressKind>()
            .unwrap();
        match ak {
            AddressKind::Name(_) => panic!("Shouldn't be a name"),
            AddressKind::Ip(ip) => {
                assert_eq!("2345:425:2ca1::567:5673:24b5", ip.to_string());
            }
        };

        let ak = "my.domain.com".parse::<AddressKind>().unwrap();
        match ak {
            AddressKind::Name(name) => {
                assert_eq!("my.domain.com", name);
            }
            AddressKind::Ip(_) => panic!("shouldn't be an ip"),
        };
    }
}
