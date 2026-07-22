use crate::filters::prelude::*;
use crate::generated::quilkin::filters::game_protocol::v1alpha1 as proto;
use std::net;

pub type ChaChaKey = [u8; 32];

/// Processes Quilkin Game Protocol packets
pub struct GameProtocol {
    config: Config,
}

impl GameProtocol {
    pub fn new(key: ChaChaKey) -> Self {
        Self {
            config: Config { key },
        }
    }
}

const MAGIC: &[u8] = b"QLKN";

const KNOWN_VERSION: Version = 1;
const VERSION: Version = 1;
type Version = u8;

impl GameProtocol {
    fn validate(packet: &[u8]) -> Option<(Version, &[u8])> {
        let magic = packet.get(packet.len() - 4..)?;
        if magic != MAGIC {
            return None;
        }

        let version = *packet.get(packet.len() - 5)?;
        if version > KNOWN_VERSION {
            return None;
        }

        // This could take the version into account to eg. read 2 bytes or whatever instead
        let len = *packet.get(packet.len() - 6)? as usize;
        let start = packet.len() - 6 - len;
        Some((version, &packet[start..start + len]))
    }

    fn decrypt_socket_addr(
        &self,
        addr: &[u8],
        nonce: &[u8],
    ) -> Result<net::SocketAddr, FilterError> {
        use chacha20::{KeyIvInit, cipher::StreamCipher};

        if addr.len() != 6 && addr.len() != 18 {
            return Err(FilterError::Custom(
                "expected socket address of 6 or 18 bytes",
            ));
        }

        let mut data = [0u8; 18];
        data[..addr.len()].copy_from_slice(addr);

        let nonce: [u8; 12] = nonce
            .try_into()
            .map_err(|_| FilterError::Custom("invalid nonce length"))?;

        let mut cipher = chacha20::ChaCha20::new(&self.config.key.into(), &nonce.into());
        cipher.apply_keystream(&mut data[..addr.len()]);

        Self::decode_socket_addr(&data[..addr.len()])
    }

    #[inline]
    fn decode_socket_addr(data: &[u8]) -> Result<net::SocketAddr, FilterError> {
        let port = (data[data.len() - 2] as u16) << 8 | data[data.len() - 1] as u16;

        let addr = match data.len() {
            6 => net::SocketAddr::V4(net::SocketAddrV4::new(
                net::Ipv4Addr::from_octets(data[..4].try_into().unwrap()),
                port,
            )),
            18 => net::SocketAddr::V6(net::SocketAddrV6::new(
                net::Ipv6Addr::from_octets(data[..16].try_into().unwrap()),
                port,
                0,
                0,
            )),
            _ => unreachable!("we've checked this already"),
        };

        Ok(addr)
    }

    fn encode_socket_addr(addr: net::SocketAddr) -> ([u8; 18], usize) {
        let mut addrb = [0u8; 18];

        let len = match addr.ip() {
            net::IpAddr::V4(v4) => {
                addrb[..4].copy_from_slice(&v4.octets());
                6
            }
            net::IpAddr::V6(v6) => {
                addrb[..16].copy_from_slice(&v6.octets());
                18
            }
        };

        addrb[len - 2] = (addr.port() >> 8) as u8;
        addrb[len - 1] = addr.port() as u8;

        (addrb, len)
    }

    #[inline]
    fn append_trailer(pm: &mut impl PacketMut, trailer: &[u8]) {
        let len = trailer.len() as u8;
        pm.extend_tail(trailer);
        pm.extend_tail(&[len, VERSION]);
        pm.extend_tail(MAGIC);
    }
}

fn cbor_to_io(de: minicbor::decode::Error) -> std::io::Error {
    if de.is_end_of_input() {
        return std::io::ErrorKind::UnexpectedEof.into();
    }

    // Unfortunately minicbor hides the message string, so for every other
    // error we just have to fallback to Display :(
    std::io::Error::new(std::io::ErrorKind::Other, de)
}

impl Filter for GameProtocol {
    fn read<P: PacketMut>(&self, ctx: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        let crate::net::endpoint::AddressKind::Ip(client_addr) = ctx.source.host else {
            return Err(FilterError::Custom("client address was a DNS name"));
        };

        {
            let Some((version, data)) = Self::validate(ctx.contents.as_slice()) else {
                return Err(FilterError::Dropped);
            };

            if version != VERSION {
                return Err(FilterError::Dropped);
            }

            let mut decoder = minicbor::Decoder::new(data);

            let addr = decoder.bytes().map_err(cbor_to_io)?;
            let nonce = decoder.bytes().map_err(cbor_to_io)?;

            let addr = self.decrypt_socket_addr(addr, nonce)?;

            ctx.destinations.push(addr.into());
            ctx.contents.remove_tail(data.len() + 6);
        }

        {
            let mut enc = minicbor::Encoder::new(CborData::<32>::new());

            let (addr, len) = Self::encode_socket_addr((client_addr, ctx.source.port).into());

            enc.bytes(&addr[..len]).expect("infallible");

            let data = enc.into_writer().inner;
            Self::append_trailer(&mut ctx.contents, &data);
        }

        Ok(())
    }

    fn write<P: PacketMut>(&self, ctx: &mut WriteContext<P>) -> Result<(), FilterError> {
        let Some((version, data)) = Self::validate(ctx.contents.as_slice()) else {
            return Err(FilterError::Dropped);
        };

        if version != VERSION {
            return Err(FilterError::Dropped);
        }

        let mut decoder = minicbor::Decoder::new(data);

        let addr = decoder.bytes().map_err(cbor_to_io)?;
        ctx.dest = Self::decode_socket_addr(&addr)?.into();
        ctx.contents.remove_tail(data.len() + 6);
        Self::append_trailer(&mut ctx.contents, &[]);

        Ok(())
    }
}

impl StaticFilter for GameProtocol {
    const NAME: &'static str = "quilkin.filters.game_protocol.v1alpha1.GameProtocol";
    type Configuration = Config;
    type BinaryConfiguration = proto::GameProtocol;

    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError> {
        Ok(Self {
            config: Self::ensure_config_exists(config)?,
        })
    }
}

/// `GameProtocol` filter's configuration.
#[derive(serde::Serialize, serde::Deserialize, Debug, schemars::JsonSchema)]
pub struct Config {
    #[serde(with = "key")]
    #[schemars(with = "String")]
    pub key: ChaChaKey,
}

mod key {
    use super::ChaChaKey;
    use serde::{Deserialize, Serialize};

    pub(super) fn deserialize<'de, D>(de: D) -> Result<ChaChaKey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = std::borrow::Cow::<'de, str>::deserialize(de)?;

        crate::codec::base64::decode(string.as_bytes())
            .map_err(serde::de::Error::custom)?
            .try_into()
            .map_err(|_e| serde::de::Error::custom("invalid key, expected 32 bytes"))
    }

    pub(super) fn serialize<S>(value: &ChaChaKey, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        crate::codec::base64::encode(value).serialize(ser)
    }
}

impl From<Config> for proto::GameProtocol {
    fn from(config: Config) -> Self {
        Self {
            key: config.key.into(),
        }
    }
}

impl TryFrom<proto::GameProtocol> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(p: proto::GameProtocol) -> Result<Self, Self::Error> {
        Ok(Self {
            key: p.key.try_into().map_err(|_e| {
                ConvertProtoConfigError::new(
                    "invalid key, expected 32 bytes",
                    Some("private_key".into()),
                )
            })?,
        })
    }
}

struct CborData<const N: usize> {
    inner: smallvec::SmallVec<[u8; N]>,
}

impl<const N: usize> CborData<N> {
    fn new() -> Self {
        Self {
            inner: smallvec::SmallVec::<[u8; N]>::new(),
        }
    }
}

impl<const N: usize> minicbor::encode::Write for CborData<N> {
    type Error = core::convert::Infallible;

    fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.inner.extend_from_slice(buf);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::alloc_buffer;
    use chacha20::{KeyIvInit, cipher::StreamCipher};

    fn write_encrypted_addr(
        buf: &mut bytes::BytesMut,
        key: [u8; 32],
        nonce: [u8; 12],
        addr: net::SocketAddr,
    ) {
        use bytes::BufMut;
        let mut encoder = minicbor::Encoder::new(super::CborData::<32>::new());

        let mut cipher = chacha20::ChaCha20::new(&key.into(), &nonce.into());
        let (mut data, len) = GameProtocol::encode_socket_addr(addr);
        cipher.apply_keystream(&mut data[..len]);
        encoder.bytes(&data[..len]).unwrap();
        encoder.bytes(&nonce).unwrap();
        let v = encoder.into_writer().inner;
        buf.extend_from_slice(&v);
        buf.put_u8(v.len() as _);
        buf.put_u8(VERSION);
        buf.extend_from_slice(MAGIC);
    }

    #[test]
    fn roundtrips() {
        let endpoints = crate::net::cluster::ClusterMap::default();
        let mut dest = Vec::new();

        let key = [0x42u8; 32];
        let nonce = [0x22u8; 12];

        let config = Config { key };
        let filter = GameProtocol::from_config(config.into());

        let clients = [
            net::SocketAddr::from((net::Ipv4Addr::new(1, 2, 3, 4), 79)),
            (net::Ipv6Addr::from_octets([0xce; _]), 788).into(),
        ];
        let servers = [
            net::SocketAddr::from((net::Ipv4Addr::new(99, 98, 97, 96), 4234)),
            (net::Ipv6Addr::from_octets([0xac; _]), 3421).into(),
        ];

        let payload = b"hello";

        for client in clients {
            for server in servers {
                let mut buf = alloc_buffer(payload);
                write_encrypted_addr(&mut buf, key, nonce, server);

                let mut ctx = ReadContext::new(&endpoints, client.into(), buf, &mut dest);
                filter.read(&mut ctx).unwrap();
                let contents = ctx.contents;

                assert_eq!(
                    dest.pop()
                        .expect("expected address")
                        .to_socket_addr()
                        .unwrap(),
                    server
                );

                let mut ctx = WriteContext::new(
                    server.into(),
                    (net::Ipv4Addr::LOCALHOST, 8).into(),
                    contents,
                );
                filter.write(&mut ctx).unwrap();

                assert_eq!(ctx.dest.to_socket_addr().unwrap(), client);
            }
        }
    }
}
