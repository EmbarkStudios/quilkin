#![cfg(target_pointer_width = "64")]

use divan::Bencher;
use prost_types::{Value, value::Kind};
use rand::SeedableRng;

use quilkin::{net::cluster::proto::Endpoint as ProtoEndpoint, xds::Resource};

mod shared;

#[derive(Default)]
struct Name {
    counter: usize,
}

impl GenAddress for Name {
    fn generate(&mut self, slim: bool) -> (usize, ProtoEndpoint) {
        let ep = if self.counter.is_multiple_of(2) {
            let host = format!("nometa-{}", self.counter);

            quilkin::net::Endpoint::new(quilkin::net::EndpointAddress {
                host: quilkin::net::endpoint::AddressKind::Name(host),
                port: 1000,
            })
        } else {
            let host = format!("meta-{}", self.counter);

            let mut md = std::collections::BTreeMap::new();
            md.insert(
                "counter".to_owned(),
                Value {
                    kind: Some(Kind::NumberValue(self.counter as _)),
                },
            );

            md.insert(
                "tokens".to_owned(),
                Value {
                    kind: Some(Kind::ListValue(prost_types::ListValue {
                        values: vec![
                            Value {
                                kind: Some(Kind::StringValue("abcd".into())),
                            },
                            Value {
                                kind: Some(Kind::StringValue("1234".into())),
                            },
                        ],
                    })),
                },
            );

            quilkin::net::Endpoint::with_metadata(
                quilkin::net::EndpointAddress {
                    host: quilkin::net::endpoint::AddressKind::Name(host),
                    port: 2000,
                },
                quilkin::net::endpoint::EndpointMetadata::try_from(prost_types::Struct {
                    fields: md,
                })
                .unwrap(),
            )
        };

        self.counter += 1;

        let ep = if slim { ep.into_proto() } else { ep.into() };

        use prost::Message;
        (ep.encoded_len(), ep)
    }
}

#[derive(Default)]
struct Ip {
    counter: u128,
}

impl GenAddress for Ip {
    fn generate(&mut self, slim: bool) -> (usize, ProtoEndpoint) {
        let ip = if self.counter.is_multiple_of(2) {
            std::net::Ipv6Addr::new(
                ((self.counter >> 112) & 0xffff) as _,
                ((self.counter >> 96) & 0xffff) as _,
                ((self.counter >> 80) & 0xffff) as _,
                ((self.counter >> 64) & 0xffff) as _,
                ((self.counter >> 48) & 0xffff) as _,
                ((self.counter >> 32) & 0xffff) as _,
                ((self.counter >> 16) & 0xffff) as _,
                (self.counter & 0xffff) as _,
            )
            .into()
        } else {
            std::net::Ipv4Addr::new(
                ((self.counter >> 24) & 0xff) as _,
                ((self.counter >> 16) & 0xff) as _,
                ((self.counter >> 8) & 0xff) as _,
                (self.counter & 0xff) as _,
            )
            .into()
        };

        let ep = quilkin::net::Endpoint::new(quilkin::net::EndpointAddress {
            host: quilkin::net::endpoint::AddressKind::Ip(ip),
            port: 3000,
        });

        self.counter += 1;

        let pep = if slim { ep.into_proto() } else { ep.into() };

        use prost::Message;
        (pep.encoded_len(), pep)
    }
}

trait GenAddress: Default {
    fn generate(&mut self, slim: bool) -> (usize, ProtoEndpoint);
}

#[divan::bench_group(sample_count = 1000)]
mod endpoint {
    use super::*;

    #[divan::bench(
        types = [Name, Ip],
    )]
    fn slower<G: GenAddress>(bencher: Bencher<'_, '_>) {
        let mut genn = G::default();
        bencher
            .with_inputs(|| genn.generate(false))
            .input_counter(|(i, _)| divan::counter::BytesCount::usize(*i))
            .bench_local_values(|(_, ep)| divan::black_box(quilkin::net::Endpoint::try_from(ep)));
    }

    #[divan::bench(
        types = [Name, Ip],
    )]
    fn faster<G: GenAddress>(bencher: Bencher<'_, '_>) {
        let mut genn = G::default();
        bencher
            .with_inputs(|| genn.generate(true))
            .input_counter(|(i, _)| divan::counter::BytesCount::usize(*i))
            .bench_local_values(|(_, ep)| divan::black_box(quilkin::net::Endpoint::from_proto(ep)));
    }
}

trait GenResource: Default {
    fn generate(&mut self, slim: bool) -> prost_types::Any;
}

#[derive(Default)]
struct Listener {
    _counter: usize,
}

impl GenResource for Listener {
    fn generate(&mut self, _slim: bool) -> prost_types::Any {
        use quilkin::filters::{self, StaticFilter};
        let filters = [quilkin::config::filter::Filter {
            name: filters::capture::Capture::NAME.into(),
            label: None,
            config: Some(
                serde_json::to_value(&filters::capture::Config {
                    metadata_key: "boop".into(),
                    strategy: filters::capture::Strategy::Suffix(filters::capture::Suffix {
                        size: 3,
                        remove: true,
                    }),
                })
                .unwrap(),
            ),
        }];

        Resource::FilterChain(quilkin::net::cluster::proto::FilterChain {
            filters: filters
                .into_iter()
                .map(|f| quilkin::net::cluster::proto::Filter {
                    name: f.name,
                    label: f.label,
                    config: f.config.map(|c| c.to_string()),
                })
                .collect(),
        })
        .try_encode()
        .unwrap()
    }
}

#[derive(Default)]
struct Cluster {
    counter: usize,
}

impl GenResource for Cluster {
    fn generate(&mut self, slim: bool) -> prost_types::Any {
        let locality = shared::LOCALITIES[self.counter % shared::LOCALITIES.len()];
        let locality: quilkin::net::endpoint::Locality = locality.parse().unwrap();

        let mut rng = rand::rngs::SmallRng::seed_from_u64(self.counter as u64);
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        let endpoints = shared::gen_endpoints(&mut rng, &mut hasher, None);

        let msg = quilkin::generated::quilkin::config::v1alpha1::Cluster {
            locality: Some(quilkin::generated::quilkin::config::v1alpha1::Locality {
                region: locality.region().into(),
                zone: locality.zone().unwrap_or_default().into(),
                sub_zone: locality.sub_zone().unwrap_or_default().into(),
            }),
            endpoints: endpoints
                .into_iter()
                .map(|ep| if slim { ep.into_proto() } else { ep.into() })
                .collect(),
        };

        Resource::Cluster(msg).try_encode().unwrap()
    }
}

// From Config::apply
fn deserialize(a: prost_types::Any) {
    match Resource::try_decode(a).unwrap() {
        Resource::FilterChain(fc) => {
            let chain: quilkin::filters::FilterChain = if fc.filters.is_empty() {
                Default::default()
            } else {
                quilkin::filters::FilterChain::try_create_fallible(fc.filters).unwrap()
            };

            drop(chain);
        }
        Resource::Datacenter(dc) => {
            let _host: std::net::IpAddr = dc.host.parse().unwrap();
            let _dc = quilkin::config::Datacenter {
                qcmp_port: dc.qcmp_port.try_into().unwrap(),
                icao_code: dc.icao_code.parse().unwrap(),
            };
        }
        Resource::Cluster(cluster) => {
            let _locality: Option<quilkin::net::endpoint::Locality> =
                cluster.locality.map(From::from);
            cluster
                .endpoints
                .into_iter()
                .map(quilkin::net::endpoint::Endpoint::try_from)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        }
    }
}

fn deserialize_faster(a: prost_types::Any) {
    match Resource::try_decode(a).unwrap() {
        Resource::FilterChain(fc) => {
            quilkin::filters::FilterChain::try_create_fallible(fc.filters).unwrap();
        }
        Resource::Datacenter(dc) => {
            let _host: std::net::IpAddr = dc.host.parse().unwrap();
            let _dc = quilkin::config::Datacenter {
                qcmp_port: dc.qcmp_port.try_into().unwrap(),
                icao_code: dc.icao_code.parse().unwrap(),
            };
        }
        Resource::Cluster(cluster) => {
            let _locality: Option<quilkin::net::endpoint::Locality> =
                cluster.locality.map(From::from);
            cluster
                .endpoints
                .into_iter()
                .map(quilkin::net::endpoint::Endpoint::from_proto)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        }
    }
}

#[divan::bench_group(sample_count = 1000)]
mod resource {
    use super::*;

    #[divan::bench(
        types = [Listener, Cluster],
    )]
    fn slower<G: GenResource>(bencher: Bencher<'_, '_>) {
        let mut genn = G::default();
        bencher
            .with_inputs(|| genn.generate(false))
            .input_counter(|a| divan::counter::BytesCount::usize(a.value.len() + a.type_url.len()))
            .bench_local_values(|a| deserialize(divan::black_box(a)));
    }

    #[divan::bench(
        types = [Listener, Cluster],
    )]
    fn faster<G: GenResource>(bencher: Bencher<'_, '_>) {
        let mut genn = G::default();
        bencher
            .with_inputs(|| genn.generate(true))
            .input_counter(|a| divan::counter::BytesCount::usize(a.value.len() + a.type_url.len()))
            .bench_local_values(|a| deserialize_faster(divan::black_box(a)));
    }
}

fn main() {
    divan::main();
}
