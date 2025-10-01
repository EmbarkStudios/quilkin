use serde::ser::SerializeMap;

use super::*;

impl Config {
    pub fn update_from_json(
        &self,
        map: serde_json::Map<String, serde_json::Value>,
        mut locality: Option<crate::net::endpoint::Locality>,
    ) -> Result<(), eyre::Error> {
        for (k, v) in map {
            match k.as_str() {
                "filters" => {
                    if let Some(filters) = self.dyn_cfg.filters() {
                        filters.store(serde_json::from_value(v)?);
                    } else {
                        tracing::trace!("ignoring FilterChain as it was not in the typemap");
                    }
                }
                "clusters" => {
                    let Some(clusters) = self.dyn_cfg.clusters() else {
                        tracing::trace!("ignoring ClusterMap as it was not in the typemap");
                        continue;
                    };

                    let cmd: cluster::ClusterMapDeser = serde_json::from_value(v)?;
                    tracing::trace!(len = cmd.endpoints.len(), "replacing clusters");
                    clusters.modify(|clusters| {
                        for cluster in cmd.endpoints {
                            clusters.insert(None, cluster.locality, cluster.endpoints);
                        }

                        if let Some(locality) = locality.take() {
                            clusters.update_unlocated_endpoints(None, locality);
                        }
                    });
                    self.apply_metrics();
                }
                "version" | "datacenters" => {
                    // Updating the version doesn't make sense at runtime, and we don't
                    // want to error out

                    // datacenters are only a resource applied from remotes, not
                    // local config files
                }
                "id" => {
                    *self.dyn_cfg.id.lock() = serde_json::from_value(v)?;
                }
                "icao_code" => {
                    self.dyn_cfg.icao_code.store(serde_json::from_value(v)?);
                }
                "qcmp_port" => {
                    if let Some(qp) = self.dyn_cfg.qcmp_port() {
                        qp.store(serde_json::from_value(v)?);
                    } else {
                        tracing::trace!("ignoring QcmpPort as it was not in the typemap");
                    }
                }
                field => {
                    eyre::bail!("unknown field '{field}'");
                }
            }
        }

        Ok(())
    }
}

impl serde::Serialize for Config {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("version", &self.dyn_cfg.version)?;
        map.serialize_entry("id", self.dyn_cfg.id.lock().as_str())?;
        map.serialize_entry("icao_code", &self.dyn_cfg.icao_code.load())?;
        if let Some(qcmp) = self.dyn_cfg.qcmp_port() {
            map.serialize_entry("qcmp_port", &qcmp.load())?;
        }
        if let Some(filters) = self.dyn_cfg.filters() {
            map.serialize_entry("filters", &*filters.load())?;
        }
        if let Some(clusters) = self.dyn_cfg.clusters() {
            map.serialize_entry("clusters", clusters)?;
        }
        if let Some(datacenters) = self.dyn_cfg.datacenters() {
            map.serialize_entry("datacenters", datacenters)?;
        }

        map.end()
    }
}

#[cfg(test)]
mod tests {
    use crate::net::endpoint::{Endpoint, Metadata};
    use serde_json::json;
    use std::net::Ipv6Addr;

    use super::*;

    #[test]
    fn deserialise_client() {
        let providers = Default::default();
        let service = Default::default();
        let config = Config::new(
            Some("deserialize_client".into()),
            Default::default(),
            &providers,
            &service,
        );
        config.dyn_cfg.clusters().unwrap().modify(|clusters| {
            clusters.insert_default([Endpoint::new("127.0.0.1:25999".parse().unwrap())].into());
        });

        serde_yaml::to_string(&config).unwrap();
    }

    #[test]
    fn deserialise_server() {
        let providers = Default::default();
        let service = Default::default();
        let config = Config::new(
            Some("deserialize_server".into()),
            Default::default(),
            &providers,
            &service,
        );
        config.dyn_cfg.clusters().unwrap().modify(|clusters| {
            clusters.insert_default(
                [
                    Endpoint::new("127.0.0.1:26000".parse().unwrap()),
                    Endpoint::new("127.0.0.1:26001".parse().unwrap()),
                ]
                .into(),
            );
        });

        serde_yaml::to_string(&config).unwrap();
    }

    #[test]
    fn parse_default_values() {
        let providers = Default::default();
        let service = Default::default();
        let before = String::from("parse_default_values");
        let config = Config::new(
            Some(before.clone()),
            Default::default(),
            &providers,
            &service,
        );
        assert!(!before.is_empty());
        config
            .update_from_json(
                serde_json::from_value(json!({
                    "version": "v1alpha1",
                     "clusters":[]
                }))
                .unwrap(),
                None,
            )
            .unwrap();

        assert_eq!(before, config.id());
    }

    #[test]
    fn parse_client() {
        let providers = Default::default();
        let service = Default::default();
        let config = Config::new(
            Some("parse_client".into()),
            Default::default(),
            &providers,
            &service,
        );
        config
            .update_from_json(
                serde_json::from_value(json!({
                    "version": "v1alpha1",
                    "clusters": [{
                        "endpoints": [{
                            "address": "127.0.0.1:25999"
                        }],
                    }]
                }))
                .unwrap(),
                None,
            )
            .unwrap();

        let value = config.dyn_cfg.clusters().unwrap().read();
        assert_eq!(
            &*value,
            &ClusterMap::new_default(
                [Endpoint::new((std::net::Ipv4Addr::LOCALHOST, 25999).into(),)].into()
            )
        );
    }

    #[test]
    fn parse_ipv6_endpoint() {
        let providers = Default::default();
        let service = Default::default();
        let config = Config::new(
            Some("parse_ipv6_endpoint".into()),
            Default::default(),
            &providers,
            &service,
        );
        config
            .update_from_json(
                serde_json::from_value(json!({
                    "version": "v1alpha1",
                    "clusters":[{
                        "endpoints": [{
                            "address": "[2345:0425:2CA1:0000:0000:0567:5673:24b5]:25999"
                        }],
                    }]
                }))
                .unwrap(),
                None,
            )
            .unwrap();

        let value = config.dyn_cfg.clusters().unwrap().read();
        assert_eq!(
            &*value,
            &ClusterMap::new_default(
                [Endpoint::new(
                    (
                        "2345:0425:2CA1:0000:0000:0567:5673:24b5"
                            .parse::<Ipv6Addr>()
                            .unwrap(),
                        25999
                    )
                        .into()
                )]
                .into()
            )
        );
    }

    #[test]
    fn parse_server() {
        let providers = Default::default();
        let service = Default::default();
        let config = Config::new(
            Some("parse_server".into()),
            Default::default(),
            &providers,
            &service,
        );
        config
            .update_from_json(
                serde_json::from_value(json!({
                    "version": "v1alpha1",
                    "clusters": [{
                        "endpoints": [
                            {
                                "address" : "127.0.0.1:26000",
                                "metadata": {
                                    "quilkin.dev": {
                                        "tokens": ["MXg3aWp5Ng==", "OGdqM3YyaQ=="],
                                    }
                                }
                            },
                            {
                                "address" : "[2345:0425:2CA1:0000:0000:0567:5673:24b5]:25999",
                                "metadata": {
                                    "quilkin.dev": {
                                        "tokens": ["bmt1eTcweA=="],
                                    }
                                }
                            }
                        ],
                    }]
                }))
                .unwrap(),
                None,
            )
            .unwrap();

        let value = config.dyn_cfg.clusters().unwrap().read();
        assert_eq!(
            &*value,
            &ClusterMap::new_default(
                [
                    Endpoint::with_metadata(
                        "127.0.0.1:26000".parse().unwrap(),
                        Metadata {
                            tokens: vec!["1x7ijy6", "8gj3v2i"]
                                .into_iter()
                                .map(From::from)
                                .collect(),
                        },
                    ),
                    Endpoint::with_metadata(
                        "[2345:0425:2CA1:0000:0000:0567:5673:24b5]:25999"
                            .parse()
                            .unwrap(),
                        Metadata {
                            tokens: vec!["nkuy70x"].into_iter().map(From::from).collect(),
                        },
                    ),
                ]
                .into()
            )
        );
    }

    #[test]
    fn deny_unused_fields() {
        let configs = vec![
            "
version: v1alpha1
foo: bar
clusters:
    - endpoints:
        - address: 127.0.0.1:7001
",
            "
# proxy
version: v1alpha1
foo: bar
id: client-proxy
port: 7000
clusters:
    - endpoints:
        - address: 127.0.0.1:7001
",
            "
# admin
version: v1alpha1
admin:
    foo: bar
    address: 127.0.0.1:7001
",
            "
# static.endpoints
version: v1alpha1
clusters:
    - endpoints:
        - address: 127.0.0.1:7001
          connection_ids:
            - Mxg3aWp5Ng==
",
            "
# static.filters
version: v1alpha1
filters:
  - name: quilkin.core.v1.rate-limiter
    foo: bar
",
            "
# dynamic.management_servers
version: v1alpha1
dynamic:
  management_servers:
    - address: 127.0.0.1:25999
      foo: bar
",
        ];

        let providers = crate::Providers::default();
        let service = crate::Service::default();
        let mut config = Config::new(
            Some("deny_unused_fields".into()),
            Default::default(),
            &providers,
            &service,
        );
        insert_default::<crate::filters::FilterChain>(&mut config.dyn_cfg.typemap);

        for cstr in configs {
            let json = serde_yaml::from_str(cstr).unwrap();
            let result = config.update_from_json(json, None);
            let error = result.unwrap_err();
            println!("here: {}", error);
            assert!(format!("{error:?}").contains("unknown field"));
        }
    }
}
