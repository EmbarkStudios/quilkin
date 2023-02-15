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

use futures::TryFutureExt;

define_port!(7800);

/// Runs Quilkin as a xDS management server, using `provider` as
/// a configuration source.
#[derive(clap::Args, Clone)]
pub struct Manage {
    /// One or more `quilkin relay` endpoints to push configuration changes to.
    #[clap(short, long, env = "QUILKIN_MANAGEMENT_SERVER")]
    pub relay: Vec<tonic::transport::Endpoint>,
    /// The TCP port to listen to, to serve discovery responses.
    #[clap(short, long, env = super::PORT_ENV_VAR, default_value_t = PORT)]
    port: u16,
    /// The `region` to set in the cluster map for any provider
    /// endpoints discovered.
    #[clap(long, env = "QUILKIN_REGION")]
    region: Option<String>,
    /// The `zone` in the `region` to set in the cluster map for any provider
    /// endpoints discovered.
    #[clap(long, env = "QUILKIN_ZONE")]
    zone: Option<String>,
    /// The `sub_zone` in the `zone` in the `region` to set in the cluster map
    /// for any provider endpoints discovered.
    #[clap(long, env = "QUILKIN_SUB_ZONE")]
    sub_zone: Option<String>,
    /// The configuration source for a management server.
    #[clap(subcommand)]
    pub provider: crate::config::Providers,
}

impl Manage {
    pub async fn manage(&self, config: std::sync::Arc<crate::Config>) -> crate::Result<()> {
        let locality = (self.region.is_some() || self.zone.is_some() || self.sub_zone.is_some())
            .then(|| crate::endpoint::Locality {
                region: self.region.clone().unwrap_or_default(),
                zone: self.zone.clone().unwrap_or_default(),
                sub_zone: self.sub_zone.clone().unwrap_or_default(),
            });

        if let Some(locality) = &locality {
            config
                .clusters
                .modify(|map| map.update_unlocated_endpoints(locality));
        }

        let provider_task = self.provider.spawn(config.clone(), locality.clone());

        let _relay_stream = if !self.relay.is_empty() {
            tracing::info!("connecting to relay server");
            let client = crate::xds::client::MdsClient::connect(
                String::clone(&config.id.load()),
                self.relay.clone(),
            )
            .await?;
            Some(client.mds_client_stream(config.clone()))
        } else {
            None
        };

        let server_task = tokio::spawn(crate::xds::server::spawn(self.port, config))
            .map_err(From::from)
            .and_then(std::future::ready);

        tokio::select! {
            result = server_task => result,
            result = provider_task => result?,
        }
    }
}
