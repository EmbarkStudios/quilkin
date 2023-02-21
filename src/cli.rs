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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::crate_version;
use tokio::{signal, sync::watch};

use crate::{admin::Mode, Config};

pub use self::{
    generate_config_schema::GenerateConfigSchema, manage::Manage, proxy::Proxy, relay::Relay,
};

macro_rules! define_port {
    ($port:expr) => {
        pub const PORT: u16 = $port;

        pub fn default_port() -> u16 {
            PORT
        }
    };
}

pub mod generate_config_schema;
pub mod manage;
pub mod proxy;
pub mod relay;

const ETC_CONFIG_PATH: &str = "/etc/quilkin/quilkin.yaml";
const PORT_ENV_VAR: &str = "QUILKIN_PORT";

/// The Command-Line Interface for Quilkin.
#[derive(Debug, clap::Parser)]
#[command(version)]
#[non_exhaustive]
pub struct Cli {
    /// Whether to spawn the admin server or not.
    #[clap(env, long)]
    pub no_admin: bool,
    /// The path to the configuration file for the Quilkin instance.
    #[clap(short, long, env = "QUILKIN_CONFIG", default_value = "quilkin.yaml")]
    pub config: PathBuf,
    /// The port to bind for the admin server
    #[clap(long, env = "QUILKIN_ADMIN_ADDRESS")]
    pub admin_address: Option<std::net::SocketAddr>,
    /// Whether Quilkin will report any results to stdout/stderr.
    #[clap(short, long, env)]
    pub quiet: bool,
    #[clap(subcommand)]
    pub command: Commands,
}

/// The various Quilkin commands.
#[derive(Clone, Debug, clap::Subcommand)]
pub enum Commands {
    Proxy(Proxy),
    GenerateConfigSchema(GenerateConfigSchema),
    Manage(Manage),
    Relay(Relay),
}

impl Commands {
    pub fn admin_mode(&self) -> Option<Mode> {
        match self {
            Self::Proxy(_) => Some(Mode::Proxy),
            Self::Relay(_) | Self::Manage(_) => Some(Mode::Xds),
            Self::GenerateConfigSchema(_) => None,
        }
    }
}

impl Cli {
    /// Drives the main quilkin application lifecycle using the command line
    /// arguments.
    #[tracing::instrument(skip_all)]
    pub async fn drive(self) -> crate::Result<()> {
        if !self.quiet {
            let env_filter = tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy();
            tracing_subscriber::fmt()
                .json()
                .with_file(true)
                .with_env_filter(env_filter)
                .init();
        }

        tracing::info!(
            version = crate_version!(),
            commit = crate::metadata::build::GIT_COMMIT_HASH,
            "Starting Quilkin"
        );

        tracing::debug!(cli = ?self, "config parameters");

        let config = Arc::new(Self::read_config(self.config)?);
        let _admin_task = self
            .command
            .admin_mode()
            .filter(|_| !self.no_admin)
            .map(|mode| {
                tokio::spawn(crate::admin::server(
                    mode,
                    config.clone(),
                    self.admin_address,
                ))
            });

        let (shutdown_tx, mut shutdown_rx) = watch::channel::<()>(());

        #[cfg(target_os = "linux")]
        let mut sig_term_fut = signal::unix::signal(signal::unix::SignalKind::terminate())?;

        tokio::spawn(async move {
            #[cfg(target_os = "linux")]
            let sig_term = sig_term_fut.recv();
            #[cfg(not(target_os = "linux"))]
            let sig_term = std::future::pending();

            let signal = tokio::select! {
                _ = signal::ctrl_c() => "SIGINT",
                _ = sig_term => "SIGTERM",
            };

            tracing::info!(%signal, "shutting down from signal");
            // Don't unwrap in order to ensure that we execute
            // any subsequent shutdown tasks.
            shutdown_tx.send(()).ok();
        });

        let fut = tryhard::retry_fn({
            let shutdown_rx = shutdown_rx.clone();
            move || match self.command.clone() {
                Commands::Proxy(runner) => {
                    let config = config.clone();
                    let shutdown_rx = shutdown_rx.clone();
                    tokio::spawn(
                        async move { runner.run(config.clone(), shutdown_rx.clone()).await },
                    )
                }
                Commands::Manage(manager) => {
                    let config = config.clone();
                    tokio::spawn(async move { manager.manage(config.clone()).await })
                }
                Commands::GenerateConfigSchema(generator) => {
                    tokio::spawn(std::future::ready(generator.generate_config_schema()))
                }
                Commands::Relay(relay) => {
                    let config = config.clone();
                    tokio::spawn(async move { relay.relay(config).await })
                }
            }
        })
        .retries(3)
        .on_retry(|_, _, error| {
            let error = error.to_string();
            async move {
                tracing::warn!(%error, "error would have caused fatal crash");
            }
        });

        tokio::select! {
            result = fut => result?,
            _ = shutdown_rx.changed() => Ok(())
        }
    }

    /// Searches for the configuration file, and panics if not found.
    fn read_config<A: AsRef<Path>>(path: A) -> Result<Config, eyre::Error> {
        let path = path.as_ref();
        let from_reader = |file| Config::from_reader(file).map_err(From::from);

        match std::fs::File::open(path) {
            Ok(file) => (from_reader)(file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(path=%path.display(), "provided path not found");
                match cfg!(unix).then(|| std::fs::File::open(ETC_CONFIG_PATH)) {
                    Some(Ok(file)) => (from_reader)(file),
                    Some(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!(path=%path.display(), "/etc path not found");
                        Ok(Config::default())
                    }
                    Some(Err(error)) => Err(error.into()),
                    None => Ok(Config::default()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    use tokio::{
        net::UdpSocket,
        time::{timeout, Duration},
    };

    use crate::{
        cluster::ClusterMap,
        config::{Filter, Providers},
        endpoint::{Endpoint, LocalityEndpoints},
        filters::{Capture, StaticFilter, TokenRouter},
    };

    #[tokio::test]
    async fn relay_routing() {
        let server_port = crate::test_utils::available_addr().await.port();
        let server_socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, server_port))
            .await
            .map(Arc::new)
            .unwrap();
        let filters_file = tempfile::NamedTempFile::new().unwrap();

        std::fs::write(filters_file.path(), {
            let config = Config::default();
            config.filters.store(
                vec![
                    Filter {
                        name: Capture::factory().name().into(),
                        config: Some(serde_json::json!({
                            "suffix": {
                                "size": 3,
                                "remove": true,
                            }
                        })),
                    },
                    Filter {
                        name: TokenRouter::factory().name().into(),
                        config: None,
                    },
                ]
                .try_into()
                .map(Arc::new)
                .unwrap(),
            );
            serde_yaml::to_string(&config).unwrap()
        })
        .unwrap();

        let endpoints_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(endpoints_file.path(), {
            let mut config = Config::default();
            config.clusters = crate::config::Watch::new(ClusterMap::new_with_default_cluster(
                LocalityEndpoints::from(vec![Endpoint::with_metadata(
                    (std::net::Ipv4Addr::LOCALHOST, server_port).into(),
                    crate::endpoint::Metadata {
                        tokens: vec!["abc".into()].into_iter().collect(),
                    },
                )]),
            ));
            serde_yaml::to_string(&config).unwrap()
        })
        .unwrap();

        let relay_admin_port = crate::test_utils::available_addr().await.port();
        let relay = Cli {
            admin_address: Some((Ipv4Addr::LOCALHOST, relay_admin_port).into()),
            config: <_>::default(),
            no_admin: false,
            quiet: true,
            command: Commands::Relay(Relay {
                providers: Some(Providers::File {
                    path: filters_file.path().to_path_buf(),
                }),
                ..<_>::default()
            }),
        };

        let control_plane_admin_port = crate::test_utils::available_addr().await.port();
        let control_plane = Cli {
            no_admin: false,
            quiet: true,
            admin_address: Some((Ipv4Addr::LOCALHOST, control_plane_admin_port).into()),
            config: <_>::default(),
            command: Commands::Manage(Manage {
                relay: vec!["http://localhost:7900".parse().unwrap()],
                port: 7801,
                region: None,
                sub_zone: None,
                zone: None,
                provider: Providers::File {
                    path: endpoints_file.path().to_path_buf(),
                },
            }),
        };

        let proxy_admin_port = crate::test_utils::available_addr().await.port();
        let proxy = Cli {
            no_admin: false,
            quiet: true,
            admin_address: Some((Ipv4Addr::LOCALHOST, proxy_admin_port).into()),
            config: <_>::default(),
            command: Commands::Proxy(Proxy {
                management_server: vec!["http://localhost:7800".parse().unwrap()],
                ..<_>::default()
            }),
        };

        tokio::spawn(relay.drive());
        tokio::spawn(control_plane.drive());
        tokio::time::sleep(Duration::from_millis(500)).await;
        tokio::spawn(proxy.drive());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let local_addr = crate::test_utils::available_addr().await;
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, local_addr.port()))
            .await
            .map(Arc::new)
            .unwrap();
        let msg = b"helloabc";
        socket
            .send_to(msg, &(std::net::Ipv4Addr::LOCALHOST, 7777))
            .await
            .unwrap();

        let recv = |socket: Arc<UdpSocket>| async move {
            let mut buf = [0; u16::MAX as usize];
            let length = socket.recv(&mut buf).await.unwrap();
            buf[0..length].to_vec()
        };

        assert_eq!(
            b"hello",
            &&*timeout(Duration::from_secs(5), (recv)(server_socket.clone()))
                .await
                .expect("should have received a packet")
        );

        // send an invalid packet
        let msg = b"helloxyz";
        socket.send_to(msg, &local_addr).await.unwrap();

        let result = timeout(Duration::from_secs(3), (recv)(server_socket.clone())).await;
        assert!(result.is_err(), "should not have received a packet");
    }
}
