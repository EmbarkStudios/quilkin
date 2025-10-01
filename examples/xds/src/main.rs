use std::{collections::BTreeSet, sync::Arc};
use xds::{config::DeltaDiscoveryRes, discovery::Resource as XdsResource};

const TYPE: &str = "example.type";

struct ClientConfig {
    items: std::sync::RwLock<BTreeSet<String>>,
    watch: tokio::sync::broadcast::Sender<()>,
}

impl ClientConfig {
    fn new() -> Self {
        Self {
            items: std::sync::RwLock::new(Default::default()),
            watch: tokio::sync::broadcast::channel(10).0,
        }
    }
}

impl xds::config::Configuration for ClientConfig {
    fn identifier(&self) -> String {
        "client".into()
    }

    fn allow_request_processing(&self, resource_type: &str) -> bool {
        resource_type == TYPE
    }

    fn apply_delta(
        &self,
        type_url: &str,
        resources: Vec<XdsResource>,
        removed_resources: &[String],
        remote_addr: Option<std::net::SocketAddr>,
    ) -> xds::Result<()> {
        eyre::ensure!(type_url == TYPE, "invalid type");

        if resources.is_empty() && removed_resources.is_empty() {
            return Ok(());
        }

        let mut items = self.items.write().unwrap();
        for removed in removed_resources {
            items.remove(removed.as_str());
        }

        for res in resources {
            items.insert(res.name);
        }

        self.watch.send(()).unwrap();
        Ok(())
    }

    fn delta_discovery_request(
        &self,
        client_state: &xds::config::ClientState,
    ) -> xds::Result<DeltaDiscoveryRes> {
        unreachable!();
    }

    fn interested_resources(
        &self,
        _server_version: &str,
    ) -> impl Iterator<Item = (&'static str, Vec<String>)> {
        [].into_iter()
    }

    fn on_changed(
        &self,
        control_plane: xds::server::ControlPlane<Self>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        async move {
            unreachable!();
        }
    }
}

struct ServerConfig {
    items: std::sync::RwLock<BTreeSet<String>>,
    watch: tokio::sync::broadcast::Sender<()>,
}

impl ServerConfig {
    fn new() -> Self {
        Self {
            items: std::sync::RwLock::new(Default::default()),
            watch: tokio::sync::broadcast::channel(10).0,
        }
    }

    fn add_string(&self, item: String) {
        if self.items.write().unwrap().insert(item) {
            let _ = self.watch.send(());
        }
    }

    fn remove_string(&self, item: &str) {
        if self.items.write().unwrap().remove(item) {
            let _ = self.watch.send(());
        }
    }
}

impl xds::config::Configuration for ServerConfig {
    fn identifier(&self) -> String {
        "server".into()
    }

    fn allow_request_processing(&self, resource_type: &str) -> bool {
        resource_type == TYPE
    }

    fn apply_delta(
        &self,
        type_url: &str,
        resources: Vec<XdsResource>,
        removed_resources: &[String],
        remote_addr: Option<std::net::SocketAddr>,
    ) -> xds::Result<()> {
        unreachable!("this does not apply deltas");
    }

    fn delta_discovery_request(
        &self,
        client_state: &xds::config::ClientState,
    ) -> xds::Result<DeltaDiscoveryRes> {
        let mut resources = Vec::new();
        let mut removed = std::collections::HashSet::new();

        eyre::ensure!(client_state.resource_type == TYPE, "unknown resource type");

        let items = self.items.read().unwrap();

        for cur in client_state.versions.keys() {
            if !items.contains(cur) {
                removed.insert(cur.to_owned());
            }
        }

        for item in items.iter() {
            if !client_state.versions.contains_key(item) {
                resources.push(XdsResource {
                    name: item.into(),
                    version: "0".into(),
                    ..Default::default()
                });
            }
        }

        Ok(DeltaDiscoveryRes { resources, removed })
    }

    fn interested_resources(
        &self,
        _server_version: &str,
    ) -> impl Iterator<Item = (&'static str, Vec<String>)> {
        [(TYPE, Vec::new())].into_iter()
    }

    fn on_changed(
        &self,
        control_plane: xds::server::ControlPlane<Self>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut item_watcher = self.watch.subscribe();
        println!("server watching for updates");

        async move {
            loop {
                if item_watcher.recv().await.is_err() {
                    break;
                };
                control_plane.push_update(TYPE);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    {
        use tracing_subscriber::prelude::*;
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }

    let sc = Arc::new(ServerConfig::new());
    let cc = Arc::new(ClientConfig::new());

    let mut client_watch = cc.watch.subscribe();

    let relay_listener = xds::net::TcpListener::bind(None).unwrap();
    let addr = relay_listener.local_addr();

    let server =
        xds::server::ControlPlane::from_arc(sc.clone(), std::time::Duration::from_secs(60))
            .management_server(relay_listener)
            .unwrap();

    tokio::task::spawn(async move { server.await });

    let client = xds::client::AdsClient::connect(
        "client".into(),
        vec![format!("http://{addr}").try_into().unwrap()],
    )
    .await
    .unwrap();

    let (stx, srx) = tokio::sync::oneshot::channel();

    tokio::task::spawn({
        let cc = cc.clone();
        async move {
            let _stream = client
                .delta_subscribe(
                    cc,
                    Arc::new(std::sync::atomic::AtomicBool::new(true)),
                    None,
                    &[("", &[(TYPE, Vec::new())])],
                )
                .await
                .map_err(|_| "failed to subscribe")
                .unwrap();

            srx.await.unwrap();
        }
    });

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    println!("starting stream");

    let mut expected = std::collections::BTreeSet::new();

    for i in 0..=100 {
        sc.add_string(i.to_string());
        expected.insert(i.to_string());
        client_watch.recv().await.unwrap();

        assert_eq!(expected, *cc.items.read().unwrap());
    }

    for i in 0..=100 {
        sc.remove_string(&i.to_string());
        expected.remove(&i.to_string());
        client_watch.recv().await.unwrap();

        assert_eq!(expected, *cc.items.read().unwrap());
    }

    assert!(cc.items.read().unwrap().is_empty());
    stx.send(());
}
