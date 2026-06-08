//! HTTP provider — exposes a CRUD API for managing game server endpoints and
//! the active filter chain over plain HTTP/JSON.
//!
//! # Endpoints
//!
//! | Method   | Path                     | Description                                                           |
//! |----------|--------------------------|-----------------------------------------------------------------------|
//! | `GET`    | `/endpoints`             | List all current endpoints                                            |
//! | `POST`   | `/endpoints`             | Upsert (add or replace) one endpoint                                  |
//! | `PUT`    | `/endpoints`             | Replace the entire endpoint set atomically (bulk upsert)              |
//! | `DELETE` | `/endpoints`             | Remove all endpoints; or, with a JSON array body, remove those listed |
//! | `POST`   | `/endpoints/bulk`       | Upsert multiple endpoints without touching others (bulk add)          |
//! | `DELETE` | `/endpoints/{address}`   | Remove the endpoint at `{address}`                                    |
//! | `GET`    | `/filterchain`           | Get the current filter chain                                          |
//! | `PUT`    | `/filterchain`           | Replace the filter chain                                              |
//! | `DELETE` | `/filterchain`           | Reset the filter chain to empty                                       |

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing,
};

use crate::{
    config,
    config::filter::FilterChainConfig,
    filters::FilterChain,
    net::{ClusterMap, endpoint::Endpoint},
    providers::FiltersAndClusters,
};

pub const DEFAULT_PORT: u16 = 9000;

#[derive(Clone)]
struct HttpState {
    filters: FilterChainConfig,
    clusters: config::Watch<ClusterMap>,
}

impl HttpState {
    fn router(self) -> axum::Router {
        axum::Router::new()
            .route(
                "/endpoints",
                routing::get(list_endpoints)
                    .post(upsert_endpoint)
                    .put(replace_endpoints)
                    .delete(remove_endpoints),
            )
            .route("/endpoints/bulk", routing::post(bulk_add_endpoints))
            .route("/endpoints/{address}", routing::delete(remove_endpoint))
            .route(
                "/filterchain",
                routing::get(get_filterchain)
                    .put(replace_filterchain)
                    .delete(clear_filterchain),
            )
            .with_state(self)
    }
}

async fn list_endpoints(State(state): State<HttpState>) -> Json<Vec<Endpoint>> {
    let clusters = state.clusters.read();
    let endpoints: Vec<Endpoint> = clusters
        .get(&None)
        .map(|es| es.endpoint_iter().collect())
        .unwrap_or_default();
    Json(endpoints)
}

async fn upsert_endpoint(
    State(state): State<HttpState>,
    Json(endpoint): Json<Endpoint>,
) -> StatusCode {
    state.clusters.modify(|clusters| {
        // Collect first to release the DashMap read lock before calling insert_default.
        let mut endpoints: BTreeSet<Endpoint> = clusters
            .get(&None)
            .map(|es| es.endpoint_iter().collect())
            .unwrap_or_default();
        endpoints.replace(endpoint);
        clusters.insert_default(endpoints);
    });
    StatusCode::OK
}

/// `POST /endpoints/bulk` — upsert multiple endpoints without touching others.
async fn bulk_add_endpoints(
    State(state): State<HttpState>,
    Json(incoming): Json<Vec<Endpoint>>,
) -> StatusCode {
    state.clusters.modify(|clusters| {
        let mut endpoints: BTreeSet<Endpoint> = clusters
            .get(&None)
            .map(|es| es.endpoint_iter().collect())
            .unwrap_or_default();
        for ep in incoming {
            endpoints.replace(ep);
        }
        clusters.insert_default(endpoints);
    });
    StatusCode::OK
}

async fn remove_endpoint(
    State(state): State<HttpState>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let Ok(addr) = address.parse::<crate::net::endpoint::EndpointAddress>() else {
        return StatusCode::BAD_REQUEST;
    };
    state.clusters.modify(|clusters| {
        let endpoints: BTreeSet<Endpoint> = clusters
            .get(&None)
            .map(|es| es.endpoint_iter().filter(|ep| ep.address != addr).collect())
            .unwrap_or_default();
        clusters.insert_default(endpoints);
    });
    StatusCode::OK
}

/// `PUT /endpoints` — replace the entire endpoint set atomically.
async fn replace_endpoints(
    State(state): State<HttpState>,
    Json(endpoints): Json<Vec<Endpoint>>,
) -> StatusCode {
    let endpoints: BTreeSet<Endpoint> = endpoints.into_iter().collect();
    state.clusters.modify(|clusters| {
        clusters.insert_default(endpoints);
    });
    StatusCode::OK
}

/// `DELETE /endpoints` — remove all endpoints, or just the listed ones.
///
/// * No body → clears the entire set.
/// * JSON body `["ip:port", ...]` → removes only those addresses, leaving
///   all others intact.
async fn remove_endpoints(
    State(state): State<HttpState>,
    addrs: Option<Json<Vec<crate::net::endpoint::EndpointAddress>>>,
) -> StatusCode {
    state.clusters.modify(|clusters| match addrs {
        None => {
            clusters.insert_default(BTreeSet::new());
        }
        Some(Json(addrs)) => {
            let to_remove: std::collections::HashSet<_> = addrs.into_iter().collect();
            let remaining: BTreeSet<Endpoint> = clusters
                .get(&None)
                .map(|es| {
                    es.endpoint_iter()
                        .filter(|ep| !to_remove.contains(&ep.address))
                        .collect()
                })
                .unwrap_or_default();
            clusters.insert_default(remaining);
        }
    });
    StatusCode::OK
}

async fn get_filterchain(State(state): State<HttpState>) -> Json<Arc<FilterChain>> {
    Json(state.filters.load().clone())
}

async fn replace_filterchain(
    State(state): State<HttpState>,
    Json(chain): Json<FilterChain>,
) -> StatusCode {
    state.filters.store(chain);
    StatusCode::OK
}

async fn clear_filterchain(State(state): State<HttpState>) -> StatusCode {
    state.filters.store(FilterChain::default());
    StatusCode::OK
}

/// Builds the HTTP provider axum router from a [`FiltersAndClusters`].
///
/// Exposed for benchmarking and integration testing; normal usage goes through [`serve`].
pub fn make_router(fc: FiltersAndClusters) -> axum::Router {
    HttpState {
        filters: fc.filters,
        clusters: fc.clusters,
    }
    .router()
}

/// Runs the HTTP provider server, updating `clusters` and `filters` in response to incoming
/// requests. Sets `health_check` to `true` once the listener is bound.
pub async fn serve(
    fc: FiltersAndClusters,
    address: SocketAddr,
    health_check: Arc<AtomicBool>,
) -> crate::Result<()> {
    let state = HttpState {
        filters: fc.filters,
        clusters: fc.clusters,
    };

    let router = state.router();
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "HTTP provider listening");
    health_check.store(true, Ordering::SeqCst);

    quilkin_system::net::http::serve("http_provider", listener, router, std::future::pending())
        .await
        .map_err(eyre::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use pretty_assertions::assert_eq;

    fn make_server() -> (TestServer, crate::Config) {
        // Enable the HTTP provider so that init_config inserts both FilterChain
        // and ClusterMap into the typemap.
        let providers = crate::Providers::default().http();
        let mut service = crate::Service::default();
        let config = crate::Config::new(None, Default::default(), &providers, &mut service);

        let fc = FiltersAndClusters::new(&config).unwrap();
        let state = HttpState {
            filters: fc.filters,
            clusters: fc.clusters,
        };
        let server = TestServer::new(state.router()).unwrap();
        (server, config)
    }

    // ── endpoints ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_endpoints_empty() {
        let (server, _cfg) = make_server();
        let resp = server.get("/endpoints").await;
        resp.assert_status_ok();
        let body: Vec<Endpoint> = resp.json();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn upsert_then_list() {
        let (server, _cfg) = make_server();
        let ep = Endpoint::new("127.0.0.1:1234".parse().unwrap());

        server.post("/endpoints").json(&ep).await.assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 1);
        assert_eq!(body[0].address, ep.address);
    }

    #[tokio::test]
    async fn upsert_updates_metadata() {
        let (server, _cfg) = make_server();
        let addr: crate::net::endpoint::EndpointAddress = "127.0.0.1:9999".parse().unwrap();

        let ep1 = Endpoint::with_metadata(
            addr.clone(),
            crate::net::endpoint::Metadata {
                tokens: [b"token_a".to_vec()].into(),
            },
        );
        server
            .post("/endpoints")
            .json(&ep1)
            .await
            .assert_status_ok();

        // Same address, different metadata — should replace.
        let ep2 = Endpoint::with_metadata(
            addr,
            crate::net::endpoint::Metadata {
                tokens: [b"token_b".to_vec()].into(),
            },
        );
        server
            .post("/endpoints")
            .json(&ep2)
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 1, "upsert should not duplicate the endpoint");
        assert_eq!(body[0].metadata.known.tokens, ep2.metadata.known.tokens);
    }

    #[tokio::test]
    async fn upsert_multiple_then_list() {
        let (server, _cfg) = make_server();
        let ep1 = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        let ep2 = Endpoint::new("127.0.0.1:2222".parse().unwrap());

        server
            .post("/endpoints")
            .json(&ep1)
            .await
            .assert_status_ok();
        server
            .post("/endpoints")
            .json(&ep2)
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 2);
    }

    #[tokio::test]
    async fn bulk_add_does_not_remove_existing() {
        let (server, _cfg) = make_server();
        let ep1 = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        let ep2 = Endpoint::new("127.0.0.1:2222".parse().unwrap());
        let ep3 = Endpoint::new("127.0.0.1:3333".parse().unwrap());

        server
            .post("/endpoints")
            .json(&ep1)
            .await
            .assert_status_ok();

        // Bulk-add ep2 and ep3 — ep1 must still be present.
        server
            .post("/endpoints/bulk")
            .json(&[&ep2, &ep3])
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 3);
    }

    #[tokio::test]
    async fn remove_specific_endpoint() {
        let (server, _cfg) = make_server();
        let ep1 = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        let ep2 = Endpoint::new("127.0.0.1:2222".parse().unwrap());

        server
            .post("/endpoints")
            .json(&ep1)
            .await
            .assert_status_ok();
        server
            .post("/endpoints")
            .json(&ep2)
            .await
            .assert_status_ok();

        server
            .delete("/endpoints/127.0.0.1:1111")
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 1);
        assert_eq!(body[0].address, ep2.address);
    }

    #[tokio::test]
    async fn remove_nonexistent_endpoint_is_ok() {
        let (server, _cfg) = make_server();
        server
            .delete("/endpoints/10.0.0.1:5000")
            .await
            .assert_status_ok();
    }

    #[tokio::test]
    async fn remove_endpoint_bad_address_returns_400() {
        let (server, _cfg) = make_server();
        server
            .delete("/endpoints/not-a-valid-address")
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn clear_endpoints() {
        let (server, _cfg) = make_server();
        let ep = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        server.post("/endpoints").json(&ep).await.assert_status_ok();

        server.delete("/endpoints").await.assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn replace_all_endpoints() {
        let (server, _cfg) = make_server();
        let ep1 = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        let ep2 = Endpoint::new("127.0.0.1:2222".parse().unwrap());
        let ep3 = Endpoint::new("127.0.0.1:3333".parse().unwrap());

        server
            .post("/endpoints")
            .json(&ep1)
            .await
            .assert_status_ok();
        server
            .post("/endpoints")
            .json(&ep2)
            .await
            .assert_status_ok();

        // PUT replaces the entire set — ep1/ep2 disappear, ep3 takes their place.
        server
            .put("/endpoints")
            .json(&[&ep3])
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 1);
        assert_eq!(body[0].address, ep3.address);
    }

    #[tokio::test]
    async fn replace_with_empty_clears_all() {
        let (server, _cfg) = make_server();
        let ep = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        server.post("/endpoints").json(&ep).await.assert_status_ok();

        server
            .put("/endpoints")
            .json(&Vec::<Endpoint>::new())
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn bulk_remove_specific_endpoints() {
        let (server, _cfg) = make_server();
        let ep1 = Endpoint::new("127.0.0.1:1111".parse().unwrap());
        let ep2 = Endpoint::new("127.0.0.1:2222".parse().unwrap());
        let ep3 = Endpoint::new("127.0.0.1:3333".parse().unwrap());

        for ep in [&ep1, &ep2, &ep3] {
            server.post("/endpoints").json(ep).await.assert_status_ok();
        }

        // Remove ep1 and ep3 in one DELETE call; ep2 must survive.
        let to_remove = vec![ep1.address.to_string(), ep3.address.to_string()];
        server
            .delete("/endpoints")
            .json(&to_remove)
            .await
            .assert_status_ok();

        let body: Vec<Endpoint> = server.get("/endpoints").await.json();
        assert_eq!(body.len(), 1);
        assert_eq!(body[0].address, ep2.address);
    }

    // ── filter chain ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_filterchain_returns_ok() {
        let (server, _cfg) = make_server();
        server.get("/filterchain").await.assert_status_ok();
    }

    #[tokio::test]
    async fn replace_filterchain_with_empty() {
        let (server, _cfg) = make_server();
        let chain = FilterChain::default();
        server
            .put("/filterchain")
            .json(&chain)
            .await
            .assert_status_ok();
        server.get("/filterchain").await.assert_status_ok();
    }

    #[tokio::test]
    async fn clear_filterchain() {
        let (server, _cfg) = make_server();
        server.delete("/filterchain").await.assert_status_ok();
        server.get("/filterchain").await.assert_status_ok();
    }

    // ── shared state ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn http_state_shares_underlying_config() {
        let (server, config) = make_server();

        // Add an endpoint via HTTP.
        let ep = Endpoint::new("127.0.0.1:4242".parse().unwrap());
        server.post("/endpoints").json(&ep).await.assert_status_ok();

        // The change should be visible in the shared Config.
        let clusters = config.dyn_cfg.clusters().unwrap().read();
        let stored: Vec<Endpoint> = clusters
            .get(&None)
            .map(|es| es.endpoint_iter().collect())
            .unwrap_or_default();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].address, ep.address);
    }
}
