/*
 * Copyright 2023 Google LLC
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

mod health;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use axum::{
    extract::State,
    http::{Response, StatusCode},
    response::{IntoResponse, Json},
};
use bytes::Bytes;
use http_body_util::Full;
type Body = Full<Bytes>;

use health::Health;

pub const PORT: u16 = 8000;
pub const PORT_LABEL: &str = "8000";

pub(crate) const IDLE_REQUEST_INTERVAL: Duration = Duration::from_secs(30);

pub fn serve(
    config: Arc<crate::Config>,
    ready: Arc<AtomicBool>,
    shutdown_tx: crate::signal::ShutdownTx,
    address: Option<std::net::SocketAddr>,
) -> std::thread::JoinHandle<()> {
    let address = address.unwrap_or_else(|| (std::net::Ipv6Addr::UNSPECIFIED, PORT).into());
    let health = Health::new(shutdown_tx);
    tracing::info!(address = %address, "Starting admin endpoint");

    let router = Admin {
        config,
        health,
        ready,
    }
    .router();

    std::thread::Builder::new()
        .name("admin-http".into())
        .spawn(move || {
            let runtime = Admin::runtime();
            runtime
                .block_on(async move {
                    let listener = quilkin_system::net::tcp::default_nonblocking_listener(address)?;
                    let tokio_listener = tokio::net::TcpListener::from_std(listener)?;

                    quilkin_system::net::http::serve(
                        "admin",
                        tokio_listener,
                        router,
                        std::future::pending(),
                    )
                    .await
                })
                .expect("failed to serve admin server");
        })
        .expect("failed to spawn admin-http thread")
}

#[derive(Clone)]
struct Admin {
    health: Health,
    config: Arc<crate::Config>,
    ready: Arc<AtomicBool>,
}

#[cfg(target_os = "linux")]
#[derive(serde::Deserialize)]
struct ProfileParams {
    seconds: Option<u64>,
}

impl Admin {
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .thread_name("admin-http-worker")
            .build()
            .expect("couldn't create tokio runtime in thread")
    }

    fn router<S>(self) -> axum::Router<S> {
        let mut router = axum::Router::new()
            .route(
                "/metrics",
                axum::routing::get(|| async { collect_metrics() }),
            )
            .route("/live", axum::routing::get(live))
            .route("/livez", axum::routing::get(live))
            .route("/ready", axum::routing::get(ready))
            .route("/readyz", axum::routing::get(ready))
            .route("/config", axum::routing::get(config));

        cfg_if::cfg_if! {
            if #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))] {
                router = router.route("/debug/pprof/allocs", axum::routing::get(|| async {
                    match handle_get_heap().await {
                        Ok(response) => response,
                        Err((status_code, msg)) => Response::builder()
                            .status(status_code)
                            .body(Body::new(Bytes::from(msg)))
                            .unwrap(),
                    }
                }));
            }
        }

        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                router = router.route("/debug/pprof/profile", axum::routing::get(|params: axum::extract::Query<ProfileParams>| async move {
                    match collect_pprof(params.seconds.map(std::time::Duration::from_secs)).await {
                        Ok(value) => value.into_response(),
                        Err(error) => {
                            tracing::warn!(%error, "admin http server error");
                            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
                        }
                    }
                }));
            }
        }

        router.with_state(self)
    }
}

async fn live(state: State<Admin>) -> StatusCode {
    if state.health.check_liveness() {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn ready(state: State<Admin>) -> StatusCode {
    if state.ready.load(Ordering::SeqCst) {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn config(state: State<Admin>) -> Json<Arc<crate::Config>> {
    Json(state.config.clone())
}

fn collect_metrics() -> Response<Body> {
    use prometheus_client::encoding::text::{encode_eof, encode_registry};
    let mut text_encoding = String::new();

    // Encode metrics from prometheus_client crate
    crate::metrics::with_registry(|registry| {
        if let Err(error) = encode_registry(&mut text_encoding, &registry) {
            tracing::error!(?error, "failed to encode registry");
        }
    });

    // Encode metrics from prometheus crate
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    match prometheus::Encoder::encode(&encoder, &crate::metrics::registry().gather(), &mut buffer) {
        Ok(_) => match String::from_utf8(buffer) {
            Ok(section) => {
                text_encoding.push_str(section.as_str());
            }
            Err(error) => {
                tracing::error!(?error, "failed to convert metrics buffer to UTF-8");
            }
        },
        Err(error) => {
            tracing::error!(?error, "failed to encode metrics to buffer");
        }
    }

    // Encode EOF
    if let Err(error) = encode_eof(&mut text_encoding) {
        tracing::error!(?error, "failed to encode eof");
    }

    Response::new(Body::new(Bytes::from(text_encoding)))
}

/// Collects profiling information using `prof` for an optional `duration` or
/// the default if `None`.
#[cfg(target_os = "linux")]
async fn collect_pprof(
    duration: Option<std::time::Duration>,
) -> Result<impl IntoResponse, eyre::Error> {
    let duration = duration.unwrap_or_else(|| std::time::Duration::from_secs(2));
    tracing::debug!(duration_seconds = duration.as_secs(), "profiling");

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        // From the pprof docs, this blocklist helps prevent deadlock with
        // libgcc's unwind.
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()?;

    tokio::time::sleep(duration).await;

    let encoded_profile = encode_pprof(guard.report().build()?)?;

    // gzip profile
    let mut encoder = libflate::gzip::Encoder::new(Vec::new())?;
    std::io::copy(&mut &encoded_profile[..], &mut encoder)?;
    let gzip_body = encoder.finish().into_result()?;
    tracing::debug!("profile encoded to gzip");

    Ok((
        axum::response::AppendHeaders([
            (
                axum::http::header::CONTENT_LENGTH,
                gzip_body.len().to_string(),
            ),
            (axum::http::header::CONTENT_ENCODING, "gzip".to_string()),
        ]),
        gzip_body,
    ))
}

/// Encodes a pprof report into a binary protobuf
///
/// We do this encoding ourselves instead of the using the built-in method in
/// pprof since pprof takes a long time to update its tonic version, and the pprof
/// protobuf never changes so the transitive dependency doesn't make any sense for us
#[cfg(target_os = "linux")]
fn encode_pprof(report: pprof::Report) -> eyre::Result<Vec<u8>> {
    use quilkin_xds::generated::perftools::profiles as protos;
    use std::collections::HashMap;

    const SAMPLES: &str = "samples";
    const COUNT: &str = "count";
    const CPU: &str = "cpu";
    const NANOSECONDS: &str = "nanoseconds";
    const THREAD: &str = "thread";

    let mut dedup_str = std::collections::HashSet::new();
    for key in report.data.keys() {
        dedup_str.insert(key.thread_name_or_id());
        for frame in key.frames.iter() {
            for symbol in frame {
                dedup_str.insert(symbol.name());
                dedup_str.insert(symbol.sys_name().into_owned());
                dedup_str.insert(symbol.filename().into_owned());
            }
        }
    }
    dedup_str.insert(SAMPLES.into());
    dedup_str.insert(COUNT.into());
    dedup_str.insert(CPU.into());
    dedup_str.insert(NANOSECONDS.into());
    dedup_str.insert(THREAD.into());
    // string table's first element must be an empty string
    let mut string_table = vec!["".to_owned()];
    string_table.extend(dedup_str);

    let mut strings = HashMap::new();
    for (index, name) in string_table.iter().enumerate() {
        strings.insert(name.as_str(), index);
    }

    let mut sample = vec![];
    let mut location = vec![];
    let mut function = vec![];
    let mut functions = HashMap::new();
    for (key, count) in report.data.iter() {
        let mut locs = vec![];
        for frame in key.frames.iter() {
            for symbol in frame {
                let name = symbol.name();
                if let Some(loc_idx) = functions.get(&name) {
                    locs.push(*loc_idx);
                    continue;
                }
                let sys_name = symbol.sys_name();
                let filename = symbol.filename();
                let lineno = symbol.lineno();
                let function_id = function.len() as u64 + 1;
                let func = protos::Function {
                    id: function_id,
                    name: *strings.get(name.as_str()).unwrap() as i64,
                    system_name: *strings.get(sys_name.as_ref()).unwrap() as i64,
                    filename: *strings.get(filename.as_ref()).unwrap() as i64,
                    ..protos::Function::default()
                };
                functions.insert(name, function_id);
                let line = protos::Line {
                    function_id,
                    line: lineno as i64,
                };
                let loc = protos::Location {
                    id: function_id,
                    line: vec![line],
                    ..protos::Location::default()
                };
                // the fn_tbl has the same length with loc_tbl
                function.push(func);
                location.push(loc);
                // current frame locations
                locs.push(function_id);
            }
        }
        let thread_name = protos::Label {
            key: *strings.get(THREAD).unwrap() as i64,
            str: *strings.get(&key.thread_name_or_id().as_str()).unwrap() as i64,
            ..protos::Label::default()
        };
        sample.push(protos::Sample {
            location_id: locs,
            value: vec![
                *count as i64,
                *count as i64 * 1_000_000_000 / report.timing.frequency as i64,
            ],
            label: vec![thread_name],
        });
    }
    let samples_value = protos::ValueType {
        ty: *strings.get(SAMPLES).unwrap() as i64,
        unit: *strings.get(COUNT).unwrap() as i64,
    };
    let time_value = protos::ValueType {
        ty: *strings.get(CPU).unwrap() as i64,
        unit: *strings.get(NANOSECONDS).unwrap() as i64,
    };
    let profile = protos::Profile {
        sample_type: vec![samples_value, time_value],
        sample,
        string_table,
        function,
        location,
        time_nanos: report
            .timing
            .start_time
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        duration_nanos: report.timing.duration.as_nanos() as i64,
        period_type: Some(time_value),
        period: 1_000_000_000 / report.timing.frequency as i64,
        ..protos::Profile::default()
    };

    Ok(crate::codec::prost::encode(&profile)?)
}

/// Checks whether jemalloc profiling is activated an returns an error response if not.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
fn require_profiling_activated(
    prof_ctl: &jemalloc_pprof::JemallocProfCtl,
) -> Result<(), (StatusCode, String)> {
    if prof_ctl.activated() {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "heap profiling not activated".into()))
    }
}

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
async fn handle_get_heap() -> Result<Response<Body>, (StatusCode, String)> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    require_profiling_activated(&prof_ctl)?;
    let pprof = prof_ctl
        .dump_pprof()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Response::builder()
        .body(Body::from(pprof))
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live() {
        let (shutdown_tx, _shutdown_rx) = crate::signal::channel();
        let health = Health::new(shutdown_tx);
        let admin = Admin {
            config: crate::test::TestHelper::new_config(),
            ready: <_>::default(),
            health,
        };

        let server = axum_test::TestServer::new(admin.router()).unwrap();

        server.get("/live").expect_success().await;

        let _unused = std::panic::catch_unwind(|| {
            panic!("oh no!");
        });

        server.get("/live").expect_failure().await;
    }

    #[tokio::test]
    async fn collect_metrics() {
        let response = super::collect_metrics();
        assert_eq!(response.status(), hyper::StatusCode::OK);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn collect_pprof() {
        // Custom time to make the test fast.
        super::collect_pprof(Some(std::time::Duration::from_millis(1)))
            .await
            .unwrap();
    }
}
