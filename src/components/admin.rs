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

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode};
type Body = Full<Bytes>;

use health::Health;

pub const PORT: u16 = 8000;

pub(crate) const IDLE_REQUEST_INTERVAL: Duration = Duration::from_secs(30);

pub fn server<C>(
    config: Arc<C>,
    ready: Arc<AtomicBool>,
    shutdown_tx: crate::signal::ShutdownTx,
    address: Option<std::net::SocketAddr>,
) -> std::thread::JoinHandle<eyre::Result<()>>
where
    C: serde::Serialize + Send + Sync + 'static,
{
    let address = address.unwrap_or_else(|| (std::net::Ipv6Addr::UNSPECIFIED, PORT).into());
    let health = Health::new(shutdown_tx);
    tracing::info!(address = %address, "Starting admin endpoint");

    std::thread::Builder::new()
        .name("admin-http".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .thread_name("admin-http-worker")
                .build()
                .expect("couldn't create tokio runtime in thread");
            runtime.block_on(async move {
                let accept_stream = tokio::net::TcpListener::bind(address).await?;
                let http_task: tokio::task::JoinHandle<eyre::Result<()>> =
                    tokio::task::spawn(async move {
                        loop {
                            let (stream, _) = accept_stream.accept().await?;
                            let stream = hyper_util::rt::TokioIo::new(stream);

                            let config = config.clone();
                            let health = health.clone();
                            let ready = ready.clone();
                            tokio::spawn(async move {
                                let svc = hyper::service::service_fn(move |req| {
                                    let config = config.clone();
                                    let health = health.clone();
                                    let ready = ready.clone();

                                    async move {
                                        Ok::<_, std::convert::Infallible>(
                                            handle_request(req, config, &ready, health).await,
                                        )
                                    }
                                });

                                let svc = tower::ServiceBuilder::new().service(svc);
                                if let Err(err) = hyper::server::conn::http1::Builder::new()
                                    .serve_connection(stream, svc)
                                    .await
                                {
                                    tracing::warn!("failed to reponse to phoenix request: {err}");
                                }
                            });
                        }
                    });

                http_task.await?
            })
        })
        .expect("failed to spawn admin-http thread")
}

async fn handle_request<C: serde::Serialize>(
    request: Request<hyper::body::Incoming>,
    config: Arc<C>,
    ready: &AtomicBool,
    health: Health,
) -> Response<Body> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => collect_metrics(),
        (&Method::GET, "/live" | "/livez") => health.check_liveness(),
        #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
        (&Method::GET, "/debug/pprof/allocs") => match handle_get_heap().await {
            Ok(response) => response,
            Err((status_code, msg)) => Response::builder()
                .status(status_code)
                .body(Body::new(Bytes::from(msg)))
                .unwrap(),
        },
        #[cfg(target_os = "linux")]
        (&Method::GET, "/debug/pprof/profile") => {
            let duration = request.uri().query().and_then(|query| {
                form_urlencoded::parse(query.as_bytes())
                    .find(|(k, _)| k == "seconds")
                    .and_then(|(_, v)| v.parse().ok())
                    .map(std::time::Duration::from_secs)
            });

            match collect_pprof(duration).await {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(%error, "admin http server error");
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::new(Bytes::from("internal error")))
                        .unwrap()
                }
            }
        }
        (&Method::GET, "/ready" | "/readyz") => check_readiness(ready),
        (&Method::GET, "/config") => match serde_json::to_string(&config) {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                .header(
                    "Content-Type",
                    hyper::header::HeaderValue::from_static("application/json"),
                )
                .body(Body::new(Bytes::from(body)))
                .unwrap(),
            Err(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::new(Bytes::from(format!(
                    "failed to create config dump: {err}"
                ))))
                .unwrap(),
        },
        (_, _) => {
            let mut response = Response::new(Body::new(Bytes::new()));
            *response.status_mut() = StatusCode::NOT_FOUND;
            response
        }
    }
}

fn check_readiness(check: &AtomicBool) -> Response<Body> {
    if check.load(Ordering::SeqCst) {
        return Response::new("ok".into());
    }

    let mut response = Response::new(bytes::Bytes::from_static(b"NOT READY").into());
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

fn collect_metrics() -> Response<Body> {
    let mut response = Response::new(Body::new(Bytes::new()));
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    let body =
        prometheus::Encoder::encode(&encoder, &crate::metrics::registry().gather(), &mut buffer)
            .map_err(|error| tracing::warn!(%error, "Failed to encode metrics"))
            .and_then(|_| {
                String::from_utf8(buffer)
                    .map(hyper::body::Bytes::from)
                    .map_err(|error| tracing::warn!(%error, "Failed to convert metrics to utf8"))
            });

    match body {
        Ok(body) => {
            *response.body_mut() = Body::new(body);
        }
        Err(_) => {
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    response
}

/// Collects profiling information using `prof` for an optional `duration` or
/// the default if `None`.
#[cfg(target_os = "linux")]
async fn collect_pprof(
    duration: Option<std::time::Duration>,
) -> Result<Response<Body>, eyre::Error> {
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

    Response::builder()
        .header(hyper::header::CONTENT_LENGTH, gzip_body.len() as u64)
        .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
        .header(hyper::header::CONTENT_ENCODING, "gzip")
        .body(Body::new(Bytes::from(gzip_body)))
        .map_err(From::from)
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
