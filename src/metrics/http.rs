use once_cell::sync::Lazy;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Unit,
};

const UNMATCHED_PATH: &str = "unmatched";
const LATENCY_BUCKETS: [f64; 7] = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct InflightLabels {
    service: String,
    path: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
struct HttpLabels {
    service: String,
    path: String,
    code: u16,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, EncodeLabelSet)]
pub(crate) struct ConnectionLabels {
    pub service: String,
}

struct HttpMetrics {
    connections: Family<ConnectionLabels, Gauge>,
    inflight_requests: Family<InflightLabels, Gauge>,
    requests: Family<HttpLabels, Counter>,
    request_duration: Family<HttpLabels, Histogram>,
}

fn get_http_metrics() -> &'static HttpMetrics {
    static HTTP_METRICS: Lazy<HttpMetrics> = Lazy::new(|| HttpMetrics {
        connections: <_>::default(),
        inflight_requests: <_>::default(),
        requests: <_>::default(),
        request_duration: Family::<HttpLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(LATENCY_BUCKETS)
        }),
    });
    &HTTP_METRICS
}

pub fn register_metrics(registry: &mut prometheus_client::registry::Registry) {
    let http_metrics = get_http_metrics();
    registry.register(
        "http_connections",
        "Number of open http connections",
        http_metrics.connections.clone(),
    );
    registry.register(
        "http_inflight_requests",
        "Number of inflight http requests",
        http_metrics.inflight_requests.clone(),
    );
    registry.register(
        "http_requests",
        "Total number of requests",
        http_metrics.requests.clone(),
    );
    registry.register_with_unit(
        "http_request_duration",
        "Histogram of request handling duration",
        Unit::Seconds,
        http_metrics.request_duration.clone(),
    );
}

/// A `tower::Layer` for instrumenting http metrics
#[derive(Clone)]
pub struct HttpMetricsLayer {
    service_name: String,
    path_buckets: Vec<String>,
}

impl HttpMetricsLayer {
    /// Create a new `HttpMetricsLayer`
    pub fn new(service_name: String) -> Self {
        Self {
            service_name,
            path_buckets: Vec::new(),
        }
    }

    /// Create a new `HttpMetricsLayer` with path buckets to divide the path label into
    ///
    /// The `path_buckets` will be used to prefix-match against the request URI path. This is in
    /// order to limit the maximum cardinality of the exported metrics.
    pub fn new_with_path_buckets(
        service_name: impl Into<String>,
        path_buckets: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        // path_buckets MUST be sorted in ascending order for the prefix-matching logic to work as
        // intended
        let mut path_buckets: Vec<String> =
            path_buckets.into_iter().map(Into::<String>::into).collect();
        path_buckets.sort();
        Self {
            service_name: service_name.into(),
            path_buckets,
        }
    }
}

impl<S> tower::Layer<S> for HttpMetricsLayer {
    type Service = HttpMetricsMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            service_name: self.service_name.clone(),
            path_buckets: self.path_buckets.clone(),
        }
    }
}

/// A `tower::Service` middleware which instruments http metrics
#[derive(Clone, Debug)]
pub struct HttpMetricsMiddleware<S> {
    inner: S,
    service_name: String,
    path_buckets: Vec<String>,
}

impl<S> HttpMetricsMiddleware<S> {
    /// Finds the appropriate `path_bucket` by prefix-matching the buckets on the given path. This
    /// depends on the `path_buckets` being sorted in ascending order, so that the last matching
    /// element will always be the longest.
    fn find_path_bucket(&self, path: &str) -> String {
        self.path_buckets
            .iter()
            .rfind(|&path_prefix| path.starts_with(path_prefix))
            .cloned()
            .unwrap_or_else(|| UNMATCHED_PATH.to_string())
    }
}

impl<S> tower::Service<axum::extract::Request> for HttpMetricsMiddleware<S>
where
    S: tower::Service<axum::extract::Request, Response = axum::response::Response>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        futures::future::BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::extract::Request) -> Self::Future {
        let start_time = std::time::Instant::now();
        let service_name = self.service_name.clone();
        let path_bucket = self.find_path_bucket(req.uri().path());
        let http_metrics = get_http_metrics();

        // Increase inflight requests metric
        let inflight_labels = InflightLabels {
            service: service_name.clone(),
            path: path_bucket.clone(),
        };
        http_metrics
            .inflight_requests
            .get_or_create(&inflight_labels)
            .inc();

        let future = self.inner.call(req);
        Box::pin(async move {
            let result = future.await;
            let request_duration = start_time.elapsed();

            let code = result
                .as_ref()
                .map(|response| response.status().as_u16())
                .unwrap_or(0);

            // Update total and latency metrics
            let http_labels = HttpLabels {
                service: service_name,
                path: path_bucket,
                code,
            };
            http_metrics.requests.get_or_create(&http_labels).inc();
            http_metrics
                .request_duration
                .get_or_create(&http_labels)
                .observe(request_duration.as_secs_f64());

            // Decrease inflight requests metric
            http_metrics
                .inflight_requests
                .get_or_create(&inflight_labels)
                .dec();

            result
        })
    }
}

/// Increases the connection gauge by one and decreases it by one when the guard is dropped
pub(crate) fn connection_guard(labels: &ConnectionLabels) -> ConnectionGuard {
    let gauge = get_http_metrics().connections.get_or_create_owned(labels);
    gauge.inc();
    ConnectionGuard { gauge }
}

pub struct ConnectionGuard {
    gauge: Gauge,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

#[cfg(test)]
mod tests {
    use tower::Layer;

    use super::*;

    #[test]
    fn test_path_buckets() {
        let layer = HttpMetricsLayer::new_with_path_buckets("test", ["/1/2", "/1/3", "/2", "/1"]);
        let middleware = layer.layer(tower::service_fn(|| {}));

        // Happy path
        assert_eq!(middleware.find_path_bucket("/1"), "/1".to_string());
        assert_eq!(middleware.find_path_bucket("/2"), "/2".to_string());
        assert_eq!(middleware.find_path_bucket("/1/2"), "/1/2".to_string());
        assert_eq!(middleware.find_path_bucket("/1/3"), "/1/3".to_string());
        assert_eq!(middleware.find_path_bucket("/1/2/3"), "/1/2".to_string());

        // Adverserial
        let unmatched = UNMATCHED_PATH.to_string();
        assert_eq!(middleware.find_path_bucket("/"), unmatched);
        assert_eq!(middleware.find_path_bucket("/foo"), unmatched);
        assert_eq!(middleware.find_path_bucket("/3"), unmatched);
        assert_eq!(middleware.find_path_bucket("/foo/1/2"), unmatched);
    }
}
