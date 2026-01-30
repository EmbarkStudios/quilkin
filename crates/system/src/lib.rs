//! A collection of modules with opinionated standard components to provide a foundation for
//! running a production-ready application

#[cfg(feature = "lifecycle")]
pub mod lifecycle;
#[cfg(any(feature = "http", feature = "sockets"))]
pub mod net;

pub fn register_metrics(registry: &mut prometheus_client::registry::Registry) {
    #[cfg(feature = "http")]
    net::http::metrics::register_metrics(registry);
    #[cfg(feature = "lifecycle")]
    lifecycle::metrics::register_metrics(registry);
}
