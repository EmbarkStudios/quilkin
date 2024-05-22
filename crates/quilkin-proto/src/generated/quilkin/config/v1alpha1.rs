#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClusterMap {
    #[prost(message, repeated, tag = "1")]
    pub clusters: ::prost::alloc::vec::Vec<Cluster>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Cluster {
    #[prost(message, optional, tag = "1")]
    pub locality: ::core::option::Option<Locality>,
    #[prost(message, repeated, tag = "2")]
    pub endpoints: ::prost::alloc::vec::Vec<Endpoint>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Locality {
    #[prost(string, tag = "1")]
    pub region: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub zone: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub sub_zone: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ipv6 {
    #[prost(fixed64, tag = "1")]
    pub first: u64,
    #[prost(fixed64, tag = "2")]
    pub second: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Host {
    #[prost(oneof = "host::Inner", tags = "1, 2, 3")]
    pub inner: ::core::option::Option<host::Inner>,
}
/// Nested message and enum types in `Host`.
pub mod host {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Inner {
        #[prost(string, tag = "1")]
        Name(::prost::alloc::string::String),
        #[prost(fixed32, tag = "2")]
        Ipv4(u32),
        #[prost(message, tag = "3")]
        Ipv6(super::Ipv6),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Endpoint {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub port: u32,
    #[prost(message, optional, tag = "3")]
    pub metadata: ::core::option::Option<::prost_types::Struct>,
    #[prost(message, optional, tag = "4")]
    pub host2: ::core::option::Option<Host>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Datacenter {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub qcmp_port: u32,
    #[prost(string, tag = "3")]
    pub icao_code: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub label: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub config: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterChain {
    #[prost(message, repeated, tag = "1")]
    pub filters: ::prost::alloc::vec::Vec<Filter>,
}
