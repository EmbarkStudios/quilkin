#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Endpoint {
    #[prost(message, optional, tag = "1")]
    pub address: ::core::option::Option<super::super::core::v3::Address>,
    #[prost(message, optional, tag = "2")]
    pub health_check_config: ::core::option::Option<endpoint::HealthCheckConfig>,
    #[prost(string, tag = "3")]
    pub hostname: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Endpoint`.
pub mod endpoint {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct HealthCheckConfig {
        #[prost(uint32, tag = "1")]
        pub port_value: u32,
        #[prost(string, tag = "2")]
        pub hostname: ::prost::alloc::string::String,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LbEndpoint {
    #[prost(enumeration = "super::super::core::v3::HealthStatus", tag = "2")]
    pub health_status: i32,
    #[prost(message, optional, tag = "3")]
    pub metadata: ::core::option::Option<super::super::core::v3::Metadata>,
    #[prost(message, optional, tag = "4")]
    pub load_balancing_weight: ::core::option::Option<u32>,
    #[prost(oneof = "lb_endpoint::HostIdentifier", tags = "1, 5")]
    pub host_identifier: ::core::option::Option<lb_endpoint::HostIdentifier>,
}
/// Nested message and enum types in `LbEndpoint`.
pub mod lb_endpoint {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum HostIdentifier {
        #[prost(message, tag = "1")]
        Endpoint(super::Endpoint),
        #[prost(string, tag = "5")]
        EndpointName(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LedsClusterLocalityConfig {
    #[prost(message, optional, tag = "1")]
    pub leds_config: ::core::option::Option<super::super::core::v3::ConfigSource>,
    #[prost(string, tag = "2")]
    pub leds_collection_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalityLbEndpoints {
    #[prost(message, optional, tag = "1")]
    pub locality: ::core::option::Option<super::super::core::v3::Locality>,
    #[prost(message, repeated, tag = "2")]
    pub lb_endpoints: ::prost::alloc::vec::Vec<LbEndpoint>,
    #[prost(message, optional, tag = "3")]
    pub load_balancing_weight: ::core::option::Option<u32>,
    #[prost(uint32, tag = "5")]
    pub priority: u32,
    #[prost(message, optional, tag = "6")]
    pub proximity: ::core::option::Option<u32>,
    #[prost(oneof = "locality_lb_endpoints::LbConfig", tags = "7, 8")]
    pub lb_config: ::core::option::Option<locality_lb_endpoints::LbConfig>,
}
/// Nested message and enum types in `LocalityLbEndpoints`.
pub mod locality_lb_endpoints {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct LbEndpointList {
        #[prost(message, repeated, tag = "1")]
        pub lb_endpoints: ::prost::alloc::vec::Vec<super::LbEndpoint>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum LbConfig {
        #[prost(message, tag = "7")]
        LoadBalancerEndpoints(LbEndpointList),
        #[prost(message, tag = "8")]
        LedsClusterLocalityConfig(super::LedsClusterLocalityConfig),
    }
}
