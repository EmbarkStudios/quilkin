#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Match {
    #[prost(message, optional, tag = "1")]
    pub on_read: ::core::option::Option<r#match::Config>,
    #[prost(message, optional, tag = "2")]
    pub on_write: ::core::option::Option<r#match::Config>,
}
/// Nested message and enum types in `Match`.
pub mod r#match {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Branch {
        #[prost(message, optional, tag = "1")]
        pub value: ::core::option::Option<::prost_types::Value>,
        #[prost(message, optional, tag = "2")]
        pub filter: ::core::option::Option<
            super::super::super::super::super::envoy::config::listener::v3::Filter,
        >,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        #[prost(message, optional, tag = "1")]
        pub metadata_key: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(message, repeated, tag = "2")]
        pub branches: ::prost::alloc::vec::Vec<Branch>,
        #[prost(message, optional, tag = "4")]
        pub fallthrough: ::core::option::Option<
            super::super::super::super::super::envoy::config::listener::v3::Filter,
        >,
    }
}
