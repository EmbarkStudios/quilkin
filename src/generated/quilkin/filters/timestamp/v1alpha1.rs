#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(message, optional, tag = "1")]
    pub metadata_key: ::core::option::Option<::prost::alloc::string::String>,
}
