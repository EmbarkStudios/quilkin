#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Debug {
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<::prost::alloc::string::String>,
}
