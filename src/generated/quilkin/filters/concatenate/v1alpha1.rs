#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Concatenate {
    #[prost(message, optional, tag = "1")]
    pub on_write: ::core::option::Option<concatenate::StrategyValue>,
    #[prost(message, optional, tag = "2")]
    pub on_read: ::core::option::Option<concatenate::StrategyValue>,
    #[prost(bytes = "vec", tag = "3")]
    pub bytes: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `Concatenate`.
pub mod concatenate {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StrategyValue {
        #[prost(enumeration = "Strategy", tag = "1")]
        pub value: i32,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Strategy {
        DoNothing = 0,
        Append = 1,
        Prepend = 2,
    }
    impl Strategy {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Strategy::DoNothing => "DoNothing",
                Strategy::Append => "Append",
                Strategy::Prepend => "Prepend",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "DoNothing" => Some(Self::DoNothing),
                "Append" => Some(Self::Append),
                "Prepend" => Some(Self::Prepend),
                _ => None,
            }
        }
    }
}
