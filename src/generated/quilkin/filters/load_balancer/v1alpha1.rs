#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoadBalancer {
    #[prost(message, optional, tag = "1")]
    pub policy: ::core::option::Option<load_balancer::PolicyValue>,
}
/// Nested message and enum types in `LoadBalancer`.
pub mod load_balancer {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PolicyValue {
        #[prost(enumeration = "Policy", tag = "1")]
        pub value: i32,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Policy {
        RoundRobin = 0,
        Random = 1,
        Hash = 2,
    }
    impl Policy {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Policy::RoundRobin => "RoundRobin",
                Policy::Random => "Random",
                Policy::Hash => "Hash",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "RoundRobin" => Some(Self::RoundRobin),
                "Random" => Some(Self::Random),
                "Hash" => Some(Self::Hash),
                _ => None,
            }
        }
    }
}
