#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Firewall {
    #[prost(message, repeated, tag = "1")]
    pub on_read: ::prost::alloc::vec::Vec<firewall::Rule>,
    #[prost(message, repeated, tag = "2")]
    pub on_write: ::prost::alloc::vec::Vec<firewall::Rule>,
}
/// Nested message and enum types in `Firewall`.
pub mod firewall {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PortRange {
        #[prost(uint32, tag = "1")]
        pub min: u32,
        #[prost(uint32, tag = "2")]
        pub max: u32,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rule {
        #[prost(enumeration = "Action", tag = "1")]
        pub action: i32,
        #[prost(string, repeated, tag = "2")]
        pub sources: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        #[prost(message, repeated, tag = "3")]
        pub ports: ::prost::alloc::vec::Vec<PortRange>,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Action {
        Allow = 0,
        Deny = 1,
    }
    impl Action {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Action::Allow => "Allow",
                Action::Deny => "Deny",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "Allow" => Some(Self::Allow),
                "Deny" => Some(Self::Deny),
                _ => None,
            }
        }
    }
}
