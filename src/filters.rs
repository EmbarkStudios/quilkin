/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Filters for processing packets.

mod chain;
mod error;
mod factory;
mod read;
mod registry;
mod set;
mod write;

pub mod capture;
pub mod concatenate;
pub mod debug;
pub mod decryptor;
pub mod drop;
pub mod firewall;
pub mod load_balancer;
pub mod local_rate_limit;
pub mod r#match;
pub mod metrics;
pub mod pass;
pub mod timestamp;
pub mod token_router;

/// Prelude containing all types and traits required to implement [`Filter`] and
/// [`FilterFactory`].
pub mod prelude {
    pub use super::{
        ConvertProtoConfigError, CreateFilterArgs, CreationError, Filter, FilterError,
        FilterInstance, Packet, PacketMut, ReadContext, StaticFilter, WriteContext,
    };
}

// Core Filter types
#[doc(inline)]
pub use self::{
    capture::Capture,
    chain::FilterChain,
    concatenate::Concatenate,
    debug::Debug,
    decryptor::Decryptor,
    drop::Drop,
    error::{ConvertProtoConfigError, CreationError, FilterError},
    factory::{CreateFilterArgs, DynFilterFactory, FilterFactory, FilterInstance},
    firewall::Firewall,
    load_balancer::LoadBalancer,
    local_rate_limit::LocalRateLimit,
    r#match::Match,
    pass::Pass,
    read::ReadContext,
    registry::FilterRegistry,
    set::{FilterMap, FilterSet},
    timestamp::Timestamp,
    token_router::{HashedTokenRouter, TokenRouter},
    write::WriteContext,
};

pub use crate::{
    net::{Packet, PacketMut},
    test::TestFilter,
};

use crate::config::filter::Filter as FilterConfig;

#[enum_dispatch::enum_dispatch(Filter)]
pub enum FilterKind {
    Capture,
    Concatenate,
    Debug,
    Decryptor,
    Drop,
    Firewall,
    LoadBalancer,
    LocalRateLimit,
    Pass,
    Match,
    Timestamp,
    TokenRouter,
    HashedTokenRouter,
    TestFilter,
}

/// Statically safe version of [`Filter`], if you're writing a Rust filter, you
/// should implement [`StaticFilter`] in addition to [`Filter`], as
/// [`StaticFilter`] guarantees all of the required properties through the type
/// system, allowing Quilkin take care of the virtual table boilerplate
/// automatically at compile-time.
pub trait StaticFilter: Filter + Sized + Into<FilterKind>
// This where clause simply states that `Configuration`'s and
// `BinaryConfiguration`'s `Error` types are compatible with `filters::Error`.
where
    CreationError: From<<Self::Configuration as TryFrom<Self::BinaryConfiguration>>::Error>
        + From<<Self::BinaryConfiguration as TryFrom<Self::Configuration>>::Error>,
{
    /// The globally unique name of the filter.
    const NAME: &'static str;
    /// The human-readable configuration of the filter. **Must** be [`serde`]
    /// compatible, have a JSON schema, and be convertible to and
    /// from [`Self::BinaryConfiguration`].
    type Configuration: schemars::JsonSchema
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + TryFrom<Self::BinaryConfiguration>;
    /// The binary configuration of the filter. **Must** be [`prost`] compatible,
    /// and be convertible to and from [`Self::Configuration`].
    type BinaryConfiguration: prost::Message
        + Default
        + TryFrom<Self::Configuration>
        + Send
        + Sync
        + Sized;

    /// Instantiates a new [`StaticFilter`] from the given configuration, if any.
    /// # Errors
    /// If the provided configuration is invalid.
    fn try_from_config(config: Option<Self::Configuration>) -> Result<Self, CreationError>;

    /// Instantiates a new [`StaticFilter`] from the given configuration, if any.
    /// # Panics
    /// If the provided configuration is invalid.
    fn from_config(config: Option<Self::Configuration>) -> Self {
        Self::try_from_config(config).unwrap()
    }

    /// Creates a new dynamic [`FilterFactory`] virtual table.
    fn factory() -> DynFilterFactory
    where
        Self: 'static,
    {
        Box::from(std::marker::PhantomData::<fn() -> Self>)
    }

    /// Convenience method for providing a consistent error message for filters
    /// which require a fully initialized [`Self::Configuration`].
    fn ensure_config_exists(
        config: Option<Self::Configuration>,
    ) -> Result<Self::Configuration, CreationError> {
        config.ok_or(CreationError::MissingConfig(Self::NAME))
    }

    fn as_filter_config(
        config: impl Into<Option<Self::Configuration>>,
    ) -> Result<FilterConfig, CreationError> {
        Ok(FilterConfig {
            name: Self::NAME.into(),
            label: None,
            config: config
                .into()
                .map(|config| serde_json::to_value(&config))
                .transpose()?,
        })
    }

    #[inline]
    fn as_labeled_filter_config(
        config: impl Into<Option<Self::Configuration>>,
        label: String,
    ) -> Result<FilterConfig, CreationError> {
        Ok(FilterConfig {
            name: Self::NAME.into(),
            label: Some(label),
            config: config
                .into()
                .map(|config| serde_json::to_value(&config))
                .transpose()?,
        })
    }
}

/// Trait for routing and manipulating packets.
///
/// An implementation of [`Filter`] provides a `read` and a `write` method. Both
/// methods are invoked by the proxy when it consults the filter chain - their
/// arguments contain information about the packet being processed.
/// - `read` is invoked when a packet is received on the local downstream port
///   and is to be sent to an upstream endpoint.
/// - `write` is invoked in the opposite direction when a packet is received
///   from an upstream endpoint and is to be sent to a downstream client.
///
/// **Metrics**
///
/// * `filter_read_duration_seconds` The duration it took for a `filter`'s
///   `read` implementation to execute.
///   * Labels
///     * `filter` The name of the filter being executed.
///
/// * `filter_write_duration_seconds` The duration it took for a `filter`'s
///   `write` implementation to execute.
///   * Labels
///     * `filter` The name of the filter being executed.
#[enum_dispatch::enum_dispatch]
pub trait Filter: Send + Sync {
    /// [`Filter::read`] is invoked when the proxy receives data from a
    /// downstream connection on the listening port.
    ///
    /// This function should return an `Some` if the packet processing should
    /// proceed. If the packet should be rejected, it will return [`None`]
    /// instead. By default, the context passes through unchanged.
    fn read<P: PacketMut>(&self, _: &mut ReadContext<'_, P>) -> Result<(), FilterError> {
        Ok(())
    }

    /// [`Filter::write`] is invoked when the proxy is about to send data to a
    /// downstream connection via the listening port after receiving it via one
    /// of the upstream Endpoints.
    ///
    /// This function should return an `Some` if the packet processing should
    /// proceed. If the packet should be rejected, it will return [`None`]
    fn write<P: PacketMut>(&self, _: &mut WriteContext<P>) -> Result<(), FilterError> {
        Ok(())
    }
}
