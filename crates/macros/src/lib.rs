/*
 * Copyright 2021 Google LLC
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

mod include;

use quote::ToTokens;
use syn::parse_macro_input;

use include::IncludeProto;

/// Includes generated Protobuf definitions from `tonic`.
///
/// Accepts a single argument for the gRPC package name, which then recreates
/// the package structure as modules.
///
/// ### Input
/// ```
/// quilkin::include_proto!("quilkin.filters.debug.v1alpha1");
/// ```
///
/// ### Output
/// ```
/// mod quilkin {
///     pub(crate) mod extensions {
///         pub(crate) mod filters {
///             pub(crate) mod debug {
///                 pub(crate) mod v1alpha1 {
///                     #![doc(hidden)]
///                     tonic::include_proto!("quilkin.filters.debug.v1alpha1");
///                 }
///             }
///         }
///     }
/// }
/// ```
#[proc_macro]
pub fn include_proto(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    parse_macro_input!(input as IncludeProto)
        .to_token_stream()
        .into()
}
