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

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, TokenStreamExt};
use syn::parse::{Parse, ParseStream};

pub(crate) struct IncludeProto {
    id: String,
}

impl Parse for IncludeProto {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let lit = input.parse::<syn::LitStr>()?;
        let id = lit.value();

        if id.is_empty() {
            Err(syn::Error::new(
                lit.span(),
                "Expected package name to not be empty.",
            ))
        } else {
            Ok(Self { id })
        }
    }
}

impl ToTokens for IncludeProto {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let id = &self.id;

        let tonic_include_proto: syn::Stmt = syn::parse_quote!(tonic::include_proto!(#id););
        let items: Vec<syn::Item> =
            vec![syn::Item::Verbatim(tonic_include_proto.to_token_stream())];

        let module = id.split('.').rev().fold::<Vec<_>, _>(items, |acc, module| {
            let module = syn::Ident::new(module, Span::mixed_site());
            let result: syn::ItemMod = syn::parse_quote! {
                #[allow(warnings, clippy::all)] pub mod #module { #(#acc)* }
            };

            vec![syn::Item::Mod(result)]
        });

        tokens.append_all(module);
    }
}
