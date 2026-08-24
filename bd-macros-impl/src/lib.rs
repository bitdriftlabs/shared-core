// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Implementation crate for procedural macros re-exported by `bd-macros`.
//!
//! `proc-macro` crates cannot expose ordinary Rust items such as traits. Keeping the token
//! generation here lets `bd-macros` provide both its macros and its shared runtime traits from one
//! ordinary library crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{Data, DeriveInput, Fields, Meta, parse_macro_input};

mod approximate_size;
mod enum_impl;
mod struct_impl;
mod validation;

use enum_impl::process_enum_variants;
use struct_impl::process_struct_fields;
use validation::ValidationConfig;

/// Configuration parsed from the `proto_serializable` macro attributes.
struct MacroConfig {
  /// Only generate serialization code (no deserialization).
  serialize_only: bool,
  /// Validation configuration when `validate_against` is specified.
  validation: ValidationConfig,
}

/// Parses the macro attribute arguments into a configuration struct.
fn parse_macro_config(attr: TokenStream) -> MacroConfig {
  let mut config = MacroConfig {
    serialize_only: false,
    validation: ValidationConfig::default(),
  };

  // Handle empty attributes.
  if attr.is_empty() {
    return config;
  }

  // Parse as a list of meta items.
  let parser = syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated;
  let Ok(metas) = parser.parse(attr) else {
    return config;
  };

  for meta in metas {
    match &meta {
      Meta::Path(path) => {
        if path.is_ident("serialize_only") {
          config.serialize_only = true;
        } else if path.is_ident("validate_partial") {
          config.validation.validate_partial = true;
        }
      },
      Meta::NameValue(nv) => {
        if nv.path.is_ident("validate_against")
          && let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit_str),
            ..
          }) = &nv.value
        {
          let path_str = lit_str.value();
          config.validation.proto_path =
            Some(syn::parse_str(&path_str).expect("Invalid path in validate_against"));
        }
      },
      // Ignore nested lists for now.
      Meta::List(_) => {},
    }
  }

  config
}

/// Main procedural macro that generates protobuf serialization code for structs and enums.
///
/// This is the entry point for the `#[proto_serializable]` attribute macro. It analyzes the input
/// type and delegates to the appropriate processing module. The generated expansion preserves the
/// original type after removing its `#[field(...)]` helper attributes, adds the relevant protobuf
/// trait implementations, and adds validation tests when requested.
#[proc_macro_attribute]
pub fn proto_serializable(attr: TokenStream, item: TokenStream) -> TokenStream {
  let config = parse_macro_config(attr);
  let serialize_only = config.serialize_only;

  let input = parse_macro_input!(item as DeriveInput);
  let name = &input.ident;
  // Create a copy of the input with `#[field(...)]` attributes stripped so the generated item
  // does not retain helper attributes the compiler does not recognize.
  let mut stripped_input = input.clone();

  // Remove `#[field(...)]` attributes from enum variants and their named fields too.
  if let Data::Struct(data_struct) = &mut stripped_input.data {
    if let Fields::Named(fields) = &mut data_struct.fields {
      for field in &mut fields.named {
        field.attrs.retain(|attr| !attr.path().is_ident("field"));
      }
    }
  } else if let Data::Enum(data_enum) = &mut stripped_input.data {
    for variant in &mut data_enum.variants {
      variant.attrs.retain(|attr| !attr.path().is_ident("field"));
      if let Fields::Named(fields) = &mut variant.fields {
        for field in &mut fields.named {
          field.attrs.retain(|attr| !attr.path().is_ident("field"));
        }
      }
    }
  }

  let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

  match &input.data {
    Data::Struct(data_struct) => match &data_struct.fields {
      Fields::Named(fields) => {
        let struct_impl::StructProcessingResult {
          proto_type_impl,
          serialize_impl,
          deserialize_impl,
          message_serialize_impl,
          message_deserialize_impl,
          validation_tests,
        } = process_struct_fields(
          fields,
          name,
          &impl_generics,
          &ty_generics,
          where_clause,
          serialize_only,
          &config.validation,
        );

        quote! {
          #stripped_input
          #proto_type_impl
          #serialize_impl
          #deserialize_impl
          #message_serialize_impl
          #message_deserialize_impl
          #validation_tests
        }
        .into()
      },
      _ => panic!("Only named fields are supported for structs"),
    },
    Data::Enum(data_enum) => {
      let enum_impl::EnumProcessingResult {
        proto_type_impl,
        serialize_impl,
        deserialize_impl,
        message_serialize_impl,
        message_deserialize_impl,
        validation_tests,
      } = process_enum_variants(
        data_enum,
        name,
        &impl_generics,
        &ty_generics,
        where_clause,
        serialize_only,
        &config.validation,
      );

      quote! {
        #stripped_input
        #proto_type_impl
        #serialize_impl
        #deserialize_impl
        #message_serialize_impl
        #message_deserialize_impl
        #validation_tests
      }
      .into()
    },
    Data::Union(_) => panic!("Only structs and enums are supported"),
  }
}

/// Implements `bd_macros::ApproximateSize` by summing owned field allocations.
#[proc_macro_derive(ApproximateSize, attributes(approximate_size))]
pub fn derive_approximate_size(item: TokenStream) -> TokenStream {
  approximate_size::derive(parse_macro_input!(item as DeriveInput)).into()
}
