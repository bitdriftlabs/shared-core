// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Procedural macro for generating protobuf serialization code.
//!
//! This crate provides the `#[proto_serializable]` attribute macro that generates efficient
//! protobuf serialization and deserialization code for Rust structs and enums.
//!
//! # Overview
//!
//! The macro generates implementations of three key traits:
//! - `ProtoType` - Defines the wire type for the field
//! - `ProtoFieldSerialize` - Serializes the value with a field number
//! - `ProtoFieldDeserialize` - Deserializes from a protobuf stream
//! - `ProtoMessage` (structs only) - Top-level message serialization
//!
//! # Supported Types
//!
//! - **Structs**: Named field structs become protobuf messages
//! - **Enums**: Become protobuf oneofs with support for:
//!   - Unit variants (markers)
//!   - Tuple variants (single wrapped value)
//!   - Struct variants (nested messages)
//!
//! # Validation
//!
//! The macro supports optional validation against protobuf descriptors:
//!
//! ```ignore
//! #[proto_serializable(validate_against = "bd_proto::proto::MyMessage")]
//! struct MyStruct { ... }
//! ```
//!
//! This generates tests that validate:
//! - Field IDs match between Rust struct and protobuf descriptor
//! - Field types are compatible
//! - Bidirectional round-trip serialization works correctly
//!
//! Use `validate_partial` to allow the Rust struct to have fewer fields than the proto:
//!
//! ```ignore
//! #[proto_serializable(validate_against = "...", validate_partial)]
//! struct MyPartialStruct { ... }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{Data, DeriveInput, Fields, Meta, parse_macro_input};

mod enum_impl;
mod struct_impl;
mod validation;

use enum_impl::process_enum_variants;
use struct_impl::process_struct_fields;
use validation::ValidationConfig;

/// Configuration parsed from the macro attributes.
struct MacroConfig {
  /// Only generate serialization code (no deserialization)
  serialize_only: bool,
  /// Validation configuration (if `validate_against` is specified)
  validation: ValidationConfig,
}

/// Parses the macro attribute arguments into a configuration struct.
fn parse_macro_config(attr: TokenStream) -> MacroConfig {
  let mut config = MacroConfig {
    serialize_only: false,
    validation: ValidationConfig::default(),
  };

  // Handle empty attributes
  if attr.is_empty() {
    return config;
  }

  // Parse as a list of meta items
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
      Meta::List(_) => {
        // Ignore nested lists for now
      },
    }
  }

  config
}

/// Main procedural macro that generates protobuf serialization code for structs and enums.
///
/// This is the entry point for the `#[proto_serializable]` attribute macro. It analyzes
/// the input type (struct or enum) and delegates to the appropriate processing module.
///
/// # Attributes
///
/// - `#[proto_serializable]` - Standard mode, generates both serialization and deserialization
/// - `#[proto_serializable(serialize_only)]` - Only generates serialization code
/// - `#[proto_serializable(validate_against = "path::to::ProtoType")]` - Generates validation tests
///   that ensure the Rust struct is compatible with the specified protobuf type
/// - `#[proto_serializable(validate_against = "...", validate_partial)]` - Same as above, but
///   allows the Rust struct to have fewer fields than the protobuf type
///
/// # High-Level Flow
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ 1. PARSE INPUT                                                  │
/// │    - Parse struct or enum definition                            │
/// │    - Check for serialize_only attribute                         │
/// │    - Check for validate_against attribute                       │
/// │    - Strip #[field(...)] attributes from output                 │
/// └─────────────────────────────────────────────────────────────────┘
///                              ↓
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ 2. BRANCH ON TYPE                                               │
/// │    ┌──────────────────┐         ┌──────────────────┐          │
/// │    │ STRUCT PATH      │         │ ENUM PATH        │          │
/// │    │ (Messages)       │         │ (Oneofs)         │          │
/// │    └──────────────────┘         └──────────────────┘          │
/// └─────────────────────────────────────────────────────────────────┘
///                ↓                              ↓
/// ┌──────────────────────────┐   ┌──────────────────────────────┐
/// │ 3. DELEGATE TO MODULE    │   │ 3. DELEGATE TO MODULE        │
/// │   struct_impl::          │   │   enum_impl::                │
/// │   process_struct_fields  │   │   process_enum_variants      │
/// └──────────────────────────┘   └──────────────────────────────┘
///                ↓                              ↓
/// ┌──────────────────────────┐   ┌──────────────────────────────┐
/// │ 4. RECEIVE TRAIT IMPLS   │   │ 4. RECEIVE TRAIT IMPLS       │
/// │ - ProtoType              │   │ - ProtoType                  │
/// │ - ProtoFieldSerialize    │   │ - ProtoFieldSerialize        │
/// │ - ProtoFieldDeserialize  │   │ - ProtoFieldDeserialize      │
/// │ - ProtoMessage           │   │ (No ProtoMessage for enums)  │
/// │ - Validation tests       │   │ - Validation tests           │
/// └──────────────────────────┘   └──────────────────────────────┘
///                ↓                              ↓
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ 5. RETURN EXPANDED CODE                                         │
/// │    - Original struct/enum (with field attrs stripped)           │
/// │    - Generated trait implementations                            │
/// │    - Generated validation tests (if validate_against specified) │
/// └─────────────────────────────────────────────────────────────────┘
/// ```
///
/// This generates `ProtoType`, `ProtoFieldSerialize`, and `ProtoFieldDeserialize`
/// implementations for the enum (oneof in protobuf terms).
#[proc_macro_attribute]
pub fn proto_serializable(attr: TokenStream, item: TokenStream) -> TokenStream {
  let config = parse_macro_config(attr);
  let serialize_only = config.serialize_only;

  let input = parse_macro_input!(item as DeriveInput);
  let name = &input.ident;

  // Create a copy of the input with #[field(...)] attributes stripped
  // This ensures the output struct/enum doesn't have our custom attributes
  let mut stripped_input = input.clone();

  // Remove #[field(...)] attributes from the output so they don't cause compiler errors
  if let Data::Struct(data_struct) = &mut stripped_input.data {
    if let Fields::Named(fields) = &mut data_struct.fields {
      for field in &mut fields.named {
        field.attrs.retain(|attr| !attr.path().is_ident("field"));
      }
    }
  } else if let Data::Enum(data_enum) = &mut stripped_input.data {
    for variant in &mut data_enum.variants {
      variant.attrs.retain(|attr| !attr.path().is_ident("field"));
      // Also strip field attributes from struct variant fields
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

        let expanded = quote! {
            #stripped_input
            #proto_type_impl
            #serialize_impl
            #deserialize_impl
            #message_serialize_impl
            #message_deserialize_impl
            #validation_tests
        };
        TokenStream::from(expanded)
      },
      _ => panic!("Only named fields are supported for structs"),
    },
    Data::Enum(data_enum) => {
      let enum_impl::EnumProcessingResult {
        proto_type_impl,
        serialize_impl,
        deserialize_impl,
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

      let expanded = quote! {
          #stripped_input
          #proto_type_impl
          #serialize_impl
          #deserialize_impl
          #validation_tests
      };
      TokenStream::from(expanded)
    },
    Data::Union(_) => panic!("Only structs and enums are supported"),
  }
}
