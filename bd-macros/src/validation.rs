// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Validation test generation for protobuf descriptor compatibility.
//!
//! This module generates runtime tests that validate a Rust struct's serialization
//! is compatible with a specified protobuf message descriptor. It handles:
//!
//! - Type canonicalization (mapping `Arc<str>`, `Box<str>`, etc. to their canonical forms)
//! - Descriptor field validation (field IDs, types, required/optional semantics)
//! - Bidirectional round-trip tests (Rust → Proto and Proto → Rust)
//! - Nested message validation

use crate::struct_impl::FieldAttrs;
use proc_macro2::TokenStream;
use quote::quote;

/// Canonical protobuf-compatible type after resolving aliases.
///
/// This enum represents the "canonical" form of a Rust type as it maps to protobuf types.
/// For example, `Arc<str>`, `Rc<String>`, `Box<str>`, and `String` all canonicalize to
/// `CanonicalType::String`.
#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalType {
  /// String types: String, Arc<str>, Rc<String>, Box<str>, &str, etc.
  String,
  /// Bytes types: Vec<u8>, Bytes, etc.
  Bytes,
  /// Boolean type
  Bool,
  /// Signed 32-bit integer
  I32,
  /// Signed 64-bit integer
  I64,
  /// Unsigned 32-bit integer
  U32,
  /// Unsigned 64-bit integer
  U64,
  /// 32-bit floating point
  F32,
  /// 64-bit floating point
  F64,
  /// Nested message type (the inner type is preserved for recursive validation)
  Message(Box<syn::Type>),
  /// Repeated field (Vec<T> where T is not u8)
  Repeated(Box<Self>),
  /// Map field (`HashMap<K, V>`, etc.)
  Map(Box<Self>, Box<Self>),
  /// Optional field (Option<T>)
  Optional(Box<Self>),
  /// Protobuf enum type (uses i32 on the wire)
  Enum,
}

/// Canonicalizes a Rust type to its protobuf-compatible form.
///
/// This function handles type aliases like `Arc<str>`, `Rc<String>`, `Box<str>`, etc.,
/// mapping them to their canonical protobuf representation.
pub fn canonicalize_rust_type(ty: &syn::Type, is_proto_enum: bool) -> CanonicalType {
  if is_proto_enum {
    return CanonicalType::Enum;
  }

  match ty {
    syn::Type::Path(type_path) => canonicalize_type_path(type_path),
    syn::Type::Reference(type_ref) => canonicalize_reference_type(type_ref),
    _ => panic!("Unsupported type for protobuf serialization: {ty:?}"),
  }
}

fn canonicalize_type_path(type_path: &syn::TypePath) -> CanonicalType {
  let Some(segment) = type_path.path.segments.last() else {
    panic!("Empty type path");
  };
  let ident = segment.ident.to_string();

  match ident.as_str() {
    // Direct scalar mappings
    "String" => CanonicalType::String,
    "bool" => CanonicalType::Bool,
    "i8" | "i16" | "i32" => CanonicalType::I32,
    "i64" | "isize" => CanonicalType::I64,
    "u8" | "u16" | "u32" => CanonicalType::U32,
    "u64" | "usize" => CanonicalType::U64,
    "f32" => CanonicalType::F32,
    "f64" => CanonicalType::F64,

    // Wrapper types that preserve the inner type's semantics
    "Arc" | "Rc" | "Box" => canonicalize_wrapper_type(segment),

    // Vec handling (special case: Vec<u8> is bytes)
    "Vec" => canonicalize_vec_type(segment),

    // Option handling
    "Option" => canonicalize_option_type(segment),

    // Map types
    "HashMap" | "AHashMap" | "BTreeMap" | "IndexMap" | "TinyMap" => canonicalize_map_type(segment),

    // Set types (treated as repeated)
    "HashSet" | "AHashSet" | "BTreeSet" | "TinySet" => canonicalize_set_type(segment),

    // Protobuf's MessageField wrapper
    "MessageField" => canonicalize_message_field_type(segment),

    // Assume it's a custom message type
    _ => CanonicalType::Message(Box::new(syn::Type::Path(type_path.clone()))),
  }
}

fn canonicalize_wrapper_type(segment: &syn::PathSegment) -> CanonicalType {
  let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
    panic!("Invalid {}: expected generic argument", segment.ident);
  };

  let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
    // Arc<str>, Box<str> - check for str
    return CanonicalType::String;
  };

  // Check if inner is `str` (e.g., Arc<str>)
  if let syn::Type::Path(inner_path) = inner
    && inner_path.path.is_ident("str")
  {
    return CanonicalType::String;
  }

  // Wrappers like Arc<String>, Box<Vec<u8>> preserve the inner type's canonical form
  canonicalize_rust_type(inner, false)
}

fn canonicalize_vec_type(segment: &syn::PathSegment) -> CanonicalType {
  let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
    panic!("Vec without generic arguments");
  };

  let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
    panic!("Vec without type argument");
  };

  // Special case: Vec<u8> is bytes
  if is_u8_type(inner) {
    return CanonicalType::Bytes;
  }

  let inner_canonical = canonicalize_rust_type(inner, false);
  CanonicalType::Repeated(Box::new(inner_canonical))
}

fn canonicalize_option_type(segment: &syn::PathSegment) -> CanonicalType {
  let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
    panic!("Option without generic arguments");
  };

  let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
    panic!("Option without type argument");
  };

  let inner_canonical = canonicalize_rust_type(inner, false);
  CanonicalType::Optional(Box::new(inner_canonical))
}

fn canonicalize_map_type(segment: &syn::PathSegment) -> CanonicalType {
  let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
    panic!("Map without generic arguments");
  };

  let mut iter = args.args.iter();
  let (Some(syn::GenericArgument::Type(key_ty)), Some(syn::GenericArgument::Type(val_ty))) =
    (iter.next(), iter.next())
  else {
    panic!("Map without key/value type arguments");
  };

  let key_canonical = canonicalize_rust_type(key_ty, false);
  let val_canonical = canonicalize_rust_type(val_ty, false);
  CanonicalType::Map(Box::new(key_canonical), Box::new(val_canonical))
}

fn canonicalize_set_type(segment: &syn::PathSegment) -> CanonicalType {
  let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
    panic!("Set without generic arguments");
  };

  let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
    panic!("Set without type argument");
  };

  let inner_canonical = canonicalize_rust_type(inner, false);
  CanonicalType::Repeated(Box::new(inner_canonical))
}

fn canonicalize_message_field_type(segment: &syn::PathSegment) -> CanonicalType {
  let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
    panic!("MessageField without generic arguments");
  };

  let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
    panic!("MessageField without type argument");
  };

  let inner_canonical = canonicalize_rust_type(inner, false);
  CanonicalType::Optional(Box::new(inner_canonical))
}

fn canonicalize_reference_type(type_ref: &syn::TypeReference) -> CanonicalType {
  // Handle &str -> String
  if let syn::Type::Path(path) = &*type_ref.elem
    && path.path.is_ident("str")
  {
    return CanonicalType::String;
  }

  // Otherwise, canonicalize the referenced type
  canonicalize_rust_type(&type_ref.elem, false)
}

fn is_u8_type(ty: &syn::Type) -> bool {
  if let syn::Type::Path(path) = ty {
    path.path.is_ident("u8")
  } else {
    false
  }
}

/// Information about a field needed for validation test generation.
#[derive(Clone)]
pub struct FieldInfo {
  pub name: syn::Ident,
  pub ty: syn::Type,
  pub tag: u32,
  pub canonical: CanonicalType,
}

/// Extracts field information from parsed field attributes.
pub fn extract_field_info(field_attrs: &[(syn::Ident, syn::Type, FieldAttrs)]) -> Vec<FieldInfo> {
  field_attrs
    .iter()
    .filter(|(_, _, attrs)| !attrs.skip && attrs.tag.is_some())
    .map(|(name, ty, attrs)| {
      let canonical = canonicalize_rust_type(ty, attrs.proto_enum);
      FieldInfo {
        name: name.clone(),
        ty: ty.clone(),
        tag: attrs.tag.unwrap(),
        canonical,
      }
    })
    .collect()
}

/// Generates a test value expression for a given canonical type.
fn generate_test_value(canonical: &CanonicalType, ty: &syn::Type) -> TokenStream {
  match canonical {
    CanonicalType::String => quote! { "test_string".to_string().into() },
    CanonicalType::Bytes => quote! { vec![1u8, 2, 3, 4, 5] },
    CanonicalType::Bool => quote! { true },
    CanonicalType::I32 => quote! { 42i32 as _ },
    CanonicalType::I64 => quote! { 42i64 as _ },
    CanonicalType::U32 => quote! { 42u32 as _ },
    CanonicalType::U64 => quote! { 42u64 as _ },
    // Use arbitrary values that don't approximate any mathematical constants
    CanonicalType::F32 => quote! { 1.5f32 },
    CanonicalType::F64 => quote! { 1.5f64 },
    CanonicalType::Enum => quote! { <#ty as Default>::default() },
    CanonicalType::Repeated(_) | CanonicalType::Map(..) | CanonicalType::Message(_) => {
      quote! { Default::default() }
    },
    CanonicalType::Optional(inner) => {
      let inner_value = generate_test_value(inner, ty);
      quote! { Some(#inner_value) }
    },
  }
}

/// Generates code that validates a field's type and produces a descriptive error on mismatch.
///
/// The generated code calls `bd_proto_util::serialization::validate_field_type` which returns
/// a `ValidationResult` with both the expected and actual types for clear error messages.
fn canonical_to_proto_type_check(
  canonical: &CanonicalType,
  field_name: &str,
  rust_type: &syn::Type,
) -> TokenStream {
  let canonical_expr = canonical_type_to_expr(canonical);

  quote! {
    {
      use bd_proto_util::serialization::{validate_field_type, ValidationResult};
      let rust_canonical = #canonical_expr;
      let proto_field_type = field_descriptor.runtime_field_type();
      let result = validate_field_type(&rust_canonical, &proto_field_type);
      if let ValidationResult::TypeMismatch { expected, actual } = result {
        panic!(
          "Field '{}' (Rust type: `{}`) is incompatible with proto field:\n  \
           - Rust canonical type: {}\n  \
           - Proto field type: {}\n  \
           Hint: The Rust type serializes as {} but the proto field expects {}.",
          #field_name,
          stringify!(#rust_type),
          expected,
          actual,
          expected,
          actual
        );
      }
    }
  }
}

/// Converts a `CanonicalType` to a `TokenStream` expression that constructs it at runtime.
fn canonical_type_to_expr(canonical: &CanonicalType) -> TokenStream {
  match canonical {
    CanonicalType::String => quote! { bd_proto_util::serialization::CanonicalType::String },
    CanonicalType::Bytes => quote! { bd_proto_util::serialization::CanonicalType::Bytes },
    CanonicalType::Bool => quote! { bd_proto_util::serialization::CanonicalType::Bool },
    CanonicalType::I32 => quote! { bd_proto_util::serialization::CanonicalType::I32 },
    CanonicalType::I64 => quote! { bd_proto_util::serialization::CanonicalType::I64 },
    CanonicalType::U32 => quote! { bd_proto_util::serialization::CanonicalType::U32 },
    CanonicalType::U64 => quote! { bd_proto_util::serialization::CanonicalType::U64 },
    CanonicalType::F32 => quote! { bd_proto_util::serialization::CanonicalType::F32 },
    CanonicalType::F64 => quote! { bd_proto_util::serialization::CanonicalType::F64 },
    CanonicalType::Enum => quote! { bd_proto_util::serialization::CanonicalType::Enum },
    CanonicalType::Message(_) => quote! { bd_proto_util::serialization::CanonicalType::Message },
    CanonicalType::Repeated(inner) => {
      let inner_expr = canonical_type_to_expr(inner);
      quote! { bd_proto_util::serialization::CanonicalType::Repeated(Box::new(#inner_expr)) }
    },
    CanonicalType::Map(key, value) => {
      let key_expr = canonical_type_to_expr(key);
      let value_expr = canonical_type_to_expr(value);
      quote! {
        bd_proto_util::serialization::CanonicalType::Map(
          Box::new(#key_expr),
          Box::new(#value_expr)
        )
      }
    },
    CanonicalType::Optional(inner) => {
      let inner_expr = canonical_type_to_expr(inner);
      quote! { bd_proto_util::serialization::CanonicalType::Optional(Box::new(#inner_expr)) }
    },
  }
}

/// Generates the descriptor validation test.
fn generate_descriptor_validation_test(
  name: &syn::Ident,
  proto_path: &syn::Path,
  field_infos: &[FieldInfo],
  validate_partial: bool,
) -> TokenStream {
  let field_validations: Vec<_> = field_infos
    .iter()
    .map(|info| {
      let field_name_str = info.name.to_string();
      let tag = info.tag;
      let rust_type = &info.ty;
      let type_check = canonical_to_proto_type_check(&info.canonical, &field_name_str, rust_type);

      quote! {
        // Validate field with tag #tag
        {
          let field_descriptor = descriptor.field_by_number(#tag)
            .unwrap_or_else(|| panic!(
              "Proto descriptor for {} is missing field with number {} (Rust field: '{}')",
              stringify!(#proto_path),
              #tag,
              #field_name_str
            ));

          // Validate field number matches
          assert_eq!(
            field_descriptor.number() as u32,
            #tag,
            "Field number mismatch for '{}': expected {}, got {}",
            #field_name_str,
            #tag,
            field_descriptor.number()
          );

          // Validate type compatibility
          #type_check
        }
      }
    })
    .collect();

  let missing_fields_check = if validate_partial {
    quote! {
      // Partial validation: only check that Rust fields exist in proto
      // (proto can have more fields than Rust)
    }
  } else {
    let rust_field_count = field_infos.len();
    quote! {
      // Full validation: check that all proto fields are covered
      let proto_field_count = descriptor.fields().count();
      assert_eq!(
        proto_field_count,
        #rust_field_count,
        "Field count mismatch: proto {} has {} fields, Rust {} has {} fields. \
         Use validate_partial if you intentionally want fewer fields.",
        stringify!(#proto_path),
        proto_field_count,
        stringify!(#name),
        #rust_field_count
      );
    }
  };

  quote! {
    #[test]
    fn test_descriptor_compatibility() {
      use protobuf::MessageFull;

      let descriptor = <#proto_path as MessageFull>::descriptor();

      #(#field_validations)*

      #missing_fields_check
    }
  }
}

/// Generates the macro-to-protobuf round-trip test.
fn generate_macro_to_proto_test(
  name: &syn::Ident,
  proto_path: &syn::Path,
  field_infos: &[FieldInfo],
) -> TokenStream {
  let test_field_values: Vec<_> = field_infos
    .iter()
    .map(|info| {
      let field_name = &info.name;
      let value = generate_test_value(&info.canonical, &info.ty);
      quote! { #field_name: #value }
    })
    .collect();

  quote! {
    #[test]
    #[allow(clippy::needless_update)]
    fn test_macro_to_protobuf_roundtrip() -> anyhow::Result<()> {
      use protobuf::Message;
      use bd_proto_util::serialization::ProtoMessage;

      // Create Rust instance with test values
      let rust_obj = #name {
        #(#test_field_values),*,
        ..Default::default()
      };

      // Serialize using our macro
      let bytes = rust_obj.serialize_message_to_bytes()?;

      // Deserialize using protobuf
      let proto_obj = <#proto_path as Message>::parse_from_bytes(&bytes)?;

      // Re-serialize using protobuf
      let bytes2 = proto_obj.write_to_bytes()?;

      // Deserialize back using our macro
      let rust_obj2 = #name::deserialize_message_from_bytes(&bytes2)?;

      // The round-trip should produce identical bytes
      let bytes3 = rust_obj2.serialize_message_to_bytes()?;
      assert_eq!(
        bytes, bytes3,
        "Round-trip serialization mismatch: Rust -> Proto -> Rust produced different bytes"
      );

      Ok(())
    }
  }
}

/// Generates the full round-trip test with non-default values.
fn generate_full_roundtrip_test(
  name: &syn::Ident,
  proto_path: &syn::Path,
  field_infos: &[FieldInfo],
) -> TokenStream {
  let test_field_values: Vec<_> = field_infos
    .iter()
    .map(|info| {
      let field_name = &info.name;
      let value = generate_test_value(&info.canonical, &info.ty);
      quote! { #field_name: #value }
    })
    .collect();

  quote! {
    #[test]
    #[allow(clippy::needless_update)]
    fn test_full_roundtrip_with_values() -> anyhow::Result<()> {
      use protobuf::Message;
      use bd_proto_util::serialization::ProtoMessage;

      // Create Rust instance with test values
      let original = #name {
        #(#test_field_values),*,
        ..Default::default()
      };

      // Rust -> bytes (macro)
      let bytes1 = original.serialize_message_to_bytes()?;

      // bytes -> Proto
      let proto_obj = <#proto_path as Message>::parse_from_bytes(&bytes1)?;

      // Proto -> bytes
      let bytes2 = proto_obj.write_to_bytes()?;

      // bytes -> Rust (macro)
      let roundtripped = #name::deserialize_message_from_bytes(&bytes2)?;

      // Rust -> bytes (macro) again
      let bytes3 = roundtripped.serialize_message_to_bytes()?;

      // Verify the serialization is stable
      assert_eq!(
        bytes1, bytes3,
        "Full round-trip produced different bytes"
      );

      Ok(())
    }
  }
}

/// Generates all validation tests for a struct.
pub fn generate_struct_validation_tests(
  name: &syn::Ident,
  _fields: &syn::FieldsNamed,
  proto_path: &syn::Path,
  field_attrs: &[(syn::Ident, syn::Type, FieldAttrs)],
  validate_partial: bool,
) -> TokenStream {
  let field_infos = extract_field_info(field_attrs);

  let descriptor_test =
    generate_descriptor_validation_test(name, proto_path, &field_infos, validate_partial);
  let macro_to_proto_test = generate_macro_to_proto_test(name, proto_path, &field_infos);
  let full_roundtrip_test = generate_full_roundtrip_test(name, proto_path, &field_infos);

  quote! {
    #descriptor_test
    #macro_to_proto_test
    #full_roundtrip_test
  }
}

/// Configuration for validation test generation.
#[derive(Default)]
pub struct ValidationConfig {
  /// The protobuf type to validate against
  pub proto_path: Option<syn::Path>,
  /// Whether to allow partial validation (Rust struct can have fewer fields)
  pub validate_partial: bool,
}
