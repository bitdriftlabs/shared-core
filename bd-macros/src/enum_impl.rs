// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Enum variant processing and code generation for protobuf serialization.
//!
//! This module handles all enum-related code generation for the `#[proto_serializable]` macro.
//! It processes enum variants with their `#[field(...)]` attributes and generates complete
//! trait implementations for `ProtoType`, `ProtoFieldSerialize`, and `ProtoFieldDeserialize`.

use crate::struct_impl::FieldAttrs;
use quote::quote;
use syn::{Fields, Meta, Variant};

/// Parsed attributes from an enum variant's `#[field(...)]` attribute.
///
/// Used for enum variants that become protobuf oneof fields. Each variant must have
/// a unique tag number, and can optionally be marked as the preferred deserialization
/// target when multiple variants share the same tag.
///
/// # Examples
///
/// ```ignore
/// enum MyEnum {
///   #[field(id = 1, deserialize)]
///   PreferredVariant(String),   // tag: 1, explicit_deserialize: true
///
///   #[field(id = 1)]
///   AlternateVariant(String),   // tag: 1, explicit_deserialize: false
///                                // Both variants can serialize to tag 1,
///                                // but PreferredVariant is used for deserialization
/// }
/// ```
pub struct VariantAttrs {
  /// The protobuf field number for this variant.
  /// Required for all enum variants.
  tag: u32,

  /// Whether this variant is explicitly marked for deserialization.
  /// Used to resolve conflicts when multiple variants share the same tag number.
  /// Only one variant per tag can have this set to true.
  explicit_deserialize: bool,
}

impl VariantAttrs {
  /// Parses `#[field(...)]` attributes from an enum variant into a `VariantAttrs` struct.
  ///
  /// Similar to `FieldAttrs::parse()` but simpler since enum variants only support:
  /// - Field number (id) - required
  /// - Deserialize flag - optional, for conflict resolution
  ///
  /// # Panics
  ///
  /// Panics if the variant is missing the required `#[field(id = N)]` attribute.
  fn parse(variant: &Variant) -> Self {
    let mut tag = None;
    let mut explicit_deserialize = false;

    for attr in &variant.attrs {
      if attr.path().is_ident("field")
        && let Meta::List(_) = &attr.meta
      {
        let _ = attr.parse_nested_meta(|meta| {
          if meta.path.is_ident("id") {
            // Parse: id = 1
            let value = meta.value()?;
            tag = Some(value.parse::<syn::LitInt>()?.base10_parse().unwrap());
          } else if meta.path.is_ident("deserialize") {
            // Parse: deserialize (flag only)
            explicit_deserialize = true;
          }
          Ok(())
        });
      }
    }

    // All enum variants MUST have a tag
    let tag = tag.expect("All enum variants must have #[field(id = N)]");
    Self {
      tag,
      explicit_deserialize,
    }
  }
}

/// Result of processing a single enum variant.
///
/// This struct contains the generated code fragments needed for serialization
/// and deserialization of one enum variant. All three `handle_*_variant` functions
/// return this type instead of an anonymous tuple.
///
/// # Fields
///
/// * `compute_size_arm` - Match arm that computes the serialized size for this variant
/// * `serialize_arm` - Match arm that serializes this variant to the output stream
/// * `deserialize_code` - Code block that deserializes wire data into this variant
pub struct VariantCodeGenResult {
  pub compute_size_arm: proc_macro2::TokenStream,
  pub serialize_arm: proc_macro2::TokenStream,
  pub deserialize_code: proc_macro2::TokenStream,
}

/// Generates serialization code for enum tuple variants (variants with a single wrapped value).
///
/// Tuple variants like `Variant(Type)` are the most common form for oneof fields.
/// They wrap a single value and serialize it directly with the variant's field number.
///
/// # Example Input
///
/// ```rust,ignore
/// enum MyEnum {
///   #[field(id = 1)]
///   StringData(String),  // variant_name: StringData, field_type: String, tag: 1
///
///   #[field(id = 2)]
///   NumberData(i32),     // variant_name: NumberData, field_type: i32, tag: 2
/// }
/// ```
///
/// # Returns
///
/// A `VariantCodeGenResult` containing the match arms and deserialization code.
fn handle_tuple_variant(
  variant_name: &syn::Ident,
  field_type: &syn::Type,
  tag: u32,
) -> VariantCodeGenResult {
  let compute_size_arm = quote! {
    Self::#variant_name(v) =>
      <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>::compute_size(v, #tag),
  };

  let serialize_arm = quote! {
    Self::#variant_name(v) =>
      <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>::serialize(v, #tag, os)?,
  };

  let deserialize_code = quote! {
    let val = <#field_type as bd_proto_util::serialization::ProtoFieldDeserialize>
      ::deserialize(is)?;
    result = Some(Self::#variant_name(val));
  };

  VariantCodeGenResult {
    compute_size_arm,
    serialize_arm,
    deserialize_code,
  }
}

/// Generates serialization code for enum unit variants (variants with no data).
///
/// Unit variants like `Variant` are markers without associated data. They serialize
/// as an empty message with just the field tag, useful for representing states or flags.
///
/// # Example Input
///
/// ```rust,ignore
/// enum Status {
///   #[field(id = 1)]
///   Active,      // variant_name: Active, tag: 1
///
///   #[field(id = 2)]
///   Inactive,    // variant_name: Inactive, tag: 2
/// }
/// ```
///
/// # Wire Format
///
/// Unit variants serialize as length-delimited fields with zero length:
/// - Tag byte(s) indicating the field number
/// - Length byte of 0
/// - No payload bytes
///
/// # Returns
///
/// A `VariantCodeGenResult` containing the match arms and deserialization code.
fn handle_unit_variant(variant_name: &syn::Ident, tag: u32) -> VariantCodeGenResult {
  let compute_size_arm = quote! {
    Self::#variant_name =>
      <() as bd_proto_util::serialization::ProtoFieldSerialize>::compute_size(&(), #tag),
  };

  let serialize_arm = quote! {
    Self::#variant_name =>
      <() as bd_proto_util::serialization::ProtoFieldSerialize>::serialize(&(), #tag, os)?,
  };

  let deserialize_code = quote! {
    let _val = <() as bd_proto_util::serialization::ProtoFieldDeserialize>::deserialize(is)?;
    result = Some(Self::#variant_name);
  };

  VariantCodeGenResult {
    compute_size_arm,
    serialize_arm,
    deserialize_code,
  }
}

/// Generates serialization code for enum struct variants (variants with named fields).
///
/// Struct variants like `Variant { field1: Type1, field2: Type2 }` represent nested
/// messages within a oneof. Each field within the struct variant must have its own
/// field number and is serialized as part of a length-delimited message.
///
/// # Example Input
///
/// ```rust,ignore
/// enum MyEnum {
///   #[field(id = 1)]
///   UserInfo {
///     #[field(id = 1)]
///     name: String,
///     #[field(id = 2)]
///     age: u32,
///   },  // variant_name: UserInfo, fields: {name, age}, tag: 1
/// }
/// ```
///
/// # Wire Format
///
/// Struct variants serialize as nested messages:
/// 1. Outer tag for the variant (e.g., field 1)
/// 2. Length varint for the nested message size
/// 3. Nested message payload containing all fields
///
/// # Returns
///
/// A `VariantCodeGenResult` containing the match arms and deserialization code.
///
/// # Panics
///
/// Panics if any field within the struct variant is missing the required `#[field(id = N)]`
/// attribute.
fn handle_struct_variant(
  variant_name: &syn::Ident,
  fields: &syn::FieldsNamed,
  tag: u32,
) -> VariantCodeGenResult {
  let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();

  // Build field info with explicit field numbers required
  // Each field in a struct variant must have #[field(id = N)]
  let field_info: Vec<_> = fields
    .named
    .iter()
    .map(|field| {
      let field_name = field.ident.as_ref().unwrap();
      let field_type = &field.ty;
      let attrs = FieldAttrs::parse(field);
      let field_num = attrs.tag.unwrap_or_else(|| {
        panic!(
          "Enum struct variant field '{field_name}' must have explicit #[field(id = N)] attribute",
        )
      });
      (field_name, field_type, field_num)
    })
    .collect();

  // Generate code to compute the serialized size of each field
  let size_computations: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      quote! {
        size += <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>
          ::compute_size(#field_name, #field_num);
      }
    })
    .collect();

  // Match arm that computes the total size including:
  // 1. Sum of all field sizes
  // 2. Tag size for the variant field number
  // 3. Length varint size for the nested message
  let compute_size_arm = quote! {
    Self::#variant_name { #(#field_names),* } => {
      let mut size = 0u64;
      #(#size_computations)*
      if size == 0 { 0 } else {
        let tag_size = protobuf::rt::tag_size(#tag);
        let len_varint_size = protobuf::rt::compute_raw_varint64_size(size);
        tag_size + len_varint_size + size
      }
    }
  };

  // Generate code to serialize each field
  let serialize_fields: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      quote! {
        <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>
          ::serialize(#field_name, #field_num, os)?;
      }
    })
    .collect();

  // Recompute size for serialization (needed to write the length prefix)
  let size_computations_for_serialize: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      quote! {
        inner_size += <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>
          ::compute_size(#field_name, #field_num);
      }
    })
    .collect();

  // Match arm that serializes:
  // 1. Computes inner message size
  // 2. Writes outer tag + length
  // 3. Writes all fields
  let serialize_arm = quote! {
    Self::#variant_name { #(#field_names),* } => {
      let mut inner_size = 0u64;
      #(#size_computations_for_serialize)*

      if inner_size > 0 {
        os.write_tag(#tag, protobuf::rt::WireType::LengthDelimited)?;
        let inner_size_u32 = u32::try_from(inner_size)
          .map_err(|_| anyhow::anyhow!(
            "Enum struct variant payload too large to serialize: {} bytes exceeds u32::MAX",
            inner_size
          ))?;
        os.write_raw_varint32(inner_size_u32)?;
        #(#serialize_fields)*
      }
    }
  };

  // Initialize optional variables for each field
  let field_vars_init: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, _)| {
      let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());
      quote! {
        let mut #var_name: Option<#field_type> = None;
      }
    })
    .collect();

  // Generate match arms to deserialize each field by its field number
  let deserialize_arms_inner: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());
      quote! {
        #field_num => {
          #var_name = Some(
            <#field_type as bd_proto_util::serialization::ProtoFieldDeserialize>
              ::deserialize(is)?
          );
          Ok(true)
        }
      }
    })
    .collect();

  // Build the final struct from optional variables, using default values for missing fields
  let field_struct_init: Vec<_> = field_info
    .iter()
    .map(|(field_name, ..)| {
      let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());
      quote! {
        #field_name: #var_name.unwrap_or_default()
      }
    })
    .collect();

  // Complete deserialization code:
  // 1. Initialize optional variables for all fields
  // 2. Use read_nested to parse the length-delimited message
  // 3. Build the struct variant with collected values
  let deserialize_code = quote! {
    #(#field_vars_init)*

    bd_proto_util::serialization::read_nested(is, |is, field_number, _wire_type| {
      match field_number {
        #(#deserialize_arms_inner)*
        _ => Ok(false),
      }
    })?;

    result = Some(Self::#variant_name {
      #(#field_struct_init),*
    });
  };

  VariantCodeGenResult {
    compute_size_arm,
    serialize_arm,
    deserialize_code,
  }
}

/// Handles tag conflicts when multiple enum variants share the same protobuf field number.
///
/// In some cases, multiple enum variants may serialize to the same tag but represent
/// different types (e.g., `StringData(String)` and `BinaryData(Vec<u8>)` both at tag 1).
/// During deserialization, only one variant can be chosen. The `#[field(deserialize)]`
/// attribute explicitly marks which variant to use for deserialization.
///
/// # Parameters
///
/// - `tag_to_variant` - Map from tag numbers to (`deserialize_code`, `is_explicit`, `variant_name`)
/// - `tag` - The field number for this variant
/// - `generate_deserialize` - Token stream for deserializing this variant
/// - `explicit_deserialize` - True if this variant has `#[field(deserialize)]`
/// - `variant_name` - Name of the variant for error messages
///
/// # Panics
///
/// - If multiple variants with the same tag don't have explicit deserialization selection
/// - If multiple variants with the same tag both have `#[field(deserialize)]`
fn insert_with_conflict_check(
  tag_to_variant: &mut std::collections::BTreeMap<
    u32,
    (proc_macro2::TokenStream, bool, syn::Ident),
  >,
  tag: u32,
  generate_deserialize: proc_macro2::TokenStream,
  explicit_deserialize: bool,
  variant_name: syn::Ident,
) {
  if let Some((_, existing_explicit, existing_ident)) = tag_to_variant.get(&tag) {
    // There's already a variant with this tag
    if explicit_deserialize {
      // New variant is explicitly marked for deserialization
      assert!(
        !*existing_explicit,
        "Multiple variants for tag {tag} are marked with #[field(deserialize)]: {existing_ident} \
         and {variant_name}"
      );
      // Replace existing with this one
      tag_to_variant.insert(tag, (generate_deserialize, true, variant_name));
    } else if !*existing_explicit {
      // Both variants lack explicit marking - ambiguous!
      panic!(
        "Multiple variants for tag {tag} found without explicit deserialization selection: \
         {existing_ident} and {variant_name}. Use #[field(deserialize)] to specify one."
      );
    }
    // else: existing is explicitly marked, new is not - keep existing
  } else {
    // No conflict, insert normally
    tag_to_variant.insert(
      tag,
      (generate_deserialize, explicit_deserialize, variant_name),
    );
  }
}

/// Result of processing enum variants, containing all generated code fragments.
pub struct EnumProcessingResult {
  pub proto_type_impl: proc_macro2::TokenStream,
  pub serialize_impl: proc_macro2::TokenStream,
  pub deserialize_impl: proc_macro2::TokenStream,
}

/// Processes all variants in an enum, generating complete trait implementations.
///
/// # Parameters
///
/// - `data_enum` - The enum data structure
/// - `name` - The enum type name
/// - `impl_generics` - Generic parameters for impl blocks
/// - `ty_generics` - Generic parameters for the type
/// - `where_clause` - Where clause for generic constraints
/// - `serialize_only` - Whether to skip generating deserialization code
pub fn process_enum_variants(
  data_enum: &syn::DataEnum,
  name: &syn::Ident,
  impl_generics: &syn::ImplGenerics<'_>,
  ty_generics: &syn::TypeGenerics<'_>,
  where_clause: Option<&syn::WhereClause>,
  serialize_only: bool,
) -> EnumProcessingResult {
  let mut compute_size_arms = Vec::new();
  let mut serialize_arms = Vec::new();
  let mut tag_to_variant: std::collections::BTreeMap<
    u32,
    (proc_macro2::TokenStream, bool, syn::Ident),
  > = std::collections::BTreeMap::new();

  // Process each enum variant
  for variant in &data_enum.variants {
    let variant_name = &variant.ident;
    let attrs = VariantAttrs::parse(variant);

    // Determine the variant type and call the appropriate handler
    let VariantCodeGenResult {
      compute_size_arm,
      serialize_arm,
      deserialize_code,
    } = match &variant.fields {
      Fields::Unnamed(fields) => {
        assert!(
          fields.unnamed.len() == 1,
          "Only single-field tuple variants supported for OneOf enums"
        );
        let field_type = &fields.unnamed[0].ty;
        handle_tuple_variant(variant_name, field_type, attrs.tag)
      },
      Fields::Unit => handle_unit_variant(variant_name, attrs.tag),
      Fields::Named(fields) => handle_struct_variant(variant_name, fields, attrs.tag),
    };

    compute_size_arms.push(compute_size_arm);
    serialize_arms.push(serialize_arm);

    insert_with_conflict_check(
      &mut tag_to_variant,
      attrs.tag,
      deserialize_code,
      attrs.explicit_deserialize,
      variant_name.clone(),
    );
  }

  // Build the deserialization match arms
  let deserialize_match_arms = tag_to_variant.iter().map(|(tag, (code, ..))| {
    quote! { #tag => { #code Ok(true) } }
  });

  // Generate trait implementations
  let proto_type_impl = quote! {
       impl #impl_generics bd_proto_util::serialization::ProtoType
           for #name #ty_generics #where_clause {
               fn wire_type() -> protobuf::rt::WireType {
                   protobuf::rt::WireType::LengthDelimited
               }
           }
  };

  let serialize_impl = quote! {
      impl #impl_generics bd_proto_util::serialization::ProtoFieldSerialize
          for #name #ty_generics #where_clause {
              fn compute_size(&self, _field_number: u32) -> u64 {
                  let mut my_size = match self {
                      #(#compute_size_arms)*
                  };

                  let inner_size = my_size;
                  if inner_size == 0 { return 0; }

                  let tag_size = protobuf::rt::tag_size(_field_number);
                  let len_varint_size = protobuf::rt::compute_raw_varint64_size(inner_size);
                  tag_size + len_varint_size + inner_size
              }

              fn serialize(&self, field_number: u32, os: &mut protobuf::CodedOutputStream)
                  -> anyhow::Result<()> {
                      let mut my_size = match self {
                          #(#compute_size_arms)*
                      };
                      let inner_size = my_size;
                      if inner_size == 0 { return Ok(()); }

                      os.write_tag(field_number, protobuf::rt::WireType::LengthDelimited)?;
                      let inner_size_u32 = u32::try_from(inner_size)
                        .map_err(|_| anyhow::anyhow!(
                          "Enum variant payload too large to serialize: {} bytes exceeds u32::MAX",
                          inner_size
                        ))?;
                      os.write_raw_varint32(inner_size_u32)?;

                      match self {
                          #(#serialize_arms)*
                      }
                      Ok(())
                  }
          }
  };

  let deserialize_impl = if serialize_only {
    quote! {}
  } else {
    quote! {
          impl #impl_generics bd_proto_util::serialization::ProtoFieldDeserialize
              for #name #ty_generics #where_clause
          {
              fn deserialize(is: &mut protobuf::CodedInputStream) -> anyhow::Result<Self> {
                let mut result = None;

                bd_proto_util::serialization::read_nested(is,
                    |is, field_number, _wire_type| {
                    match field_number {
                        #(#deserialize_match_arms)*
                        _ => Ok(false),
                    }
                })?;

                result.ok_or_else(|| anyhow::anyhow!(concat!(
                    "No variant set for enum ",
                    stringify!(#name),
                    ". Use Option<",
                    stringify!(#name),
                    "> if this field should be optional."
                )))
            }
        }
    }
  };

  EnumProcessingResult {
    proto_type_impl,
    serialize_impl,
    deserialize_impl,
  }
}
