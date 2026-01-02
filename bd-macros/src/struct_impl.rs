// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Struct field processing and code generation for protobuf serialization.
//!
//! This module handles all struct-related code generation for the `#[proto_serializable]` macro.
//! It processes struct fields with their `#[field(...)]` attributes and generates complete
//! trait implementations for `ProtoType`, `ProtoFieldSerialize`, `ProtoFieldDeserialize`, and
//! `ProtoMessage`.

use quote::quote;
use syn::{Field, Meta};

/// Parsed attributes from a struct field's `#[field(...)]` attribute.
///
/// # Examples
///
/// ```ignore
/// #[field(id = 1)]                    // tag: Some(1), others: default
/// #[field(id = 2, required)]          // tag: Some(2), required: true
/// #[field(id = 3, repeated)]          // tag: Some(3), repeated: true
/// ```
#[derive(Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct FieldAttrs {
  /// The protobuf field number (e.g., `#[field(id = 1)]`).
  /// Required for all non-skipped fields.
  pub tag: Option<u32>,

  /// Whether to skip this field during serialization/deserialization (`#[field(skip)]`).
  /// Skipped fields use their default value or `default_expr` during deserialization.
  pub skip: bool,

  /// Whether this field is required (`#[field(required)]`).
  /// Deserialization fails if a required field is missing.
  pub required: bool,

  /// Whether this is a repeated field like Vec or `HashMap` (`#[field(repeated)]`).
  /// Can be auto-detected for known collection types.
  pub repeated: bool,

  /// Custom default expression (`#[field(default = "expr")]`).
  /// Used for skipped fields or when the field is missing during deserialization.
  pub default_expr: Option<String>,

  /// Serialize using a different type (`#[field(serialize_as = "Type")]`).
  /// Useful for converting types (e.g., serialize `usize` as `u64`).
  pub serialize_as: Option<syn::Type>,

  /// Whether this field is a protobuf enum type (`#[field(proto_enum)]`).
  /// Enables special handling for proto3 enum default values.
  pub proto_enum: bool,
}

impl FieldAttrs {
  /// Parses `#[field(...)]` attributes from a struct field into a `FieldAttrs` struct.
  ///
  /// This function extracts all protobuf-related metadata from a field's attributes,
  /// including field numbering, serialization options, and special handling flags.
  pub fn parse(field: &Field) -> Self {
    let mut tag = None;
    let mut skip = false;
    let mut required = false;
    let mut repeated = false;
    let mut default_expr = None;
    let mut serialize_as = None;
    let mut proto_enum = false;

    for attr in &field.attrs {
      // Only process #[field(...)] attributes
      if attr.path().is_ident("field")
        && let Meta::List(meta_list) = &attr.meta
      {
        // Quick check for simple #[field(skip)] case (no `=` sign)
        let content = meta_list.tokens.to_string();
        if content == "skip" {
          skip = true;
        }

        // Parse nested metadata for key-value pairs
        let _ = attr.parse_nested_meta(|meta| {
          if meta.path.is_ident("id") {
            // Parse: id = 1
            let value = meta.value()?;
            tag = Some(value.parse::<syn::LitInt>()?.base10_parse().unwrap());
          } else if meta.path.is_ident("skip") {
            // Parse: skip (flag only)
            skip = true;
          } else if meta.path.is_ident("default") {
            // Parse: default = "expression"
            let value = meta.value()?;
            let s: syn::LitStr = value.parse()?;
            default_expr = Some(s.value());
          } else if meta.path.is_ident("required") {
            // Parse: required (flag only)
            required = true;
          } else if meta.path.is_ident("repeated") {
            // Parse: repeated (flag only)
            repeated = true;
          } else if meta.path.is_ident("proto_enum") {
            // Parse: proto_enum (flag only)
            proto_enum = true;
          } else if meta.path.is_ident("serialize_as") {
            // Parse: serialize_as = "Type"
            let value = meta.value()?;
            let s: syn::LitStr = value.parse()?;
            serialize_as = Some(syn::parse_str(&s.value()).expect("Invalid type in serialize_as"));
          }
          Ok(())
        });
      }
    }

    Self {
      tag,
      skip,
      required,
      repeated,
      default_expr,
      serialize_as,
      proto_enum,
    }
  }
}

/// Checks if a type is a repeated field type (`Vec`, `HashMap`, `AHashMap`, `TinyMap`, etc.)
/// Returns true if the type should use `RepeatedFieldDeserialize` trait.
/// This is used for auto-detection of common collection types.
fn is_repeated_field_type(ty: &syn::Type) -> bool {
  // Extract the base type path
  if let syn::Type::Path(type_path) = ty {
    type_path.path.segments.last().is_some_and(|segment| {
      let ident = segment.ident.to_string();

      // Special case: Vec<u8> is a bytes field, not a repeated field
      if ident == "Vec" {
        // Check if the generic argument is u8
        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments
          && let Some(syn::GenericArgument::Type(syn::Type::Path(inner))) = args.args.first()
          && inner.path.is_ident("u8")
        {
          return false; // Vec<u8> is bytes, not repeated
        }
      }

      // Check for known repeated field types
      matches!(
        ident.as_str(),
        "Vec"
          | "HashMap"
          | "AHashMap"
          | "TinyMap"
          | "TinySet"
          | "BTreeMap"
          | "BTreeSet"
          | "IndexMap"
      )
    })
  } else {
    false
  }
}

/// Result of processing struct fields, containing complete trait implementations.
pub struct StructProcessingResult {
  pub proto_type_impl: proc_macro2::TokenStream,
  pub serialize_impl: proc_macro2::TokenStream,
  pub deserialize_impl: proc_macro2::TokenStream,
  pub message_impl: proc_macro2::TokenStream,
}

/// Processes all fields in a struct, generating complete trait implementations.
///
/// This function handles all struct field processing and generates the complete
/// trait implementations for `ProtoType`, `ProtoFieldSerialize`, `ProtoFieldDeserialize`,
/// and `ProtoMessage`.
///
/// # Parameters
///
/// - `fields` - The named fields of the struct
/// - `name` - The struct type name
/// - `impl_generics` - Generic parameters for impl blocks
/// - `ty_generics` - Generic parameters for the type
/// - `where_clause` - Where clause for generic constraints
/// - `serialize_only` - Whether to skip generating deserialization code
///
/// # Returns
///
/// A `StructProcessingResult` containing all four trait implementations.
pub fn process_struct_fields(
  fields: &syn::FieldsNamed,
  name: &syn::Ident,
  impl_generics: &syn::ImplGenerics<'_>,
  ty_generics: &syn::TypeGenerics<'_>,
  where_clause: Option<&syn::WhereClause>,
  serialize_only: bool,
) -> StructProcessingResult {
  let mut field_processing = Vec::new();
  let mut deserialize_arms = Vec::new();
  let mut size_computations = Vec::new();
  let mut field_vars_init = Vec::new();
  let mut field_struct_init = Vec::new();

  // First pass: Parse all field attributes
  let fields_with_attrs: Vec<_> = fields
    .named
    .iter()
    .map(|field| {
      let attrs = FieldAttrs::parse(field);
      (field, attrs)
    })
    .collect();

  // Second pass: Generate code for each field
  for (field, attrs) in &fields_with_attrs {
    let field_name = field.ident.as_ref().unwrap();
    let field_type = &field.ty;
    let attrs = attrs.clone();

    // Handle skipped fields
    if attrs.skip {
      let init_expr = attrs.default_expr.map_or_else(
        || quote! { Default::default() },
        |expr_str| {
          let expr: syn::Expr = syn::parse_str(&expr_str).expect("Invalid default expression");
          quote! { #expr }
        },
      );
      field_struct_init.push(quote! {
          #field_name: #init_expr
      });
      continue;
    }

    // All non-skipped fields must have explicit field numbering
    let tag = attrs.tag.unwrap_or_else(|| {
      panic!("Field '{field_name}' must have explicit #[field(id = N)] attribute")
    });

    // Determine serialization type and conversion expressions
    let (serialize_type, serialize_with_ref, deserialize_convert) =
      attrs.serialize_as.as_ref().map_or_else(
        || {
          (
            field_type.clone(),
            quote! { &self.#field_name },
            quote! { val },
          )
        },
        |ser_type| {
          (
            ser_type.clone(),
            quote! { &(self.#field_name as #ser_type) },
            quote! {
              #field_type::try_from(val).map_err(|_| {
                anyhow::anyhow!(
                  "Field '{}' value cannot be converted from {} to {}",
                  stringify!(#field_name),
                  stringify!(#ser_type),
                  stringify!(#field_type)
                )
              })?
            },
          )
        },
      );

    // Handle proto_enum vs regular field serialization
    if attrs.proto_enum {
      size_computations.push(quote! {
        {
          use protobuf::Enum;
          let val = self.#field_name.value();
          if val != 0 {
            my_size += protobuf::rt::int32_size(#tag, val);
          }
        }
      });

      field_processing.push(quote! {
        {
          use protobuf::Enum;
          let val = self.#field_name.value();
          if val != 0 {
            os.write_enum(#tag, val)?;
          }
        }
      });
    } else {
      size_computations.push(quote! {
          my_size += <#serialize_type as
              bd_proto_util::serialization::ProtoFieldSerialize>::compute_size(
                  #serialize_with_ref, #tag);
      });

      field_processing.push(quote! {
          <#serialize_type as
              bd_proto_util::serialization::ProtoFieldSerialize>::serialize(
                  #serialize_with_ref, #tag, os)?;
      });
    }

    let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());

    // Check if this is a repeated field
    let is_repeated = attrs.repeated || is_repeated_field_type(field_type);

    // Validation
    assert!(
      !(attrs.required && is_repeated),
      "Field '{field_name}' cannot be both required and a repeated field type"
    );
    assert!(
      !(attrs.serialize_as.is_some() && is_repeated),
      "Field '{field_name}' cannot use serialize_as with repeated field types"
    );

    // Handle repeated vs non-repeated deserialization
    if is_repeated {
      field_vars_init.push(quote! {
          let mut #var_name: #field_type = Default::default();
      });

      deserialize_arms.push(quote! {
          #tag => {
              use bd_proto_util::serialization::RepeatedFieldDeserialize;
              let elem = <#field_type as RepeatedFieldDeserialize>
                  ::deserialize_element(is)?;
              <#field_type as RepeatedFieldDeserialize>
                  ::add_element(&mut #var_name, elem);
              Ok(true)
          }
      });
    } else {
      field_vars_init.push(quote! {
          let mut #var_name: Option<#field_type> = None;
      });

      if attrs.proto_enum {
        deserialize_arms.push(quote! {
            #tag => {
                use protobuf::Enum;
                let val = is.read_int32()?;
                #var_name = Some(
                  <#field_type as protobuf::Enum>::from_i32(val)
                    .unwrap_or_else(|| <#field_type as Default>::default())
                );
                Ok(true)
            }
        });
      } else {
        deserialize_arms.push(quote! {
            #tag => {
                let val = <#serialize_type as
                    bd_proto_util::serialization::ProtoFieldDeserialize>::deserialize(is)?;
                #var_name = Some(#deserialize_convert);
                Ok(true)
            }
        });
      }
    }

    // Build the final struct field initialization expression
    let init_expr = if is_repeated {
      quote! { #var_name }
    } else if attrs.required {
      quote! { #var_name.ok_or_else(|| anyhow::anyhow!(concat!("Field ", stringify!(#field_name), " is required")))? }
    } else {
      attrs.default_expr.map_or_else(
        || quote! { #var_name.unwrap_or_default() },
        |expr_str| {
          let expr: syn::Expr = syn::parse_str(&expr_str).expect("Invalid default expression");
          quote! { #var_name.unwrap_or_else(|| #expr) }
        },
      )
    };

    field_struct_init.push(quote! {
        #field_name: #init_expr
    });
  }

  // Generate the ProtoType trait implementation
  let proto_type_impl = quote! {
      impl #impl_generics bd_proto_util::serialization::ProtoType
          for #name #ty_generics #where_clause
      {
          fn wire_type() -> protobuf::rt::WireType {
              protobuf::rt::WireType::LengthDelimited
          }
      }
  };

  // Generate the ProtoFieldSerialize trait implementation
  let serialize_impl = quote! {
      impl #impl_generics bd_proto_util::serialization::ProtoFieldSerialize
          for #name #ty_generics #where_clause
      {
          fn compute_size(&self, _field_number: u32) -> u64 {
              let mut my_size = 0;
              #(#size_computations)*

              let inner_size = my_size;

              if inner_size == 0 {
                  return 0;
              }

              let tag_size = protobuf::rt::tag_size(_field_number);
              let len_varint_size = protobuf::rt::compute_raw_varint64_size(inner_size);

              tag_size + len_varint_size + inner_size
          }

          fn serialize(&self, field_number: u32, os: &mut protobuf::CodedOutputStream)
              -> anyhow::Result<()> {
                  let mut my_size = 0;
                  #(#size_computations)*
                  let inner_size = my_size;

                  if inner_size == 0 {
                      return Ok(());
                  }

                  os.write_tag(field_number, protobuf::rt::WireType::LengthDelimited)?;
                  let inner_size_u32 = u32::try_from(inner_size)
                    .map_err(|_| anyhow::anyhow!(
                      "Message payload too large to serialize: {} bytes exceeds u32::MAX",
                      inner_size
                    ))?;
                  os.write_raw_varint32(inner_size_u32)?;

                  #(#field_processing)*

                  Ok(())
              }

          fn compute_size_explicit(&self, _field_number: u32) -> u64 {
              // Messages always have explicit presence (empty message is different from not
              // present), so explicit and implicit are the same
              self.compute_size(_field_number)
          }

          fn serialize_explicit(&self, field_number: u32, os: &mut protobuf::CodedOutputStream)
              -> anyhow::Result<()> {
              // Messages always have explicit presence (empty message is different from not
              // present), so explicit and implicit are the same
              self.serialize(field_number, os)
          }
      }
  };

  // Generate the ProtoFieldDeserialize trait implementation
  let deserialize_impl = if serialize_only {
    quote! {}
  } else {
    quote! {
        impl #impl_generics bd_proto_util::serialization::ProtoFieldDeserialize
            for #name #ty_generics #where_clause
        {
            fn deserialize(is: &mut protobuf::CodedInputStream) -> anyhow::Result<Self> {
                #(#field_vars_init)*

                bd_proto_util::serialization::read_nested(
                    is,
                    |is, tag| {
                        match tag.field_number {
                            #(#deserialize_arms)*
                            _ => Ok(false),
                        }
                    }
                )?;

                Ok(Self {
                    #(#field_struct_init),*
                })
            }
        }
    }
  };

  // Generate the ProtoMessage trait implementation (top-level message serialization)
  let message_impl = if serialize_only {
    quote! {}
  } else {
    quote! {
        impl #impl_generics bd_proto_util::serialization::ProtoMessage
            for #name #ty_generics #where_clause
        {
            fn serialize_message(
                &self,
                os: &mut protobuf::CodedOutputStream,
            ) -> anyhow::Result<()> {
                // Serialize fields directly without outer tag+length wrapper
                #(#field_processing)*
                Ok(())
            }

            fn deserialize_message(
                is: &mut protobuf::CodedInputStream,
            ) -> anyhow::Result<Self> {
                // Deserialize fields directly without expecting outer tag+length wrapper
                #(#field_vars_init)*


                while !is.eof()? {
                    let tag = is.read_raw_varint32()?;
                    let tag = bd_proto_util::serialization::runtime::Tag::new(tag)?;

                    let handled: bool = match tag.field_number {
                        #(#deserialize_arms)*
                        _ => Ok::<bool, anyhow::Error>(false),
                    }?;

                    if !handled {
                        is.skip_field(tag.wire_type)?;
                    }
                }

                Ok(Self {
                    #(#field_struct_init),*
                })
            }
        }
    }
  };

  StructProcessingResult {
    proto_type_impl,
    serialize_impl,
    deserialize_impl,
    message_impl,
  }
}
