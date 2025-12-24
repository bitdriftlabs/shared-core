// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Field, Fields, Meta, Variant, parse_macro_input};

// === Helper structs for parsed attributes ===

#[derive(Clone)]
struct FieldAttrs {
  tag: Option<u32>,
  skip: bool,
  required: bool,
  repeated: bool,
  default_expr: Option<String>,
}

struct VariantAttrs {
  tag: u32,
  explicit_deserialize: bool,
}

// === Attribute parsing helpers ===

fn parse_field_attrs(field: &Field) -> FieldAttrs {
  let mut tag = None;
  let mut skip = false;
  let mut required = false;
  let mut repeated = false;
  let mut default_expr = None;

  for attr in &field.attrs {
    if attr.path().is_ident("field")
      && let Meta::List(meta_list) = &attr.meta
    {
      let content = meta_list.tokens.to_string();
      if content == "skip" {
        skip = true;
      }
      let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("id") {
          let value = meta.value()?;
          tag = Some(value.parse::<syn::LitInt>()?.base10_parse().unwrap());
        } else if meta.path.is_ident("skip") {
          skip = true;
        } else if meta.path.is_ident("default") {
          let value = meta.value()?;
          let s: syn::LitStr = value.parse()?;
          default_expr = Some(s.value());
        } else if meta.path.is_ident("required") {
          required = true;
        } else if meta.path.is_ident("repeated") {
          repeated = true;
        }
        Ok(())
      });
    }
  }

  FieldAttrs {
    tag,
    skip,
    required,
    repeated,
    default_expr,
  }
}

fn parse_variant_attrs(variant: &Variant) -> VariantAttrs {
  let mut tag = None;
  let mut explicit_deserialize = false;

  for attr in &variant.attrs {
    if attr.path().is_ident("field")
      && let Meta::List(_) = &attr.meta
    {
      let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("id") {
          let value = meta.value()?;
          tag = Some(value.parse::<syn::LitInt>()?.base10_parse().unwrap());
        } else if meta.path.is_ident("deserialize") {
          explicit_deserialize = true;
        }
        Ok(())
      });
    }
  }

  let tag = tag.expect("All enum variants must have #[field(id = N)]");
  VariantAttrs {
    tag,
    explicit_deserialize,
  }
}

// === Type detection helpers ===

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
        "Vec" | "HashMap" | "AHashMap" | "TinyMap" | "BTreeMap" | "IndexMap"
      )
    })
  } else {
    false
  }
}

// === Enum variant handlers ===

fn handle_tuple_variant(
  variant_name: &syn::Ident,
  field_type: &syn::Type,
  tag: u32,
) -> (
  proc_macro2::TokenStream,
  proc_macro2::TokenStream,
  proc_macro2::TokenStream,
) {
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

  (compute_size_arm, serialize_arm, deserialize_code)
}

fn handle_unit_variant(
  variant_name: &syn::Ident,
  tag: u32,
) -> (
  proc_macro2::TokenStream,
  proc_macro2::TokenStream,
  proc_macro2::TokenStream,
) {
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

  (compute_size_arm, serialize_arm, deserialize_code)
}

fn handle_struct_variant(
  variant_name: &syn::Ident,
  fields: &syn::FieldsNamed,
  tag: u32,
) -> (
  proc_macro2::TokenStream,
  proc_macro2::TokenStream,
  proc_macro2::TokenStream,
) {
  let field_names: Vec<_> = fields.named.iter().map(|f| &f.ident).collect();

  // Build field info with auto-assigned numbers
  let field_info: Vec<_> = fields
    .named
    .iter()
    .enumerate()
    .map(|(idx, field)| {
      let field_name = field.ident.as_ref().unwrap();
      let field_type = &field.ty;
      #[allow(clippy::cast_possible_truncation)]
      let field_num = (idx + 1) as u32;
      (field_name, field_type, field_num)
    })
    .collect();

  // Size computation
  let size_computations: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      quote! {
        size += <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>
          ::compute_size(#field_name, #field_num);
      }
    })
    .collect();

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

  // Serialization
  let serialize_fields: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      quote! {
        <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>
          ::serialize(#field_name, #field_num, os)?;
      }
    })
    .collect();

  let size_computations_for_serialize: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, field_num)| {
      quote! {
        inner_size += <#field_type as bd_proto_util::serialization::ProtoFieldSerialize>
          ::compute_size(#field_name, #field_num);
      }
    })
    .collect();

  let serialize_arm = quote! {
    Self::#variant_name { #(#field_names),* } => {
      let mut inner_size = 0u64;
      #(#size_computations_for_serialize)*

      if inner_size > 0 {
        os.write_tag(#tag, protobuf::rt::WireType::LengthDelimited)?;
        os.write_raw_varint32(inner_size as u32)?;
        #(#serialize_fields)*
      }
    }
  };

  // Deserialization
  let field_vars_init: Vec<_> = field_info
    .iter()
    .map(|(field_name, field_type, _)| {
      let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());
      quote! {
        let mut #var_name: Option<#field_type> = None;
      }
    })
    .collect();

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

  let field_struct_init: Vec<_> = field_info
    .iter()
    .map(|(field_name, ..)| {
      let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());
      quote! {
        #field_name: #var_name.unwrap_or_default()
      }
    })
    .collect();

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

  (compute_size_arm, serialize_arm, deserialize_code)
}

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
    if explicit_deserialize {
      assert!(
        !*existing_explicit,
        "Multiple variants for tag {tag} are marked with #[field(deserialize)]: {existing_ident} \
         and {variant_name}"
      );
      tag_to_variant.insert(tag, (generate_deserialize, true, variant_name));
    } else if !*existing_explicit {
      panic!(
        "Multiple variants for tag {tag} found without explicit deserialization selection: \
         {existing_ident} and {variant_name}. Use #[field(deserialize)] to specify one."
      );
    }
  } else {
    tag_to_variant.insert(
      tag,
      (generate_deserialize, explicit_deserialize, variant_name),
    );
  }
}

#[proc_macro_attribute]
pub fn proto_serializable(attr: TokenStream, item: TokenStream) -> TokenStream {
  // Check for arguments
  let attr_str = attr.to_string();
  // DEBUG: panic!("Attr: '{}'", attr_str);
  let serialize_only = attr_str.contains("serialize_only");

  // We expect this attribute to be placed on a struct.
  // We will parse the struct, then return the struct AS IS (so it exists),
  // followed by the implementation of the ProtoType, ProtoFieldSerialize, and ProtoFieldDeserialize
  // traits.

  let input = parse_macro_input!(item as DeriveInput);
  let name = &input.ident;

  let mut field_processing = Vec::new();
  let mut deserialize_arms = Vec::new();
  let mut size_computations = Vec::new();

  // Default initializers for the struct construction during deserialization.
  // Since we need to construct the struct at the end, and fields can be in any order in the wire
  // format, we generally need to start with Default values or Options.
  // For this prototype, let's assume all fields implement Default or are Option wrapped by
  // convention OR, we declare local variables as Option<Type> for every field, set them as we
  // parse, and then unwrap/default them at the end.
  let mut field_vars_init = Vec::new();
  let mut field_struct_init = Vec::new();

  let mut stripped_input = input.clone();

  // Strip attributes upfront
  if let Data::Struct(data_struct) = &mut stripped_input.data {
    if let Fields::Named(fields) = &mut data_struct.fields {
      for field in &mut fields.named {
        field.attrs.retain(|attr| !attr.path().is_ident("field"));
      }
    }
  } else if let Data::Enum(data_enum) = &mut stripped_input.data {
    for variant in &mut data_enum.variants {
      variant.attrs.retain(|attr| !attr.path().is_ident("field"));
    }
  }

  let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

  match &input.data {
    Data::Struct(data_struct) => {
      // ... existing struct logic ...
      match &data_struct.fields {
        Fields::Named(fields) => {
          // First pass: collect all field attrs and check for explicit numbering mode
          let fields_with_attrs: Vec<_> = fields
            .named
            .iter()
            .map(|field| {
              let attrs = parse_field_attrs(field);
              (field, attrs)
            })
            .collect();

          // Check if any non-skipped field has explicit numbering
          let has_any_explicit = fields_with_attrs
            .iter()
            .any(|(_, attrs)| !attrs.skip && attrs.tag.is_some());

          // Second pass: process fields with assigned numbers
          for (field_idx, (field, attrs)) in fields_with_attrs.iter().enumerate() {
            let field_name = field.ident.as_ref().unwrap();
            let field_type = &field.ty;
            let attrs = attrs.clone(); // Clone so we can consume attrs later

            if attrs.skip {
              let init_expr = attrs.default_expr.map_or_else(
                || quote! { Default::default() },
                |expr_str| {
                  let expr: syn::Expr =
                    syn::parse_str(&expr_str).expect("Invalid default expression");
                  quote! { #expr }
                },
              );
              field_struct_init.push(quote! {
                  #field_name: #init_expr
              });
              continue;
            }

            // Determine field number: explicit or auto-assigned
            let tag = if has_any_explicit {
              attrs.tag.expect(
                "Some fields have explicit #[field = N], so all non-skipped fields must be \
                 explicit",
              )
            } else {
              // Auto-assign based on position (skip counted fields)
              let auto_idx = fields_with_attrs[..= field_idx]
                .iter()
                .filter(|(_, a)| !a.skip)
                .count();
              #[allow(clippy::cast_possible_truncation)]
              let field_num = auto_idx as u32;
              field_num
            };

            size_computations.push(quote! {
                my_size += <#field_type as
                    bd_proto_util::serialization::ProtoFieldSerialize>::compute_size(
                        &self.#field_name, #tag);
            });

            field_processing.push(quote! {
                <#field_type as
                    bd_proto_util::serialization::ProtoFieldSerialize>::serialize(
                        &self.#field_name, #tag, os)?;
            });

            let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());

            // Check if this is a repeated field:
            // 1. Explicitly marked with #[field(repeated)]
            // 2. Auto-detected as a known collection type (Vec, HashMap, etc.)
            let is_repeated = attrs.repeated || is_repeated_field_type(field_type);

            // Error if the field is marked as required but is also a repeated type
            assert!(
              !(attrs.required && is_repeated),
              "Field '{field_name}' cannot be both required and a repeated field type (Vec, \
               HashMap, etc.)"
            );

            if is_repeated {
              // For repeated fields, use the optimized RepeatedFieldDeserialize path
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

              deserialize_arms.push(quote! {
                  #tag => {
                      #var_name = Some(<#field_type as
                          bd_proto_util::serialization::ProtoFieldDeserialize>::deserialize(is)?);
                      Ok(true)
                  }
              });
            }

            let var_name = syn::Ident::new(&format!("var_{field_name}"), field_name.span());

            let init_expr = if is_repeated {
              // For repeated fields, we already initialized with default and inserted all elements
              quote! { #var_name }
            } else if attrs.required {
              quote! { #var_name.ok_or_else(|| anyhow::anyhow!(concat!("Field ", stringify!(#field_name), " is required")))? }
            } else {
              attrs.default_expr.map_or_else(
                || quote! { #var_name.unwrap_or_default() },
                |expr_str| {
                  let expr: syn::Expr =
                    syn::parse_str(&expr_str).expect("Invalid default expression");
                  quote! { #var_name.unwrap_or_else(|| #expr) }
                },
              )
            };

            field_struct_init.push(quote! {
                #field_name: #init_expr
            });
          }
        },
        _ => panic!("Only named fields are supported for structs"),
      }
    },
    Data::Enum(data_enum) => {
      let mut compute_size_arms = Vec::new();
      let mut serialize_arms = Vec::new();
      let mut tag_to_variant: std::collections::BTreeMap<
        u32,
        (proc_macro2::TokenStream, bool, syn::Ident),
      > = std::collections::BTreeMap::new();

      for variant in &data_enum.variants {
        let variant_name = &variant.ident;
        let attrs = parse_variant_attrs(variant);

        let (compute_size_arm, serialize_arm, deserialize_code) = match &variant.fields {
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

      let deserialize_match_arms = tag_to_variant.iter().map(|(tag, (code, ..))| {
        quote! { #tag => { #code Ok(true) } }
      });


      // OneOf Enum Implementation
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
                          os.write_raw_varint32(inner_size as u32)?;

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

                    // Enums must have at least one variant set.
                    // If the enum should be optional, use Option<MyEnum> in the parent struct.
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

      let expanded = quote! {
          #stripped_input
          #proto_type_impl
          #serialize_impl
          #deserialize_impl
      };
      return TokenStream::from(expanded);
    },
    Data::Union(_) => panic!("Only structs and enums are supported"),
  }

  // Struct implementation (used if we matched Data::Struct above)

  let proto_type_impl = quote! {
      impl #impl_generics bd_proto_util::serialization::ProtoType
          for #name #ty_generics #where_clause
      {
          fn wire_type() -> protobuf::rt::WireType {
              protobuf::rt::WireType::LengthDelimited
          }
      }
  };

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
                  os.write_raw_varint32(inner_size as u32)?;

                  #(#field_processing)*

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
                #(#field_vars_init)*

                bd_proto_util::serialization::read_nested(is, |is, field_number, _wire_type| {
                    match field_number {
                        #(#deserialize_arms)*
                        _ => Ok(false),
                    }
                })?;

                Ok(Self {
                    #(#field_struct_init),*
                })
            }
        }
    }
  };

  // ProtoMessage implementation for structs (top-level message serialization)
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
                    let field_number = tag >> 3;
                    let wire_type_bits = tag & 0x07;
                    let wire_type = match wire_type_bits {
                        0 => protobuf::rt::WireType::Varint,
                        1 => protobuf::rt::WireType::Fixed64,
                        2 => protobuf::rt::WireType::LengthDelimited,
                        3 => protobuf::rt::WireType::StartGroup,
                        4 => protobuf::rt::WireType::EndGroup,
                        5 => protobuf::rt::WireType::Fixed32,
                        _ => return Err(anyhow::anyhow!("Unknown wire type {} (tag={}, field={})", wire_type_bits, tag, field_number)),
                    };

                    let handled: bool = match field_number {
                        #(#deserialize_arms)*
                        _ => Ok::<bool, anyhow::Error>(false),
                    }?;

                    if !handled {
                        is.skip_field(wire_type)?;
                    }
                }

                Ok(Self {
                    #(#field_struct_init),*
                })
            }
        }
    }
  };

  let expanded = quote! {
      #stripped_input
      #proto_type_impl
      #serialize_impl
      #deserialize_impl
      #message_impl
  };
  TokenStream::from(expanded)
}
