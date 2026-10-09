use super::{decoding_generics, read_field};
use crate::struct_impl::FieldAttrs;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::{Data, DeriveInput, Error, Fields, Type, Visibility};

pub fn expand(mut input: DeriveInput, target: Option<&syn::Path>) -> syn::Result<TokenStream> {
  let Data::Enum(data) = &mut input.data else {
    return Err(Error::new_spanned(input, "oneof requires an enum"));
  };
  let mut tags = HashSet::new();
  let mut numbers = Vec::new();
  let mut arms = Vec::new();
  let mut selections = Vec::new();
  for variant in &mut data.variants {
    let field: syn::Field = syn::parse_quote! { value: () };
    let mut field = field;
    field.attrs.clone_from(&variant.attrs);
    let attrs = FieldAttrs::parse_deserialize(&field)?;
    let tag = attrs
      .tag
      .ok_or_else(|| Error::new_spanned(&*variant, "expected #[field(id = N)]"))?;
    if tag == 0 || tag > 536_870_911 || !tags.insert(tag) {
      return Err(Error::new_spanned(
        &*variant,
        "invalid or duplicate protobuf field number",
      ));
    }
    let name = &variant.ident;
    let value = match &variant.fields {
      Fields::Unit => quote!({ message.selected_message(#tag, alternatives)?; Self::#name }),
      Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
        let target = &fields.unnamed[0].ty;
        let wire = attrs.decode_as.as_ref().unwrap_or(target);
        let read = if is_message(wire) {
          if matches!(wire, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Message"))
          {
            quote!(message.selected_message(#tag, alternatives)?)
          } else {
            quote! {
              <#wire as bd_proto_util::serialization::inline::ProtoDeserialize<'_>>::from_inline(
                &message.selected_message(#tag, alternatives)?,
              )?
            }
          }
        } else {
          read_field(wire, tag)?
        };
        let read = attrs
          .deserialize_with
          .map_or_else(|| read.clone(), |convert| quote!(#convert(#read)?));
        quote!(Self::#name(#read))
      },
      _ => {
        return Err(Error::new_spanned(
          &*variant,
          "oneof variants require zero or one value",
        ));
      },
    };
    numbers.push(tag);
    let mut predicate = String::from("is_");
    for character in name.to_string().chars() {
      if character.is_uppercase() {
        if predicate.len() > 3 {
          predicate.push('_');
        }
        predicate.push(character.to_ascii_lowercase());
      } else {
        predicate.push(character);
      }
    }
    selections.push((format_ident!("{predicate}"), tag));
    arms.push(quote!(#tag => #value));
    variant
      .attrs
      .retain(|attribute| !attribute.path().is_ident("field"));
  }
  let name = &input.ident;
  let (_, ty_generics, _) = input.generics.split_for_impl();
  let target_type = target
    .as_ref()
    .map_or_else(|| quote!(#name #ty_generics), |target| quote!(#target));
  let declaration = if target.is_some() {
    quote!()
  } else {
    quote!(#input)
  };
  let generics = decoding_generics(&input.generics);
  let (impl_generics, _, where_clause) = generics.split_for_impl();
  let predicates = if target.is_none() && matches!(&input.vis, Visibility::Public(_)) {
    let (original_impl, original_type, original_where) = input.generics.split_for_impl();
    let methods = selections.iter().map(|(predicate, tag)| {
      quote! {
        pub fn #predicate(
          message: &bd_proto_util::serialization::inline::Message<'_>,
        ) -> anyhow::Result<bool> {
          Ok(message.oneof(&[#(#numbers),*])?.is_some_and(|field| field.number == #tag))
        }
      }
    });
    quote! {
      impl #original_impl #name #original_type #original_where { #(#methods)* }
    }
  } else {
    quote!()
  };
  Ok(quote! {
    #declaration
    #predicates
    impl #impl_generics bd_proto_util::serialization::inline::ProtoOneofDeserialize<'__proto>
      for #target_type #where_clause
    {
      const FIELD_NUMBERS: &'static [u32] = &[#(#numbers),*];
      fn from_oneof(
        message: &bd_proto_util::serialization::inline::Message<'__proto>,
      ) -> anyhow::Result<Option<Self>> {
        let alternatives = Self::FIELD_NUMBERS;
        let Some(field) = message.oneof(alternatives)? else { return Ok(None); };
        Ok(Some(match field.number { #(#arms,)* _ => anyhow::bail!("invalid protobuf oneof field") }))
      }
    }
    impl #impl_generics bd_proto_util::serialization::inline::ProtoDeserialize<'__proto>
      for #target_type #where_clause
    {
      fn from_inline(
        message: &bd_proto_util::serialization::inline::Message<'__proto>,
      ) -> anyhow::Result<Self> {
        <Self as bd_proto_util::serialization::inline::ProtoOneofDeserialize<'_>>
          ::from_oneof(message)?
          .ok_or_else(|| anyhow::anyhow!("missing protobuf oneof"))
      }
    }
  })
}

fn is_message(wire: &Type) -> bool {
  let Type::Path(path) = wire else {
    return false;
  };
  path.path.segments.last().is_some_and(|segment| {
    !matches!(
      segment.ident.to_string().as_str(),
      "String" | "Vec" | "u32" | "u64" | "i32" | "i64" | "bool" | "f64" | "f32" | "EnumOrUnknown"
    )
  })
}
