#[cfg(test)]
#[path = "./proto_deserialize_test.rs"]
mod tests;

mod fields;
mod oneof;
mod view;

use crate::struct_impl::FieldAttrs;
use fields::read_field;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::parse::Parser;
use syn::{Data, DeriveInput, Error, Fields, GenericArgument, PathArguments, Type};

pub fn expand_config(attr: TokenStream, input: DeriveInput) -> syn::Result<TokenStream> {
  let mut target = None;
  let mut borrowed_view = false;
  let parser = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated;
  for meta in parser.parse2(attr)? {
    match meta {
      syn::Meta::Path(path) if path.is_ident("view") => borrowed_view = true,
      syn::Meta::NameValue(value) if value.path.is_ident("target") => {
        let syn::Expr::Lit(syn::ExprLit {
          lit: syn::Lit::Str(value),
          ..
        }) = value.value
        else {
          return Err(Error::new_spanned(
            value,
            "target requires a type path string",
          ));
        };
        target = Some(value.parse::<syn::Path>()?);
      },
      other => {
        return Err(Error::new_spanned(
          other,
          "unknown proto_deserialize option",
        ));
      },
    }
  }
  if borrowed_view {
    if target.is_some() {
      return Err(Error::new_spanned(
        input,
        "view and target cannot be combined",
      ));
    }
    return view::expand(&input);
  }
  expand_target(input, target)
}

#[cfg(test)]
pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
  expand_target(input, None)
}

fn expand_target(mut input: DeriveInput, target: Option<syn::Path>) -> syn::Result<TokenStream> {
  if matches!(input.data, Data::Enum(_)) {
    return oneof::expand(input, target.as_ref());
  }
  let Data::Struct(data) = &mut input.data else {
    return Err(Error::new_spanned(
      input,
      "proto_deserialize requires a named-field struct",
    ));
  };
  let Fields::Named(fields) = &mut data.fields else {
    return Err(Error::new_spanned(
      input,
      "proto_deserialize requires named fields",
    ));
  };
  let mut tags = HashSet::new();
  let mut reads = Vec::new();
  let mut members = Vec::new();
  for field in &mut fields.named {
    let attrs = FieldAttrs::parse_deserialize(field)?;
    if attrs.required && attrs.default_expr.is_some() {
      return Err(Error::new_spanned(
        &*field,
        "required and default cannot be combined",
      ));
    }
    if attrs.skip
      && (attrs.required || attrs.decode_as.is_some() || attrs.deserialize_with.is_some())
    {
      return Err(Error::new_spanned(
        &*field,
        "skipped fields cannot have decoding options",
      ));
    }
    let name = field
      .ident
      .as_ref()
      .ok_or_else(|| Error::new_spanned(&*field, "missing name"))?;
    let default = attrs
      .default_expr
      .as_ref()
      .map(|value| syn::parse_str::<syn::Expr>(value))
      .transpose()?;
    if attrs.skip {
      let value = default.map_or_else(|| quote!(Default::default()), |value| quote!(#value));
      members.push(quote!(#name: #value));
    } else if attrs.message {
      if attrs.tag.is_some() || attrs.required || attrs.default_expr.is_some() {
        return Err(Error::new_spanned(
          &*field,
          "message fields cannot have id, required, or default",
        ));
      }
      let convert = attrs
        .deserialize_with
        .ok_or_else(|| Error::new_spanned(&*field, "message requires deserialize_with"))?;
      members.push(quote!(#name: #convert(message)?.into()));
    } else if attrs.oneof {
      let wire_type = attrs.decode_as.as_ref().unwrap_or(&field.ty);
      let Type::Path(path) = wire_type else {
        return Err(Error::new_spanned(wire_type, "oneof requires Option<Enum>"));
      };
      let segment = path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(wire_type, "missing type"))?;
      if segment.ident != "Option" {
        return Err(Error::new_spanned(wire_type, "oneof requires Option<Enum>"));
      }
      let inner = type_argument(&segment.arguments)?;
      let variable = format_ident!("proto_{}", name);
      reads.push(quote! {
        let #variable =
          <#inner as bd_proto_util::serialization::inline::ProtoOneofDeserialize<'_>>
            ::from_oneof(message)?;
      });
      let value = attrs
        .deserialize_with
        .map_or_else(|| quote!(#variable), |convert| quote!(#convert(#variable)?));
      members.push(quote!(#name: #value.into()));
    } else {
      let tag = attrs
        .tag
        .ok_or_else(|| Error::new_spanned(&*field, "expected #[field(id = N)]"))?;
      if tag == 0 || tag > 536_870_911 || (19_000 .. 20_000).contains(&tag) || !tags.insert(tag) {
        return Err(Error::new_spanned(
          &*field,
          "invalid or duplicate protobuf field number",
        ));
      }
      if attrs.serialize_as.is_some() || attrs.proto_enum {
        return Err(Error::new_spanned(
          &*field,
          "use decode_as and deserialize_with for conversions",
        ));
      }
      let wire_type = attrs.decode_as.as_ref().unwrap_or(&field.ty);
      if attrs.repeated
        && !matches!(wire_type, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Vec"))
      {
        return Err(Error::new_spanned(
          &*field,
          "repeated requires a supported Vec wire type",
        ));
      }
      let variable = format_ident!("proto_{}", name);
      let read = read_field(wire_type, tag)?;
      let read = if attrs.required {
        quote!({
          if message.oneof(&[#tag])?.is_none() {
            anyhow::bail!(concat!("Field ", stringify!(#name), " is required"));
          }
          #read
        })
      } else if let Some(default) = default {
        quote!({
          if message.oneof(&[#tag])?.is_some() { #read } else { #default }
        })
      } else {
        read
      };
      reads.push(quote!(let #variable: #wire_type = #read;));
      let value = attrs
        .deserialize_with
        .map_or_else(|| quote!(#variable), |convert| quote!(#convert(#variable)?));
      members.push(quote!(#name: #value.into()));
    }
    field
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
  let default_members = target.map(|_| quote!(.. Default::default()));
  let generics = decoding_generics(&input.generics);
  let (impl_generics, _, where_clause) = generics.split_for_impl();
  Ok(quote! {
    #declaration
    impl #impl_generics bd_proto_util::serialization::inline::ProtoDeserialize<'__proto>
      for #target_type #where_clause
    {
      fn from_inline(
        message: &bd_proto_util::serialization::inline::Message<'__proto>,
      ) -> anyhow::Result<Self> {
        #(#reads)*
        Ok(Self { #(#members,)* #default_members })
      }
    }
  })
}

pub fn decoding_generics(original: &syn::Generics) -> syn::Generics {
  let mut generics = original.clone();
  generics.params.insert(0, syn::parse_quote!('__proto));
  for lifetime in original.lifetimes() {
    let lifetime = &lifetime.lifetime;
    generics
      .make_where_clause()
      .predicates
      .push(syn::parse_quote!('__proto: #lifetime));
  }
  generics
}

fn type_argument(arguments: &PathArguments) -> syn::Result<&Type> {
  if let PathArguments::AngleBracketed(arguments) = arguments
    && arguments.args.len() == 1
    && let Some(GenericArgument::Type(inner)) = arguments.args.first()
  {
    return Ok(inner);
  }
  Err(Error::new_spanned(arguments, "expected one type argument"))
}
