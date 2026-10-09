use super::type_argument;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, GenericArgument, PathArguments, Type};

pub fn read_field(wire_type: &Type, tag: u32) -> syn::Result<TokenStream> {
  if let Type::Reference(reference) = wire_type {
    if matches!(&*reference.elem, Type::Path(path) if path.path.is_ident("str")) {
      return Ok(quote!(message.string(#tag)?));
    }
    if matches!(&*reference.elem, Type::Slice(slice) if matches!(&*slice.elem, Type::Path(path) if path.path.is_ident("u8")))
    {
      return Ok(quote!(message.bytes(#tag)?));
    }
    return Err(Error::new_spanned(
      wire_type,
      "only borrowed str and byte slice fields are supported",
    ));
  }
  let Type::Path(path) = wire_type else {
    return Err(Error::new_spanned(wire_type, "unsupported wire type"));
  };
  let segment = path
    .path
    .segments
    .last()
    .ok_or_else(|| Error::new_spanned(wire_type, "missing type"))?;
  let name = segment.ident.to_string();
  if name == "HashMap" {
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
      return Err(Error::new_spanned(
        wire_type,
        "map requires key and value types",
      ));
    };
    let types = arguments
      .args
      .iter()
      .filter_map(|argument| {
        if let GenericArgument::Type(value) = argument {
          Some(value)
        } else {
          None
        }
      })
      .collect::<Vec<_>>();
    if types.len() != 2 {
      return Err(Error::new_spanned(
        wire_type,
        "map requires key and value types",
      ));
    }
    let key = read_field(types[0], 1)?;
    let value = read_field(types[1], 2)?;
    return Ok(quote!({
      let mut entries = std::collections::HashMap::new();
      message.visit_messages(#tag, &mut |message| {
        entries.insert(#key, #value);
        Ok(())
      })?;
      entries
    }));
  }
  if name == "EnumOrUnknown" {
    return Ok(quote!(protobuf::EnumOrUnknown::from_i32(message.int32(#tag)?)));
  }
  if name == "MessageField" {
    let inner = type_argument(&segment.arguments)?;
    return Ok(quote!(message.message(#tag)?.as_ref()
      .map(<#inner as bd_proto_util::serialization::inline::ProtoDeserialize<'_>>::from_inline)
      .transpose()?.into()));
  }
  if name == "Option" {
    let inner = type_argument(&segment.arguments)?;
    if matches!(inner, Type::Reference(reference) if matches!(&*reference.elem, Type::Path(path) if path.path.is_ident("str")))
    {
      return Ok(quote!(message.optional_string(#tag)?));
    }
    let read = read_field(inner, tag)?;
    return Ok(quote!({
      if message.oneof(&[#tag])?.is_some() { Some(#read) } else { None }
    }));
  }
  if name == "Vec" {
    let inner = type_argument(&segment.arguments)?;
    if matches!(inner, Type::Path(path) if path.path.is_ident("String")) {
      return Ok(quote!(message.strings(#tag)?));
    }
    if matches!(inner, Type::Path(path) if path.path.is_ident("u8")) {
      return Ok(quote!(message.bytes(#tag)?.to_vec()));
    }
    if matches!(inner, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Message"))
    {
      return Ok(quote!(message.messages(#tag)?));
    }
    return Ok(quote!({
      let mut values = Vec::new();
      message.visit_messages(#tag, &mut |message| {
        values.push(
          <#inner as bd_proto_util::serialization::inline::ProtoDeserialize<'_>>
            ::from_inline(message)?
        );
        Ok(())
      })?;
      values
    }));
  }
  let expression = match name.as_str() {
    "String" => quote!(message.string(#tag)?.to_owned()),
    "u32" => quote!(message.uint32(#tag)?),
    "u64" => quote!(message.uint(#tag)?),
    "i32" => quote!(message.int32(#tag)?),
    "i64" => quote!(message.int64(#tag)?),
    "bool" => quote!(message.uint(#tag)? != 0),
    "f64" => quote!(message.double(#tag)?),
    "Message" => quote!(message.required_message(#tag)?),
    _ => quote!(message.message(#tag)?.as_ref()
      .map(<#wire_type as bd_proto_util::serialization::inline::ProtoDeserialize<'_>>
        ::from_inline)
      .transpose()?.unwrap_or_default()),
  };
  Ok(expression)
}
