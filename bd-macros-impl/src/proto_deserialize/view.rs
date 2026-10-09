use super::{decoding_generics, read_field, type_argument};
use crate::struct_impl::FieldAttrs;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Expr, Fields, Type};

pub fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
  let Data::Struct(data) = &input.data else {
    return Err(Error::new_spanned(input, "view requires a named struct"));
  };
  let Fields::Named(fields) = &data.fields else {
    return Err(Error::new_spanned(input, "view requires named fields"));
  };
  let lifetime = input
    .generics
    .lifetimes()
    .next()
    .ok_or_else(|| Error::new_spanned(input, "view requires a lifetime"))?;
  let lifetime = &lifetime.lifetime;
  let mut getters = Vec::new();
  for field in &fields.named {
    let attrs = FieldAttrs::parse_deserialize(field)?;
    let name = &field.ident;
    let target = &field.ty;
    let wire = attrs.decode_as.as_ref().unwrap_or(target);
    let read = if attrs.oneof {
      let Type::Path(path) = wire else {
        return Err(Error::new_spanned(wire, "oneof requires Option"));
      };
      let segment = path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(wire, "missing type"))?;
      let inner = type_argument(&segment.arguments)?;
      quote! {
        <#inner as bd_proto_util::serialization::inline::ProtoOneofDeserialize<'_>>
          ::from_oneof(message)?
      }
    } else {
      let tag = attrs
        .tag
        .ok_or_else(|| Error::new_spanned(field, "expected field id"))?;
      read_field(wire, tag)?
    };
    let read = attrs
      .deserialize_with
      .map_or_else(|| read.clone(), |convert| quote!(#convert(#read)?));
    let result = match syn::parse2::<Expr>(read)? {
      Expr::Try(expression) => {
        let expression = expression.expr;
        quote!(#expression)
      },
      expression => quote!(Ok(#expression)),
    };
    getters.push(quote! {
      pub fn #name(&self) -> anyhow::Result<#target> {
        let message = &self.message;
        #result
      }
    });
  }
  let name = &input.ident;
  let visibility = &input.vis;
  let original = &input.generics;
  let (impl_generics, ty_generics, where_clause) = original.split_for_impl();
  let generics = decoding_generics(original);
  let (decode_generics, _, decode_where) = generics.split_for_impl();
  Ok(quote! {
    #[derive(Clone, Debug, Default)]
    #visibility struct #name #original {
      message: bd_proto_util::serialization::inline::Message<#lifetime>,
    }
    impl #impl_generics #name #ty_generics #where_clause {
      pub fn as_message(&self) -> &bd_proto_util::serialization::inline::Message<#lifetime> {
        &self.message
      }
      #(#getters)*
    }
    impl #decode_generics bd_proto_util::serialization::inline::ProtoDeserialize<'__proto>
      for #name #ty_generics #decode_where
    {
      fn from_inline(
        message: &bd_proto_util::serialization::inline::Message<'__proto>,
      ) -> anyhow::Result<Self> {
        Ok(Self { message: message.clone() })
      }
    }
  })
}
