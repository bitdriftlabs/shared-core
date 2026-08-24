// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Generics, Type, parse_quote};

/// Generates the allocation-only portion of `ApproximateSize` for a user type.
///
/// The runtime trait, in `bd-macros`, adds a value's inline size. This generated implementation
/// therefore walks only the currently active fields and sums their child allocations. Keeping
/// those responsibilities separate prevents nested derives from charging inline storage twice.
pub fn derive(input: DeriveInput) -> TokenStream {
  let name = input.ident;
  // Capture both the expression to evaluate for a concrete value and every field type that must
  // satisfy the trait. Collecting the types here allows generic structs and enums to receive the
  // required bounds automatically.
  let (children, field_types) = match input.data {
    Data::Struct(data) => struct_children(&data.fields),
    Data::Enum(data) => enum_children(&data.variants.into_iter().collect::<Vec<_>>()),
    Data::Union(_) => panic!("ApproximateSize cannot be derived for unions"),
  };
  let generics = add_bounds(input.generics, &field_types, &name);
  let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

  // Use an absolute facade path so callers only need the `bd-macros` dependency; unlike the old
  // unqualified expansion, no local `use ApproximateSize` import is required at derive sites.
  quote! {
    impl #impl_generics ::bd_macros::ApproximateSize for #name #ty_generics #where_clause {
      fn approximate_size_children_bytes(&self) -> usize {
        #children
      }
    }
  }
}

fn struct_children(fields: &Fields) -> (TokenStream, Vec<Type>) {
  match fields {
    Fields::Named(fields) => {
      // Named fields can be accessed directly from `self` in the generated implementation.
      let field_types = fields.named.iter().map(|field| field.ty.clone()).collect();
      let values = fields.named.iter().map(|field| {
        let name = field.ident.as_ref().unwrap();
        quote! { ::bd_macros::ApproximateSize::approximate_size_children_bytes(&self.#name) }
      });
      (sum_children(values), field_types)
    },
    Fields::Unnamed(fields) => {
      // Tuple structs use positional member access but otherwise follow the named-field rule.
      let field_types = fields
        .unnamed
        .iter()
        .map(|field| field.ty.clone())
        .collect();
      let values = fields.unnamed.iter().enumerate().map(|(index, _)| {
        let index = syn::Index::from(index);
        quote! { ::bd_macros::ApproximateSize::approximate_size_children_bytes(&self.#index) }
      });
      (sum_children(values), field_types)
    },
    Fields::Unit => (quote! { 0 }, vec![]),
  }
}

fn enum_children(variants: &[syn::Variant]) -> (TokenStream, Vec<Type>) {
  let mut field_types = Vec::new();
  let arms = variants.iter().map(|variant| {
    let variant_name = &variant.ident;
    match &variant.fields {
      Fields::Named(fields) => {
        // Destructure only the active variant so an enum charges allocations for its current
        // payload, not every payload type it could contain.
        let bindings = fields
          .named
          .iter()
          .map(|field| field.ident.as_ref().unwrap());
        let values = fields.named.iter().map(|field| {
          field_types.push(field.ty.clone());
          let name = field.ident.as_ref().unwrap();
          quote! { ::bd_macros::ApproximateSize::approximate_size_children_bytes(#name) }
        });
        let children = sum_children(values);
        quote! { Self::#variant_name { #(#bindings),* } => #children }
      },
      Fields::Unnamed(fields) => {
        // Generate deterministic bindings for tuple-variant fields before summing their children.
        let bindings = (0 .. fields.unnamed.len())
          .map(|index| syn::Ident::new(&format!("field_{index}"), variant_name.span()))
          .collect::<Vec<_>>();
        let values = fields
          .unnamed
          .iter()
          .zip(&bindings)
          .map(|(field, binding)| {
            field_types.push(field.ty.clone());
            quote! { ::bd_macros::ApproximateSize::approximate_size_children_bytes(#binding) }
          });
        let children = sum_children(values);
        quote! { Self::#variant_name(#(#bindings),*) => #children }
      },
      Fields::Unit => quote! { Self::#variant_name => 0 },
    }
  });

  (quote! { match self { #(#arms),* } }, field_types)
}

fn sum_children(values: impl Iterator<Item = TokenStream>) -> TokenStream {
  // Saturation keeps admission accounting total even for adversarial or extremely large values.
  quote! { 0usize #(.saturating_add(#values))* }
}

fn add_bounds(mut generics: Generics, field_types: &[Type], derived_type: &syn::Ident) -> Generics {
  // A derived implementation calls the trait method for every field, so the implementation must
  // state the same requirement in its where clause. A self-referential field resolves through the
  // implementation being defined, though, so adding its predicate would make trait resolution
  // recurse forever. Preserve user-supplied bounds while adding only non-recursive ones.
  let where_clause = generics.make_where_clause();
  for field_type in field_types
    .iter()
    .filter(|field_type| !contains_derived_type(field_type, derived_type))
  {
    where_clause
      .predicates
      .push(parse_quote!(#field_type: ::bd_macros::ApproximateSize));
  }
  generics
}

fn contains_derived_type(field_type: &Type, derived_type: &syn::Ident) -> bool {
  match field_type {
    Type::Array(array) => contains_derived_type(&array.elem, derived_type),
    Type::Group(group) => contains_derived_type(&group.elem, derived_type),
    Type::Paren(parenthesized) => contains_derived_type(&parenthesized.elem, derived_type),
    Type::Path(type_path) => {
      type_path
        .qself
        .as_ref()
        .is_some_and(|qself| contains_derived_type(&qself.ty, derived_type))
        || path_contains_derived_type(&type_path.path, derived_type)
    },
    Type::Ptr(pointer) => contains_derived_type(&pointer.elem, derived_type),
    Type::Reference(reference) => contains_derived_type(&reference.elem, derived_type),
    Type::Slice(slice) => contains_derived_type(&slice.elem, derived_type),
    Type::Tuple(tuple) => tuple
      .elems
      .iter()
      .any(|element| contains_derived_type(element, derived_type)),
    _ => false,
  }
}

fn path_contains_derived_type(path: &syn::Path, derived_type: &syn::Ident) -> bool {
  path.segments.iter().any(|segment| {
    segment.ident == "Self"
      || segment.ident.eq(derived_type)
      || path_arguments_contain_derived_type(&segment.arguments, derived_type)
  })
}

fn path_arguments_contain_derived_type(
  arguments: &syn::PathArguments,
  derived_type: &syn::Ident,
) -> bool {
  match arguments {
    syn::PathArguments::None => false,
    syn::PathArguments::AngleBracketed(arguments) => {
      arguments.args.iter().any(|argument| match argument {
        syn::GenericArgument::Type(field_type) => contains_derived_type(field_type, derived_type),
        syn::GenericArgument::AssocType(associated_type) => {
          contains_derived_type(&associated_type.ty, derived_type)
        },
        _ => false,
      })
    },
    syn::PathArguments::Parenthesized(arguments) => {
      arguments
        .inputs
        .iter()
        .any(|input| contains_derived_type(&input.ty, derived_type))
        || return_type_contains_derived_type(&arguments.output, derived_type)
    },
  }
}

fn return_type_contains_derived_type(
  return_type: &syn::ReturnType,
  derived_type: &syn::Ident,
) -> bool {
  match return_type {
    syn::ReturnType::Default => false,
    syn::ReturnType::Type(_, field_type) => contains_derived_type(field_type, derived_type),
  }
}
