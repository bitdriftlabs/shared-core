#![allow(clippy::unwrap_used)]

use super::{expand, expand_config};
use quote::quote;
use syn::{DeriveInput, Item};

fn rejects(source: &str, message: &str) {
  let input: DeriveInput = syn::parse_str(source).unwrap();
  let error = expand(input).unwrap_err();
  assert!(error.to_string().contains(message), "{error}");
}

#[test]
fn retained_target_expansion_has_no_staging_model_or_encoder() {
  let input: DeriveInput = syn::parse_quote! {
    struct Declaration {
      #[field(id = 1)]
      label: String,
      #[field(id = 2)]
      children: Vec<FinalNode>,
      #[field(id = 3)]
      values: std::collections::HashMap<String, FinalNode>,
      #[field(oneof)]
      choice: Option<FinalChoice>,
    }
  };
  let expanded = expand_config(quote!(target = "RetainedMessage"), input).unwrap();
  let parsed: syn::File = syn::parse2(expanded.clone()).unwrap();
  assert_eq!(parsed.items.len(), 1);
  let Item::Impl(implementation) = &parsed.items[0] else {
    panic!("decoder implementation expected")
  };
  assert_eq!(
    implementation
      .trait_
      .as_ref()
      .unwrap()
      .0
      .segments
      .last()
      .unwrap()
      .ident,
    "ProtoDeserialize"
  );
  eprintln!("retained decoder expansion: {expanded}");
}

#[test]
fn rejects_invalid_field_attributes() {
  rejects(
    "struct Bad { #[field(id = 1, unknown)] value: u32 }",
    "unknown",
  );
  rejects(
    "struct Bad { #[field(id = 1, decode_as = \"[\")] value: u32 }",
    "cannot parse",
  );
  rejects(
    "struct Bad { #[field(id = 1, deserialize_with = \"[\")] value: u32 }",
    "cannot parse",
  );
  rejects(
    "struct Bad { #[field(id = 4294967296)] value: u32 }",
    "too large",
  );
}

#[test]
fn rejects_invalid_or_duplicate_tags() {
  for tag in [0, 19_000, 536_870_912] {
    rejects(
      &format!("struct Bad {{ #[field(id = {tag})] value: u32 }}"),
      "field number",
    );
  }
  rejects(
    "struct Bad { #[field(id = 1)] first: u32, #[field(id = 1)] second: u32 }",
    "duplicate",
  );
  rejects("struct Bad { value: u32 }", "expected #[field");
}

#[test]
fn rejects_unsupported_shapes_and_conflicting_options() {
  rejects("enum Bad { Value }", "expected #[field");
  rejects("struct Bad(u32);", "named fields");
  rejects(
    "struct Bad { #[field(id = 1, repeated)] value: u32 }",
    "repeated requires",
  );
  rejects(
    "struct Bad { #[field(id = 1, required, default = \"0\")] value: u32 }",
    "cannot be combined",
  );
  rejects(
    "struct Bad { #[field(skip, required)] value: u32 }",
    "skipped fields",
  );
}
