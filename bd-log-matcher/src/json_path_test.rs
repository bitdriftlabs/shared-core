// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::JsonPathToken;
use super::json_path::resolve;

fn key(key: &str) -> JsonPathToken {
  JsonPathToken::Key(key.to_owned())
}

#[test]
fn resolves_escaped_unicode_keys_and_values() {
  let path = [key("snowman ☃"), key("value")];
  assert_eq!(
    resolve(
      r#"{"snowman \u2603":{"emoji":"\uD83D\uDE80","value":"line\ntext"}}"#,
      &path
    )
    .as_deref(),
    Some("line\ntext"),
  );
  assert_eq!(
    resolve(
      r#"{"snowman \u2603":{"emoji":"\uD83D\uDE80","value":"\uD83D\uDE80"}}"#,
      &path
    )
    .as_deref(),
    Some("🚀"),
  );
}

#[test]
fn skips_nested_containers_and_resolves_positive_indexes() {
  let json =
    r#"{"ignored":{"deep":[{"value":false},[1,2,3]]},"items":["zero",{"name":"one"},"two"]}"#;
  assert_eq!(
    resolve(json, &[key("items"), JsonPathToken::Index(1), key("name")]).as_deref(),
    Some("one"),
  );
}

#[test]
fn validates_json_before_the_target() {
  let path = [key("value")];
  for json in [
    r#"{"broken":[1,],"value":"ok"}"#,
    r#"{"broken":{"nested":"#,
    r#"{"broken":"\uD800","value":"ok"}"#,
  ] {
    assert_eq!(resolve(json, &path), None, "{json}");
  }
}

#[test]
fn returns_a_found_scalar_without_validating_trailing_json() {
  let path = [key("value")];
  for json in [
    r#"{"value":"ok"} trailing"#,
    r#"{"value":"ok","broken":[1,]}"#,
    r#"{"value":"ok","broken":"\uD800"}"#,
  ] {
    assert_eq!(resolve(json, &path).as_deref(), Some("ok"), "{json}");
  }
}

#[test]
fn uses_the_first_duplicate_key() {
  let path = [key("value")];
  assert_eq!(
    resolve(r#"{"value":"first","value":"second"}"#, &path).as_deref(),
    Some("first")
  );
}

#[test]
fn accepts_the_maximum_container_depth() {
  let json = format!("{}true{}", "[".repeat(128), "]".repeat(128));
  let path = vec![JsonPathToken::Index(0); 128];
  assert_eq!(resolve(&json, &path).as_deref(), Some("true"));
}

#[test]
fn rejects_excessive_container_depth() {
  let json = format!("{}true{}", "[".repeat(129), "]".repeat(129));
  let path = vec![JsonPathToken::Index(0); 129];
  assert_eq!(resolve(&json, &path), None);
}
