// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::json_extractor::{JsonPath, PathPart};
use assert_matches::assert_matches;

#[test]
fn path_parsing() {
  assert_matches!(JsonPath::parse("a.b.c"), Some(path) => {
      assert_eq!(path.key, PathPart::Key("c".to_string()));
      assert_eq!(path.path, vec![PathPart::Key("a".to_string()), PathPart::Key("b".to_string())]);
  });
  assert_matches!(JsonPath::parse("a"), Some(path) => {
      assert_eq!(path.key, PathPart::Key("a".to_string()));
      assert!(path.path.is_empty());
  });
  assert!(JsonPath::parse("").is_none());
  assert_matches!(JsonPath::parse("^&.((.9sxs"), Some(path) => {
      assert_eq!(path.key, PathPart::Key("9sxs".to_string()));
      assert_eq!(path.path, vec![PathPart::Key("^&".to_string()), PathPart::Key("((".to_string())]);
  });
  assert_matches!(JsonPath::parse("[0]"), Some(path) => {
      assert_eq!(path.key, PathPart::Key("[0]".to_string()));
      assert_eq!(path.path, vec![]);
  });
  assert_matches!(JsonPath::parse("foo[0]"), Some(path) => {
      assert_eq!(path.key, PathPart::KeyAndIndex("foo".to_string(), 0));
      assert_eq!(path.path, vec![]);
  });
}

#[test]
fn extraction() {
  macro_rules! assert_extracted_value_eq {
    ($json:expr, $path:expr, $expected:expr) => {
      let extractor = crate::json_extractor::JsonExtractor::new($json).unwrap();
      let path = crate::json_extractor::JsonPath::parse($path).unwrap();
      assert_eq!(extractor.extract(&path), $expected);
    };
  }

  let json = r#"{"a": {"b": {"c": "foo"}}, "b": "bar", "c": [{"inner": "value"}]}"#;

  assert_extracted_value_eq!(json, "a.b.c", Some(&"foo".to_string()));

  assert_extracted_value_eq!(json, "a.b.d", None);

  assert_extracted_value_eq!(json, "b", Some(&"bar".to_string()));

  assert_extracted_value_eq!(json, "c[0].inner", Some(&"value".to_string()));
}

#[test]
fn invalid_json() {
  assert_eq!(
    crate::json_extractor::JsonExtractor::new("foo")
      .unwrap_err()
      .to_string(),
    "Parse error at line:1, col:2: Unexpected character 'a' while parsing 'false'"
  );
}
