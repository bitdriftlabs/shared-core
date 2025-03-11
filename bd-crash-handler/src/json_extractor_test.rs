use crate::json_extractor::JsonPath;
use assert_matches::assert_matches;

#[test]
fn path_parsing() {
  assert_matches!(JsonPath::parse("a.b.c"), Some(path) => {
      assert_eq!(path.key, "c");
      assert_eq!(path.path, vec!["a".to_string(), "b".to_string()]);
  });
  assert_matches!(JsonPath::parse("a"), Some(path) => {
      assert_eq!(path.key, "a");
      assert!(path.path.is_empty());
  });
  assert!(JsonPath::parse("").is_none());
  assert_matches!(JsonPath::parse("^&.((.9sxs"), Some(path) => {
      assert_eq!(path.key, "9sxs");
      assert_eq!(path.path, vec!["^&".to_string(), "((".to_string()]);
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

  let json = r#"{"a": {"b": {"c": "foo"}}, "b": "bar"}"#;

  assert_extracted_value_eq!(json, "a.b.c", Some(&"foo".to_string()));

  assert_extracted_value_eq!(json, "a.b.d", None);

  assert_extracted_value_eq!(json, "b", Some(&"bar".to_string()));
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
