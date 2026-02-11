// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

#[test]
fn simple_object_string() {
  let json = r#"{"key": "value"}"#;
  assert_eq!(extract(json, &["key"]), Some(Value::String("value")));
}

#[test]
fn simple_object_number() {
  let json = r#"{"count": 42}"#;
  assert_eq!(extract(json, &["count"]), Some(Value::Number("42")));
}

#[test]
fn nested_object() {
  let json = r#"{"a": {"b": {"c": "deep"}}}"#;
  assert_eq!(extract(json, &["a", "b", "c"]), Some(Value::String("deep")));
}

#[test]
fn array_access() {
  let json = r#"{"arr": [1, 2, 3]}"#;
  assert_eq!(extract(json, &["arr", "0"]), Some(Value::Number("1")));
  assert_eq!(extract(json, &["arr", "1"]), Some(Value::Number("2")));
  assert_eq!(extract(json, &["arr", "2"]), Some(Value::Number("3")));
}

#[test]
fn array_of_objects() {
  let json = r#"{"users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]}"#;
  assert_eq!(
    extract(json, &["users", "0", "name"]),
    Some(Value::String("Alice"))
  );
  assert_eq!(
    extract(json, &["users", "0", "age"]),
    Some(Value::Number("30"))
  );
  assert_eq!(
    extract(json, &["users", "1", "name"]),
    Some(Value::String("Bob"))
  );
  assert_eq!(
    extract(json, &["users", "1", "age"]),
    Some(Value::Number("25"))
  );
}

#[test]
fn root_array() {
  let json = r#"["a", "b", "c"]"#;
  assert_eq!(extract(json, &["0"]), Some(Value::String("a")));
  assert_eq!(extract(json, &["1"]), Some(Value::String("b")));
  assert_eq!(extract(json, &["2"]), Some(Value::String("c")));
}

#[test]
fn negative_number() {
  let json = r#"{"temp": -42}"#;
  assert_eq!(extract(json, &["temp"]), Some(Value::Number("-42")));
}

#[test]
fn float_number() {
  let json = r#"{"pi": 3.14159}"#;
  assert_eq!(extract(json, &["pi"]), Some(Value::Number("3.14159")));
}

#[test]
fn scientific_notation() {
  let json = r#"{"big": 1.23e10, "small": 4.56E-7}"#;
  assert_eq!(extract(json, &["big"]), Some(Value::Number("1.23e10")));
  assert_eq!(extract(json, &["small"]), Some(Value::Number("4.56E-7")));
}

#[test]
fn empty_string() {
  let json = r#"{"empty": ""}"#;
  assert_eq!(extract(json, &["empty"]), Some(Value::String("")));
}

#[test]
fn string_with_escapes() {
  let json = r#"{"msg": "hello\nworld"}"#;
  assert_eq!(
    extract(json, &["msg"]),
    Some(Value::String(r"hello\nworld"))
  );
}

#[test]
fn string_with_escaped_quote() {
  let json = r#"{"quote": "say \"hello\""}"#;
  assert_eq!(
    extract(json, &["quote"]),
    Some(Value::String(r#"say \"hello\""#))
  );
}

#[test]
fn unicode_string() {
  let json = r#"{"greeting": "こんにちは"}"#;
  assert_eq!(
    extract(json, &["greeting"]),
    Some(Value::String("こんにちは"))
  );
}

#[test]
fn missing_key() {
  let json = r#"{"a": 1, "b": 2}"#;
  assert_eq!(extract(json, &["c"]), None);
}

#[test]
fn index_out_of_bounds() {
  let json = r"[1, 2, 3]";
  assert_eq!(extract(json, &["3"]), None);
  assert_eq!(extract(json, &["100"]), None);
}

#[test]
fn wrong_type_object_for_array() {
  let json = r#"{"obj": {"a": 1}}"#;
  assert_eq!(extract(json, &["obj", "0"]), None);
}

#[test]
fn wrong_type_array_for_object() {
  let json = r#"{"arr": [1, 2, 3]}"#;
  assert_eq!(extract(json, &["arr", "a"]), None);
}

#[test]
fn empty_object() {
  let json = r"{}";
  assert_eq!(extract(json, &["key"]), None);
}

#[test]
fn empty_array() {
  let json = r"[]";
  assert_eq!(extract(json, &["0"]), None);
}

#[test]
fn empty_path_on_string() {
  let json = r#""hello""#;
  assert_eq!(extract(json, &[]), Some(Value::String("hello")));
}

#[test]
fn empty_path_on_number() {
  let json = "42";
  assert_eq!(extract(json, &[]), Some(Value::Number("42")));
}

#[test]
fn empty_path_on_object() {
  let json = r#"{"a": 1}"#;
  assert_eq!(extract(json, &[]), None);
}

#[test]
fn empty_path_on_array() {
  let json = r"[1, 2]";
  assert_eq!(extract(json, &[]), None);
}

#[test]
fn value_is_null() {
  let json = r#"{"x": null}"#;
  assert_eq!(extract(json, &["x"]), None);
}

#[test]
fn value_is_bool() {
  let json = r#"{"flag": true}"#;
  assert_eq!(extract(json, &["flag"]), None);
}

#[test]
fn value_is_nested_object() {
  let json = r#"{"data": {"nested": 1}}"#;
  assert_eq!(extract(json, &["data"]), None);
}

#[test]
fn value_is_nested_array() {
  let json = r#"{"items": [1, 2]}"#;
  assert_eq!(extract(json, &["items"]), None);
}

#[test]
fn whitespace_variations() {
  let json = r#"  {  "key"  :  "value"  }  "#;
  assert_eq!(extract(json, &["key"]), Some(Value::String("value")));

  let json_newlines = "{\n  \"key\": \"value\"\n}";
  assert_eq!(
    extract(json_newlines, &["key"]),
    Some(Value::String("value"))
  );

  let json_tabs = "{\t\"key\":\t\"value\"\t}";
  assert_eq!(extract(json_tabs, &["key"]), Some(Value::String("value")));
}

#[test]
fn deeply_nested() {
  let json = r#"{"a":{"b":{"c":{"d":{"e":"deep"}}}}}"#;
  assert_eq!(
    extract(json, &["a", "b", "c", "d", "e"]),
    Some(Value::String("deep"))
  );
}

#[test]
fn numeric_key_in_object() {
  let json = r#"{"0": "zero", "1": "one"}"#;
  assert_eq!(extract(json, &["0"]), Some(Value::String("zero")));
  assert_eq!(extract(json, &["1"]), Some(Value::String("one")));
}

#[test]
fn malformed_truncated() {
  let json = r#"{"key": "val"#;
  assert_eq!(extract(json, &["key"]), None);
}

#[test]
fn malformed_missing_colon() {
  let json = r#"{"key" "value"}"#;
  assert_eq!(extract(json, &["key"]), None);
}

#[test]
fn malformed_missing_comma() {
  let json = r#"{"a": 1 "b": 2}"#;
  assert_eq!(extract(json, &["b"]), None);
}

#[test]
fn skip_complex_values() {
  let json = r#"{"skip": {"nested": [1, {"deep": true}]}, "target": "found"}"#;
  assert_eq!(extract(json, &["target"]), Some(Value::String("found")));
}

#[test]
fn skip_array_with_nested() {
  let json = r#"{"arr": [[1,2], [3,4]], "val": 42}"#;
  assert_eq!(extract(json, &["val"]), Some(Value::Number("42")));
}

#[test]
fn large_array_skip() {
  let json = r#"[0,1,2,3,4,5,6,7,8,9,"target"]"#;
  assert_eq!(extract(json, &["10"]), Some(Value::String("target")));
}

#[test]
fn key_with_special_chars() {
  let json = r#"{"key-with-dash": "a", "key.with.dots": "b", "key:colon": "c"}"#;
  assert_eq!(
    extract(json, &["key-with-dash"]),
    Some(Value::String("a"))
  );
  assert_eq!(
    extract(json, &["key.with.dots"]),
    Some(Value::String("b"))
  );
  assert_eq!(extract(json, &["key:colon"]), Some(Value::String("c")));
}

#[test]
fn zero_number() {
  let json = r#"{"zero": 0}"#;
  assert_eq!(extract(json, &["zero"]), Some(Value::Number("0")));
}

#[test]
fn negative_float() {
  let json = r#"{"n": -3.14}"#;
  assert_eq!(extract(json, &["n"]), Some(Value::Number("-3.14")));
}

#[test]
fn leading_zero_index_invalid() {
  let json = r"[1, 2, 3]";
  assert_eq!(extract(json, &["01"]), None);
}

#[test]
fn negative_index_invalid() {
  let json = r"[1, 2, 3]";
  assert_eq!(extract(json, &["-1"]), None);
}

#[test]
fn empty_json() {
  let json = "";
  assert_eq!(extract(json, &["key"]), None);
  assert_eq!(extract(json, &[]), None);
}

#[test]
fn just_whitespace() {
  let json = "   ";
  assert_eq!(extract(json, &["key"]), None);
}

#[test]
fn trailing_content_ignored() {
  let json = r#"{"key": "value"} garbage"#;
  assert_eq!(extract(json, &["key"]), Some(Value::String("value")));
}

#[test]
fn multiple_same_keys_finds_first() {
  let json = r#"{"key": "first", "key": "second"}"#;
  assert_eq!(extract(json, &["key"]), Some(Value::String("first")));
}

#[test]
fn value_comparison() {
  assert_eq!(Value::String("a"), Value::String("a"));
  assert_ne!(Value::String("a"), Value::String("b"));
  assert_ne!(Value::String("42"), Value::Number("42"));
}

#[test]
fn value_debug() {
  let s = Value::String("test");
  let n = Value::Number("123");
  assert!(format!("{s:?}").contains("String"));
  assert!(format!("{n:?}").contains("Number"));
}

#[test]
fn parser_reuse() {
  let json = r#"{"a": 1, "b": 2}"#;
  let mut parser = Parser::new(json, &["a"]);
  assert_eq!(parser.extract(), Some(Value::Number("1")));

  let mut parser2 = Parser::new(json, &["b"]);
  assert_eq!(parser2.extract(), Some(Value::Number("2")));
}

#[test]
fn mixed_nesting() {
  let json = r#"{"data": [{"items": [{"value": "found"}]}]}"#;
  assert_eq!(
    extract(json, &["data", "0", "items", "0", "value"]),
    Some(Value::String("found"))
  );
}

#[test]
fn bool_values_skipped() {
  let json = r#"{"a": true, "b": false, "c": "target"}"#;
  assert_eq!(extract(json, &["c"]), Some(Value::String("target")));
}

#[test]
fn null_values_skipped() {
  let json = r#"{"a": null, "b": "target"}"#;
  assert_eq!(extract(json, &["b"]), Some(Value::String("target")));
}

#[test]
fn array_with_mixed_types() {
  let json = r#"[null, true, false, 42, "str", {}, []]"#;
  assert_eq!(extract(json, &["3"]), Some(Value::Number("42")));
  assert_eq!(extract(json, &["4"]), Some(Value::String("str")));
}

#[test]
fn exponent_with_sign() {
  let json = r#"{"pos": 1e+10, "neg": 1e-10}"#;
  assert_eq!(extract(json, &["pos"]), Some(Value::Number("1e+10")));
  assert_eq!(extract(json, &["neg"]), Some(Value::Number("1e-10")));
}

#[test]
fn large_integer() {
  let json = r#"{"big": 9223372036854775807}"#;
  assert_eq!(
    extract(json, &["big"]),
    Some(Value::Number("9223372036854775807"))
  );
}

#[test]
fn verify_against_serde_json() {
  let test_cases: [(&str, &[&str], Option<&str>); 4] = [
    (r#"{"a": "b"}"#, &["a"], Some("b")),
    (r#"{"x": {"y": 42}}"#, &["x", "y"], Some("42")),
    (r"[1, 2, 3]", &["1"], Some("2")),
    (r#"{"arr": [{"k": "v"}]}"#, &["arr", "0", "k"], Some("v")),
  ];

  for (json, path, expected_raw) in test_cases {
    let our_result = extract(json, path);

    let serde_value: serde_json::Value = serde_json::from_str(json).unwrap();
    let mut current = &serde_value;
    let mut serde_found = true;

    for segment in path {
      if let Ok(idx) = segment.parse::<usize>() {
        if let Some(v) = current.get(idx) {
          current = v;
        } else {
          serde_found = false;
          break;
        }
      } else if let Some(v) = current.get(*segment) {
        current = v;
      } else {
        serde_found = false;
        break;
      }
    }

    if let Some(expected) = expected_raw {
      assert!(serde_found, "serde should find path for: {json}");
      match our_result {
        Some(Value::String(s)) => assert_eq!(s, expected),
        Some(Value::Number(n)) => assert_eq!(n, expected),
        None => panic!("Expected to find value '{expected}' in {json}"),
      }
    } else {
      assert!(
        our_result.is_none(),
        "Expected None for {json} with path {path:?}"
      );
    }
  }
}
