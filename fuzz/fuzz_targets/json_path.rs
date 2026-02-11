// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![no_main]

use arbitrary::Arbitrary;
use bd_json_path::{Value, extract};
use libfuzzer_sys::fuzz_target;
use serde_json::json;

#[derive(Arbitrary, Debug, Clone)]
enum JsonValue {
  Null,
  Bool(bool),
  Int(i64),
  String(String),
  Array(Vec<Self>),
  Object(Vec<(String, Self)>),
}

impl JsonValue {
  fn to_serde(&self) -> serde_json::Value {
    match self {
      Self::Null => serde_json::Value::Null,
      Self::Bool(b) => json!(*b),
      Self::Int(n) => json!(*n),
      Self::String(s) => json!(s),
      Self::Array(arr) => serde_json::Value::Array(arr.iter().map(Self::to_serde).collect()),
      Self::Object(pairs) => {
        let map: serde_json::Map<String, serde_json::Value> = pairs
          .iter()
          .map(|(k, v)| (k.clone(), v.to_serde()))
          .collect();
        serde_json::Value::Object(map)
      },
    }
  }
}

#[derive(Arbitrary, Debug, Clone)]
enum PathSegment {
  Key(String),
  Index(u8),
}

impl PathSegment {
  fn as_str(&self) -> String {
    match self {
      Self::Key(k) => k.clone(),
      Self::Index(i) => i.to_string(),
    }
  }
}

#[derive(Arbitrary, Debug)]
struct JsonPathFuzzCase {
  json_value: JsonValue,
  path: Vec<PathSegment>,
}

fn parse_index(s: &str) -> Option<usize> {
  let bytes = s.as_bytes();
  if bytes.is_empty() {
    return None;
  }
  if bytes.len() > 1 && bytes[0] == b'0' {
    return None;
  }
  s.parse().ok()
}

fn extract_with_serde<'a>(
  value: &'a serde_json::Value,
  path: &[String],
) -> Option<&'a serde_json::Value> {
  let mut current = value;
  for segment in path {
    if let Some(idx) = parse_index(segment) {
      if current.is_array() {
        current = current.get(idx)?;
      } else {
        current = current.get(segment)?;
      }
    } else {
      current = current.get(segment)?;
    }
  }
  Some(current)
}

fuzz_target!(|case: JsonPathFuzzCase| {
  let serde_value = case.json_value.to_serde();
  let json_string = serde_json::to_string(&serde_value).unwrap();

  let path_strings: Vec<String> = case.path.iter().map(PathSegment::as_str).collect();
  let path_refs: Vec<&str> = path_strings.iter().map(String::as_str).collect();

  let our_result = extract(&json_string, &path_refs);
  let serde_result = extract_with_serde(&serde_value, &path_strings);

  match (our_result, serde_result) {
    (None, None) => {},
    (None, Some(serde_val)) => match serde_val {
      serde_json::Value::String(_) | serde_json::Value::Number(_) => {
        assert!(
          !(!json_string_has_escapes(&json_string)
            && !path_has_non_ascii_or_control(&path_strings)),
          "Mismatch: we returned None, serde found {serde_val:?}\nJSON: {json_string}\nPath: \
           {path_strings:?}"
        );
      },
      _ => {},
    },
    (Some(_), None) => {
      panic!(
        "Mismatch: we found something, serde returned None\nJSON: {json_string}\nPath: \
         {path_strings:?}"
      );
    },
    (Some(Value::String(our_str)), Some(serde_json::Value::String(serde_str))) => {
      // Our parser returns raw string content without decoding escape sequences.
      // Only compare when there are no escapes in the JSON representation.
      // Check if the JSON string contains any escape sequences (backslash followed by something).
      if !json_string_has_escapes(&json_string) {
        assert_eq!(
          our_str, serde_str,
          "String mismatch\nJSON: {json_string}\nPath: {path_strings:?}"
        );
      }
    },
    (Some(Value::Number(our_num)), Some(serde_json::Value::Number(serde_num))) => {
      let serde_str = serde_num.to_string();
      assert_eq!(
        our_num, serde_str,
        "Number mismatch\nJSON: {json_string}\nPath: {path_strings:?}"
      );
    },
    (Some(our_val), Some(serde_val)) => {
      panic!(
        "Type mismatch: our {our_val:?} vs serde {serde_val:?}\nJSON: {json_string}\nPath: \
         {path_strings:?}"
      );
    },
  }
});

fn json_string_has_escapes(json: &str) -> bool {
  let bytes = json.as_bytes();
  for i in 0 .. bytes.len().saturating_sub(1) {
    if bytes[i] == b'\\' {
      return true;
    }
  }
  false
}

fn path_has_non_ascii_or_control(path: &[String]) -> bool {
  for segment in path {
    for c in segment.chars() {
      if !c.is_ascii() || c.is_ascii_control() || c == '\\' || c == '"' {
        return true;
      }
    }
  }
  false
}
