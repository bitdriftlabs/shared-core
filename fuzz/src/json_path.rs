// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use arbitrary::{Arbitrary, Unstructured};
use bd_log_matcher::matcher::{JsonPathToken, resolve_json_path_for_testing};
use serde_json::Value;

#[derive(Debug)]
pub struct JsonPathFuzzTestCase {
  raw_json: Vec<u8>,
  raw_path: Vec<JsonPathToken>,
  valid_json: Value,
  valid_path: Vec<JsonPathToken>,
  nesting_depth: u8,
}

impl<'a> Arbitrary<'a> for JsonPathFuzzTestCase {
  fn arbitrary(input: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let valid_json = generate_value(input, 0);
    let valid_path = if input.arbitrary::<bool>().unwrap_or_default() {
      matching_path(&valid_json, input)
    } else {
      generate_path(input)
    };

    Ok(Self {
      raw_path: generate_path(input),
      raw_json: input.bytes(input.len())?.to_vec(),
      valid_json,
      valid_path,
      nesting_depth: input.arbitrary()?,
    })
  }
}

fn generate_value(input: &mut Unstructured<'_>, depth: usize) -> Value {
  if depth == 4 {
    return generate_scalar(input);
  }

  match input.arbitrary::<u8>().unwrap_or_default() % 5 {
    0 => generate_scalar(input),
    1 => {
      let mut map = serde_json::Map::new();
      for index in 0 .. usize::from(input.arbitrary::<u8>().unwrap_or_default() % 4) {
        map.insert(
          format!(
            "key_{index}_{}",
            input.arbitrary::<u8>().unwrap_or_default()
          ),
          generate_value(input, depth + 1),
        );
      }
      Value::Object(map)
    },
    _ => Value::Array(
      (0 .. usize::from(input.arbitrary::<u8>().unwrap_or_default() % 4))
        .map(|_| generate_value(input, depth + 1))
        .collect(),
    ),
  }
}

fn generate_scalar(input: &mut Unstructured<'_>) -> Value {
  match input.arbitrary::<u8>().unwrap_or_default() % 4 {
    0 => Value::Null,
    1 => Value::Bool(input.arbitrary().unwrap_or_default()),
    2 => Value::Number(input.arbitrary::<i64>().unwrap_or_default().into()),
    _ => {
      let length = usize::from(input.arbitrary::<u8>().unwrap_or_default() % 32);
      let bytes = input.bytes(length.min(input.len())).unwrap_or_default();
      Value::String(String::from_utf8_lossy(bytes).into_owned())
    },
  }
}

fn generate_path(input: &mut Unstructured<'_>) -> Vec<JsonPathToken> {
  let path_len = usize::from(input.arbitrary::<u8>().unwrap_or_default() % 8);
  let mut path = Vec::with_capacity(path_len);
  for _ in 0 .. path_len {
    if input.arbitrary().unwrap_or_default() {
      let key_len = usize::from(input.arbitrary::<u8>().unwrap_or_default() % 32);
      let key = String::from_utf8_lossy(input.bytes(key_len.min(input.len())).unwrap_or_default())
        .into_owned();
      path.push(JsonPathToken::Key(key));
    } else {
      path.push(JsonPathToken::Index(input.arbitrary().unwrap_or_default()));
    }
  }
  path
}

fn matching_path(value: &Value, input: &mut Unstructured<'_>) -> Vec<JsonPathToken> {
  let mut path = Vec::new();
  let mut current = value;
  while path.len() < 8 {
    match current {
      Value::Object(map) if !map.is_empty() => {
        let index = usize::from(input.arbitrary::<u8>().unwrap_or_default()) % map.len();
        let (key, value) = map.iter().nth(index).unwrap();
        path.push(JsonPathToken::Key(key.clone()));
        current = value;
      },
      Value::Array(values) if !values.is_empty() => {
        let index = usize::from(input.arbitrary::<u8>().unwrap_or_default()) % values.len();
        path.push(JsonPathToken::Index(index.try_into().unwrap()));
        current = &values[index];
      },
      _ => break,
    }
  }
  path
}

fn oracle(value: &Value, path: &[JsonPathToken]) -> Option<String> {
  let mut current = value;
  for token in path {
    current = match token {
      JsonPathToken::Key(key) => current.as_object()?.get(key)?,
      JsonPathToken::Index(index) => current.as_array()?.get(usize::try_from(*index).ok()?)?,
    };
  }

  match current {
    Value::String(value) => Some(value.clone()),
    Value::Bool(value) => Some(value.to_string()),
    Value::Number(value) => Some(value.to_string()),
    Value::Null | Value::Array(_) | Value::Object(_) => None,
  }
}

pub fn run(test_case: &JsonPathFuzzTestCase) {
  let raw_json = String::from_utf8_lossy(&test_case.raw_json);
  let _ = resolve_json_path_for_testing(&raw_json, &test_case.raw_path);

  let valid_json = serde_json::to_string(&test_case.valid_json).unwrap();
  assert_eq!(
    resolve_json_path_for_testing(&valid_json, &test_case.valid_path)
      .as_deref()
      .map(str::to_owned),
    oracle(&test_case.valid_json, &test_case.valid_path),
    "json={valid_json}, path={:?}",
    test_case.valid_path,
  );

  let depth = usize::from(test_case.nesting_depth % 140);
  let deeply_nested_json = format!("{}\"value\"{}", "[".repeat(depth), "]".repeat(depth));
  let deeply_nested_path = vec![JsonPathToken::Index(0); depth];
  let _ = resolve_json_path_for_testing(&deeply_nested_json, &deeply_nested_path);

  // Parser correctness for malformed input matters as much as matching valid generated documents.
  let truncated_json = &valid_json[.. valid_json.len().saturating_sub(1)];
  let _ = resolve_json_path_for_testing(truncated_json, &test_case.valid_path);
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus("corpus/json_path", |input| run(&input));
}
