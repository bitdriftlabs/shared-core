// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{DataValue, EMPTY_FIELDS, FieldsRef, LogFields, LogMessage, log_level};
use bd_proto::protos::log_matcher::log_matcher::{LogMatcher, log_matcher};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::value_matcher::value_matcher::json_path_value_match::{
  KeyOrIndex,
  key_or_index,
};
use bd_proto::protos::value_matcher::value_matcher::{JsonPathValueMatch, Operator};
use std::hint::black_box;
use std::time::Instant;

const JSON_PATH_BENCHMARK_ITERATIONS: usize = 100_000;

fn json_string_matcher(path: Vec<KeyOrIndex>, value: &str) -> Tree {
  Tree::new(&LogMatcher {
    matcher: Some(log_matcher::Matcher::BaseMatcher(
      log_matcher::BaseLogMatcher {
        match_type: Some(log_matcher::base_log_matcher::Match_type::TagMatch(
          log_matcher::base_log_matcher::TagMatch {
            tag_key: "payload".to_string(),
            value_match: Some(
              log_matcher::base_log_matcher::tag_match::Value_match::JsonValueMatch(
                JsonPathValueMatch {
                  operator: Operator::OPERATOR_EQUALS.into(),
                  match_value: value.to_string(),
                  key_or_index: path,
                  ..Default::default()
                },
              ),
            ),
            ..Default::default()
          },
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  })
  .unwrap()
}

fn key(key: &str) -> KeyOrIndex {
  KeyOrIndex {
    key_or_index: Some(key_or_index::Key_or_index::Key(key.to_string())),
    ..Default::default()
  }
}

fn index(index: i32) -> KeyOrIndex {
  KeyOrIndex {
    key_or_index: Some(key_or_index::Key_or_index::Index(index)),
    ..Default::default()
  }
}

fn matches(tree: &Tree, json: &str) -> bool {
  let fields: LogFields = [("payload".into(), DataValue::String(json.to_string()))].into();
  tree.do_match(
    log_level::DEBUG,
    LogType::NORMAL,
    &LogMessage::String("message".to_string()),
    FieldsRef::new(&fields, &EMPTY_FIELDS),
    &bd_state::InMemoryStateReader::new(),
    &TinyMap::default(),
    0,
  )
}

#[test]
fn matches_json_string_object_and_array_values() {
  let plan = json_string_matcher(vec![key("user"), key("plan")], "pro");
  let item = json_string_matcher(vec![key("items"), index(0), key("id")], "shirt");

  assert!(matches(&plan, r#"{"user":{"plan":"pro"}}"#));
  assert!(!matches(&plan, r#"{"user":{"plan":"basic"}}"#));
  assert!(!matches(&plan, r#"{"user":{"plan":"pro"}"#));
  assert!(matches(&item, r#"{"items":[{"id":"shirt"}]}"#));
}

#[test]
fn matches_json_string_boolean_and_number_values() {
  let enabled = json_string_matcher(vec![key("enabled")], "true");
  let count = json_string_matcher(vec![key("count")], "42");

  assert!(matches(&enabled, r#"{"enabled":true}"#));
  assert!(matches(&count, r#"{"count":42}"#));
}

#[test]
#[ignore = "run manually with --release -- --ignored --nocapture"]
fn benchmark_json_string_path_resolution() {
  let value = DataValue::String(
    r#"{"metadata":{"request_id":"r_456"},"users":[{"id":"user_123"},{"id":"user_456"}]}"#
      .to_string(),
  );
  let path = [
    JsonPathToken::Key("users".to_string()),
    JsonPathToken::Index(1),
    JsonPathToken::Key("id".to_string()),
  ];

  let started_at = Instant::now();
  for _ in 0 .. JSON_PATH_BENCHMARK_ITERATIONS {
    black_box(resolve_json_path(&value, &path));
  }
  eprintln!(
    "json string path ({JSON_PATH_BENCHMARK_ITERATIONS} iterations): {:?}",
    started_at.elapsed()
  );
}
