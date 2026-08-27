// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
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
    MatchContext::default(),
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
fn disabled_context_does_not_match_json_strings() {
  let tree = json_string_matcher(vec![key("user"), key("plan")], "pro");
  let fields: LogFields = [(
    "payload".into(),
    DataValue::String(r#"{"user":{"plan":"pro"}}"#.to_string()),
  )]
  .into();

  assert!(matches(&tree, r#"{"user":{"plan":"pro"}}"#));
  assert!(!tree.do_match(
    log_level::DEBUG,
    LogType::NORMAL,
    &LogMessage::String("message".to_string()),
    FieldsRef::new(&fields, &EMPTY_FIELDS),
    &bd_state::InMemoryStateReader::new(),
    &TinyMap::default(),
    0,
    MatchContext {
      json_path_string_matching_enabled: false,
    },
  ));
}

#[test]
fn disabled_context_does_not_invert_json_string_matches() {
  let tree = Tree::Not(Box::new(json_string_matcher(
    vec![key("user"), key("plan")],
    "pro",
  )));
  let fields: LogFields = [(
    "payload".into(),
    DataValue::String(r#"{"user":{"plan":"pro"}}"#.to_string()),
  )]
  .into();
  let context = MatchContext {
    json_path_string_matching_enabled: false,
  };

  assert!(!matches(&tree, r#"{"user":{"plan":"pro"}}"#));
  assert!(matches(&tree, r#"{"user":{"plan":"basic"}}"#));
  assert!(!tree.do_match(
    log_level::DEBUG,
    LogType::NORMAL,
    &LogMessage::String("message".to_string()),
    FieldsRef::new(&fields, &EMPTY_FIELDS),
    &bd_state::InMemoryStateReader::new(),
    &TinyMap::default(),
    0,
    context,
  ));
}
