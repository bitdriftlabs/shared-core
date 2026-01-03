// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Tests for bd-log-matcher legacy matcher (bd-matcher compatibility)

use crate::matcher::Tree;
use assert_matches::assert_matches;
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{EMPTY_FIELDS, FieldsRef, LogFields, LogLevel, LogMessage, log_level};
use bd_proto::protos::config::v1::config::log_matcher::base_log_matcher::AnyMatch;
use bd_proto::protos::config::v1::config::log_matcher::{
  BaseLogMatcher as LegacyBaseLogMatcherMsg,
  MatcherList as LegacyMatcherList,
  base_log_matcher as LegacyBaseLogMatcher,
};
use bd_proto::protos::config::v1::config::{
  LogMatcher as LegacyLogMatcher,
  log_matcher as legacy_log_matcher,
};
use bd_proto::protos::logging::payload::LogType;

type Input<'a> = (LogType, LogLevel, LogMessage, LogFields);

fn log_msg(message: &str) -> Input<'_> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    LogMessage::String(message.to_string()),
    LogFields::default(),
  )
}

fn log_tag(key: &'static str, value: &'static str) -> Input<'static> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    "message".into(),
    [(key.into(), value.into())].into(),
  )
}

fn binary_log_msg(message: &[u8]) -> Input<'_> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    LogMessage::Bytes(message.into()),
    LogFields::default(),
  )
}

fn binary_log_tag(key: &'static str, value: &'static [u8]) -> Input<'static> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    "message".into(),
    [(key.into(), value.into())].into(),
  )
}

#[allow(clippy::needless_pass_by_value)]
fn match_test_runner(config: LegacyLogMatcher, cases: Vec<(Input<'_>, bool)>) {
  // Note: Tree::new_legacy triggers legacy matcher
  let match_tree = Tree::new_legacy(&config).unwrap();

  for (input, should_match) in cases {
    let (log_type, log_level, message, fields) = input.clone();

    let fields_ref = FieldsRef::new(&fields, &EMPTY_FIELDS);
    let reader = bd_state::test::TestStateReader::default();

    assert_eq!(
      should_match,
      match_tree.do_match(
        log_level,
        log_type,
        &message,
        fields_ref,
        &reader,
        &TinyMap::default()
      ),
      "{input:?} should result in {should_match} but did not",
    );
  }
}

#[test]
fn test_any_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::AnyMatch(
    AnyMatch::default(),
  ));

  match_test_runner(config, vec![(log_msg("anything"), true)]);
}

#[test]
fn test_message_exact_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
    LegacyBaseLogMatcher::MessageMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::EXACT.into(),
      match_value: "exact".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![(log_msg("exact"), true), (log_msg("exactx"), false)],
  );
}

#[test]
fn test_binary_message_exact_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TagMatch(
    LegacyBaseLogMatcher::TagMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::EXACT.into(),
      match_value: "exact".to_string(),
      tag_key: "key".to_string(),
      ..Default::default()
    },
  ));

  // We ignore binary messages for now as they don't work well with some of the matchers (e.g.
  // regex), so even though this appear to be an exact match we still expect to see no match.
  match_test_runner(config, vec![(binary_log_msg(b"exact"), false)]);
}

#[test]
fn test_message_prefix_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
    LegacyBaseLogMatcher::MessageMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::PREFIX.into(),
      match_value: "pref".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![
      (log_msg("prefix"), true),
      (log_msg("pref"), true),
      (log_msg("pre"), false),
    ],
  );
}

#[test]
fn test_message_regex_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
    LegacyBaseLogMatcher::MessageMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
      match_value: "fo.*r".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![
      (log_msg("foobar"), true),
      (log_msg("for"), true),
      (log_msg("bar"), false),
    ],
  );
}

#[test]
fn test_tag_exact_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TagMatch(
    LegacyBaseLogMatcher::TagMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::EXACT.into(),
      match_value: "exact".to_string(),
      tag_key: "key".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![
      (log_tag("key", "exact"), true),
      (log_tag("key", "exactx"), false),
      // The tag to match on does not exist.
      (log_tag("keyx", "exactx"), false),
      (log_msg("no fields"), false),
    ],
  );
}

#[test]
fn test_binary_tag_exact_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TagMatch(
    LegacyBaseLogMatcher::TagMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::EXACT.into(),
      match_value: "exact".to_string(),
      tag_key: "key".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(config, vec![(binary_log_tag("key", b"exact"), false)]);
}

#[test]
fn test_tag_prefix_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TagMatch(
    LegacyBaseLogMatcher::TagMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::PREFIX.into(),
      match_value: "pref".to_string(),
      tag_key: "key".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![
      (log_tag("key", "prefix"), true),
      (log_tag("key", "pref"), true),
      (log_tag("key", "pre"), false),
      // The tag to match on does not exist.
      (log_tag("keyx", "exactx"), false),
      (log_msg("no fields"), false),
    ],
  );
}

#[test]
fn test_tag_regex_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TagMatch(
    LegacyBaseLogMatcher::TagMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
      match_value: "fo.*r".to_string(),
      tag_key: "key".to_string(),
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![
      (log_tag("key", "foobar"), true),
      (log_tag("key", "for"), true),
      (log_tag("key", "bar"), false),
      // The tag to match on does not exist.
      (log_tag("keyx", "exactx"), false),
      (log_msg("no fields"), false),
    ],
  );
}

#[test]
fn type_matcher() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TypeMatch(
    LegacyBaseLogMatcher::TypeMatch {
      type_: 1,
      ..Default::default()
    },
  ));

  match_test_runner(
    config,
    vec![
      (
        (LogType::NORMAL, log_level::DEBUG, "foo".into(), [].into()),
        false,
      ),
      (
        (LogType::REPLAY, log_level::DEBUG, "foo".into(), [].into()),
        true,
      ),
    ],
  );
}

#[test]
fn test_invalid_regex() {
  let config = simple_log_matcher(LegacyBaseLogMatcher::Match_type::TagMatch(
    LegacyBaseLogMatcher::TagMatch {
      match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
      match_value: "*r".to_string(),
      tag_key: "key".to_string(),
      ..Default::default()
    },
  ));

  assert_matches!(Tree::new_legacy(&config), Err(_));
}

#[test]
fn test_or_matcher() {
  let config = LegacyLogMatcher {
    match_type: Some(legacy_log_matcher::Match_type::OrMatcher(
      LegacyMatcherList {
        matcher: vec![
          simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
            LegacyBaseLogMatcher::MessageMatch {
              match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
              match_value: "foo".to_string(),
              ..Default::default()
            },
          )),
          simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
            LegacyBaseLogMatcher::MessageMatch {
              match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
              match_value: "bar".to_string(),
              ..Default::default()
            },
          )),
        ],
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  match_test_runner(
    config,
    vec![
      (log_msg("foo"), true),
      (log_msg("bar"), true),
      (log_msg("baz"), false),
    ],
  );
}

#[test]
fn test_and_matcher() {
  let config = LegacyLogMatcher {
    match_type: Some(legacy_log_matcher::Match_type::AndMatcher(
      LegacyMatcherList {
        matcher: vec![
          simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
            LegacyBaseLogMatcher::MessageMatch {
              match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
              match_value: "foo".to_string(),
              ..Default::default()
            },
          )),
          simple_log_matcher(LegacyBaseLogMatcher::Match_type::MessageMatch(
            LegacyBaseLogMatcher::MessageMatch {
              match_type: LegacyBaseLogMatcher::StringMatchType::REGEX.into(),
              match_value: "bar".to_string(),
              ..Default::default()
            },
          )),
        ],
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  match_test_runner(
    config,
    vec![
      (log_msg("foo"), false),
      (log_msg("bar"), false),
      (log_msg("foobar"), true),
    ],
  );
}

#[test]
fn test_not_matcher() {
  let config = LegacyLogMatcher {
    match_type: Some(legacy_log_matcher::Match_type::NotMatcher(Box::new(
      simple_log_matcher(LegacyBaseLogMatcher::Match_type::AnyMatch(
        AnyMatch::default(),
      )),
    ))),
    ..Default::default()
  };

  match_test_runner(
    config,
    vec![
      (log_msg("foo"), false),
      (log_msg("bar"), false),
      (log_msg("foobar"), false),
    ],
  );
}

fn simple_log_matcher(match_type: LegacyBaseLogMatcher::Match_type) -> LegacyLogMatcher {
  LegacyLogMatcher {
    match_type: Some(legacy_log_matcher::Match_type::BaseMatcher(
      LegacyBaseLogMatcherMsg {
        match_type: Some(match_type),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}
