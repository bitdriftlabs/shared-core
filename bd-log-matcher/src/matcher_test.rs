// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::builder;
use crate::matcher::Tree;
use crate::matcher::base_log_matcher::tag_match::Value_match::DoubleValueMatch;
use crate::test::TestMatcher;
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{
  EMPTY_FIELDS,
  FieldsRef,
  LogFields,
  LogLevel,
  LogMessage,
  StringOrBytes,
  TypedLogLevel,
  log_level,
};
use bd_proto::protos::log_matcher::log_matcher::{LogMatcher, log_matcher};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::state::matcher::state_value_match;
use bd_proto::protos::state::scope::StateScope;
use bd_proto::protos::value_matcher::value_matcher::Operator;
use bd_proto::protos::value_matcher::value_matcher::double_value_match::Double_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::int_value_match::Int_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::string_value_match::String_value_match_type;
use bd_state::StateReader;
use log_matcher::base_log_matcher::Match_type::{MessageMatch, StateMatch, TagMatch};
use log_matcher::base_log_matcher::tag_match::Value_match::{
  IntValueMatch,
  IsSetMatch,
  SemVerValueMatch,
  StringValueMatch,
};
use log_matcher::{BaseLogMatcher, Matcher, MatcherList, base_log_matcher};
use pretty_assertions::assert_eq;
use protobuf::{Enum, MessageField};

type Input<'a> = (LogType, LogLevel, LogMessage, LogFields);

fn log_msg(message: &str) -> Input<'_> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    LogMessage::String(message.to_string()),
    [].into(),
  )
}

fn binary_log_msg(message: &[u8]) -> Input<'_> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    LogMessage::Bytes(message.to_vec()),
    [].into(),
  )
}

fn log_tag(key: &'static str, value: &'static str) -> Input<'static> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    LogMessage::String("message".into()),
    [(key.into(), StringOrBytes::String(value.into()))].into(),
  )
}

fn binary_log_tag(key: &'static str, value: &'static [u8]) -> Input<'static> {
  (
    LogType::NORMAL,
    log_level::DEBUG,
    LogMessage::String("message".into()),
    [(key.into(), StringOrBytes::Bytes(value.into()))].into(),
  )
}

fn log_type(log_type: LogType) -> Input<'static> {
  (
    log_type,
    log_level::DEBUG,
    LogMessage::String("message".into()),
    [].into(),
  )
}

fn log_level(log_level: LogLevel) -> Input<'static> {
  (
    LogType::NORMAL,
    log_level,
    LogMessage::String("message".into()),
    [].into(),
  )
}

#[test]
fn test_message_string_eq_matcher() {
  match_test_runner(
    builder::message_equals("exact"),
    vec![
      (log_msg("exact"), true),
      (log_msg("EXACT"), false),
      (log_msg("exactx"), false),
    ],
  );
}

#[test]
fn test_message_string_lte_matcher() {
  let config = simple_log_matcher(make_message_match(
    Operator::OPERATOR_LESS_THAN_OR_EQUAL,
    "abcd",
  ));

  match_test_runner(
    config,
    vec![
      (log_msg("abcd"), true),
      (log_msg("ABCD"), true),
      (log_msg("aacd"), true),
      (log_msg("abce"), false),
      (log_msg("abcda"), false),
    ],
  );
}

#[test]
fn test_message_string_lt_matcher() {
  let config = simple_log_matcher(make_message_match(Operator::OPERATOR_LESS_THAN, "abcd"));

  match_test_runner(
    config,
    vec![
      (log_msg("abcd"), false),
      (log_msg("ABCD"), true),
      (log_msg("aacd"), true),
      (log_msg("abce"), false),
      (log_msg("abcda"), false),
    ],
  );
}

#[test]
fn test_message_string_gte_matcher() {
  let config = simple_log_matcher(make_message_match(
    Operator::OPERATOR_GREATER_THAN_OR_EQUAL,
    "abcd",
  ));

  match_test_runner(
    config,
    vec![
      (log_msg("abcd"), true),
      (log_msg("ABCD"), false),
      (log_msg("aacd"), false),
      (log_msg("abce"), true),
      (log_msg("abcda"), true),
    ],
  );
}

#[test]
fn test_message_string_gt_matcher() {
  let config = simple_log_matcher(make_message_match(Operator::OPERATOR_GREATER_THAN, "abcd"));

  match_test_runner(
    config,
    vec![
      (log_msg("abcd"), false),
      (log_msg("ABCD"), false),
      (log_msg("aacd"), false),
      (log_msg("abce"), true),
      (log_msg("abcda"), true),
    ],
  );
}

#[test]
fn test_message_binary_string_eq_matcher() {
  // We ignore binary messages for now as they don't work well with some of the matchers (e.g.
  // regex), so even though this appear to be an exact match we still expect to see no match.
  match_test_runner(
    builder::message_equals("exact_binary"),
    vec![(binary_log_msg(b"exact_binary"), false)],
  );
}

#[test]
fn test_message_string_regex_matcher() {
  match_test_runner(
    builder::message_regex_matches("fo.*r"),
    vec![
      (log_msg("foobar"), true),
      (log_msg("for"), true),
      (log_msg("bar"), false),
    ],
  );
}

#[test]
fn test_message_string_invalid_regex_config() {
  let config = builder::message_regex_matches("*r");

  assert_eq!(
    Tree::new(&config).err().unwrap().to_string(),
    "invalid regex"
  );
}

#[test]
fn test_extracted_string_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        string_value_match_type: Some(String_value_match_type::SaveFieldId("id1".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner_with_extractions(
    config.clone(),
    vec![
      (log_tag("key", "exact"), false),
      (log_tag("keyx", "exact"), false),
      (log_msg("no fields"), false),
    ],
    &TinyMap::default(),
    &bd_state::test::TestStateReader::new(),
  );

  match_test_runner_with_extractions(
    config,
    vec![
      (log_tag("key", "exact"), true),
      (log_tag("keyx", "exact"), false),
      (log_msg("no fields"), false),
    ],
    &[("id1".to_string(), "exact".to_string())].into(),
    &bd_state::test::TestStateReader::new(),
  );
}

#[test]
fn test_tag_string_eq_matcher() {
  let config = builder::field_equals("key", "exact");

  match_test_runner(
    config,
    vec![
      (log_tag("key", "exact"), true),
      // the value does not exist
      (log_tag("key", "exactx"), false),
      // The tag key to match on does not exist
      (log_tag("keyx", "exact"), false),
      (log_msg("no fields"), false),
    ],
  );
}

#[test]
fn test_tag_binary_string_eq_matcher() {
  let config = builder::field_equals("key", "exact_binary");

  // We ignore binary fields for now as they don't work well with some of the matchers (e.g.
  // regex), so even though this appear to be an exact match we still expect to see no match.
  match_test_runner(
    config,
    vec![(binary_log_tag("key", b"exact_binary"), false)],
  );
}

#[test]
fn test_extracted_double_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(DoubleValueMatch(
      bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        double_value_match_type: Some(Double_value_match_type::SaveFieldId("id1".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner_with_extractions(
    config.clone(),
    vec![
      (log_tag("key", "13.0"), false),
      (log_tag("key", "13"), false),
    ],
    &TinyMap::default(),
    &bd_state::test::TestStateReader::new(),
  );
  match_test_runner_with_extractions(
    config.clone(),
    vec![
      (log_tag("key", "13.0"), false),
      (log_tag("key", "13"), false),
    ],
    &[("id1".to_string(), "bad".to_string())].into(),
    &bd_state::test::TestStateReader::new(),
  );
  match_test_runner_with_extractions(
    config,
    vec![(log_tag("key", "13.0"), true), (log_tag("key", "13"), true)],
    &[("id1".to_string(), "13".to_string())].into(),
    &bd_state::test::TestStateReader::new(),
  );
}

#[test]
fn test_tag_double_matcher() {
  fn make_config(match_value: f64, operator: Operator) -> LogMatcher {
    simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
      tag_key: "key".to_string(),
      value_match: Some(DoubleValueMatch(
        bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
          operator: operator.into(),
          double_value_match_type: Some(Double_value_match_type::MatchValue(match_value)),
          ..Default::default()
        },
      )),
      ..Default::default()
    }))
  }

  match_test_runner(
    make_config(12.0, Operator::OPERATOR_LESS_THAN_OR_EQUAL),
    vec![
      (log_tag("key", "13.0"), false),
      (log_tag("key", "12.0"), true),
      (log_tag("key", "11"), true),
      (log_tag("key", "NaN"), false),
      (log_tag("key", "+Inf"), false),
      (log_tag("key", "-Inf"), true),
    ],
  );

  match_test_runner(
    make_config(f64::INFINITY, Operator::OPERATOR_LESS_THAN_OR_EQUAL),
    vec![
      (log_tag("key", "13.0"), true),
      (log_tag("key", "12.0"), true),
      (log_tag("key", "11"), true),
      (log_tag("key", "NaN"), false),
      (log_tag("key", "+Inf"), true),
      (log_tag("key", "-Inf"), true),
    ],
  );

  match_test_runner(
    make_config(f64::NEG_INFINITY, Operator::OPERATOR_LESS_THAN_OR_EQUAL),
    vec![
      (log_tag("key", "13.0"), false),
      (log_tag("key", "12.0"), false),
      (log_tag("key", "11"), false),
      (log_tag("key", "NaN"), false),
      (log_tag("key", "+Inf"), false),
      (log_tag("key", "-Inf"), true),
    ],
  );

  match_test_runner(
    make_config(f64::NAN, Operator::OPERATOR_LESS_THAN_OR_EQUAL),
    vec![
      (log_tag("key", "13.0"), false),
      (log_tag("key", "12.0"), false),
      (log_tag("key", "11"), false),
      (log_tag("key", "NaN"), false),
      (log_tag("key", "+Inf"), false),
      (log_tag("key", "-Inf"), false),
    ],
  );

  match_test_runner(
    make_config(f64::NAN, Operator::OPERATOR_EQUALS),
    vec![
      (log_tag("key", "13.0"), false),
      (log_tag("key", "12.0"), false),
      (log_tag("key", "11"), false),
      (log_tag("key", "NaN"), true),
      (log_tag("key", "+Inf"), false),
      (log_tag("key", "-Inf"), false),
    ],
  );
}

#[test]
fn test_extracted_int_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        int_value_match_type: Some(Int_value_match_type::SaveFieldId("id1".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner_with_extractions(
    config.clone(),
    vec![
      (log_tag("key", "13"), false),
      (log_tag("key", "13.0"), false),
    ],
    &TinyMap::default(),
    &bd_state::test::TestStateReader::new(),
  );

  match_test_runner_with_extractions(
    config,
    vec![(log_tag("key", "13"), true), (log_tag("key", "13.0"), true)],
    &[("id1".to_string(), "13".to_string())].into(),
    &bd_state::test::TestStateReader::new(),
  );
}

#[test]
fn test_tag_int_lte_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_LESS_THAN_OR_EQUAL.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(12)),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner(
    config,
    vec![
      // invalid u32::MAX input for matcher
      (log_tag("key", "4294967295"), false),
      // invalid string input for matcher
      (log_tag("key", "invalid"), false),
      // The tag key to match on does not exist
      (log_tag("keyx", ""), false),
      (log_msg("no fields"), false),
      (log_tag("key", "13"), false),
      (log_tag("key", "12"), true),
      (log_tag("key", "11"), true),
    ],
  );
}

// Test string comparison using a number since the raw custom attributes in front-end
// use a matchType of String
#[test]
fn test_tag_string_gt_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_GREATER_THAN.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue("40".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner(
    config,
    vec![
      // The tag key to match on does not exist
      (log_tag("keyx", ""), false),
      (log_msg("no fields"), false),
      (log_tag("key", "0"), false),
      (log_tag("key", "39.999"), false),
      (log_tag("key", "40"), false),
      (log_tag("key", "45"), true),
      (log_tag("key", "45.5"), true),
      (log_tag("key", "100"), false), // string uses lexicographic comparisons
    ],
  );
}

#[test]
fn test_tag_int_invalid_regex_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_REGEX.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(12)),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  assert_eq!(
    Tree::new(&config).err().unwrap().to_string(),
    "regex does not support int32"
  );
}

#[test]
fn test_tag_log_type_invalid_config_value() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "log_type".to_string(),
    value_match: Some(IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_REGEX.into(), // this is ignored
        int_value_match_type: Some(Int_value_match_type::MatchValue(-1)), // invalid
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  assert_eq!(
    Tree::new(&config).err().unwrap().to_string(),
    "out of range integral type conversion attempted"
  );
}

#[test]
fn test_tag_log_type() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "log_type".to_string(),
    value_match: Some(IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_REGEX.into(), // this is ignored
        int_value_match_type: Some(Int_value_match_type::MatchValue(3)),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner(
    config,
    vec![
      // invalid u32::MAX input for matcher
      (
        log_type(LogType::from_i32(i32::MAX).unwrap_or_default()),
        false,
      ),
      (log_type(LogType::RESOURCE), true),
    ],
  );
}

#[test]
fn test_tag_log_level() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "log_level".to_string(),
    value_match: Some(IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_GREATER_THAN_OR_EQUAL.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(2)), // INFO
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner(
    config,
    vec![
      // invalid input
      (log_level(u32::MAX), false),
      (log_level(log_level::DEBUG), false),
      (log_level(log_level::INFO), true),
      (log_level(log_level::WARNING), true),
    ],
  );
}

fn semver_tag_matcher_config(value: &str, operator: Operator) -> LogMatcher {
  simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(SemVerValueMatch(
      bd_proto::protos::value_matcher::value_matcher::SemVerValueMatch {
        operator: operator.into(),
        match_value: value.to_string(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }))
}

#[test]
fn tag_semver_lte_matcher() {
  // Verifies <value> <= 1.5.19.0;
  let config = semver_tag_matcher_config("1.5.19.0", Operator::OPERATOR_LESS_THAN_OR_EQUAL);

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), true),     // 2 < 19
      (log_tag("key", "1.5.2.0"), true),   // 2 < 19
      (log_tag("key", "1.5.2.0.4"), true), // 2 < 19
      (log_tag("key", "1.5.19"), true),    // 19 == 19, 0 == 9
      (log_tag("key", "1.5.19.0"), true),  // correct number of segments, 19 == 19
      (log_tag("key", "1.5.20"), false),   // 20 > 19
      (log_tag("key", "1.5.20.0"), false), // 20 > 19
      // Note that this one diverges from semver conventions.
      (log_tag("key", "1.5.19.0-rc2"), true), // 0-rc2 == 0 as suffix is ignored
    ],
  );
}

#[test]
fn tag_semver_lt_matcher() {
  // Verifies <value> < 1.5.19.0;
  let config = semver_tag_matcher_config("1.5.19.0", Operator::OPERATOR_LESS_THAN);

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), true),       // 2 < 19
      (log_tag("key", "1.5.2.0"), true),     // 2 < 19
      (log_tag("key", "1.5.2.0.4"), true),   // 2 < 19
      (log_tag("key", "1.5.19"), false),     // 19 == 19, 0 == 0
      (log_tag("key", "1.5.19.0"), false),   // 19 == 19, 0 == 0
      (log_tag("key", "1.5.19.0.4"), false), // 19 == 19, 0 == 0, 4 > 0
      (log_tag("key", "1.5.20"), false),     // 20 > 19
      // Note that this one diverges from semver conventions.
      (log_tag("key", "1.5.19.0-rc2"), false), // 0-rc2 == 0 as suffix is ignored
    ],
  );
}

#[test]
fn tag_semver_gt_matcher() {
  // Verifies <value> > 1.5.19.0;
  let config = semver_tag_matcher_config("1.5.19.0", Operator::OPERATOR_GREATER_THAN);

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), false),      // 2 < 19
      (log_tag("key", "1.5.2.0"), false),    // 2 < 19
      (log_tag("key", "1.5.2.0.0"), false),  // 2 < 19
      (log_tag("key", "1.5.19"), false),     // 19 == 19, 0 == 0
      (log_tag("key", "1.5.19.0"), false),   // 19 == 19, 0 == 0
      (log_tag("key", "1.5.19.0.0"), false), // 19 == 19, 0 == 0
      (log_tag("key", "1.5.20"), true),      // 20 > 19
      (log_tag("key", "1.5.20.0"), true),    // 20 > 19
      // Note that this one diverges from semver conventions.
      (log_tag("key", "1.5.19.0-rc2"), false), // 0-rc2 == 0 as suffix is ignored
    ],
  );
}

#[test]
fn tag_semver_gte_matcher() {
  // Verifies <value> >= 1.5.19.0;
  let config = semver_tag_matcher_config("1.5.19.0", Operator::OPERATOR_GREATER_THAN_OR_EQUAL);

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), false),     // 2 < 19
      (log_tag("key", "1.5.2.0"), false),   // 2 < 19
      (log_tag("key", "1.5.2.0.4"), false), // 2 < 19
      (log_tag("key", "1.5.19"), true),     // 19 == 19, 0 == 0
      (log_tag("key", "1.5.19.0"), true),   // 19 == 19, 0 == 0
      (log_tag("key", "1.5.19.0.4"), true), // 19 == 19, 0 == 0, 4 > 0
      (log_tag("key", "1.5.20"), true),     // 20 > 19
      // Note that this one diverges from semver conventions.
      (log_tag("key", "1.5.19.0-rc2"), true), // 0-rc2 == 0 as suffix is ignored
    ],
  );
}

#[test]
fn tag_semver_eq_matcher() {
  // Verifies <value> == 1.5.19.0;
  let config = semver_tag_matcher_config("1.5.19.0", Operator::OPERATOR_EQUALS);

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), false),      // 2 < 19
      (log_tag("key", "1.4.2"), false),      // 2 < 19
      (log_tag("key", "1.5.19.0"), true),    // exact match
      (log_tag("key", "1.5.19.0.4"), false), // 4 != 0
      (log_tag("key", "1.5.19"), true),      // 0 == 0
      (log_tag("key", "1.6"), false),        // 6 > 5
    ],
  );
}

#[test]
fn tag_semver_neq_matcher() {
  // Verifies <value> != 1.5.19.0;
  let config = semver_tag_matcher_config("1.5.19.0", Operator::OPERATOR_NOT_EQUALS);

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), true),      // 2 < 19
      (log_tag("key", "1.4.2"), true),      // 4 != 5
      (log_tag("key", "1.5.19.0"), false),  // exact match
      (log_tag("key", "1.5.19.1"), true),   // 1 != 0
      (log_tag("key", "1.5.19.0.4"), true), // 4 != 0
      (log_tag("key", "1.5.19"), false),    // 0 == 0
      (log_tag("key", "1.6"), true),        // 6 > 5
    ],
  );
}

#[test]
fn mixed_segment_semver_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(SemVerValueMatch(
      bd_proto::protos::value_matcher::value_matcher::SemVerValueMatch {
        operator: Operator::OPERATOR_LESS_THAN_OR_EQUAL.into(),
        match_value: "1.5.foo.0".to_string(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.fox"), false), // fox > foo
      (log_tag("key", "1.5.fo"), true),   // fo < foo
      (log_tag("key", "1.05.fo"), true),  // fo < foo
      (log_tag("key", "1.5.fo.0"), true), // fo < foo
      // This awkwardly evaluates to true since foo > 1 per string lexo sort.
      (log_tag("key", "1.5.1.0"), true),
    ],
  );
}

#[test]
fn test_tag_semver_eq_regex_matcher() {
  let config = simple_log_matcher(TagMatch(base_log_matcher::TagMatch {
    tag_key: "key".to_string(),
    value_match: Some(SemVerValueMatch(
      bd_proto::protos::value_matcher::value_matcher::SemVerValueMatch {
        operator: Operator::OPERATOR_REGEX.into(),
        match_value: "1\\.5\\.*".to_string(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }));

  match_test_runner(
    config,
    vec![
      (log_tag("key", "1.5.2"), true),
      (log_tag("key", "1.5.20"), true),
      (log_tag("key", "1.50.20"), true),
    ],
  );
}

#[test]
fn test_or_matcher() {
  let config = LogMatcher {
    matcher: Some(log_matcher::Matcher::OrMatcher(MatcherList {
      log_matchers: vec![
        simple_log_matcher(make_message_match(Operator::OPERATOR_REGEX, "foo")),
        simple_log_matcher(make_message_match(Operator::OPERATOR_REGEX, "bar")),
      ],
      ..Default::default()
    })),
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
  let config = LogMatcher {
    matcher: Some(log_matcher::Matcher::AndMatcher(MatcherList {
      log_matchers: vec![
        simple_log_matcher(make_message_match(Operator::OPERATOR_REGEX, "foo")),
        simple_log_matcher(make_message_match(Operator::OPERATOR_REGEX, "bar")),
      ],
      ..Default::default()
    })),
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
  let config = builder::not(simple_log_matcher(make_message_match(
    Operator::OPERATOR_REGEX,
    "foo",
  )));

  match_test_runner(
    config,
    vec![
      (log_msg("foo"), false),
      (log_msg("bar"), true),
      (log_msg("foobar"), false),
    ],
  );
}

#[test]
fn test_is_set_matcher() {
  let matcher = TagMatch(base_log_matcher::TagMatch {
    tag_key: "foo".to_string(),
    value_match: Some(IsSetMatch(
      bd_proto::protos::value_matcher::value_matcher::IsSetMatch::default(),
    )),
    ..Default::default()
  });

  let config = simple_log_matcher(matcher);

  match_test_runner(
    config,
    vec![
      (log_tag("foo", "sth"), true),
      (log_tag("bar", "sth"), false),
    ],
  );
}

#[test]
fn feature_flag_matcher() {
  struct Input {
    flags: Vec<(&'static str, Option<&'static str>)>,
    matcher: LogMatcher,
    matches: bool,
  }

  for (idx, input) in [
    Input {
      flags: vec![("flag1", Some("value1"))],
      matcher: make_string_feature_flag_matcher("flag1", Operator::OPERATOR_EQUALS, "value1"),
      matches: true,
    },
    Input {
      flags: vec![("flag1", None)],
      matcher: make_string_feature_flag_matcher("flag1", Operator::OPERATOR_EQUALS, ""),
      matches: true,
    },
    Input {
      flags: vec![("flag1", Some("value1"))],
      matcher: make_string_feature_flag_matcher("flag1", Operator::OPERATOR_EQUALS, "value2"),
      matches: false,
    },
    Input {
      flags: vec![("flag2", Some("value2"))],
      matcher: make_string_feature_flag_matcher("flag2", Operator::OPERATOR_NOT_EQUALS, "value1"),
      matches: true,
    },
    Input {
      flags: vec![("flag3", None)],
      matcher: make_string_feature_flag_matcher("flag3", Operator::OPERATOR_NOT_EQUALS, "value1"),
      matches: true,
    },
    Input {
      flags: vec![("flag3", None)],
      matcher: make_feature_flag_is_set_matcher("flag3"),
      matches: true,
    },
    Input {
      flags: vec![("flag3", None)],
      matcher: make_feature_flag_is_set_matcher("flag2"),
      matches: false,
    },
  ]
  .into_iter()
  .enumerate()
  {
    let matcher = TestMatcher::new(&input.matcher).unwrap();

    let mut state = bd_state::test::TestStateReader::default();
    for (key, value) in input.flags {
      state.insert(
        bd_state::Scope::FeatureFlagExposure,
        key,
        value.unwrap_or("").to_string(),
      );
    }

    let actual =
      matcher.match_log_with_state(TypedLogLevel::Debug, LogType::NORMAL, "foo", [], &state);

    assert_eq!(
      input.matches, actual,
      "Test case {} failed: expected {}, got {}",
      idx, input.matches, actual
    );
  }
}

#[test]
fn state_match_int_values() {
  struct Input {
    state_values: Vec<(&'static str, &'static str)>,
    matcher: LogMatcher,
    matches: bool,
  }

  for (idx, input) in [
    Input {
      state_values: vec![("count", "42")],
      matcher: make_int_state_matcher("count", Operator::OPERATOR_EQUALS, 42),
      matches: true,
    },
    Input {
      state_values: vec![("count", "42")],
      matcher: make_int_state_matcher("count", Operator::OPERATOR_EQUALS, 41),
      matches: false,
    },
    Input {
      state_values: vec![("count", "42")],
      matcher: make_int_state_matcher("count", Operator::OPERATOR_GREATER_THAN, 40),
      matches: true,
    },
    Input {
      state_values: vec![("count", "42")],
      matcher: make_int_state_matcher("count", Operator::OPERATOR_LESS_THAN, 50),
      matches: true,
    },
    Input {
      state_values: vec![("count", "42")],
      matcher: make_int_state_matcher("count", Operator::OPERATOR_NOT_EQUALS, 41),
      matches: true,
    },
  ]
  .into_iter()
  .enumerate()
  {
    let matcher = TestMatcher::new(&input.matcher).unwrap();

    let mut state = bd_state::test::TestStateReader::default();
    for (key, value) in input.state_values {
      state.insert(bd_state::Scope::FeatureFlagExposure, key, value.to_string());
    }

    let actual =
      matcher.match_log_with_state(TypedLogLevel::Debug, LogType::NORMAL, "foo", [], &state);

    assert_eq!(
      input.matches, actual,
      "Test case {} failed: expected {}, got {}",
      idx, input.matches, actual
    );
  }
}

#[test]
fn state_match_double_values() {
  struct Input {
    state_values: Vec<(&'static str, &'static str)>,
    matcher: LogMatcher,
    matches: bool,
  }

  for (idx, input) in [
    Input {
      state_values: vec![("temperature", "98.6")],
      matcher: make_double_state_matcher("temperature", Operator::OPERATOR_EQUALS, 98.6),
      matches: true,
    },
    Input {
      state_values: vec![("temperature", "98.6")],
      matcher: make_double_state_matcher("temperature", Operator::OPERATOR_GREATER_THAN, 98.0),
      matches: true,
    },
    Input {
      state_values: vec![("temperature", "98.6")],
      matcher: make_double_state_matcher("temperature", Operator::OPERATOR_LESS_THAN, 99.0),
      matches: true,
    },
    Input {
      state_values: vec![("temperature", "98.6")],
      matcher: make_double_state_matcher("temperature", Operator::OPERATOR_NOT_EQUALS, 97.0),
      matches: true,
    },
  ]
  .into_iter()
  .enumerate()
  {
    let matcher = TestMatcher::new(&input.matcher).unwrap();

    let mut state = bd_state::test::TestStateReader::default();
    for (key, value) in input.state_values {
      state.insert(bd_state::Scope::FeatureFlagExposure, key, value.to_string());
    }

    let actual =
      matcher.match_log_with_state(TypedLogLevel::Debug, LogType::NORMAL, "foo", [], &state);

    assert_eq!(
      input.matches, actual,
      "Test case {} failed: expected {}, got {}",
      idx, input.matches, actual
    );
  }
}

#[test]
fn state_match_is_set() {
  struct Input {
    state_values: Vec<(&'static str, &'static str)>,
    matcher: LogMatcher,
    matches: bool,
  }

  for (idx, input) in [
    Input {
      state_values: vec![("flag1", "")],
      matcher: make_state_is_set_matcher("flag1"),
      matches: true,
    },
    Input {
      state_values: vec![("flag1", "value")],
      matcher: make_state_is_set_matcher("flag1"),
      matches: true,
    },
    Input {
      state_values: vec![],
      matcher: make_state_is_set_matcher("flag2"),
      matches: false,
    },
  ]
  .into_iter()
  .enumerate()
  {
    let matcher = TestMatcher::new(&input.matcher).unwrap();

    let mut state = bd_state::test::TestStateReader::default();
    for (key, value) in input.state_values {
      state.insert(bd_state::Scope::FeatureFlagExposure, key, value.to_string());
    }

    let actual =
      matcher.match_log_with_state(TypedLogLevel::Debug, LogType::NORMAL, "foo", [], &state);

    assert_eq!(
      input.matches, actual,
      "Test case {} failed: expected {}, got {}",
      idx, input.matches, actual
    );
  }
}

fn simple_log_matcher(match_type: base_log_matcher::Match_type) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(match_type),
      ..Default::default()
    })),
    ..Default::default()
  }
}

fn make_message_match(operator: Operator, match_value: &str) -> base_log_matcher::Match_type {
  MessageMatch(base_log_matcher::MessageMatch {
    string_value_match: MessageField::from_option(Some(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: operator.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue(match_value.to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  })
}

fn make_string_feature_flag_matcher(
  flag_name: &str,
  operator: Operator,
  match_value: &str,
) -> LogMatcher {
  simple_log_matcher(StateMatch(base_log_matcher::StateMatch {
    scope: StateScope::FEATURE_FLAG.into(),
    state_key: flag_name.to_string(),
    state_value_match: MessageField::from_option(Some(
      bd_proto::protos::state::matcher::StateValueMatch {
        value_match: Some(state_value_match::Value_match::StringValueMatch(
          bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
            operator: operator.into(),
            string_value_match_type: Some(String_value_match_type::MatchValue(
              match_value.to_string(),
            )),
            ..Default::default()
          },
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  }))
}

fn make_feature_flag_is_set_matcher(flag_name: &str) -> LogMatcher {
  simple_log_matcher(StateMatch(base_log_matcher::StateMatch {
    scope: StateScope::FEATURE_FLAG.into(),
    state_key: flag_name.to_string(),
    state_value_match: MessageField::from_option(Some(
      bd_proto::protos::state::matcher::StateValueMatch {
        value_match: Some(state_value_match::Value_match::IsSetMatch(
          bd_proto::protos::value_matcher::value_matcher::IsSetMatch::default(),
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  }))
}

fn make_int_state_matcher(state_key: &str, operator: Operator, match_value: i32) -> LogMatcher {
  simple_log_matcher(StateMatch(base_log_matcher::StateMatch {
    scope: StateScope::FEATURE_FLAG.into(),
    state_key: state_key.to_string(),
    state_value_match: MessageField::from_option(Some(
      bd_proto::protos::state::matcher::StateValueMatch {
        value_match: Some(state_value_match::Value_match::IntValueMatch(
          bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
            operator: operator.into(),
            int_value_match_type: Some(Int_value_match_type::MatchValue(match_value)),
            ..Default::default()
          },
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  }))
}

fn make_double_state_matcher(state_key: &str, operator: Operator, match_value: f64) -> LogMatcher {
  simple_log_matcher(StateMatch(base_log_matcher::StateMatch {
    scope: StateScope::FEATURE_FLAG.into(),
    state_key: state_key.to_string(),
    state_value_match: MessageField::from_option(Some(
      bd_proto::protos::state::matcher::StateValueMatch {
        value_match: Some(state_value_match::Value_match::DoubleValueMatch(
          bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
            operator: operator.into(),
            double_value_match_type: Some(Double_value_match_type::MatchValue(match_value)),
            ..Default::default()
          },
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  }))
}

fn make_state_is_set_matcher(state_key: &str) -> LogMatcher {
  simple_log_matcher(StateMatch(base_log_matcher::StateMatch {
    scope: StateScope::FEATURE_FLAG.into(),
    state_key: state_key.to_string(),
    state_value_match: MessageField::from_option(Some(
      bd_proto::protos::state::matcher::StateValueMatch {
        value_match: Some(state_value_match::Value_match::IsSetMatch(
          bd_proto::protos::value_matcher::value_matcher::IsSetMatch::default(),
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  }))
}

#[allow(clippy::needless_pass_by_value)]
fn match_test_runner(config: LogMatcher, cases: Vec<(Input<'_>, bool)>) {
  let state = bd_state::test::TestStateReader::new();
  match_test_runner_with_extractions(config, cases, &TinyMap::default(), &state);
}

#[allow(clippy::needless_pass_by_value)]
fn match_test_runner_with_extractions(
  config: LogMatcher,
  cases: Vec<(Input<'_>, bool)>,
  extracted_fields: &TinyMap<String, String>,
  state: &dyn StateReader,
) {
  let match_tree = Tree::new(&config).unwrap();

  for (input, should_match) in cases {
    let (log_type, log_level, message, fields) = input.clone();

    let fields = FieldsRef::new(&fields, &EMPTY_FIELDS);

    assert_eq!(
      should_match,
      match_tree.do_match(
        log_level,
        log_type,
        &message,
        fields,
        state,
        extracted_fields
      ),
      "{input:?} should result in {should_match} but did not",
    );
  }
}
