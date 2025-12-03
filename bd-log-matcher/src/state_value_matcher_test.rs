// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::StateValueMatcher;
use bd_log_primitives::tiny_set::TinyMap;
use bd_proto::protos::state::matcher::state_value_match::Value_match;
use bd_proto::protos::state::matcher::StateValueMatch;
use bd_proto::protos::value_matcher::value_matcher::double_value_match::Double_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::int_value_match::Int_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::string_value_match::String_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::Operator;

#[test]
fn string_match_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue("test".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("test"), &empty_fields));
  assert!(!matcher.matches(Some("other"), &empty_fields));
  assert!(!matcher.matches(Some("TEST"), &empty_fields)); // case sensitive
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn string_match_not_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_NOT_EQUALS.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue("test".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(!matcher.matches(Some("test"), &empty_fields));
  assert!(matcher.matches(Some("other"), &empty_fields));
  assert!(matcher.matches(Some("TEST"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields)); // None doesn't match
}

#[test]
fn string_match_regex() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_REGEX.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue("^test.*$".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("test"), &empty_fields));
  assert!(matcher.matches(Some("test123"), &empty_fields));
  assert!(matcher.matches(Some("testing"), &empty_fields));
  assert!(!matcher.matches(Some("other"), &empty_fields));
  assert!(!matcher.matches(Some("atest"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn string_match_greater_than() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_GREATER_THAN.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue("40".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  // String comparison is lexicographic
  assert!(matcher.matches(Some("45"), &empty_fields));
  assert!(matcher.matches(Some("5"), &empty_fields)); // "5" > "40" lexicographically
  assert!(!matcher.matches(Some("40"), &empty_fields));
  assert!(!matcher.matches(Some("39"), &empty_fields));
  assert!(!matcher.matches(Some("100"), &empty_fields)); // "100" < "40" lexicographically
}

#[test]
fn int_match_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(42)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("42"), &empty_fields));
  assert!(!matcher.matches(Some("43"), &empty_fields));
  assert!(!matcher.matches(Some("41"), &empty_fields));
  assert!(!matcher.matches(Some("not_a_number"), &empty_fields));
  assert!(!matcher.matches(Some("42.5"), &empty_fields)); // Not an integer
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn int_match_greater_than() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_GREATER_THAN.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(10)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("11"), &empty_fields));
  assert!(matcher.matches(Some("100"), &empty_fields));
  assert!(matcher.matches(Some("999"), &empty_fields));
  assert!(!matcher.matches(Some("10"), &empty_fields));
  assert!(!matcher.matches(Some("9"), &empty_fields));
  assert!(!matcher.matches(Some("0"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn int_match_less_than_or_equal() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_LESS_THAN_OR_EQUAL.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(10)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("10"), &empty_fields));
  assert!(matcher.matches(Some("9"), &empty_fields));
  assert!(matcher.matches(Some("0"), &empty_fields));
  assert!(matcher.matches(Some("-5"), &empty_fields));
  assert!(!matcher.matches(Some("11"), &empty_fields));
  assert!(!matcher.matches(Some("100"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn int_match_with_negative_values() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_LESS_THAN.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(0)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("-1"), &empty_fields));
  assert!(matcher.matches(Some("-100"), &empty_fields));
  assert!(matcher.matches(Some("-999"), &empty_fields));
  assert!(!matcher.matches(Some("0"), &empty_fields));
  assert!(!matcher.matches(Some("1"), &empty_fields));
}

#[test]
fn double_match_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(
      bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        double_value_match_type: Some(Double_value_match_type::MatchValue(3.14)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("3.14"), &empty_fields));
  assert!(!matcher.matches(Some("3.15"), &empty_fields));
  assert!(!matcher.matches(Some("3.13"), &empty_fields));
  assert!(!matcher.matches(Some("not_a_number"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn double_match_greater_than() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(
      bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
        operator: Operator::OPERATOR_GREATER_THAN.into(),
        double_value_match_type: Some(Double_value_match_type::MatchValue(1.5)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("2.0"), &empty_fields));
  assert!(matcher.matches(Some("1.51"), &empty_fields));
  assert!(matcher.matches(Some("100.0"), &empty_fields));
  assert!(!matcher.matches(Some("1.5"), &empty_fields));
  assert!(!matcher.matches(Some("1.0"), &empty_fields));
  assert!(!matcher.matches(Some("0.0"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn double_match_less_than_or_equal() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(
      bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
        operator: Operator::OPERATOR_LESS_THAN_OR_EQUAL.into(),
        double_value_match_type: Some(Double_value_match_type::MatchValue(5.0)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("5.0"), &empty_fields));
  assert!(matcher.matches(Some("4.9"), &empty_fields));
  assert!(matcher.matches(Some("0.0"), &empty_fields));
  assert!(matcher.matches(Some("-10.5"), &empty_fields));
  assert!(!matcher.matches(Some("5.1"), &empty_fields));
  assert!(!matcher.matches(Some("10.0"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn double_match_with_negative_values() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(
      bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
        operator: Operator::OPERATOR_GREATER_THAN_OR_EQUAL.into(),
        double_value_match_type: Some(Double_value_match_type::MatchValue(-5.0)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  assert!(matcher.matches(Some("-5.0"), &empty_fields));
  assert!(matcher.matches(Some("-4.9"), &empty_fields));
  assert!(matcher.matches(Some("0.0"), &empty_fields));
  assert!(matcher.matches(Some("10.5"), &empty_fields));
  assert!(!matcher.matches(Some("-5.1"), &empty_fields));
  assert!(!matcher.matches(Some("-10.0"), &empty_fields));
}

#[test]
fn is_set_matcher() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IsSetMatch(
      bd_proto::protos::value_matcher::value_matcher::IsSetMatch::default(),
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  // IsSet only checks if value is Some, regardless of content
  assert!(matcher.matches(Some("any_value"), &empty_fields));
  assert!(matcher.matches(Some(""), &empty_fields));
  assert!(matcher.matches(Some("123"), &empty_fields));
  assert!(matcher.matches(Some("true"), &empty_fields));
  assert!(matcher.matches(Some("false"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn missing_value_match() {
  let proto = StateValueMatch {
    value_match: None,
    ..Default::default()
  };

  let result = StateValueMatcher::try_from_proto(&proto);
  assert!(result.is_err());
  assert!(result
    .unwrap_err()
    .to_string()
    .contains("missing value_match"));
}

#[test]
fn invalid_regex() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_REGEX.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue("[invalid".to_string())),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let result = StateValueMatcher::try_from_proto(&proto);
  assert!(result.is_err());
}

#[test]
fn string_match_with_extracted_field() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(
      bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        string_value_match_type: Some(String_value_match_type::MatchValue(
          "12345".to_string(),
        )),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let mut extracted_fields = TinyMap::default();
  extracted_fields.insert("user_id".to_string(), "12345".to_string());

  // The matcher just does literal matching, it doesn't use extracted fields
  assert!(matcher.matches(Some("12345"), &extracted_fields));
  assert!(!matcher.matches(Some("67890"), &extracted_fields));
}

#[test]
fn int_parse_failure() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(
      bd_proto::protos::value_matcher::value_matcher::IntValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        int_value_match_type: Some(Int_value_match_type::MatchValue(42)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  // These should all fail to parse and return false
  assert!(!matcher.matches(Some("not_a_number"), &empty_fields));
  assert!(!matcher.matches(Some("42.5"), &empty_fields));
  assert!(!matcher.matches(Some(""), &empty_fields));
  assert!(!matcher.matches(Some("4294967296"), &empty_fields)); // Overflow i32
}

#[test]
fn double_parse_failure() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(
      bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch {
        operator: Operator::OPERATOR_EQUALS.into(),
        double_value_match_type: Some(Double_value_match_type::MatchValue(3.14)),
        ..Default::default()
      },
    )),
    ..Default::default()
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::default();

  // These should all fail to parse and return false
  assert!(!matcher.matches(Some("not_a_number"), &empty_fields));
  assert!(!matcher.matches(Some(""), &empty_fields));
  assert!(!matcher.matches(Some("NaN"), &empty_fields));
}
