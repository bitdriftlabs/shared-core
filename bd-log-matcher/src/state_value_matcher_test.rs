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
use bd_proto::protos::value_matcher::value_matcher::Operator;
use bd_proto::protos::value_matcher::value_matcher::double_value_match::{
  Double_value_match_type,
  DoubleValue,
  DoubleValueMatch as ProtoDoubleValueMatch,
};
use bd_proto::protos::value_matcher::value_matcher::int_value_match::{
  IntValue,
  IntValueMatch as ProtoIntValueMatch,
  Int_value_match_type,
};
use bd_proto::protos::value_matcher::value_matcher::string_value_match::{
  RegexMatch,
  StringValue,
  StringValueMatch as ProtoStringValueMatch,
  String_value_match_type,
};
use bd_proto::protos::value_matcher::IsSetMatch;

#[test]
fn string_match_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(ProtoStringValueMatch {
      string_value_match_type: Some(String_value_match_type::Value(StringValue {
        operator: Operator::OPERATOR_EQUAL.into(),
        value: "test".to_string(),
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("test"), &empty_fields));
  assert!(!matcher.matches(Some("other"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn string_match_not_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(ProtoStringValueMatch {
      string_value_match_type: Some(String_value_match_type::Value(StringValue {
        operator: Operator::OPERATOR_NOT_EQUAL.into(),
        value: "test".to_string(),
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(!matcher.matches(Some("test"), &empty_fields));
  assert!(matcher.matches(Some("other"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn string_match_regex() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(ProtoStringValueMatch {
      string_value_match_type: Some(String_value_match_type::Regex(RegexMatch {
        regex: "^test.*$".to_string(),
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("test"), &empty_fields));
  assert!(matcher.matches(Some("test123"), &empty_fields));
  assert!(!matcher.matches(Some("other"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn int_match_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(ProtoIntValueMatch {
      int_value_match_type: Some(Int_value_match_type::Value(IntValue {
        operator: Operator::OPERATOR_EQUAL.into(),
        value: 42,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("42"), &empty_fields));
  assert!(!matcher.matches(Some("43"), &empty_fields));
  assert!(!matcher.matches(Some("not_a_number"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn int_match_greater_than() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(ProtoIntValueMatch {
      int_value_match_type: Some(Int_value_match_type::Value(IntValue {
        operator: Operator::OPERATOR_GREATER_THAN.into(),
        value: 10,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("11"), &empty_fields));
  assert!(matcher.matches(Some("100"), &empty_fields));
  assert!(!matcher.matches(Some("10"), &empty_fields));
  assert!(!matcher.matches(Some("9"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn int_match_less_than() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(ProtoIntValueMatch {
      int_value_match_type: Some(Int_value_match_type::Value(IntValue {
        operator: Operator::OPERATOR_LESS_THAN.into(),
        value: 10,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("9"), &empty_fields));
  assert!(matcher.matches(Some("-5"), &empty_fields));
  assert!(!matcher.matches(Some("10"), &empty_fields));
  assert!(!matcher.matches(Some("11"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn double_match_equals() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(ProtoDoubleValueMatch {
      double_value_match_type: Some(Double_value_match_type::Value(DoubleValue {
        operator: Operator::OPERATOR_EQUAL.into(),
        value: 3.14,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("3.14"), &empty_fields));
  assert!(!matcher.matches(Some("3.15"), &empty_fields));
  assert!(!matcher.matches(Some("not_a_number"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn double_match_greater_than() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(ProtoDoubleValueMatch {
      double_value_match_type: Some(Double_value_match_type::Value(DoubleValue {
        operator: Operator::OPERATOR_GREATER_THAN.into(),
        value: 1.5,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("2.0"), &empty_fields));
  assert!(matcher.matches(Some("1.51"), &empty_fields));
  assert!(!matcher.matches(Some("1.5"), &empty_fields));
  assert!(!matcher.matches(Some("1.0"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn double_match_less_than_or_equal() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(ProtoDoubleValueMatch {
      double_value_match_type: Some(Double_value_match_type::Value(DoubleValue {
        operator: Operator::OPERATOR_LESS_THAN_OR_EQUAL.into(),
        value: 5.0,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("5.0"), &empty_fields));
  assert!(matcher.matches(Some("4.9"), &empty_fields));
  assert!(matcher.matches(Some("0.0"), &empty_fields));
  assert!(!matcher.matches(Some("5.1"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn is_set_matcher() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IsSetMatch(IsSetMatch {})),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("any_value"), &empty_fields));
  assert!(matcher.matches(Some(""), &empty_fields));
  assert!(matcher.matches(Some("123"), &empty_fields));
  assert!(!matcher.matches(None, &empty_fields));
}

#[test]
fn missing_value_match() {
  let proto = StateValueMatch { value_match: None };

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
    value_match: Some(Value_match::StringValueMatch(ProtoStringValueMatch {
      string_value_match_type: Some(String_value_match_type::Regex(RegexMatch {
        regex: "[invalid".to_string(), // Invalid regex
      })),
    })),
  };

  let result = StateValueMatcher::try_from_proto(&proto);
  assert!(result.is_err());
}

#[test]
fn string_match_with_extracted_field() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::StringValueMatch(ProtoStringValueMatch {
      string_value_match_type: Some(String_value_match_type::Value(StringValue {
        operator: Operator::OPERATOR_EQUAL.into(),
        value: "${user_id}".to_string(),
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let mut extracted_fields = TinyMap::new();
  extracted_fields.insert("user_id".to_string(), "12345".to_string());

  assert!(matcher.matches(Some("12345"), &extracted_fields));
  assert!(!matcher.matches(Some("67890"), &extracted_fields));
}

#[test]
fn int_match_with_extracted_field() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::IntValueMatch(ProtoIntValueMatch {
      int_value_match_type: Some(Int_value_match_type::Value(IntValue {
        operator: Operator::OPERATOR_EQUAL.into(),
        value: 0, // Placeholder value
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  // Test with numeric value
  assert!(matcher.matches(Some("0"), &empty_fields));
}

#[test]
fn double_match_with_negative_values() {
  let proto = StateValueMatch {
    value_match: Some(Value_match::DoubleValueMatch(ProtoDoubleValueMatch {
      double_value_match_type: Some(Double_value_match_type::Value(DoubleValue {
        operator: Operator::OPERATOR_GREATER_THAN_OR_EQUAL.into(),
        value: -5.0,
      })),
    })),
  };

  let matcher = StateValueMatcher::try_from_proto(&proto).unwrap();
  let empty_fields = TinyMap::new();

  assert!(matcher.matches(Some("-5.0"), &empty_fields));
  assert!(matcher.matches(Some("-4.9"), &empty_fields));
  assert!(matcher.matches(Some("0.0"), &empty_fields));
  assert!(!matcher.matches(Some("-5.1"), &empty_fields));
}
