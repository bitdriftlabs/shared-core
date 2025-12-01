// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Log matching utilities for building `LogMatcher` instances.
//!
//! This module provides a clean, namespaced API for creating log matchers.
//! All functions return `LogMatcher` instances that can be used in workflows,
//! filters, and other log processing configurations.
//!
//! # Example
//! ```ignore
//! use bd_log_matcher::builder;
//!
//! let matcher = builder::and(vec![
//!   builder::message_equals("AppStarted"),
//!   builder::field_equals("os", "iOS"),
//! ]);
//! ```

use base_log_matcher::tag_match;
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;
use bd_proto::protos::log_matcher::log_matcher::log_matcher::{
  BaseLogMatcher,
  Matcher,
  MatcherList,
  base_log_matcher,
};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::value_matcher::value_matcher;
use tag_match::Value_match;
use tag_match::Value_match::IntValueMatch;
use value_matcher::int_value_match::Int_value_match_type;
use value_matcher::string_value_match::String_value_match_type;
use value_matcher::{IsSetMatch, Operator};

/// Creates a matcher that matches if all of the provided matchers match (logical AND).
///
/// # Example
/// ```ignore
/// let matcher = builder::and(vec![
///   builder::message_equals("foo"),
///   builder::field_equals("key", "value"),
/// ]);
/// ```
#[must_use]
pub fn and(matchers: Vec<LogMatcher>) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::AndMatcher(MatcherList {
      log_matchers: matchers,
      ..Default::default()
    })),
    ..Default::default()
  }
}

/// Creates a matcher that matches if any of the provided matchers match (logical OR).
///
/// # Example
/// ```ignore
/// let matcher = builder::or(vec![
///   builder::message_equals("foo"),
///   builder::field_equals("key", "value"),
/// ]);
/// ```
#[must_use]
pub fn or(matchers: Vec<LogMatcher>) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::OrMatcher(MatcherList {
      log_matchers: matchers,
      ..Default::default()
    })),
    ..Default::default()
  }
}

/// Creates a matcher that inverts the provided matcher (logical NOT).
///
/// # Example
/// ```ignore
/// let matcher = builder::not(builder::message_equals("foo"));
/// ```
#[must_use]
pub fn not(matcher: LogMatcher) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::NotMatcher(Box::new(matcher))),
    ..Default::default()
  }
}

/// Creates a matcher for logs where message equals the specified value.
///
/// # Example
/// ```ignore
/// let matcher = builder::message_equals("AppFinishedLaunching");
/// ```
#[inline]
#[must_use]
pub fn message_equals(msg: &str) -> LogMatcher {
  make_log_message_matcher(msg, Operator::OPERATOR_EQUALS)
}

/// Creates a matcher for logs where message matches the specified regex pattern.
///
/// # Example
/// ```ignore
/// let matcher = builder::message_regex_matches("^ERROR.*");
/// ```
#[inline]
#[must_use]
pub fn message_regex_matches(pattern: &str) -> LogMatcher {
  make_log_message_matcher(pattern, Operator::OPERATOR_REGEX)
}

/// Creates a matcher for logs where a field value equals the specified value.
///
/// # Example
/// ```ignore
/// let matcher = builder::field_equals("app_id", "beta");
/// ```
#[inline]
#[must_use]
pub fn field_equals(key: &str, value: &str) -> LogMatcher {
  make_log_tag_matcher(key, value)
}

/// Creates a matcher for logs where tag value does not equal the specified value.
///
/// # Example
/// ```ignore
/// let matcher = builder::field_not_equals("status", "error");
/// ```
#[inline]
#[must_use]
pub fn field_not_equals(key: &str, value: &str) -> LogMatcher {
  log_field_matcher(key, value, Operator::OPERATOR_NOT_EQUALS)
}

/// Creates a matcher for logs where field value matches the specified regex pattern.
///
/// # Example
/// ```ignore
/// let matcher = builder::field_regex_matches("log", "^ERROR.*");
/// ```
#[inline]
#[must_use]
pub fn field_regex_matches(key: &str, pattern: &str) -> LogMatcher {
  log_field_matcher(key, pattern, Operator::OPERATOR_REGEX)
}

/// Creates a matcher for logs where a field contains a double value equal to the specified value.
///
/// # Example
/// ```ignore
/// let matcher = builder::field_double_equals("temperature", 98.6);
/// ```
#[inline]
#[must_use]
pub fn field_double_equals(key: &str, value: f64) -> LogMatcher {
  log_field_double_matcher(key, value, Operator::OPERATOR_EQUALS)
}

/// Creates a matcher that matches when a field is set.
///
/// # Example
/// ```ignore
/// let matcher = builder::field_is_set("user_id");
/// ```
#[must_use]
pub fn field_is_set(field: &str) -> LogMatcher {
  use base_log_matcher::Match_type::TagMatch;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: field.to_string(),
        value_match: Some(Value_match::IsSetMatch(IsSetMatch::default())),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

/// Creates a matcher for logs where `log_level` equals the specified value.
///
/// # Example
/// ```ignore
/// let matcher = builder::log_level_equals(2);
/// ```
#[inline]
#[must_use]
pub fn log_level_equals(level: i32) -> LogMatcher {
  use base_log_matcher::Match_type::TagMatch;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: "log_level".to_string(),
        value_match: Some(IntValueMatch(value_matcher::IntValueMatch {
          operator: Operator::OPERATOR_EQUALS.into(),
          int_value_match_type: Some(Int_value_match_type::MatchValue(level)),
          ..Default::default()
        })),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

/// Creates a matcher for logs where `log_type` equals the specified value.
///
/// # Example
/// ```ignore
/// let matcher = builder::log_type_equals(LogType::Lifecycle);
/// ```
#[must_use]
#[allow(clippy::cast_possible_wrap)] // LogType values are small and safe to cast
pub fn log_type_equals(log_type: LogType) -> LogMatcher {
  use base_log_matcher::Match_type::TagMatch;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: "log_type".to_string(),
        value_match: Some(IntValueMatch(value_matcher::IntValueMatch {
          operator: Operator::OPERATOR_EQUALS.into(),
          int_value_match_type: Some(Int_value_match_type::MatchValue(log_type as i32)),
          ..Default::default()
        })),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

/// Returns a log matcher matching iOS logs (logs with OS tag equal to "iOS").
///
/// # Example
/// ```ignore
/// let matcher = builder::ios();
/// ```
#[inline]
#[must_use]
pub fn ios() -> LogMatcher {
  log_field_matcher("os", "iOS", Operator::OPERATOR_EQUALS)
}

/// Returns a log matcher matching Android logs (logs with OS tag equal to "Android").
///
/// # Example
/// ```ignore
/// let matcher = builder::android();
/// ```
#[inline]
#[must_use]
pub fn android() -> LogMatcher {
  log_field_matcher("os", "Android", Operator::OPERATOR_EQUALS)
}

// Helper functions

/// Creates a log field matcher that matches when a field is equal to the provided string value.
#[must_use]
fn log_field_matcher(field: &str, value: &str, operator: Operator) -> LogMatcher {
  use base_log_matcher::Match_type::TagMatch;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: field.to_string(),
        value_match: Some(Value_match::StringValueMatch(
          value_matcher::StringValueMatch {
            operator: operator.into(),
            string_value_match_type: Some(String_value_match_type::MatchValue(value.to_string())),
            ..Default::default()
          },
        )),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

/// Creates a log field matcher that matches when a field is equal to the provided double value.
#[must_use]
fn log_field_double_matcher(key: &str, value: f64, operator: Operator) -> LogMatcher {
  use base_log_matcher::Match_type::TagMatch;
  use value_matcher::DoubleValueMatch;
  use value_matcher::double_value_match::Double_value_match_type;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: key.to_string(),
        value_match: Some(Value_match::DoubleValueMatch(DoubleValueMatch {
          operator: operator.into(),
          double_value_match_type: Some(Double_value_match_type::MatchValue(value)),
          ..Default::default()
        })),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
fn make_log_message_matcher(value: &str, operator: Operator) -> LogMatcher {
  use base_log_matcher::Match_type::MessageMatch;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(MessageMatch(base_log_matcher::MessageMatch {
        string_value_match: protobuf::MessageField::from_option(Some(
          value_matcher::StringValueMatch {
            operator: operator.into(),
            string_value_match_type: Some(String_value_match_type::MatchValue(value.to_string())),
            ..Default::default()
          },
        )),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
fn make_log_tag_matcher(name: &str, value: &str) -> LogMatcher {
  use base_log_matcher::Match_type::TagMatch;

  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: name.to_string(),
        value_match: Some(Value_match::StringValueMatch(
          value_matcher::StringValueMatch {
            operator: Operator::OPERATOR_EQUALS.into(),
            string_value_match_type: Some(String_value_match_type::MatchValue(value.to_string())),
            ..Default::default()
          },
        )),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}
