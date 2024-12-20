// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./matcher_test.rs"]
mod matcher_test;

use crate::version;
use anyhow::{anyhow, Result};
use base_log_matcher::tag_match::Value_match::{
  DoubleValueMatch,
  IntValueMatch,
  IsSetMatch,
  SemVerValueMatch,
  StringValueMatch,
};
use base_log_matcher::Match_type::{MessageMatch, TagMatch};
use base_log_matcher::Operator;
use bd_log_primitives::{LogLevel, LogMessage, LogType};
pub use bd_matcher::FieldProvider;
use bd_proto::protos::log_matcher::log_matcher::log_matcher::{
  base_log_matcher,
  BaseLogMatcher,
  Matcher,
};
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;
use regex::Regex;
use std::borrow::Cow;

const LOG_LEVEL_KEY: &str = "log_level";
const LOG_TYPE_KEY: &str = "log_type";

/// A compiled matching tree that supports evaluating an input log. Matching involves
/// evaluating the match criteria starting from the top, possibly recursing into subtree matching.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tree {
  // A direct matching node, specifying a match criteria. This tree matches if the base matcher
  // evaluates to true.
  Base(Leaf),

  // A composite matching node. This tree matches if any of the subtrees evaluates to true.
  Or(Vec<Tree>),

  // A composite matching node. This tree matches if all of the subtrees evaluates to true.
  And(Vec<Tree>),

  // An invert of a predicate. This tree matches if its subtree doesn't match.
  Not(Box<Tree>),
}

impl Tree {
  // Compiles a match tree from the matching config. Return an error if the config is invalid
  pub fn new(config: &LogMatcher) -> Result<Self> {
    match config
      .matcher
      .as_ref()
      .ok_or_else(|| anyhow!("missing log matcher"))?
    {
      Matcher::BaseMatcher(matcher) => Ok(Self::Base(Leaf::new(matcher)?)),
      Matcher::OrMatcher(sub_matchers) => Ok(Self::Or(
        sub_matchers
          .log_matchers
          .iter()
          .map(Self::new)
          .collect::<Result<Vec<Self>>>()?,
      )),
      Matcher::AndMatcher(sub_matchers) => Ok(Self::And(
        sub_matchers
          .log_matchers
          .iter()
          .map(Self::new)
          .collect::<Result<Vec<Self>>>()?,
      )),
      Matcher::NotMatcher(matcher) => Ok(Self::Not(Box::new(Self::new(matcher)?))),
    }
  }

  // Evaluates the match tree against a set of inputs. Return false if payload is invalid (e.g.
  // wrong type)
  #[must_use]
  pub fn do_match(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    message: &LogMessage,
    fields: &impl FieldProvider,
  ) -> bool {
    match self {
      Self::Base(base_matcher) => match base_matcher {
        Leaf::LogLevel(log_level_matcher) => log_level
          .try_into()
          .map_or(false, |log_level| log_level_matcher.evaluate(log_level)),
        Leaf::LogType(l_type) => *l_type == log_type.0,
        Leaf::IntValue(input, criteria) =>
        {
          #[allow(clippy::cast_possible_truncation)]
          input.get(message, fields).map_or(false, |input| {
            input
              .parse::<f64>()
              .map_or(false, |v| criteria.evaluate(v as i32))
          })
        },
        Leaf::DoubleValue(input, criteria) => input.get(message, fields).map_or(false, |input| {
          input.parse().map_or(false, |v| criteria.evaluate(v))
        }),
        Leaf::StringValue(input, criteria) => input
          .get(message, fields)
          .map_or(false, |input| criteria.evaluate(input.as_ref())),
        Leaf::VersionValue(input, criteria) => input
          .get(message, fields)
          .map_or(false, |input| criteria.evaluate(input.as_ref())),
        Leaf::IsSetValue(input) => input.get(message, fields).is_some(),
      },
      Self::Or(or_matchers) => or_matchers
        .iter()
        .any(|matcher| matcher.do_match(log_level, log_type, message, fields)),
      Self::And(and_matchers) => and_matchers
        .iter()
        .all(|matcher| matcher.do_match(log_level, log_type, message, fields)),
      Self::Not(matcher) => !matcher.do_match(log_level, log_type, message, fields),
    }
  }
}

/// Describes a comparison match criteria against a int32 value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntMatch {
  operator: Operator,
  value: i32,
}

/// Supports comparison between two integers
impl IntMatch {
  fn new(operator: Operator, value: i32) -> Result<Self> {
    match operator {
      // Regex operator is not valid for int32
      Operator::OPERATOR_REGEX => Err(anyhow!("regex does not support int32")),
      _ => Ok(Self { operator, value }),
    }
  }

  const fn evaluate(&self, candidate: i32) -> bool {
    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < self.value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= self.value,
      Operator::OPERATOR_GREATER_THAN => candidate > self.value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= self.value,
      Operator::OPERATOR_NOT_EQUALS => candidate != self.value,
      // Real Regex is not supported.
      Operator::OPERATOR_EQUALS | Operator::OPERATOR_REGEX => candidate == self.value,
    }
  }
}

/// A float which compares as equal when both are NaN.
#[derive(Clone, Debug, PartialOrd)]
struct NanEqualFloat(f64);

impl PartialEq for NanEqualFloat {
  fn eq(&self, other: &Self) -> bool {
    (self.0.is_nan() && other.0.is_nan()) || ((self.0 - other.0).abs() < f64::EPSILON)
  }
}

impl Eq for NanEqualFloat {}

/// Describes a comparison match criteria against a f64 value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DoubleMatch {
  operator: Operator,
  value: NanEqualFloat,
}

/// Supports comparison between two integers
impl DoubleMatch {
  fn new(operator: Operator, value: f64) -> Result<Self> {
    match operator {
      // Regex operator is not valid for f64
      Operator::OPERATOR_REGEX => Err(anyhow!("regex does not support f64")),
      _ => Ok(Self {
        operator,
        value: NanEqualFloat(value),
      }),
    }
  }

  fn evaluate(&self, candidate: f64) -> bool {
    let candidate = NanEqualFloat(candidate);
    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < self.value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= self.value,
      Operator::OPERATOR_GREATER_THAN => candidate > self.value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= self.value,
      Operator::OPERATOR_NOT_EQUALS => candidate != self.value,
      // Real Regex is not supported.
      Operator::OPERATOR_EQUALS | Operator::OPERATOR_REGEX => candidate == self.value,
    }
  }
}

/// Describes a comparison match criteria against a String value
#[derive(Clone, Debug)]
pub struct StringMatch {
  operator: Operator,
  value: String,
  regex: Option<Regex>,
}

// `Regex` doesn't implement `PartialEq`.
impl std::cmp::PartialEq for StringMatch {
  fn eq(&self, other: &Self) -> bool {
    self.operator == other.operator
    && self.value == other.value
    // regex's `as_str` returns the underlying regex string.
    && self.regex.as_ref().map_or("", |r| r.as_str()) == other.regex.as_ref().map_or("", |r| r.as_str())
  }
}

impl std::cmp::Eq for StringMatch {}

/// Supports comparison between two Strings
impl StringMatch {
  fn new(operator: Operator, value: &str) -> Result<Self> {
    if operator == Operator::OPERATOR_UNSPECIFIED {
      return Err(anyhow!("UNSPECIFIED operator"));
    }

    // Compile the regex on tree creation to avoid recompiling it on every match.
    let regex = match operator {
      Operator::OPERATOR_REGEX => Some(match Regex::new(value) {
        Ok(regex) => regex,
        Err(e) => return Err(anyhow::Error::new(e).context("invalid regex")),
      }),
      _ => None,
    };
    Ok(Self {
      operator,
      value: value.to_string(),
      regex,
    })
  }

  fn evaluate(&self, candidate: &str) -> bool {
    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < self.value.as_str(),
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= self.value.as_str(),
      Operator::OPERATOR_EQUALS => candidate == self.value.as_str(),
      Operator::OPERATOR_GREATER_THAN => candidate > self.value.as_str(),
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= self.value.as_str(),
      Operator::OPERATOR_NOT_EQUALS => candidate != self.value.as_str(),
      Operator::OPERATOR_REGEX => self.regex.as_ref().unwrap().is_match(candidate),
    }
  }
}

/// Represents either the log message or the field key-value to match against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputType {
  Message,
  Field(String),
}

impl InputType {
  fn get<'a>(
    &self,
    message: &'a LogMessage,
    fields: &'a impl FieldProvider,
  ) -> Option<Cow<'a, str>> {
    match self {
      Self::Message => message.as_str().map(Cow::Borrowed),
      Self::Field(field_key) => fields.field_value(field_key),
    }
  }
}

/// Describes a compiled leaf node in the match tree. Each tree node evaluates to either
/// true or false based on its match criteria.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Leaf {
  /// Match against the log level.
  LogLevel(IntMatch),

  /// Match against either the tag or the message using an Int matcher.
  IntValue(InputType, IntMatch),

  /// Match against either the tag or the message using a Double matcher.
  DoubleValue(InputType, DoubleMatch),

  /// Match against either the tag or the message using a String matcher.
  StringValue(InputType, StringMatch),

  /// Match against either the tag or the message using a Version matcher.
  VersionValue(InputType, version::VersionMatch),

  /// Match against a specific log type.
  LogType(u32),

  /// Whether a given tag is set or not.
  IsSetValue(InputType),
}

impl Leaf {
  fn new(log_matcher: &BaseLogMatcher) -> Result<Self> {
    match log_matcher
      .match_type
      .as_ref()
      .ok_or_else(|| anyhow!("missing log_matcher"))?
    {
      MessageMatch(message_match) => Ok(Self::StringValue(
        InputType::Message,
        StringMatch::new(
          message_match
            .string_value_match
            .operator
            .enum_value()
            .map_err(|_| anyhow!("unknown field or enum"))?,
          message_match.string_value_match.match_value.as_str(),
        )?,
      )),
      TagMatch(tag_match) => match tag_match
        .value_match
        .as_ref()
        .ok_or_else(|| anyhow!("missing tag_match value_match"))?
      {
        IntValueMatch(int_value_match) => match tag_match.tag_key.as_str() {
          // Special case for key="log_level"
          // We're special casing log level because we need to look for this tag outside of the
          // regular fields map It should be a bd_matcher::log_level enum value, so
          // using an IntMatch should work
          LOG_LEVEL_KEY => Ok(Self::LogLevel(IntMatch::new(
            int_value_match
              .operator
              .enum_value()
              .map_err(|_| anyhow!("unknown field or enum"))?,
            int_value_match.match_value,
          )?)),
          // Special case for key="log_type"
          // We're special casing log type because we need to look for this tag outside of the
          // regular fields map It should be a bd_matcher::LogType u32 value, so we
          // try to convert it from i32
          LOG_TYPE_KEY => Ok(Self::LogType(int_value_match.match_value.try_into()?)),
          // Any other int uses the IntValue match
          _ => Ok(Self::IntValue(
            InputType::Field(tag_match.tag_key.clone()),
            IntMatch::new(
              int_value_match
                .operator
                .enum_value()
                .map_err(|_| anyhow!("unknown field or enum"))?,
              int_value_match.match_value,
            )?,
          )),
        },
        DoubleValueMatch(double_value_match) => Ok(Self::DoubleValue(
          InputType::Field(tag_match.tag_key.clone()),
          DoubleMatch::new(
            double_value_match
              .operator
              .enum_value()
              .map_err(|_| anyhow!("unknown field or enum"))?,
            double_value_match.match_value,
          )?,
        )),
        StringValueMatch(string_value_match) => Ok(Self::StringValue(
          InputType::Field(tag_match.tag_key.clone()),
          StringMatch::new(
            string_value_match
              .operator
              .enum_value()
              .map_err(|_| anyhow!("unknown field or enum"))?,
            string_value_match.match_value.as_str(),
          )?,
        )),
        SemVerValueMatch(sem_ver_value_match) => Ok(Self::VersionValue(
          InputType::Field(tag_match.tag_key.clone()),
          version::VersionMatch::new(
            sem_ver_value_match
              .operator
              .enum_value()
              .map_err(|_| anyhow!("unknown field or enum"))?,
            sem_ver_value_match.match_value.as_str(),
          )?,
        )),
        IsSetMatch(_) => Ok(Self::IsSetValue(InputType::Field(
          tag_match.tag_key.clone(),
        ))),
      },
    }
  }
}
