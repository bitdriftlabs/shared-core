// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./matcher_test.rs"]
mod matcher_test;

#[cfg(test)]
#[path = "./legacy_matcher_test.rs"]
mod legacy_matcher_test;

use crate::version;
use anyhow::{Result, anyhow};
use base_log_matcher::Match_type::{FeatureFlagMatch, MessageMatch, TagMatch};
use base_log_matcher::Operator;
use base_log_matcher::tag_match::Value_match::{
  DoubleValueMatch,
  IntValueMatch,
  IsSetMatch,
  SemVerValueMatch,
  StringValueMatch,
};
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{FieldsRef, LogLevel, LogMessage};
use bd_proto::protos::config::v1::config::log_matcher::base_log_matcher::StringMatchType;
use bd_proto::protos::config::v1::config::log_matcher::{
  BaseLogMatcher as LegacyBaseLogMatcher,
  base_log_matcher as legacy_base_log_matcher,
};
use bd_proto::protos::config::v1::config::{
  LogMatcher as LegacyLogMatcher,
  log_matcher as legacy_log_matcher,
};
use bd_proto::protos::log_matcher::log_matcher;
use bd_proto::protos::logging::payload::LogType;
use log_matcher::LogMatcher;
use log_matcher::log_matcher::base_log_matcher::double_value_match::Double_value_match_type;
use log_matcher::log_matcher::base_log_matcher::int_value_match::Int_value_match_type;
use log_matcher::log_matcher::base_log_matcher::string_value_match::String_value_match_type;
use log_matcher::log_matcher::base_log_matcher::{
  IntValueMatch as IntValueMatch_type,
  StringValueMatch as StringValueMatch_type,
};
use log_matcher::log_matcher::{BaseLogMatcher, Matcher, base_log_matcher};
use regex::Regex;
use std::borrow::Cow;
use std::ops::Deref;

const LOG_LEVEL_KEY: &str = "log_level";
const LOG_TYPE_KEY: &str = "log_type";

//
// ValueOrSavedFieldId
//

// This entire dance is so that we can either store a value T, or an ID to a saved field, and then
// resolve it to T later, in either value or reference form, with minimal copying.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ValueOrSavedFieldId<T> {
  Value(T),
  SaveFieldId(String),
}
enum ValueOrRef<'a, T> {
  Value(T),
  Ref(&'a T),
}
trait MakeValueOrRef<'a, T> {
  #[allow(clippy::ptr_arg)]
  fn make_value_or_ref(value: &'a String) -> Option<ValueOrRef<'a, T>>;
}

impl<T> From<T> for ValueOrSavedFieldId<T> {
  fn from(value: T) -> Self {
    Self::Value(value)
  }
}

impl<'a, T: MakeValueOrRef<'a, T>> ValueOrSavedFieldId<T> {
  fn load(&'a self, extracted_fields: &'a TinyMap<String, String>) -> Option<ValueOrRef<'a, T>> {
    match self {
      Self::Value(v) => Some(ValueOrRef::Ref(v)),
      Self::SaveFieldId(field_id) => extracted_fields
        .get(field_id)
        .and_then(|v| T::make_value_or_ref(v)),
    }
  }
}

impl<T> Deref for ValueOrRef<'_, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    match self {
      ValueOrRef::Value(v) => v,
      ValueOrRef::Ref(v) => v,
    }
  }
}

impl<'a> MakeValueOrRef<'a, Self> for i32 {
  fn make_value_or_ref(value: &String) -> Option<ValueOrRef<'a, Self>> {
    value.parse::<Self>().ok().map(ValueOrRef::Value)
  }
}

impl<'a> MakeValueOrRef<'a, Self> for NanEqualFloat {
  fn make_value_or_ref(value: &String) -> Option<ValueOrRef<'a, Self>> {
    value
      .parse::<f64>()
      .ok()
      .map(|v| ValueOrRef::Value(Self(v)))
  }
}

impl<'a> MakeValueOrRef<'a, Self> for String {
  fn make_value_or_ref(value: &'a Self) -> Option<ValueOrRef<'a, Self>> {
    Some(ValueOrRef::Ref(value))
  }
}

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
  pub fn new_legacy(config: &LegacyLogMatcher) -> Result<Self> {
    match config
      .match_type
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("missing legacy match type"))?
    {
      legacy_log_matcher::Match_type::BaseMatcher(matcher) => {
        Ok(Self::Base(Leaf::new_legacy(matcher)?))
      },
      legacy_log_matcher::Match_type::OrMatcher(sub_matchers) => Ok(Self::Or(
        sub_matchers
          .matcher
          .iter()
          .map(Self::new_legacy)
          .collect::<Result<Vec<Self>>>()?,
      )),
      legacy_log_matcher::Match_type::AndMatcher(sub_matchers) => Ok(Self::And(
        sub_matchers
          .matcher
          .iter()
          .map(Self::new_legacy)
          .collect::<Result<Vec<Self>>>()?,
      )),
      legacy_log_matcher::Match_type::NotMatcher(matcher) => {
        Ok(Self::Not(Box::new(Self::new_legacy(matcher)?)))
      },
    }
  }

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
    fields: FieldsRef<'_>,
    state: &dyn bd_state::StateReader,
    extracted_fields: &TinyMap<String, String>,
  ) -> bool {
    match self {
      Self::Base(base_matcher) => match base_matcher {
        Leaf::LogLevel(log_level_matcher) => log_level
          .try_into()
          .is_ok_and(|log_level| log_level_matcher.evaluate(log_level, extracted_fields)),
        Leaf::LogType(l_type) => *l_type == log_type as u32,
        Leaf::IntValue(input, criteria) =>
        {
          #[allow(clippy::cast_possible_truncation)]
          input.get(message, fields, state).is_some_and(|input| {
            input
              .parse::<f64>()
              .is_ok_and(|v| criteria.evaluate(v as i32, extracted_fields))
          })
        },
        Leaf::DoubleValue(input, criteria) => {
          input.get(message, fields, state).is_some_and(|input| {
            input
              .parse()
              .is_ok_and(|v| criteria.evaluate(v, extracted_fields))
          })
        },
        Leaf::StringValue(input, criteria) => input
          .get(message, fields, state)
          .is_some_and(|input| criteria.evaluate(input.as_ref(), extracted_fields)),
        Leaf::VersionValue(input, criteria) => input
          .get(message, fields, state)
          .is_some_and(|input| criteria.evaluate(input.as_ref())),
        Leaf::IsSetValue(input) => input.get(message, fields, state).is_some(),
        Leaf::Any => true,
      },
      Self::Or(or_matchers) => or_matchers.iter().any(|matcher| {
        matcher.do_match(
          log_level,
          log_type,
          message,
          fields,
          state,
          extracted_fields,
        )
      }),
      Self::And(and_matchers) => and_matchers.iter().all(|matcher| {
        matcher.do_match(
          log_level,
          log_type,
          message,
          fields,
          state,
          extracted_fields,
        )
      }),
      Self::Not(matcher) => !matcher.do_match(
        log_level,
        log_type,
        message,
        fields,
        state,
        extracted_fields,
      ),
    }
  }
}

/// Describes a comparison match criteria against a int32 value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntMatch {
  operator: Operator,
  value: ValueOrSavedFieldId<i32>,
}

/// Supports comparison between two integers
impl IntMatch {
  fn new(operator: Operator, value: ValueOrSavedFieldId<i32>) -> Result<Self> {
    match operator {
      // Regex operator is not valid for int32
      Operator::OPERATOR_REGEX => Err(anyhow!("regex does not support int32")),
      _ => Ok(Self { operator, value }),
    }
  }

  fn evaluate(&self, candidate: i32, extracted_fields: &TinyMap<String, String>) -> bool {
    let Some(value) = self.value.load(extracted_fields) else {
      return false;
    };

    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < *value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= *value,
      Operator::OPERATOR_GREATER_THAN => candidate > *value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= *value,
      Operator::OPERATOR_NOT_EQUALS => candidate != *value,
      // Real Regex is not supported.
      Operator::OPERATOR_EQUALS | Operator::OPERATOR_REGEX => candidate == *value,
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
  value: ValueOrSavedFieldId<NanEqualFloat>,
}

/// Supports comparison between two integers
impl DoubleMatch {
  fn new(operator: Operator, value: ValueOrSavedFieldId<NanEqualFloat>) -> Result<Self> {
    match operator {
      // Regex operator is not valid for f64
      Operator::OPERATOR_REGEX => Err(anyhow!("regex does not support f64")),
      _ => Ok(Self { operator, value }),
    }
  }

  fn evaluate(&self, candidate: f64, extracted_fields: &TinyMap<String, String>) -> bool {
    let candidate = NanEqualFloat(candidate);
    let Some(value) = self.value.load(extracted_fields) else {
      return false;
    };

    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < *value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= *value,
      Operator::OPERATOR_GREATER_THAN => candidate > *value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= *value,
      Operator::OPERATOR_NOT_EQUALS => candidate != *value,
      // Real Regex is not supported.
      Operator::OPERATOR_EQUALS | Operator::OPERATOR_REGEX => candidate == *value,
    }
  }
}

/// Describes a comparison match criteria against a String value
#[derive(Clone, Debug)]
pub struct StringMatch {
  operator: Operator,
  value: ValueOrSavedFieldId<String>,
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
  fn new(operator: Operator, value: ValueOrSavedFieldId<String>) -> Result<Self> {
    if operator == Operator::OPERATOR_UNSPECIFIED {
      return Err(anyhow!("UNSPECIFIED operator"));
    }

    // Compile the regex on tree creation to avoid recompiling it on every match.
    let regex = match operator {
      Operator::OPERATOR_REGEX => {
        let ValueOrSavedFieldId::Value(value) = &value else {
          return Err(anyhow!("regex operator requires a value"));
        };

        Some(match Regex::new(value) {
          Ok(regex) => regex,
          Err(e) => return Err(anyhow::Error::new(e).context("invalid regex")),
        })
      },
      _ => None,
    };
    Ok(Self {
      operator,
      value,
      regex,
    })
  }

  fn evaluate(&self, candidate: &str, extracted_fields: &TinyMap<String, String>) -> bool {
    let Some(value) = self.value.load(extracted_fields) else {
      return false;
    };

    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => *candidate < **value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => *candidate <= **value,
      Operator::OPERATOR_EQUALS => candidate == *value,
      Operator::OPERATOR_GREATER_THAN => *candidate > **value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => *candidate >= **value,
      Operator::OPERATOR_NOT_EQUALS => candidate != *value,
      Operator::OPERATOR_REGEX => self
        .regex
        .as_ref()
        .is_some_and(|regex| regex.is_match(candidate)),
    }
  }
}

/// Represents either the log message or the field key-value to match against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputType {
  Message,
  Field(String),
  FeatureFlag(String),
}

impl InputType {
  fn get<'a>(
    &self,
    message: &'a LogMessage,
    fields: FieldsRef<'a>,
    state: &'a dyn bd_state::StateReader,
  ) -> Option<Cow<'a, str>> {
    match self {
      Self::Message => message.as_str().map(Cow::Borrowed),
      Self::Field(field_key) => fields.field_value(field_key),
      Self::FeatureFlag(flag_key) => state
        .get(bd_state::Scope::FeatureFlag, flag_key)
        .map(Cow::Borrowed),
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

  /// Always true.
  Any,
}

impl Leaf {
  fn new_legacy(log_matcher: &LegacyBaseLogMatcher) -> Result<Self> {
    fn map_string_value(value: &str, match_type: StringMatchType) -> Result<StringMatch> {
      let (value, operator) = match match_type {
        legacy_log_matcher::base_log_matcher::StringMatchType::EXACT => {
          (value.to_string(), Operator::OPERATOR_EQUALS)
        },
        legacy_log_matcher::base_log_matcher::StringMatchType::PREFIX => (
          format!("^{}.*", regex::escape(value)),
          Operator::OPERATOR_REGEX,
        ),
        legacy_log_matcher::base_log_matcher::StringMatchType::REGEX => {
          (value.to_string(), Operator::OPERATOR_REGEX)
        },
      };

      StringMatch::new(operator, value.into())
    }

    match log_matcher
      .match_type
      .as_ref()
      .ok_or_else(|| anyhow!("missing legacy log matcher"))?
    {
      legacy_log_matcher::base_log_matcher::Match_type::LogLevelMatch(log_level_match) => {
        Ok(Self::LogLevel(IntMatch {
          operator: match log_level_match.operator.enum_value_or_default() {
            legacy_base_log_matcher::log_level_match::ComparisonOperator::LESS_THAN => {
              Operator::OPERATOR_LESS_THAN
            },
            legacy_base_log_matcher::log_level_match::ComparisonOperator::LESS_THAN_OR_EQUAL => {
              Operator::OPERATOR_LESS_THAN_OR_EQUAL
            },
            legacy_base_log_matcher::log_level_match::ComparisonOperator::EQUALS => {
              Operator::OPERATOR_EQUALS
            },
            legacy_base_log_matcher::log_level_match::ComparisonOperator::GREATER_THAN => {
              Operator::OPERATOR_GREATER_THAN
            },
            legacy_base_log_matcher::log_level_match::ComparisonOperator::GREATER_THAN_OR_EQUAL => {
              Operator::OPERATOR_GREATER_THAN_OR_EQUAL
            },
          },
          value: log_level_match.log_level.value().into(),
        }))
      },
      legacy_log_matcher::base_log_matcher::Match_type::MessageMatch(message_match) => {
        Ok(Self::StringValue(
          InputType::Message,
          map_string_value(
            &message_match.match_value,
            message_match.match_type.enum_value_or_default(),
          )?,
        ))
      },
      legacy_log_matcher::base_log_matcher::Match_type::TagMatch(tag_match) => {
        Ok(Self::StringValue(
          InputType::Field(tag_match.tag_key.clone()),
          map_string_value(
            &tag_match.match_value,
            tag_match.match_type.enum_value_or_default(),
          )?,
        ))
      },
      legacy_log_matcher::base_log_matcher::Match_type::TypeMatch(type_match) => {
        Ok(Self::LogType(type_match.type_))
      },
      legacy_log_matcher::base_log_matcher::Match_type::AnyMatch(_) => Ok(Self::Any),
    }
  }

  fn new(log_matcher: &BaseLogMatcher) -> Result<Self> {
    fn transform_int_value_match(int_value_match: &IntValueMatch_type) -> ValueOrSavedFieldId<i32> {
      // This used to not be a oneof so supply an equivalent default if the field is not set.
      match int_value_match
        .int_value_match_type
        .as_ref()
        .unwrap_or(&Int_value_match_type::MatchValue(0))
      {
        Int_value_match_type::MatchValue(v) => ValueOrSavedFieldId::Value(*v),
        Int_value_match_type::SaveFieldId(s) => ValueOrSavedFieldId::SaveFieldId(s.clone()),
      }
    }

    fn transform_string_value_match(
      string_value_match: &StringValueMatch_type,
    ) -> ValueOrSavedFieldId<String> {
      // This used to not be a oneof so supply an equivalent default if the field is not set.
      match string_value_match
        .string_value_match_type
        .as_ref()
        .unwrap_or(&String_value_match_type::MatchValue(String::new()))
      {
        String_value_match_type::MatchValue(s) => ValueOrSavedFieldId::Value(s.clone()),
        String_value_match_type::SaveFieldId(s) => ValueOrSavedFieldId::SaveFieldId(s.clone()),
      }
    }

    Ok(
      match log_matcher
        .match_type
        .as_ref()
        .ok_or_else(|| anyhow!("missing log_matcher"))?
      {
        MessageMatch(message_match) => Self::StringValue(
          InputType::Message,
          StringMatch::new(
            message_match
              .string_value_match
              .operator
              .enum_value()
              .map_err(|_| anyhow!("unknown field or enum"))?,
            transform_string_value_match(&message_match.string_value_match),
          )?,
        ),
        FeatureFlagMatch(feature_flag_match) => match feature_flag_match
          .value_match
          .as_ref()
          .ok_or_else(|| anyhow!("missing feature_flag_match value_match"))?
        {
          base_log_matcher::feature_flag_match::Value_match::StringValueMatch(
            string_value_match,
          ) => Self::StringValue(
            InputType::FeatureFlag(feature_flag_match.flag_name.clone()),
            StringMatch::new(
              string_value_match
                .operator
                .enum_value()
                .map_err(|_| anyhow!("unknown field or enum"))?,
              transform_string_value_match(string_value_match),
            )?,
          ),
          base_log_matcher::feature_flag_match::Value_match::IsSetMatch(_) => {
            Self::IsSetValue(InputType::FeatureFlag(feature_flag_match.flag_name.clone()))
          },
        },
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
            LOG_LEVEL_KEY => Self::LogLevel(IntMatch::new(
              int_value_match
                .operator
                .enum_value()
                .map_err(|_| anyhow!("unknown field or enum"))?,
              transform_int_value_match(int_value_match),
            )?),
            // Special case for key="log_type"
            // We're special casing log type because we need to look for this tag outside of the
            // regular fields map It should be a bd_matcher::LogType u32 value, so we
            // try to convert it from i32
            LOG_TYPE_KEY => Self::LogType(match transform_int_value_match(int_value_match) {
              ValueOrSavedFieldId::Value(v) => v.try_into()?,
              ValueOrSavedFieldId::SaveFieldId(_) => {
                return Err(anyhow!("log_type must be a value"));
              },
            }),
            // Any other int uses the IntValue match
            _ => Self::IntValue(
              InputType::Field(tag_match.tag_key.clone()),
              IntMatch::new(
                int_value_match
                  .operator
                  .enum_value()
                  .map_err(|_| anyhow!("unknown field or enum"))?,
                transform_int_value_match(int_value_match),
              )?,
            ),
          },
          DoubleValueMatch(double_value_match) => {
            Self::DoubleValue(
              InputType::Field(tag_match.tag_key.clone()),
              DoubleMatch::new(
                double_value_match
                  .operator
                  .enum_value()
                  .map_err(|_| anyhow!("unknown field or enum"))?,
                // This used to not be a oneof so supply an equivalent default if the field is not
                // set.
                match double_value_match
                  .double_value_match_type
                  .as_ref()
                  .unwrap_or(&Double_value_match_type::MatchValue(0.0))
                {
                  Double_value_match_type::MatchValue(d) => {
                    ValueOrSavedFieldId::Value(NanEqualFloat(*d))
                  },
                  Double_value_match_type::SaveFieldId(s) => {
                    ValueOrSavedFieldId::SaveFieldId(s.clone())
                  },
                },
              )?,
            )
          },
          StringValueMatch(string_value_match) => Self::StringValue(
            InputType::Field(tag_match.tag_key.clone()),
            StringMatch::new(
              string_value_match
                .operator
                .enum_value()
                .map_err(|_| anyhow!("unknown field or enum"))?,
              transform_string_value_match(string_value_match),
            )?,
          ),
          SemVerValueMatch(sem_ver_value_match) => Self::VersionValue(
            InputType::Field(tag_match.tag_key.clone()),
            version::VersionMatch::new(
              sem_ver_value_match
                .operator
                .enum_value()
                .map_err(|_| anyhow!("unknown field or enum"))?,
              sem_ver_value_match.match_value.as_str(),
            )?,
          ),
          IsSetMatch(_) => Self::IsSetValue(InputType::Field(tag_match.tag_key.clone())),
        },
      },
    )
  }
}
