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

use crate::value_matcher::{DoubleMatch, IntMatch, StringMatch, ValueOrSavedFieldId};
use crate::version;
use anyhow::{Result, anyhow};
use base_log_matcher::Match_type::{MessageMatch, StateMatch, TagMatch};
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
use bd_proto::protos::state::scope::StateScope;
use bd_proto::protos::value_matcher::value_matcher::Operator;
use bd_state::Scope;
use log_matcher::LogMatcher;
use log_matcher::log_matcher::{BaseLogMatcher, Matcher, base_log_matcher};
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
  Or(Vec<Self>),

  // A composite matching node. This tree matches if all of the subtrees evaluates to true.
  And(Vec<Self>),

  // An invert of a predicate. This tree matches if its subtree doesn't match.
  Not(Box<Self>),
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


/// Represents either the input type to match against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputType {
  Message,
  Field(String),
  State(Scope, String),
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
      Self::State(scope, flag_key) => state.get(*scope, flag_key).map(Cow::Borrowed),
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

      StringMatch::new(operator.into(), value.into())
    }

    match log_matcher
      .match_type
      .as_ref()
      .ok_or_else(|| anyhow!("missing legacy log matcher"))?
    {
      legacy_log_matcher::base_log_matcher::Match_type::LogLevelMatch(log_level_match) => {
        Ok(Self::LogLevel(IntMatch::new(
          match log_level_match.operator.enum_value_or_default() {
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
          }
          .into(),
          ValueOrSavedFieldId::Value(log_level_match.log_level.value()),
        )?))
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
    Ok(
      match log_matcher
        .match_type
        .as_ref()
        .ok_or_else(|| anyhow!("missing log_matcher"))?
      {
        MessageMatch(message_match) => Self::StringValue(
          InputType::Message,
          StringMatch::new(
            message_match.string_value_match.operator,
            ValueOrSavedFieldId::<String>::from_proto(&message_match.string_value_match),
          )?,
        ),
        StateMatch(state_match) => {
          let state_key = state_match.state_key.clone();
          let scope = match state_match.scope.enum_value_or_default() {
            StateScope::FEATURE_FLAG => Scope::FeatureFlagExposure,
            StateScope::GLOBAL_STATE => Scope::GlobalState,
            StateScope::UNSPECIFIED => {
              // For now, we only support feature flags. Other scopes would need additional
              // handling.
              // We'll need to config version guard any new scopes.
              return Err(anyhow!("Unsupported state scope"));
            },
          };

          let input_type = InputType::State(scope, state_key);

          // Handle the value match
          match state_match
            .state_value_match
            .as_ref()
            .ok_or_else(|| anyhow!("missing state_value_match"))?
            .value_match
            .as_ref()
            .ok_or_else(|| anyhow!("missing state value_match"))?
          {
            bd_proto::protos::state::matcher::state_value_match::Value_match::StringValueMatch(
              string_value_match,
            ) => Self::StringValue(
              input_type,
              StringMatch::new(
                string_value_match.operator,
                ValueOrSavedFieldId::<String>::from_proto(string_value_match),
              )?,
            ),
            bd_proto::protos::state::matcher::state_value_match::Value_match::IsSetMatch(_) => {
              Self::IsSetValue(input_type)
            },
            bd_proto::protos::state::matcher::state_value_match::Value_match::IntValueMatch(
              int_value_match,
            ) => Self::IntValue(input_type, IntMatch::from_proto(int_value_match)?),
            bd_proto::protos::state::matcher::state_value_match::Value_match::DoubleValueMatch(
              double_value_match,
            ) => Self::DoubleValue(input_type, DoubleMatch::from_proto(double_value_match)?),
          }
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
            LOG_LEVEL_KEY => Self::LogLevel(IntMatch::from_proto(int_value_match)?),
            // Special case for key="log_type"
            // We're special casing log type because we need to look for this tag outside of the
            // regular fields map It should be a bd_matcher::LogType u32 value, so we
            // try to convert it from i32
            LOG_TYPE_KEY => Self::LogType(
              match ValueOrSavedFieldId::<i32>::from_proto(int_value_match) {
                ValueOrSavedFieldId::Value(v) => v.try_into()?,
                ValueOrSavedFieldId::SaveFieldId(_) => {
                  return Err(anyhow!("log_type must be a value"));
                },
              },
            ),
            // Any other int uses the IntValue match
            _ => Self::IntValue(
              InputType::Field(tag_match.tag_key.clone()),
              IntMatch::from_proto(int_value_match)?,
            ),
          },
          DoubleValueMatch(double_value_match) => Self::DoubleValue(
            InputType::Field(tag_match.tag_key.clone()),
            DoubleMatch::from_proto(double_value_match)?,
          ),
          StringValueMatch(string_value_match) => Self::StringValue(
            InputType::Field(tag_match.tag_key.clone()),
            StringMatch::from_proto(string_value_match)?,
          ),
          SemVerValueMatch(sem_ver_value_match) => Self::VersionValue(
            InputType::Field(tag_match.tag_key.clone()),
            version::VersionMatch::from_proto(sem_ver_value_match)?,
          ),
          IsSetMatch(_) => Self::IsSetValue(InputType::Field(tag_match.tag_key.clone())),
        },
      },
    )
  }
}
