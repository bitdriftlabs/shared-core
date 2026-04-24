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
use base_log_matcher::Match_type::{MessageMatch, SampledMatch, StateMatch, TagMatch};
use base_log_matcher::tag_match::Value_match::{
  DoubleValueMatch,
  IntValueMatch,
  IsSetMatch,
  JsonValueMatch,
  SemVerValueMatch,
  StringValueMatch,
};
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{DataValue, FieldsRef, LogLevel, LogMessage};
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
use bd_proto::protos::value_matcher::value_matcher::json_path_value_match::{
  KeyOrIndex,
  key_or_index,
};
use bd_state::Scope;
use log_matcher::LogMatcher;
use log_matcher::log_matcher::{BaseLogMatcher, Matcher, base_log_matcher};
use rand::RngExt;
use std::borrow::Cow;

const LOG_LEVEL_KEY: &str = "log_level";
const LOG_TYPE_KEY: &str = "log_type";
pub const SAMPLE_RATE_DENOMINATOR: u32 = 1_000_000;

pub trait RandomNumberGenerator {
  fn random_u32(&mut self, upper_bound_exclusive: u32) -> u32;
}

//
// ThreadRngGenerator
//

#[derive(Debug, Default)]
struct ThreadRngGenerator;

impl RandomNumberGenerator for ThreadRngGenerator {
  fn random_u32(&mut self, upper_bound_exclusive: u32) -> u32 {
    rand::rng().random_range(0 .. upper_bound_exclusive)
  }
}

#[must_use]
pub fn random_sample_roll() -> u32 {
  let mut rng = ThreadRngGenerator;
  rng.random_u32(SAMPLE_RATE_DENOMINATOR)
}

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
    sampled_roll: u32,
  ) -> bool {
    self.do_match_with_sampled_roll(
      log_level,
      log_type,
      message,
      fields,
      state,
      extracted_fields,
      sampled_roll,
    )
  }

  #[must_use]
  pub fn do_match_with_rng(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    message: &LogMessage,
    fields: FieldsRef<'_>,
    state: &dyn bd_state::StateReader,
    extracted_fields: &TinyMap<String, String>,
    rng: &mut dyn RandomNumberGenerator,
  ) -> bool {
    let sampled_roll = rng.random_u32(SAMPLE_RATE_DENOMINATOR);
    self.do_match_with_sampled_roll(
      log_level,
      log_type,
      message,
      fields,
      state,
      extracted_fields,
      sampled_roll,
    )
  }

  fn do_match_with_sampled_roll(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    message: &LogMessage,
    fields: FieldsRef<'_>,
    state: &dyn bd_state::StateReader,
    extracted_fields: &TinyMap<String, String>,
    sampled_roll: u32,
  ) -> bool {
    match self {
      Self::Base(base_matcher) => match base_matcher {
        Leaf::LogLevel(log_level_matcher) => log_level
          .try_into()
          .is_ok_and(|log_level| log_level_matcher.evaluate(log_level, extracted_fields)),
        Leaf::LogType(l_type) => *l_type == log_type as u32,
        Leaf::IntValue(input, criteria) => input
          .get_as_i32(message, fields, state)
          .is_some_and(|v| criteria.evaluate(v, extracted_fields)),
        Leaf::DoubleValue(input, criteria) => input
          .get_as_f64(message, fields, state)
          .is_some_and(|v| criteria.evaluate(v, extracted_fields)),
        Leaf::StringValue(input, criteria) => input
          .get(message, fields, state)
          .is_some_and(|input| criteria.evaluate(input.as_ref(), extracted_fields)),
        Leaf::VersionValue(input, criteria) => input
          .get(message, fields, state)
          .is_some_and(|input| criteria.evaluate(input.as_ref())),
        Leaf::IsSetValue(input) => input.get(message, fields, state).is_some(),
        Leaf::JsonPathValue {
          field_key,
          path,
          matcher,
        } => fields
          .field(field_key)
          .and_then(|value| resolve_json_path(value, path))
          .is_some_and(|input| matcher.evaluate(input.as_ref(), extracted_fields)),
        Leaf::Sampled(sample_rate) => sample_matches_with_roll(*sample_rate, sampled_roll),
        Leaf::Any => true,
      },
      Self::Or(or_matchers) => or_matchers.iter().any(|matcher| {
        matcher.do_match_with_sampled_roll(
          log_level,
          log_type,
          message,
          fields,
          state,
          extracted_fields,
          sampled_roll,
        )
      }),
      Self::And(and_matchers) => and_matchers.iter().all(|matcher| {
        matcher.do_match_with_sampled_roll(
          log_level,
          log_type,
          message,
          fields,
          state,
          extracted_fields,
          sampled_roll,
        )
      }),
      Self::Not(matcher) => !matcher.do_match_with_sampled_roll(
        log_level,
        log_type,
        message,
        fields,
        state,
        extracted_fields,
        sampled_roll,
      ),
    }
  }
}

fn sample_matches_with_roll(sample_rate: u32, roll: u32) -> bool {
  if sample_rate == 0 {
    return false;
  }
  if sample_rate >= SAMPLE_RATE_DENOMINATOR {
    return true;
  }

  debug_assert!(roll < SAMPLE_RATE_DENOMINATOR);
  roll < sample_rate
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
      Self::State(scope, flag_key) => state.get(*scope, flag_key).map(|v| {
        use bd_state::Value_type;
        match v.value_type {
          Some(Value_type::StringValue(ref s)) => Cow::Borrowed(s.as_str()),
          // TODO(snowp): Ideally we would avoid allocating here, but since state values are
          // stored as Value_type enum, we need to convert them to strings. Int/Double state values
          // are not really used right now, but if we ever do consider either caching this or
          // making the comparison machinery work against the different types directly.
          Some(Value_type::IntValue(i)) => Cow::Owned(i.to_string()),
          Some(Value_type::DoubleValue(d)) => Cow::Owned(d.to_string()),
          Some(Value_type::BoolValue(true)) => Cow::Borrowed("true"),
          Some(Value_type::BoolValue(false)) => Cow::Borrowed("false"),
          None => Cow::Borrowed(""),
        }
      }),
    }
  }

  /// Extracts a value as i32 for use with `IntMatch`. Handles numeric `DataValue` types directly,
  /// falling back to string parsing for string types.
  #[allow(clippy::cast_possible_truncation)]
  fn get_as_i32(
    &self,
    message: &LogMessage,
    fields: FieldsRef<'_>,
    state: &dyn bd_state::StateReader,
  ) -> Option<i32> {
    match self {
      Self::Message => message.as_str().and_then(|s| s.parse().ok()),
      Self::Field(field_key) => {
        let field = fields.field(field_key)?;
        match field {
          DataValue::I64(v) => i32::try_from(*v).ok(),
          DataValue::U64(v) => i32::try_from(*v).ok(),
          DataValue::Double(v) => Some(**v as i32),
          DataValue::String(_) | DataValue::SharedString(_) | DataValue::StaticString(_) => {
            // Parse as f64 first then truncate to preserve backward compatibility with strings
            // like "13.0" that were previously accepted.
            Some(field.as_str()?.parse::<f64>().ok()? as i32)
          },
          DataValue::Bytes(_) | DataValue::Boolean(_) | DataValue::Map(_) | DataValue::Array(_) => {
            None
          },
        }
      },
      Self::State(scope, flag_key) => {
        use bd_state::Value_type;
        let v = state.get(*scope, flag_key)?;
        match v.value_type {
          Some(Value_type::IntValue(i)) => i32::try_from(i).ok(),
          Some(Value_type::DoubleValue(d)) => Some(d as i32),
          Some(Value_type::StringValue(ref s)) => s.parse().ok(),
          Some(Value_type::BoolValue(_)) | None => None,
        }
      },
    }
  }

  /// Extracts a value as f64 for use with `DoubleMatch`. Handles numeric `DataValue` types
  /// directly, falling back to string parsing for string types.
  #[allow(clippy::cast_precision_loss)]
  fn get_as_f64(
    &self,
    message: &LogMessage,
    fields: FieldsRef<'_>,
    state: &dyn bd_state::StateReader,
  ) -> Option<f64> {
    match self {
      Self::Message => message.as_str().and_then(|s| s.parse().ok()),
      Self::Field(field_key) => {
        let field = fields.field(field_key)?;
        match field {
          DataValue::Double(v) => Some(**v),
          DataValue::I64(v) => Some(*v as f64),
          DataValue::U64(v) => Some(*v as f64),
          DataValue::String(_) | DataValue::SharedString(_) | DataValue::StaticString(_) => {
            field.as_str()?.parse().ok()
          },
          DataValue::Bytes(_) | DataValue::Boolean(_) | DataValue::Map(_) | DataValue::Array(_) => {
            None
          },
        }
      },
      Self::State(scope, flag_key) => {
        use bd_state::Value_type;
        let v = state.get(*scope, flag_key)?;
        match v.value_type {
          Some(Value_type::DoubleValue(d)) => Some(d),
          Some(Value_type::IntValue(i)) => Some(i as f64),
          Some(Value_type::StringValue(ref s)) => s.parse().ok(),
          Some(Value_type::BoolValue(_)) | None => None,
        }
      },
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

  /// Uses a destructured JSON path to match against a string value within a Map structure stored in
  /// a tag.
  JsonPathValue {
    field_key: String,
    path: Vec<JsonPathToken>,
    matcher: StringMatch,
  },

  /// Match based on a pseudo-random sample decision.
  Sampled(u32),

  /// Always true.
  Any,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonPathToken {
  Key(String),
  Index(i32),
}

impl Leaf {
  fn new_legacy(log_matcher: &LegacyBaseLogMatcher) -> Result<Self> {
    fn map_string_value(value: &str, match_type: StringMatchType) -> Result<StringMatch> {
      let (value, operator) = match match_type {
        legacy_log_matcher::base_log_matcher::StringMatchType::EXACT => {
          (value.to_string(), Operator::OPERATOR_EQUALS)
        },
        legacy_log_matcher::base_log_matcher::StringMatchType::PREFIX => (
          format!("^{}.*", regex_lite::escape(value)),
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
            StateScope::SYSTEM => Scope::System,
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
        SampledMatch(sampled_match) => Self::Sampled(sampled_match.sample_rate),
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
          JsonValueMatch(json_value_match) => {
            let path = json_value_match
              .key_or_index
              .iter()
              .map(parse_json_path)
              .collect::<Result<Vec<_>>>()?;
            Self::JsonPathValue {
              field_key: tag_match.tag_key.clone(),
              path,
              matcher: StringMatch::new(
                json_value_match.operator,
                ValueOrSavedFieldId::Value(json_value_match.match_value.clone()),
              )?,
            }
          },
        },
      },
    )
  }
}

fn parse_json_path(key_or_index: &KeyOrIndex) -> Result<JsonPathToken> {
  match key_or_index
    .key_or_index
    .as_ref()
    .ok_or_else(|| anyhow!("missing json path key or index"))?
  {
    key_or_index::Key_or_index::Key(key) => Ok(JsonPathToken::Key(key.clone())),
    key_or_index::Key_or_index::Index(index) => Ok(JsonPathToken::Index(*index)),
  }
}

fn resolve_json_path<'a>(value: &'a DataValue, path: &[JsonPathToken]) -> Option<Cow<'a, str>> {
  let mut current = value;
  for token in path {
    match token {
      JsonPathToken::Key(key) => {
        let DataValue::Map(map_data) = current else {
          return None;
        };
        current = map_data.entries().get(key)?;
      },
      JsonPathToken::Index(index) => {
        let DataValue::Array(array_data) = current else {
          return None;
        };
        let items = array_data.items();
        let len = i32::try_from(items.len()).ok()?;
        let index = if *index < 0 { len + *index } else { *index };
        let index: usize = index.try_into().ok()?;
        current = items.get(index)?;
      },
    }
  }

  match current {
    DataValue::String(value) => Some(Cow::Borrowed(value.as_str())),
    DataValue::SharedString(value) => Some(Cow::Borrowed(value.as_ref())),
    DataValue::StaticString(value) => Some(Cow::Borrowed(value)),
    DataValue::Bytes(_)
    | DataValue::Boolean(_)
    | DataValue::U64(_)
    | DataValue::I64(_)
    | DataValue::Double(_)
    | DataValue::Map(_)
    | DataValue::Array(_) => None,
  }
}
