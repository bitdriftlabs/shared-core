// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./matcher_test.rs"]
mod matcher_test;

#[cfg(test)]
#[path = "./legacy_matcher_test.rs"]
mod legacy_matcher_test;

#[cfg(test)]
#[path = "./json_string_matcher_test.rs"]
mod json_string_matcher_test;

#[cfg(test)]
#[path = "./json_path_test.rs"]
mod json_path_test;

#[path = "./json_path.rs"]
mod json_path;

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
use bd_log_primitives::{DataValue, FieldsRef, LogLevel, LogMessage, data_to_string_value};
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
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{Data, LogType};
use bd_proto::protos::state::scope::StateScope;
use bd_proto::protos::value_matcher::value_matcher::Operator;
use bd_proto::protos::value_matcher::value_matcher::json_path_value_match::{
  KeyOrIndex,
  key_or_index,
};
use bd_state::{Scope, Value_type};
use log_matcher::LogMatcher;
use log_matcher::log_matcher::{BaseLogMatcher, Matcher, base_log_matcher};
use rand::RngExt;
use std::borrow::Cow;

#[derive(Clone, Copy, Debug)]
pub struct MatchContext {
  pub json_path_string_matching_enabled: bool,
}

impl Default for MatchContext {
  fn default() -> Self {
    Self {
      json_path_string_matching_enabled: true,
    }
  }
}

//
// MatchResult
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatchResult {
  Matched,
  NotMatched,
  Disabled,
}

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
    context: MatchContext,
  ) -> bool {
    self.do_match_with_sampled_roll(
      log_level,
      log_type,
      message,
      fields,
      state,
      extracted_fields,
      sampled_roll,
      context,
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
    context: MatchContext,
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
      context,
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
    context: MatchContext,
  ) -> bool {
    self.do_match_result_with_sampled_roll(
      log_level,
      log_type,
      message,
      fields,
      state,
      extracted_fields,
      sampled_roll,
      context,
    ) == MatchResult::Matched
  }

  fn do_match_result_with_sampled_roll(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    message: &LogMessage,
    fields: FieldsRef<'_>,
    state: &dyn bd_state::StateReader,
    extracted_fields: &TinyMap<String, String>,
    sampled_roll: u32,
    context: MatchContext,
  ) -> MatchResult {
    match self {
      Self::Base(base_matcher) => MatchResult::from(match base_matcher {
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
        } => {
          let Some(value) = resolved_field_value_with_state(fields, state, field_key) else {
            return MatchResult::NotMatched;
          };
          // TODO: Fold Disabled into the planned general matcher evaluation context/cache work.
          if !context.json_path_string_matching_enabled && value.is_json_string() {
            return MatchResult::Disabled;
          }
          let Some(input) = value.resolve_json_path(path) else {
            return MatchResult::NotMatched;
          };
          matcher.evaluate(input.as_ref(), extracted_fields)
        },
        Leaf::Sampled(sample_rate) => sample_matches_with_roll(*sample_rate, sampled_roll),
        Leaf::Any => true,
      }),
      Self::Or(or_matchers) => {
        let mut result = MatchResult::NotMatched;
        for matcher in or_matchers {
          match matcher.do_match_result_with_sampled_roll(
            log_level,
            log_type,
            message,
            fields,
            state,
            extracted_fields,
            sampled_roll,
            context,
          ) {
            MatchResult::Matched => return MatchResult::Matched,
            MatchResult::Disabled => result = MatchResult::Disabled,
            MatchResult::NotMatched => {},
          }
        }
        result
      },
      Self::And(and_matchers) => {
        let mut result = MatchResult::Matched;
        for matcher in and_matchers {
          match matcher.do_match_result_with_sampled_roll(
            log_level,
            log_type,
            message,
            fields,
            state,
            extracted_fields,
            sampled_roll,
            context,
          ) {
            MatchResult::NotMatched => return MatchResult::NotMatched,
            MatchResult::Disabled => result = MatchResult::Disabled,
            MatchResult::Matched => {},
          }
        }
        result
      },
      Self::Not(matcher) => match matcher.do_match_result_with_sampled_roll(
        log_level,
        log_type,
        message,
        fields,
        state,
        extracted_fields,
        sampled_roll,
        context,
      ) {
        MatchResult::Matched => MatchResult::NotMatched,
        MatchResult::NotMatched => MatchResult::Matched,
        MatchResult::Disabled => MatchResult::Disabled,
      },
    }
  }
}

impl From<bool> for MatchResult {
  fn from(value: bool) -> Self {
    if value {
      Self::Matched
    } else {
      Self::NotMatched
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

/// A log-field value resolved from either the current log or its persistent state overlay.
///
/// The state representation is deliberately borrowed as protobuf `Data`; converting it to a
/// `DataValue` here would recursively allocate for maps and arrays on every matcher evaluation.
#[derive(Clone, Copy)]
enum ResolvedFieldValue<'a> {
  Log(&'a DataValue),
  State(&'a bd_state::Value),
}

impl<'a> ResolvedFieldValue<'a> {
  fn as_cow(self) -> Option<Cow<'a, str>> {
    match self {
      Self::Log(value) => value.to_string_value(),
      Self::State(value) => state_value_as_cow(value),
    }
  }

  fn as_i32(self) -> Option<i32> {
    match self {
      Self::Log(value) => log_field_as_i32(value),
      Self::State(value) => state_value_as_i32(value),
    }
  }

  fn as_f64(self) -> Option<f64> {
    match self {
      Self::Log(value) => log_field_as_f64(value),
      Self::State(value) => state_value_as_f64(value),
    }
  }

  fn resolve_json_path(self, path: &[JsonPathToken]) -> Option<Cow<'a, str>> {
    match self {
      Self::Log(value) => resolve_json_path(value, path),
      Self::State(value) => resolve_json_path_from_state(value, path),
    }
  }

  fn is_json_string(self) -> bool {
    match self {
      Self::Log(value) => value.as_str().is_some(),
      Self::State(value) => matches!(
        value.value_type.as_ref(),
        Some(Value_type::Data(data))
          if matches!(data.data_type.as_ref(), Some(Data_type::StringData(_)))
      ),
    }
  }
}

/// Converts a state value into the string representation used by state matchers and extractors.
///
/// State-backed custom and OOTB fields retain their logging `Data` representation, so this reads
/// directly from the protobuf instead of allocating an intermediate `DataValue`.
#[must_use]
pub fn state_value_as_cow(value: &bd_state::Value) -> Option<Cow<'_, str>> {
  use bd_state::Value_type;

  match value.value_type.as_ref() {
    Some(Value_type::StringValue(value)) => Some(Cow::Borrowed(value.as_str())),
    Some(Value_type::IntValue(value)) => Some(Cow::Owned(value.to_string())),
    Some(Value_type::DoubleValue(value)) => Some(Cow::Owned(value.to_string())),
    Some(Value_type::BoolValue(true)) => Some(Cow::Borrowed("true")),
    Some(Value_type::BoolValue(false)) => Some(Cow::Borrowed("false")),
    Some(Value_type::Data(value)) => match value.data_type.as_ref() {
      Some(Data_type::BoolData(true)) => Some(Cow::Borrowed("true")),
      Some(Data_type::BoolData(false)) => Some(Cow::Borrowed("false")),
      _ => data_to_string_value(value),
    },
    None => None,
  }
}

/// Views a logging `Data` value from a state entry with `DataValue` string semantics.
///
/// Virtual fields are only written as `Value_type::Data`, and must preserve the behavior of the
/// equivalent inline `DataValue`. In particular, booleans are not string-matchable log fields.
fn persisted_log_field_as_cow(value: &bd_state::Value) -> Option<Cow<'_, str>> {
  let Value_type::Data(data) = value.value_type.as_ref()? else {
    return None;
  };

  data_to_string_value(data)
}

/// Resolves a field using the metadata collector's persistent-field precedence.
///
/// OOTB SDK state fields have the highest priority. Concrete log fields retain their existing
/// provider and per-log precedence and take priority over custom SDK state fields, which are the
/// lowest virtual layer. This lets callers read state-backed fields exactly like regular fields
/// without materializing them in the log's captured field map.
fn resolved_field_value_with_state<'a>(
  fields: FieldsRef<'a>,
  state: &'a dyn bd_state::StateReader,
  field_key: &str,
) -> Option<ResolvedFieldValue<'a>> {
  state
    .get(Scope::OotbFields, field_key)
    .map(ResolvedFieldValue::State)
    .or_else(|| fields.field(field_key).map(ResolvedFieldValue::Log))
    .or_else(|| {
      state
        .get(Scope::CustomFields, field_key)
        .map(ResolvedFieldValue::State)
    })
}

/// Resolves a field using the metadata collector's persistent-field precedence.
///
/// OOTB SDK state fields have the highest priority. Concrete log fields retain their existing
/// provider and per-log precedence and take priority over custom SDK state fields, which are the
/// lowest virtual layer. This lets callers read state-backed fields exactly like regular fields
/// without materializing them in the log's captured field map.
#[must_use]
pub fn field_value_with_state<'a>(
  fields: FieldsRef<'a>,
  state: &'a dyn bd_state::StateReader,
  field_key: &str,
) -> Option<Cow<'a, str>> {
  // An OOTB state entry is authoritative even when it cannot be represented as a string. After
  // that, retain FieldsRef::field_value's captured-to-matching-only fallback before considering
  // the lowest-priority custom state layer.
  if let Some(value) = state.get(Scope::OotbFields, field_key) {
    return persisted_log_field_as_cow(value);
  }

  fields.field_value(field_key).or_else(|| {
    state
      .get(Scope::CustomFields, field_key)
      .and_then(persisted_log_field_as_cow)
  })
}

/// Views an integer-compatible log-field value persisted in state without cloning its protobuf.
#[allow(clippy::cast_possible_truncation)]
fn persisted_log_field_as_i32(data: &Data) -> Option<i32> {
  match data.data_type.as_ref()? {
    Data_type::IntData(value) => i32::try_from(*value).ok(),
    Data_type::SintData(value) => i32::try_from(*value).ok(),
    Data_type::DoubleData(value) if !value.is_nan() => Some(*value as i32),
    Data_type::StringData(value) => value.parse::<f64>().ok().map(|value| value as i32),
    Data_type::BinaryData(_)
    | Data_type::BoolData(_)
    | Data_type::MapData(_)
    | Data_type::ArrayData(_)
    | Data_type::DoubleData(_) => None,
  }
}

/// Views a double-compatible log-field value persisted in state without cloning its protobuf.
#[allow(clippy::cast_precision_loss)]
fn persisted_log_field_as_f64(data: &Data) -> Option<f64> {
  match data.data_type.as_ref()? {
    Data_type::DoubleData(value) if !value.is_nan() => Some(*value),
    Data_type::SintData(value) => Some(*value as f64),
    Data_type::IntData(value) => Some(*value as f64),
    Data_type::StringData(value) => value.parse().ok(),
    Data_type::BinaryData(_)
    | Data_type::BoolData(_)
    | Data_type::MapData(_)
    | Data_type::ArrayData(_)
    | Data_type::DoubleData(_) => None,
  }
}

fn state_value_as_i32(value: &bd_state::Value) -> Option<i32> {
  use bd_state::Value_type;

  match value.value_type.as_ref() {
    Some(Value_type::IntValue(value)) => i32::try_from(*value).ok(),
    #[allow(clippy::cast_possible_truncation)]
    Some(Value_type::DoubleValue(value)) => Some(*value as i32),
    Some(Value_type::StringValue(value)) => value.parse().ok(),
    Some(Value_type::Data(value)) => persisted_log_field_as_i32(value),
    Some(Value_type::BoolValue(_)) | None => None,
  }
}

fn state_value_as_f64(value: &bd_state::Value) -> Option<f64> {
  use bd_state::Value_type;

  match value.value_type.as_ref() {
    Some(Value_type::DoubleValue(value)) => Some(*value),
    #[allow(clippy::cast_precision_loss)]
    Some(Value_type::IntValue(value)) => Some(*value as f64),
    Some(Value_type::StringValue(value)) => value.parse().ok(),
    Some(Value_type::Data(value)) => persisted_log_field_as_f64(value),
    Some(Value_type::BoolValue(_)) | None => None,
  }
}

#[allow(clippy::cast_possible_truncation)]
fn log_field_as_i32(field: &DataValue) -> Option<i32> {
  match field {
    DataValue::I64(value) => i32::try_from(*value).ok(),
    DataValue::U64(value) => i32::try_from(*value).ok(),
    DataValue::Double(value) => Some(**value as i32),
    DataValue::String(_) | DataValue::SharedString(_) | DataValue::StaticString(_) => {
      // Parse as f64 first then truncate to preserve backward compatibility with strings like
      // "13.0" that were previously accepted.
      Some(field.as_str()?.parse::<f64>().ok()? as i32)
    },
    DataValue::Bytes(_) | DataValue::Boolean(_) | DataValue::Map(_) | DataValue::Array(_) => None,
  }
}

#[allow(clippy::cast_precision_loss)]
fn log_field_as_f64(field: &DataValue) -> Option<f64> {
  match field {
    DataValue::Double(value) => Some(**value),
    DataValue::I64(value) => Some(*value as f64),
    DataValue::U64(value) => Some(*value as f64),
    DataValue::String(_) | DataValue::SharedString(_) | DataValue::StaticString(_) => {
      field.as_str()?.parse().ok()
    },
    DataValue::Bytes(_) | DataValue::Boolean(_) | DataValue::Map(_) | DataValue::Array(_) => None,
  }
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
      Self::Field(field_key) => field_value_with_state(fields, state, field_key),
      Self::State(scope, flag_key) => state
        .get(*scope, flag_key)
        .and_then(|value| ResolvedFieldValue::State(value).as_cow()),
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
      Self::Field(field_key) => resolved_field_value_with_state(fields, state, field_key)
        .and_then(ResolvedFieldValue::as_i32),
      Self::State(scope, flag_key) => state
        .get(*scope, flag_key)
        .and_then(|value| ResolvedFieldValue::State(value).as_i32()),
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
      Self::Field(field_key) => resolved_field_value_with_state(fields, state, field_key)
        .and_then(ResolvedFieldValue::as_f64),
      Self::State(scope, flag_key) => state
        .get(*scope, flag_key)
        .and_then(|value| ResolvedFieldValue::State(value).as_f64()),
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JsonPathToken {
  Key(String),
  Index(i32),
}

#[cfg(any(feature = "fuzzing", feature = "benchmark"))]
#[must_use]
pub fn resolve_json_path_for_testing<'a>(
  input: &'a str,
  path: &[JsonPathToken],
) -> Option<Cow<'a, str>> {
  json_path::resolve(input, path)
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
            StateScope::SYSTEM => Scope::System,
            StateScope::CUSTOM_FIELDS => Scope::CustomFields,
            StateScope::OOTB_FIELDS => Scope::OotbFields,
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
  // Existing SDK APIs send JSON as a string. Parsing is only reached from JsonPathValue matchers.
  if let Some(maybe_json) = value.as_str() {
    return resolve_json_string_path(maybe_json, path);
  }

  match value {
    // Future optimized SDK APIs can emit native structured values directly and avoid JSON parsing.
    DataValue::Map(_) | DataValue::Array(_) => resolve_structured_json_path(value, path),

    _ => None,
  }
}

fn resolve_json_string_path<'a>(value: &'a str, path: &[JsonPathToken]) -> Option<Cow<'a, str>> {
  json_path::resolve(value, path)
}

fn resolve_structured_json_path<'a>(
  value: &'a DataValue,
  path: &[JsonPathToken],
) -> Option<Cow<'a, str>> {
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

fn resolve_json_path_from_state<'a>(
  value: &'a bd_state::Value,
  path: &[JsonPathToken],
) -> Option<Cow<'a, str>> {
  let Value_type::Data(value) = value.value_type.as_ref()? else {
    return None;
  };

  // JSON-string fields retain their existing parsing behavior after moving into state.
  if let Some(Data_type::StringData(value)) = value.data_type.as_ref() {
    return resolve_json_string_path(value, path);
  }

  let mut current = value;
  for token in path {
    match token {
      JsonPathToken::Key(key) => {
        let Data_type::MapData(map_data) = current.data_type.as_ref()? else {
          return None;
        };
        current = map_data.entries.get(key)?;
      },
      JsonPathToken::Index(index) => {
        let Data_type::ArrayData(array_data) = current.data_type.as_ref()? else {
          return None;
        };
        let len = i32::try_from(array_data.items.len()).ok()?;
        let index = if *index < 0 { len + *index } else { *index };
        let index: usize = index.try_into().ok()?;
        current = array_data.items.get(index)?;
      },
    }
  }

  match current.data_type.as_ref()? {
    Data_type::StringData(value) => Some(Cow::Borrowed(value)),
    Data_type::BinaryData(_)
    | Data_type::BoolData(_)
    | Data_type::IntData(_)
    | Data_type::SintData(_)
    | Data_type::DoubleData(_)
    | Data_type::MapData(_)
    | Data_type::ArrayData(_) => None,
  }
}
