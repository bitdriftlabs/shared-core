// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./matcher_test.rs"]
mod matcher_test;

use crate::{Error, Result};
use bd_log_primitives::{FieldsRef, LogLevel, LogMessage, LogType};
use bd_proto::protos;
use protos::config::v1::config::LogMatcher;
use protos::config::v1::config::log_matcher::base_log_matcher::log_level_match::ComparisonOperator;
use protos::config::v1::config::log_matcher::base_log_matcher::{self, StringMatchType};
use protos::config::v1::config::log_matcher::{BaseLogMatcher, Match_type};
use regex::Regex;
use std::borrow::Cow;

/// A compiled matching tree that supports evaluating a an input log. Matching involves
/// evaluating the match criteria starting from the top, possibly recursing into subtree matching.
#[derive(Debug)]
pub enum Tree {
  // A direct matching node, specifying a match criteria. This tree matches if the base matcher
  // evaluates to true.
  Base(Leaf),

  // A composite matching node. This tree matches if any of the subtrees evaluates to true.
  Or(Vec<Tree>),

  // A composite matching node. This tree matches if all of the subtrees evaluates to true.
  And(Vec<Tree>),

  // An inverted matching node. This tree matches if the subtree evaluates to false.
  Not(Box<Tree>),
}

impl Tree {
  // Compiles a match tree from the matching config.
  pub fn new(config: &LogMatcher) -> Result<Self> {
    match config.match_type.as_ref().ok_or(Error::InvalidConfig)? {
      Match_type::BaseMatcher(matcher) => Ok(Self::Base(Leaf::new(matcher)?)),
      Match_type::OrMatcher(sub_matchers) => Ok(Self::Or(
        sub_matchers
          .matcher
          .iter()
          .map(Self::new)
          .collect::<Result<Vec<Self>>>()?,
      )),
      Match_type::AndMatcher(sub_matchers) => Ok(Self::And(
        sub_matchers
          .matcher
          .iter()
          .map(Self::new)
          .collect::<Result<Vec<Self>>>()?,
      )),
      Match_type::NotMatcher(sub_matcher) => Ok(Self::Not(Box::new(Self::new(sub_matcher)?))),
    }
  }

  // Evaluates the match tree against a set of inputs.
  #[must_use]
  pub fn do_match(
    &self,
    log_type: LogType,
    log_level: LogLevel,
    message: &LogMessage,
    fields: FieldsRef<'_>,
  ) -> bool {
    match self {
      Self::Base(base_matcher) => match base_matcher {
        Leaf::LogLevel(log_level_matcher) => log_level_matcher.do_match(log_level),
        Leaf::LogType(level) => *level == log_type.0,
        Leaf::String(input, criteria) => input
          .get(message, fields)
          .is_some_and(|input| criteria.evaluate(input.as_ref())),
        Leaf::Any => true,
      },
      Self::Or(or_matchers) => or_matchers
        .iter()
        .any(|matcher| matcher.do_match(log_type, log_level, message, fields)),
      Self::And(and_matchers) => and_matchers
        .iter()
        .all(|matcher| matcher.do_match(log_type, log_level, message, fields)),
      Self::Not(sub_matcher) => !sub_matcher.do_match(log_type, log_level, message, fields),
    }
  }
}

// Describes a string match criteria.
#[derive(Debug)]
pub enum Str {
  // Evalutes to true if the candidates string matches a regex.
  Regex(Regex),

  // Evalutes to true if the candidate string begins with a prefix.
  Prefix(String),

  // Evaluates to true if the candidate string is an exact match.
  Exact(String),
}

impl Str {
  fn new(match_type: StringMatchType, value: &str) -> Result<Self> {
    match match_type {
      StringMatchType::EXACT => Ok(Self::Exact(value.to_string())),
      StringMatchType::PREFIX => Ok(Self::Prefix(value.to_string())),
      StringMatchType::REGEX => Ok(Self::Regex(Regex::new(value)?)),
    }
  }

  fn evaluate(&self, candidate: &str) -> bool {
    match self {
      Self::Regex(r) => r.is_match(candidate),
      Self::Prefix(prefix) => candidate.starts_with(prefix),
      Self::Exact(exact) => candidate == exact,
    }
  }
}

/// Describes a match criteria against an integer (used for log level matching).
#[derive(Debug)]
pub struct Integer {
  operator: ComparisonOperator,
  value: u32,
}

impl Integer {
  const fn new(operator: ComparisonOperator, value: u32) -> Self {
    Self { operator, value }
  }

  const fn do_match(&self, log_level: u32) -> bool {
    match self.operator {
      ComparisonOperator::LESS_THAN => log_level < self.value,
      ComparisonOperator::LESS_THAN_OR_EQUAL => log_level <= self.value,
      ComparisonOperator::EQUALS => log_level == self.value,
      ComparisonOperator::GREATER_THAN => log_level > self.value,
      ComparisonOperator::GREATER_THAN_OR_EQUAL => log_level >= self.value,
    }
  }
}

#[derive(Debug)]
pub enum InputType {
  Message,
  Field(String),
}

impl InputType {
  fn get<'a>(&self, message: &'a LogMessage, fields: FieldsRef<'a>) -> Option<Cow<'a, str>> {
    match self {
      Self::Message => message.as_str().map(Cow::Borrowed),
      Self::Field(field_key) => fields.field_value(field_key),
    }
  }
}

/// Describes a compiled leaf node in the match tree. Each tree node evaluates to either
/// true or false based on its match criteria.
#[derive(Debug)]
pub enum Leaf {
  /// Match against the log level.
  LogLevel(Integer),

  /// Match against either the tag or the message using a string matcher.
  String(InputType, Str),

  // Match against a specific log type.
  LogType(u32),

  /// Always matches.
  Any,
}

impl Leaf {
  fn new(config: &BaseLogMatcher) -> Result<Self> {
    match config.match_type.as_ref().ok_or(Error::InvalidConfig)? {
      base_log_matcher::Match_type::LogLevelMatch(log_level) => Ok(Self::LogLevel(Integer::new(
        log_level
          .operator
          .enum_value()
          .map_err(|_| Error::UnknownFieldOrEnum)?,
        log_level
          .log_level
          .value()
          .try_into()
          .map_err(|_| Error::UnknownFieldOrEnum)?,
      ))),
      base_log_matcher::Match_type::MessageMatch(message_match) => Ok(Self::String(
        InputType::Message,
        Str::new(
          message_match
            .match_type
            .enum_value()
            .map_err(|_| Error::UnknownFieldOrEnum)?,
          message_match.match_value.as_str(),
        )?,
      )),
      base_log_matcher::Match_type::TagMatch(tag_match) => Ok(Self::String(
        InputType::Field(tag_match.tag_key.clone()),
        Str::new(
          tag_match
            .match_type
            .enum_value()
            .map_err(|_| Error::UnknownFieldOrEnum)?,
          tag_match.match_value.as_str(),
        )?,
      )),
      base_log_matcher::Match_type::AnyMatch(_) => Ok(Self::Any),
      base_log_matcher::Match_type::TypeMatch(type_match) => Ok(Self::LogType(type_match.type_)),
    }
  }
}
