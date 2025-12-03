// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./state_value_matcher_test.rs"]
mod tests;

use crate::matcher::IntMatch;
use crate::value_matcher::{DoubleMatch, StringMatch};
use anyhow::{Result, anyhow};
use bd_log_primitives::tiny_set::TinyMap;
use bd_proto::protos::state::matcher::StateValueMatch;
use bd_proto::protos::state::matcher::state_value_match::Value_match;

/// Matches state values (strings, ints, doubles, or checks if set).
#[derive(Clone, Debug)]
pub enum StateValueMatcher {
  String(StringMatch),
  Int(IntMatch),
  Double(DoubleMatch),
  IsSet,
}

impl StateValueMatcher {
  /// Creates a new `StateValueMatcher` from a proto `StateValueMatch`.
  pub fn try_from_proto(proto: &StateValueMatch) -> Result<Self> {
    let value_match = proto
      .value_match
      .as_ref()
      .ok_or_else(|| anyhow!("missing value_match in StateValueMatch"))?;

    match value_match {
      Value_match::StringValueMatch(m) => Ok(Self::String(StringMatch::from_proto(m)?)),
      Value_match::IntValueMatch(m) => Ok(Self::Int(IntMatch::from_proto(m)?)),
      Value_match::DoubleValueMatch(m) => Ok(Self::Double(DoubleMatch::from_proto(m)?)),
      Value_match::IsSetMatch(_) => Ok(Self::IsSet),
    }
  }

  /// Matches the given value (represented as an optional string slice).
  /// - For `IsSet` matcher: returns true if value is Some
  /// - For other matchers: returns false if value is None, otherwise parses and matches the value
  #[must_use]
  pub fn matches(&self, value: Option<&str>, extracted_fields: &TinyMap<String, String>) -> bool {
    match self {
      Self::IsSet => value.is_some(),
      Self::String(m) => value.is_some_and(|v| m.evaluate(v, extracted_fields)),
      Self::Int(m) => value
        .and_then(|v| v.parse::<i32>().ok())
        .is_some_and(|v| m.evaluate(v, extracted_fields)),
      Self::Double(m) => value
        .and_then(|v| v.parse::<f64>().ok())
        .is_some_and(|v| m.evaluate(v, extracted_fields)),
    }
  }
}
