// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./generate_log_test.rs"]
mod generate_log_test;

use crate::workflow::TraversalExtractions;
use action::action_generate_log::generated_field::Generated_field_value_type;
use action::action_generate_log::value_reference::Value_reference_type;
use action::action_generate_log::ValueReference;
use action::ActionGenerateLog;
use bd_log_primitives::{log_level, FieldsRef, Log, LogField, LogType, StringOrBytes};
use bd_matcher::FieldProvider;
use bd_proto::protos::workflow::workflow::workflow::action;
use bd_proto::protos::workflow::workflow::workflow::action::action_generate_log::ValueReferencePair;
use std::borrow::Cow;
use std::fmt::Display;
use time::OffsetDateTime;

enum StringOrFloat<'a> {
  String(Cow<'a, str>),
  Float(f64),
}

impl Display for StringOrFloat<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      StringOrFloat::String(s) => write!(f, "{s}"),
      StringOrFloat::Float(float) => write!(f, "{float}"),
    }
  }
}

#[allow(clippy::cast_precision_loss)]
fn fractional_seconds_since_epoch(time: OffsetDateTime) -> f64 {
  let seconds = time.unix_timestamp();
  let nanoseconds = time.nanosecond();
  seconds as f64 + (f64::from(nanoseconds) / 1_000_000_000.0)
}

fn resolve_reference<'a>(
  extractions: &'a TraversalExtractions,
  reference: &'a ValueReference,
  current_log_fields: &'a FieldsRef<'a>,
) -> Option<StringOrFloat<'a>> {
  match reference.value_reference_type.as_ref()? {
    Value_reference_type::Fixed(value) => Some(StringOrFloat::String(value.into())),
    Value_reference_type::FieldFromCurrentLog(field_name) => current_log_fields
      .field_value(field_name)
      .map(StringOrFloat::String),
    Value_reference_type::SavedFieldId(saved_field_id) => extractions
      .fields
      .as_ref()
      .and_then(|fields| fields.get(saved_field_id))
      .map(|field| StringOrFloat::String(field.into())),
    Value_reference_type::SavedTimestampId(saved_timestamp_id) => extractions
      .timestamps
      .as_ref()
      .and_then(|timestamps| timestamps.get(saved_timestamp_id))
      .map(|timestamp| StringOrFloat::Float(fractional_seconds_since_epoch(*timestamp))),
  }
}

// This will end up returning NaN if the pair is invalid in any way.
fn pair_to_floats(
  extractions: &TraversalExtractions,
  pair: &ValueReferencePair,
  current_log_fields: &FieldsRef<'_>,
) -> (f64, f64) {
  fn to_float(string_or_float: Option<StringOrFloat<'_>>) -> f64 {
    match string_or_float {
      Some(StringOrFloat::String(string)) => string.parse::<f64>().unwrap_or(f64::NAN),
      Some(StringOrFloat::Float(float)) => float,
      None => f64::NAN,
    }
  }

  let lhs = to_float(resolve_reference(
    extractions,
    &pair.lhs,
    current_log_fields,
  ));
  let rhs = to_float(resolve_reference(
    extractions,
    &pair.rhs,
    current_log_fields,
  ));
  (lhs, rhs)
}

pub fn generate_log_action(
  extractions: &TraversalExtractions,
  action: &ActionGenerateLog,
  current_log_fields: &FieldsRef<'_>,
) -> Option<Log> {
  let message = action.message.clone();
  let mut fields = vec![];
  for field in &action.fields {
    let value = match field.generated_field_value_type.as_ref()? {
      Generated_field_value_type::Single(reference) => {
        resolve_reference(extractions, reference, current_log_fields)
      },
      Generated_field_value_type::Subtract(pair) => {
        let (lhs, rhs) = pair_to_floats(extractions, pair, current_log_fields);
        Some(StringOrFloat::Float(lhs - rhs))
      },
      Generated_field_value_type::Add(pair) => {
        let (lhs, rhs) = pair_to_floats(extractions, pair, current_log_fields);
        Some(StringOrFloat::Float(lhs + rhs))
      },
      Generated_field_value_type::Multiply(pair) => {
        let (lhs, rhs) = pair_to_floats(extractions, pair, current_log_fields);
        Some(StringOrFloat::Float(lhs * rhs))
      },
      Generated_field_value_type::Divide(pair) => {
        let (lhs, rhs) = pair_to_floats(extractions, pair, current_log_fields);
        Some(StringOrFloat::Float(lhs / rhs))
      },
    };

    if let Some(value) = value {
      fields.push(LogField {
        key: field.name.clone(),
        value: StringOrBytes::String(value.to_string()),
      });
    }
  }

  Some(Log {
    log_level: log_level::DEBUG,
    log_type: LogType::Normal,
    fields,
    message: StringOrBytes::String(message),
    matching_fields: vec![LogField {
      key: "_generate_log_id".to_string(),
      value: StringOrBytes::String(action.id.to_string()),
    }],
    // These will be filled in later via the log processor.
    session_id: String::new(),
    occurred_at: OffsetDateTime::UNIX_EPOCH,
  })
}
