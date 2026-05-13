// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::ScriptValue;
use crate::input::Scriptable;
use crate::target::ReportTarget;
use bd_log_primitives::{LogArrayData, LogFieldValue, LogMapData};
use ordered_float::NotNan;
use std::collections::BTreeMap;
use std::ops::Add;
use vrl::value::value::{Value, simdutf_bytes_utf8_lossy};

#[derive(Clone, Debug, Default)]
pub struct Output {
  pub grouping_hints: GroupingHints,
  pub metrics: BTreeMap<String, LogFieldValue>,
}

pub type ErrorIndex = isize;
pub type StackFrameIndex = isize;
pub type FrameData = BTreeMap<ErrorIndex, BTreeMap<StackFrameIndex, bool>>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GroupingHints {
  pub grouping_key: Option<String>,
  /// mapping of error index to frame index to whether a frame is significant
  /// for grouping
  significant_frame_data: FrameData,
}

impl GroupingHints {
  #[must_use]
  pub fn is_significant_frame(&self, error_index: isize, frame_index: isize) -> Option<bool> {
    self
      .significant_frame_data
      .get(&error_index)
      .and_then(|error| error.get(&frame_index))
      .copied()
  }

  #[must_use]
  pub fn new(grouping_key: Option<String>, frame_data: FrameData) -> Self {
    Self {
      grouping_key,
      significant_frame_data: frame_data,
    }
  }
}

impl Add for GroupingHints {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    let significant_frame_data = rhs
      .significant_frame_data
      .keys()
      .chain(self.significant_frame_data.keys())
      .map(|err_index| {
        let rhs_frame_data = rhs
          .significant_frame_data
          .get(err_index)
          .map(ToOwned::to_owned);
        let frame_data = self
          .significant_frame_data
          .get(err_index)
          .map(|frame_data| {
            let mut frame_data = frame_data.to_owned();
            frame_data.append(&mut rhs_frame_data.clone().unwrap_or_default());
            frame_data
          })
          .or(rhs_frame_data)
          .unwrap_or_default();
        (*err_index, frame_data)
      })
      .collect();
    Self {
      grouping_key: rhs.grouping_key.or(self.grouping_key),
      significant_frame_data,
    }
  }
}

impl Output {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }
}

impl Add for Output {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    let mut metrics = self.metrics;
    metrics.append(&mut rhs.metrics.clone());
    Self {
      grouping_hints: self.grouping_hints + rhs.grouping_hints,
      metrics,
    }
  }
}

impl<T: Scriptable> From<ReportTarget<'_, T>> for Output {
  fn from(value: ReportTarget<'_, T>) -> Self {
    Self {
      grouping_hints: GroupingHints {
        grouping_key: value.grouping_key,
        significant_frame_data: value.significant_frame_data,
      },
      metrics: value
        .metrics
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), into_log_field_value(&value.0)))
        .collect(),
    }
  }
}

impl From<&ScriptValue> for LogFieldValue {
  fn from(value: &ScriptValue) -> Self {
    into_log_field_value(&value.0)
  }
}

fn into_log_field_value(value: &Value) -> LogFieldValue {
  match value {
    Value::Null => LogFieldValue::I64(0),
    Value::Bytes(bytes) => LogFieldValue::String(simdutf_bytes_utf8_lossy(bytes).to_string()),
    Value::Integer(value) => LogFieldValue::I64(*value),
    // using different major versions of ordered_float dep
    Value::Float(value) => LogFieldValue::Double(NotNan::new(**value).unwrap_or_default()),
    Value::Boolean(value) => LogFieldValue::Boolean(*value),
    Value::Regex(value) => LogFieldValue::String(value.as_str().to_owned()),
    Value::Timestamp(value) => LogFieldValue::String(value.to_rfc3339()),
    Value::Array(values) => LogFieldValue::Array(LogArrayData::new(
      values.iter().map(into_log_field_value).collect(),
    )),
    Value::Object(values) => LogFieldValue::Map(LogMapData::new(
      values
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), into_log_field_value(value)))
        .collect(),
    )),
  }
}
