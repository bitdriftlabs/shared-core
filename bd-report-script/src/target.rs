// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{FeatureFlag, Output, ScriptValue};
use bd_log_primitives::{LogArrayData, LogFieldValue, LogMapData};
use ordered_float::NotNan;
use std::collections::BTreeMap;
use vrl::compiler::{OwnedValueOrRef, SecretTarget, Target};
use vrl::path::{OwnedSegment, OwnedTargetPath, PathPrefix};
use vrl::value::value::simdutf_bytes_utf8_lossy;

const GROUPING_KEY: &str = "grouping_key";
const ERRORS_KEY: &str = "errors";
const METRICS_KEY: &str = "metrics";
const STACKTRACE_KEY: &str = "stack_trace";
const IS_SIGNIFICANT_KEY: &str = "is_significant";

#[derive(Debug)]
pub struct ReportTarget {
  report: ScriptValue,
  feature_flags: ScriptValue,
  grouping_key: Option<String>,
  metrics: BTreeMap<String, ScriptValue>,
  significant_frame_data: BTreeMap<isize, BTreeMap<isize, bool>>,
}

impl ReportTarget {
  pub fn new<T: Into<ScriptValue>>(report: T, feature_flags: &[FeatureFlag]) -> Self {
    Self {
      report: report.into(),
      feature_flags: ScriptValue::Array(feature_flags.iter().map(Into::into).collect()),
      grouping_key: None,
      metrics: BTreeMap::new(),
      significant_frame_data: BTreeMap::new(),
    }
  }

  pub fn significant_frame_path(error_index: isize, frame_index: isize) -> OwnedTargetPath {
    OwnedTargetPath::metadata_root()
      .with_field_appended(ERRORS_KEY)
      .with_index_appended(error_index)
      .with_field_appended(STACKTRACE_KEY)
      .with_index_appended(frame_index)
      .with_field_appended(IS_SIGNIFICANT_KEY)
  }

  pub fn metrics_path(name: &str) -> OwnedTargetPath {
    OwnedTargetPath::metadata_root()
      .with_field_appended(METRICS_KEY)
      .with_field_appended(name)
  }

  pub fn grouping_key_path() -> OwnedTargetPath {
    OwnedTargetPath::metadata_root().with_field_appended(GROUPING_KEY)
  }
}

impl From<&FeatureFlag> for ScriptValue {
  fn from(flag: &FeatureFlag) -> Self {
    Self::Object(BTreeMap::from([
      ("name".into(), Self::Bytes(flag.name.clone().into())),
      (
        "value".into(),
        flag
          .value
          .clone()
          .map_or(Self::Null, |v| Self::Bytes(v.into())),
      ),
      (
        "last_updated".into(),
        Self::Timestamp(std::time::SystemTime::from(flag.last_updated).into()),
      ),
    ]))
  }
}

impl From<ReportTarget> for Output {
  fn from(value: ReportTarget) -> Self {
    Self {
      grouping_key: value.grouping_key,
      metrics: value
        .metrics
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), into_log_field_value(value)))
        .collect(),
      significant_frame_data: value.significant_frame_data,
    }
  }
}

fn into_log_field_value(value: &ScriptValue) -> LogFieldValue {
  match value {
    ScriptValue::Null => LogFieldValue::I64(0),
    ScriptValue::Bytes(bytes) => LogFieldValue::String(simdutf_bytes_utf8_lossy(bytes).to_string()),
    ScriptValue::Integer(value) => LogFieldValue::I64(*value),
    // using different major versions of ordered_float dep
    ScriptValue::Float(value) => LogFieldValue::Double(NotNan::new(**value).unwrap_or_default()),
    ScriptValue::Boolean(value) => LogFieldValue::Boolean(*value),
    ScriptValue::Regex(value) => LogFieldValue::String(value.as_str().to_owned()),
    ScriptValue::Timestamp(value) => LogFieldValue::String(value.to_rfc3339()),
    ScriptValue::Array(values) => LogFieldValue::Array(LogArrayData::new(
      values.iter().map(into_log_field_value).collect(),
    )),
    ScriptValue::Object(values) => LogFieldValue::Map(LogMapData::new(
      values
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), into_log_field_value(value)))
        .collect(),
    )),
  }
}

impl SecretTarget for ReportTarget {
  fn get_secret(&self, _key: &str) -> Option<&str> {
    None
  }

  fn insert_secret(&mut self, _key: &str, _value: &str) {}

  fn remove_secret(&mut self, _key: &str) {}
}

impl Target for ReportTarget {
  fn target_insert(&mut self, path: &OwnedTargetPath, value: ScriptValue) -> Result<(), String> {
    let components = &path.path.segments;
    if components.is_empty() || path.prefix != PathPrefix::Metadata {
      return Ok(());
    }

    let Some(OwnedSegment::Field(name)) = components.first() else {
      return Ok(());
    };
    match name.as_str() {
      GROUPING_KEY if components.len() == 1 => {
        self.grouping_key = value.as_str().as_deref().map(ToString::to_string);
        Ok(())
      },
      METRICS_KEY // insert metrics with path like `metrics.{name}`
        if components.len() == 2
          && let Some(OwnedSegment::Field(metric_name)) = components.get(1) =>
      {
        self.metrics.insert(metric_name.as_str().to_owned(), value);
        Ok(())
      },
      ERRORS_KEY // set grouping hints with path like `errors[n].stack_trace[m].is_significant`
        if components.len() == 5
          && let Some(OwnedSegment::Index(error_index)) = components.get(1)
          && let Some(OwnedSegment::Field(error_field)) = components.get(2)
          && error_field.as_str() == STACKTRACE_KEY
          && let Some(OwnedSegment::Index(frame_index)) = components.get(3)
          && let Some(OwnedSegment::Field(frame_field)) = components.get(4)
          && frame_field.as_str() == IS_SIGNIFICANT_KEY =>
      {
        let error_data = self.significant_frame_data.entry(*error_index).or_default();
        if value.is_null() {
          error_data.remove(frame_index);
        } else if let Some(bool_value) = value.as_boolean() {
          error_data.insert(*frame_index, bool_value);
        } else {
          return Err("significant frame data must be a null or boolean value".to_string());
        }
        Ok(())
      },
      _ => Err("metadata must correspond to known keys".to_string()),
    }
  }

  fn target_get(
    &mut self,
    path: &vrl::path::OwnedTargetPath,
  ) -> Result<Option<vrl::compiler::OwnedValueOrRef<'_>>, String> {
    let components = &path.path.segments;
    if path.prefix != PathPrefix::Event || components.is_empty() {
      return Ok(None);
    }
    let OwnedSegment::Field(name) = &components[0] else {
      return Err("scope is not an array".to_string());
    };
    match name.as_str() {
      "feature_flags" => Ok(
        self
          .feature_flags
          .get(&components[1 ..])
          .map(OwnedValueOrRef::Ref),
      ),
      _ => Ok(self.report.get(&components[..]).map(OwnedValueOrRef::Ref)),
    }
  }

  fn target_remove(
    &mut self,
    _path: &vrl::path::OwnedTargetPath,
    _compact: bool,
  ) -> Result<Option<vrl::prelude::Value>, String> {
    Ok(None)
  }
}
