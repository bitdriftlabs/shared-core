// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::input::Scriptable;
use crate::output::FrameData;
use crate::{FeatureFlag, ScriptValue};
use std::collections::BTreeMap;
use vrl::compiler::{OwnedValueOrRef, SecretTarget, Target};
use vrl::core::Value;
use vrl::path::{OwnedSegment, OwnedTargetPath, PathPrefix};

const GROUPING_KEY: &str = "grouping_key";
const ERRORS_KEY: &str = "errors";
const METRICS_KEY: &str = "metrics";
const STACKTRACE_KEY: &str = "stack_trace";
const IS_SIGNIFICANT_KEY: &str = "is_significant";

pub struct ReportTarget<'a, T: Scriptable> {
  report: &'a T,
  feature_flags: ScriptValue,
  pub grouping_key: Option<String>,
  pub metrics: BTreeMap<String, ScriptValue>,
  pub significant_frame_data: FrameData,
}

impl<T: Scriptable> std::fmt::Debug for ReportTarget<'_, T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ReportTarget")
      .field("feature_flags", &self.feature_flags)
      .field("grouping_key", &self.grouping_key)
      .field("metrics", &self.metrics)
      .field("significant_frame_data", &self.significant_frame_data)
      .finish()
  }
}

impl<'a, T: Scriptable> ReportTarget<'a, T> {
  pub fn new(report: &'a T, feature_flags: &[FeatureFlag]) -> Self {
    Self {
      report,
      feature_flags: Value::Array(feature_flags.iter().map(Into::into).collect()).into(),
      grouping_key: None,
      metrics: BTreeMap::new(),
      significant_frame_data: BTreeMap::new(),
    }
  }
}

impl<T: Scriptable> SecretTarget for ReportTarget<'_, T> {
  fn get_secret(&self, _key: &str) -> Option<&str> {
    None
  }

  fn insert_secret(&mut self, _key: &str, _value: &str) {}

  fn remove_secret(&mut self, _key: &str) {}
}

impl<T: Scriptable> Target for ReportTarget<'_, T> {
  fn target_insert(&mut self, path: &OwnedTargetPath, value: Value) -> Result<(), String> {
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
        self.metrics.insert(metric_name.as_str().to_owned(), value.into());
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
      "feature_flags" => match self.feature_flags.resolve(&components[1 ..]) {
        Ok(value) => Ok(value.map(|v| OwnedValueOrRef::Owned(v.0))),
        Err(path_err) => Err(path_err.to_string()),
      },
      _ => match self.report.resolve(&components[..]) {
        Ok(value) => Ok(value.map(|v| OwnedValueOrRef::Owned(v.0))),
        Err(path_err) => Err(path_err.to_string()),
      },
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
