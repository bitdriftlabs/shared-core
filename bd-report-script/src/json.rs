// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./json_test.rs"]
mod test;

use serde_json::Value as JSONValue;
use std::fmt::Display;
use vrl::compiler::OwnedValueOrRef;
use vrl::core::Value as ScriptValue;
use vrl::path::{OwnedSegment, OwnedTargetPath, PathPrefix};
use vrl::prelude::NotNan;

/// Retrieve an element from a JSON value using a target path
///
/// # Example
///
/// ```
/// use vrl::compiler::OwnedValueOrRef;
/// use vrl::core::Value;
/// use bd_report_script::get_json_value;
///
/// let json = serde_json::json!({"key": ["one", 2, null]});
/// let value = get_json_value(&json, &".key[1]".parse().unwrap());
/// let unwrapped = value.unwrap().unwrap().into_owned_value();
/// assert_eq!(unwrapped, Value::Integer(2));
/// ```
pub fn get_value<'a>(
  base_value: &'a JSONValue,
  path: &OwnedTargetPath,
) -> Result<Option<OwnedValueOrRef<'a>>, JSONFetchError> {
  if path.prefix == PathPrefix::Metadata {
    return Ok(None);
  }

  let mut path_value = base_value;
  let components = &path.path.segments;
  for (index, component) in components.iter().enumerate() {
    match component {
      OwnedSegment::Index(ary_index) => {
        if let Some(ary) = path_value.as_array() {
          if let Some(element) = &ary.get(usize::try_from(*ary_index).unwrap_or_default()) {
            path_value = element;
          } else {
            return Err(JSONFetchError::from_segments(
              components,
              index,
              JSONFetchErrorType::IndexOutOfRange,
            ));
          }
        } else {
          return Err(JSONFetchError::from_segments(
            components,
            if index > 0 { index - 1 } else { 0 }, // the index of the non-array
            JSONFetchErrorType::NotAnArray,
          ));
        }
      },
      OwnedSegment::Field(name) => {
        if let Some(obj) = path_value.as_object() {
          if let Some(element) = obj.get(name.as_str()) {
            path_value = element;
          } else {
            return Err(JSONFetchError::from_segments(
              components,
              index,
              JSONFetchErrorType::NoFieldWithName,
            ));
          }
        } else {
          return Err(JSONFetchError::from_segments(
            components,
            if index > 0 { index - 1 } else { 0 }, // the index of the non-object
            JSONFetchErrorType::NotAnObject,
          ));
        }
      },
    }
  }

  Ok(Some(OwnedValueOrRef::Owned(convert_value_type(path_value))))
}

fn convert_value_type(value: &JSONValue) -> ScriptValue {
  match value {
    JSONValue::Array(values) => ScriptValue::Array(values.iter().map(convert_value_type).collect()),
    JSONValue::Bool(bool_val) => ScriptValue::Boolean(*bool_val),
    JSONValue::Null => ScriptValue::Null,
    JSONValue::Number(num_val) => num_val.as_i64().map_or_else(
      || {
        let float_val = num_val.as_f64().unwrap_or_default();
        ScriptValue::Float(NotNan::new(float_val).unwrap_or_default())
      },
      ScriptValue::Integer,
    ),
    JSONValue::Object(values) => ScriptValue::Object(
      values
        .iter()
        .map(|(k, v)| (k.as_str().into(), convert_value_type(v)))
        .collect(),
    ),
    JSONValue::String(str_val) => ScriptValue::Bytes(str_val.to_owned().into()),
  }
}

#[derive(Debug, PartialEq, Eq)]
pub enum JSONFetchErrorType {
  NoFieldWithName,
  NotAnArray,
  NotAnObject,
  IndexOutOfRange,
}

#[derive(Debug, PartialEq, Eq)]
pub struct JSONFetchError {
  pub index: usize,
  pub segment_path: String,
  pub error_type: JSONFetchErrorType,
}

impl JSONFetchError {
  fn from_segments(
    segments: &[OwnedSegment],
    index: usize,
    error_type: JSONFetchErrorType,
  ) -> Self {
    Self {
      index,
      segment_path: segments[0 ..= index]
        .iter()
        .map(|c| match c {
          OwnedSegment::Index(idx) => format!("[{idx}]"),
          OwnedSegment::Field(key) => format!(".{key}"),
        })
        .collect::<String>(),
      error_type,
    }
  }
}

impl Display for JSONFetchError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let message = match self.error_type {
      JSONFetchErrorType::NotAnArray => "is not an array",
      JSONFetchErrorType::NoFieldWithName => "is not an existing field",
      JSONFetchErrorType::NotAnObject => "is not an object",
      JSONFetchErrorType::IndexOutOfRange => "is out of range",
    };
    write!(f, "{} {message}", self.segment_path)
  }
}
