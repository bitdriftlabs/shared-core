// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::json::{JSONFetchError, JSONFetchErrorType, get_value};
use vrl::compiler::OwnedValueOrRef;
use vrl::core::Value as ScriptValue;
use vrl::prelude::NotNan;

#[rstest::rstest]
// missing value
#[case(".missing", Err(JSONFetchError { index: 0, segment_path: ".missing".to_string(), error_type: JSONFetchErrorType::NoFieldWithName }))]
// out of range array index
#[case(".items[5][0]", Err(JSONFetchError { index: 1, segment_path: ".items[5]".to_string(), error_type: JSONFetchErrorType::IndexOutOfRange }))]
// array index on non-array
#[case(".counter[1]", Err(JSONFetchError { index: 0, segment_path: ".counter".to_string(), error_type: JSONFetchErrorType::NotAnArray }))]
// fetch field on non-object
#[case(".done.foo", Err(JSONFetchError { index: 0, segment_path: ".done".to_string(), error_type: JSONFetchErrorType::NotAnObject }))]
// string value in array
#[case(".items[0]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Bytes("beets".into())))))]
// float value in array
#[case(".items[1]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Float(NotNan::new(2.5).unwrap())))))]
// integer value in array
#[case(
  ".items[2]",
  Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Integer(319))))
)]
// string value in object in array
#[case(".items[3].age", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Bytes("old".into())))))]
// bool value
#[case(".done", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Boolean(false)))))]
// null value
#[case(".counter", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Null))))]
fn test_get_json_object_value(
  #[case] path: &str,
  #[case] expected: Result<Option<OwnedValueOrRef<'_>>, JSONFetchError>,
) {
  let json = serde_json::json!({"items": ["beets", 2.5, 319, {"age": "old"}, ["a", null]], "done": false, "counter": null});
  let actual = get_value(&json, &path.parse().unwrap());
  if expected.is_ok() {
    let actual_val = actual
      .expect("is not ok")
      .map(OwnedValueOrRef::into_owned_value);
    let expected_val = expected.unwrap().map(OwnedValueOrRef::into_owned_value);
    assert_eq!(actual_val, expected_val);
  } else {
    assert_eq!(expected.err().unwrap(), actual.err().unwrap());
  }
}

#[rstest::rstest]
// out of range array index
#[case("[25][0]", Err(JSONFetchError { index: 0, segment_path: "[25]".to_string(), error_type: JSONFetchErrorType::IndexOutOfRange }))]
// array index on non-array
#[case("[0][1]", Err(JSONFetchError { index: 0, segment_path: "[0]".to_string(), error_type: JSONFetchErrorType::NotAnArray }))]
// fetch field on non-object
#[case("[0].foo", Err(JSONFetchError { index: 0, segment_path: "[0]".to_string(), error_type: JSONFetchErrorType::NotAnObject }))]
// string value
#[case("[0]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Bytes("beets".into())))))]
// float value
#[case("[1]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Float(NotNan::new(2.5).unwrap())))))]
// integer value
#[case("[2]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Integer(319)))))]
// string value in object
#[case("[3].age", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Bytes("old".into())))))]
// bool value
#[case("[5]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Boolean(true)))))]
// null value
#[case("[6]", Ok(Some(OwnedValueOrRef::Owned(ScriptValue::Null))))]
fn test_get_json_array_value(
  #[case] path: &str,
  #[case] expected: Result<Option<OwnedValueOrRef<'_>>, JSONFetchError>,
) {
  let json = serde_json::json!(["beets", 2.5, 319, {"age": "old"}, ["a", null], true, null]);
  let actual = get_value(&json, &path.parse().unwrap());
  if expected.is_ok() {
    let actual_val = actual
      .expect("is not ok")
      .map(OwnedValueOrRef::into_owned_value);
    let expected_val = expected.unwrap().map(OwnedValueOrRef::into_owned_value);
    assert_eq!(actual_val, expected_val);
  } else {
    assert_eq!(expected.err().unwrap(), actual.err().unwrap());
  }
}
