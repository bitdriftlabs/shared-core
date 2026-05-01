// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{FeatureFlag, Script};
use bd_log_primitives::{LogFieldValue, LogMapData};
use ordered_float::NotNan;
use serde_json::json;
use time::OffsetDateTime;

#[test]
fn compute_interpolated_key() {
  let script = Script::new(
    "
    app_id = .app_metrics.app_id
    error_name = .errors[0].name
    class_name = .errors[0].stacktrace[0].class_name
    set_grouping_key(\"{{ app_id }}-{{ error_name }}-{{ class_name }}\")
    ",
  )
  .expect("is ok");
  let report = json!({
    "app_metrics": {"app_id": "com.example.myapp"},
    "errors": [{
      "name": "NullPointerException",
      "stacktrace": [{"class_name": "Builder", "symbol_name": "build()"}]
    }],
  });
  let output = script.run(&report, &[]).expect("can run");
  assert_eq!(
    Some("com.example.myapp-NullPointerException-Builder".to_string()),
    output.grouping_key
  );
}

#[test]
fn unset_grouping_key() {
  let script = Script::new(
    "
    app_id = .app_metrics.app_id
    error_name = .errors[0].name
    class_name = .errors[0].stacktrace[0].class_name
    _key = set_grouping_key(\"{{ app_id }}-{{ error_name }}-{{ class_name }}\")
    set_grouping_key(null)
    ",
  )
  .expect("is ok");
  let report = json!({
    "app_metrics": {"app_id": "com.example.myapp"},
    "errors": [{
      "name": "NullPointerException",
      "stacktrace": [{"class_name": "Builder", "symbol_name": "build()"}]
    }],
  });
  let output = script.run(&report, &[]).expect("can run");
  assert_eq!(None, output.grouping_key);
}

#[test]
fn type_hints_are_working() {
  // report.type is a known string key, so we can call contains()
  Script::new(
    "
    is_web = contains(.type, \"web\")
    set_grouping_key(string!(is_web))
    ",
  )
  .expect("is ok");

  // unknown_field hasn't been declared anywhere, and needs to be type-cast
  let script = Script::new(
    "
    has_unknown = contains(.unknown_field, \"thing\")
    set_grouping_key(string!(has_unknown))
    ",
  )
  .expect_err("is not ok");
  assert!(script.to_string().contains("exact type"));
}

#[test]
fn set_significant_frame() {
  let script = Script::new(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stack_trace) -> |frame_index, frame| {
        if starts_with(frame.class_name, \"os.\") || contains(frame.class_name, \".rx.\") {
          set_significant_frame(error_index, frame_index, false)
        } else if starts_with(frame.class_name, \"com.myapp.\") {
          set_significant_frame(error_index, frame_index)
        }
      }
    }
    ",
  )
  .expect("is ok");
  let report = json!({
  "app_metrics": {"app_id": "com.example.myapp"},
  "errors": [
    {"name": "NullPointerException",
       "stack_trace": [
         {"class_name": "os.Builder", "symbol_name": "build()"},
         {"class_name": "com.myapp.Layout", "symbol_name": "repaint()"},
         {"class_name": "com.somelib.Engine", "symbol_name": "drawScreen()"},
         {"class_name": "com.myapp.App", "symbol_name": "new()"},
       ],
    },
    {"name": "SomeRxProblem",
       "stack_trace": [
         {"class_name": "com.myapp.rx.Layout", "symbol_name": "repaint()"},
       ],
    },
  ]});
  let output = script.run(&report, &[]).expect("can run");
  assert_eq!(Some(false), output.is_significant_frame(0, 0));
  assert_eq!(Some(true), output.is_significant_frame(0, 1));
  assert_eq!(None, output.is_significant_frame(0, 2));
  assert_eq!(Some(true), output.is_significant_frame(0, 3));
  assert_eq!(Some(false), output.is_significant_frame(1, 0));
}

#[test]
fn unset_significant_frame() {
  let script = Script::new(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stack_trace) -> |frame_index, frame| {
        if starts_with(frame.class_name, \"os.\") {
          set_significant_frame(error_index, frame_index, true)
          set_significant_frame(error_index, frame_index, null)
        }
      }
    }
    ",
  )
  .expect("is ok");
  let report = json!({
  "app_metrics": {"app_id": "com.example.myapp"},
  "errors": [{
    "name": "NullPointerException",
    "stack_trace": [
      {"class_name": "os.Builder", "symbol_name": "build()"},
      {"class_name": "com.myapp.Layout", "symbol_name": "repaint()"},
      {"class_name": "com.somelib.Engine", "symbol_name": "drawScreen()"},
      {"class_name": "com.myapp.App", "symbol_name": "new()"},
    ]}
  ]});
  let output = script.run(&report, &[]).expect("can run");
  assert_eq!(None, output.is_significant_frame(0, 0));
}

#[test]
fn emit_fields() {
  let script = Script::new(
    "
    app_id = .app_metrics.app_id
    error_name = .errors[0].name
    _a = add_field(\"some_tag\", \"{{ app_id }}-{{ error_name }}\")
    _b = add_field(\"chain_len\", length(.errors))
    add_field(\"stuff\", {\"x\": 1, \"y\": 15.0, \"z\": .feature_flags[0].name})
    ",
  )
  .expect("is ok");
  let report = json!({
  "app_metrics": {"app_id": "com.example.myapp"},
  "errors": [{
    "name": "NullPointerException",
    "stack_trace": [
      {"class_name": "os.Builder", "symbol_name": "build()"},
      {"class_name": "com.myapp.Layout", "symbol_name": "repaint()"},
      {"class_name": "com.somelib.Engine", "symbol_name": "drawScreen()"},
      {"class_name": "com.myapp.App", "symbol_name": "new()"},
    ]}
  ]});
  let output = script
    .run(
      &report,
      &[FeatureFlag {
        name: "in_slice".to_string(),
        value: None,
        last_updated: OffsetDateTime::now_utc(),
      }],
    )
    .expect("can run");
  assert_eq!(3, output.metrics.len());
  assert_eq!(
    &LogFieldValue::I64(1),
    output.metrics.get("chain_len").expect("has_value")
  );
  assert_eq!(
    &LogFieldValue::Map(LogMapData::new(
      [
        ("x".to_string(), LogFieldValue::I64(1)),
        (
          "y".to_string(),
          LogFieldValue::Double(NotNan::new(15.0).unwrap())
        ),
        ("z".to_string(), LogFieldValue::String("in_slice".into())),
      ]
      .into()
    )),
    output.metrics.get("stuff").expect("has value")
  );
  assert_eq!(
    &LogFieldValue::String("com.example.myapp-NullPointerException".to_owned()),
    output.metrics.get("some_tag").expect("has value")
  );
}
