// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::feature_flag::{FeatureFlag, FeatureFlagWrapper};
use crate::input::{PathError, Scriptable};
use crate::report::{ReportOutput, report_functions};
use crate::{Script, ScriptValue};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use vrl::core::Value;
use vrl::path::OwnedSegment;
use vrl::prelude::Collection;
use vrl::value::Kind;

#[derive(Clone, Debug)]
struct Error {
  name: String,
  stacktrace: Vec<String>,
}

#[derive(Clone, Debug)]
struct Report {
  app_id: String,
  errors: Vec<Error>,
}

#[test]
fn compute_interpolated_key() {
  let script = Script::new::<Report>(
    "
    app_id = .app_id
    error_name = .errors[0].name
    frame = .errors[0].stacktrace[0]
    set_grouping_key(\"{{ app_id }}-{{ error_name }}-{{ frame }}\")
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec!["Builder.build()".to_string(), "App.start()".to_string()],
    }],
  };
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some("com.example.myapp-NullPointerException-Builder.build()".to_string()),
    output.grouping_hints.grouping_key
  );
}

#[test]
fn unset_grouping_key() {
  let script = Script::new::<Report>(
    "
    app_id = .app_metrics.app_id
    error_name = .errors[0].name
    _key = set_grouping_key(\"{{ app_id }}-{{ error_name }}\")
    set_grouping_key(null)
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec!["Builder.build()".to_string(), "App.start()".to_string()],
    }],
  };
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(None, output.grouping_hints.grouping_key);
}

#[test]
fn abort_execution() {
  let script = Script::new::<Report>(
    "
    error_name = string!(.errors[0].name)
    if contains(error_name, \"Null\") {
      abort(\"skipping NPE\")
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec!["Builder.build()".to_string(), "App.start()".to_string()],
    }],
  };
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(Some("skipping NPE".to_string()), output.abort_message);
}

#[test]
fn abort_execution_without_message() {
  let script = Script::new::<Report>(
    "
    error_name = string!(.errors[0].name)
    if contains(error_name, \"Null\") {
      abort()
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec!["Builder.build()".to_string(), "App.start()".to_string()],
    }],
  };
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some("ended execution using abort()".to_string()),
    output.abort_message
  );
}

#[test]
fn type_hints_are_working() {
  // report.type is a known string key, so we can call contains()
  Script::new::<Report>(
    "
    is_mine = contains(.app_id, \"myapp\")
    set_grouping_key(string!(is_mine))
    ",
    report_functions(),
  )
  .expect("is ok");

  // unknown_field hasn't been declared anywhere, and needs to be type-cast
  let script = Script::new::<Report>(
    "
    has_unknown = contains(.unknown_field, \"thing\")
    set_grouping_key(string!(has_unknown))
    ",
    report_functions(),
  )
  .expect_err("is not ok");
  assert!(script.to_string().contains("exact type"));
}

#[test]
fn set_significant_frame() {
  let script = Script::new::<Report>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stacktrace) -> |frame_index, frame| {
        if starts_with(frame, \"os.\") || contains(frame, \".rx.\") {
          set_significant_frame(error_index, frame_index, false)
        } else if starts_with(frame, \"com.myapp.\") {
          set_significant_frame(error_index, frame_index)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![
      Error {
        name: "NullPointerException".to_string(),
        stacktrace: vec![
          "os.Builder.build()".to_string(),
          "com.myapp.Layout.repaint()".to_string(),
          "com.somelib.Engine.drawScreen()".to_string(),
          "com.myapp.App.new()".to_string(),
        ],
      },
      Error {
        name: "SomeRxProblem".to_string(),
        stacktrace: vec!["com.myapp.rx.Layout.repaint()".to_string()],
      },
    ],
  };
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some(false),
    output.grouping_hints.is_significant_frame(0, 0)
  );
  assert_eq!(Some(true), output.grouping_hints.is_significant_frame(0, 1));
  assert_eq!(None, output.grouping_hints.is_significant_frame(0, 2));
  assert_eq!(Some(true), output.grouping_hints.is_significant_frame(0, 3));
  assert_eq!(
    Some(false),
    output.grouping_hints.is_significant_frame(1, 0)
  );
}

#[test]
fn combined_significant_frame() {
  let script1 = Script::new::<Report>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stacktrace) -> |frame_index, frame| {
        if starts_with(frame, \"com.myapp.\") {
          set_significant_frame(error_index, frame_index, true)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let script2 = Script::new::<Report>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stacktrace) -> |frame_index, frame| {
        if starts_with(frame, \"os.\") || contains(frame, \".rx.\") {
          set_significant_frame(error_index, frame_index, false)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![
      Error {
        name: "NullPointerException".to_string(),
        stacktrace: vec![
          "os.Builder.build()".to_string(),
          "com.myapp.Layout.repaint()".to_string(),
          "com.somelib.Engine.drawScreen()".to_string(),
          "com.myapp.App.new()".to_string(),
        ],
      },
      Error {
        name: "SomeRxProblem".to_string(),
        stacktrace: vec!["com.myapp.rx.Layout.repaint()".to_string()],
      },
    ],
  };
  let output1: ReportOutput = script1.run(&report).expect("can run");
  let output2: ReportOutput = script2.run(&report).expect("can run");
  let combined = output1 + output2;
  assert_eq!(
    Some(false),
    combined.grouping_hints.is_significant_frame(0, 0)
  );
  assert_eq!(
    Some(true),
    combined.grouping_hints.is_significant_frame(0, 1)
  );
  assert_eq!(None, combined.grouping_hints.is_significant_frame(0, 2));
  assert_eq!(
    Some(true),
    combined.grouping_hints.is_significant_frame(0, 3)
  );
  assert_eq!(
    Some(false),
    combined.grouping_hints.is_significant_frame(1, 0)
  );
}

#[test]
fn unset_significant_frame() {
  let script = Script::new::<Report>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stacktrace) -> |frame_index, frame| {
        if starts_with(frame, \"os.\") {
          set_significant_frame(error_index, frame_index, true)
          set_significant_frame(error_index, frame_index, null)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec![
        "os.Builder.build()".to_string(),
        "com.myapp.Layout.repaint()".to_string(),
        "com.somelib.Engine.drawScreen()".to_string(),
        "com.myapp.App.new()".to_string(),
      ],
    }],
  };
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(None, output.grouping_hints.is_significant_frame(0, 0));
}

#[test]
fn emit_fields() {
  let script = Script::new::<Report>(
    "
    app_id = .app_id
    error_name = .errors[0].name
    _a = add_field(\"some_tag\", \"{{ app_id }}-{{ error_name }}\")
    add_field(\"flag_name\", string!(.feature_flags[0].name))
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec![
        "os.Builder.build()".to_string(),
        "com.myapp.Layout.repaint()".to_string(),
        "com.somelib.Engine.drawScreen()".to_string(),
        "com.myapp.App.new()".to_string(),
      ],
    }],
  };
  let object = FeatureFlagWrapper::new(
    &report,
    &[FeatureFlag {
      name: "in_slice".to_string(),
      value: None,
      last_updated: Some(OffsetDateTime::now_utc()),
    }],
  );
  let output: ReportOutput = script.run(&object).expect("can run");
  assert_eq!(2, output.metrics.len());
  assert_eq!(
    &"in_slice".to_string(),
    output.metrics.get("flag_name").expect("has value")
  );
  assert_eq!(
    &"com.example.myapp-NullPointerException".to_string(),
    output.metrics.get("some_tag").expect("has value")
  );
}

#[test]
fn combine_script_output_fields() {
  let script1 = Script::new::<Report>(
    "
    app_id = .app_id
    error_name = .errors[0].name
    _a = add_field(\"some_tag\", \"{{ app_id }}-{{ error_name }}\")
    add_field(\"chain_len\", string!(.feature_flags[0].name))
    ",
    report_functions(),
  )
  .expect("is ok");
  let script2 = Script::new::<Report>(
    "
    _b = add_field(\"chain_len\", \"override\")
    add_field(\"new_field\", \"x\")
    ",
    report_functions(),
  )
  .expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec![
        "os.Builder.build()".to_string(),
        "com.myapp.Layout.repaint()".to_string(),
        "com.somelib.Engine.drawScreen()".to_string(),
        "com.myapp.App.new()".to_string(),
      ],
    }],
  };

  let object = FeatureFlagWrapper::new(
    &report,
    &[FeatureFlag {
      name: "in_slice".to_string(),
      value: None,
      last_updated: Some(OffsetDateTime::now_utc()),
    }],
  );
  let output1: ReportOutput = script1.run(&object).expect("can run");
  let output2: ReportOutput = script2.run(&object).expect("can run");

  let combined = output1 + output2;
  assert_eq!(3, combined.metrics.len());
  assert_eq!(
    &"override".to_string(),
    combined.metrics.get("chain_len").expect("has_value")
  );
  assert_eq!(
    &"x".to_string(),
    combined.metrics.get("new_field").expect("has value")
  );
  assert_eq!(
    &"com.example.myapp-NullPointerException".to_string(),
    combined.metrics.get("some_tag").expect("has value")
  );
}

#[test]
fn override_set_value_for_script_output_grouping_key() {
  let script1 =
    Script::new::<Report>("set_grouping_key(\"some value\")", report_functions()).expect("is ok");
  let script2 =
    Script::new::<Report>("set_grouping_key(\"other value\")", report_functions()).expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec![
        "os.Builder.build()".to_string(),
        "com.myapp.Layout.repaint()".to_string(),
        "com.somelib.Engine.drawScreen()".to_string(),
        "com.myapp.App.new()".to_string(),
      ],
    }],
  };
  let output1: ReportOutput = script1.run(&report).expect("can run");
  let output2: ReportOutput = script2.run(&report).expect("can run");

  let combined = output1 + output2;
  assert_eq!(
    Some("other value".to_owned()),
    combined.grouping_hints.grouping_key
  );
}

#[test]
fn override_empty_value_for_script_output_grouping_key() {
  let script1 = Script::new::<Report>("", vec![]).expect("is ok");
  let script2 =
    Script::new::<Report>("set_grouping_key(\"other value\")", report_functions()).expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec![
        "os.Builder.build()".to_string(),
        "com.myapp.Layout.repaint()".to_string(),
        "com.somelib.Engine.drawScreen()".to_string(),
        "com.myapp.App.new()".to_string(),
      ],
    }],
  };
  let output1: ReportOutput = script1.run(&report).expect("can run");
  let output2: ReportOutput = script2.run(&report).expect("can run");

  let combined = output1 + output2;
  assert_eq!(
    Some("other value".to_owned()),
    combined.grouping_hints.grouping_key
  );
}

#[test]
fn use_set_value_for_script_output_grouping_key() {
  let script1 =
    Script::new::<Report>("set_grouping_key(\"set value\")", report_functions()).expect("is ok");
  let script2 = Script::new::<Report>("", vec![]).expect("is ok");
  let report = Report {
    app_id: "com.example.myapp".to_string(),
    errors: vec![Error {
      name: "NullPointerException".to_string(),
      stacktrace: vec![
        "os.Builder.build()".to_string(),
        "com.myapp.Layout.repaint()".to_string(),
        "com.somelib.Engine.drawScreen()".to_string(),
        "com.myapp.App.new()".to_string(),
      ],
    }],
  };
  let output1: ReportOutput = script1.run(&report).expect("can run");
  let output2: ReportOutput = script2.run(&report).expect("can run");

  let combined = output1 + output2;
  assert_eq!(
    Some("set value".to_owned()),
    combined.grouping_hints.grouping_key
  );
}

impl Scriptable for Report {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, crate::input::PathError> {
    if path.is_empty() {
      return Ok(Some(self.to_owned().into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(".".to_string()));
    };

    match base.as_str() {
      "app_id" => self.app_id.resolve(&path[1 ..]),
      "errors" => self.errors.resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(base.to_string())),
    }
  }

  fn schema() -> vrl::prelude::Kind {
    Kind::object(
      Collection::empty()
        .with_known("app_id", Kind::bytes())
        .with_known(
          "errors",
          Kind::array(Collection::empty().with_unknown(Error::schema())),
        ),
    )
  }
}

impl Scriptable for Error {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, crate::input::PathError> {
    if path.is_empty() {
      return Ok(Some(self.to_owned().into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(".".to_string()));
    };

    match base.as_str() {
      "name" => self.name.resolve(&path[1 ..]),
      "stacktrace" => self.stacktrace.resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(base.to_string())),
    }
  }

  fn schema() -> vrl::prelude::Kind {
    Kind::object(
      Collection::empty()
        .with_known("name", Kind::bytes())
        .with_known(
          "stacktrace",
          Kind::array(Collection::empty().with_unknown(Kind::bytes())),
        ),
    )
  }
}

impl From<Report> for ScriptValue {
  fn from(value: Report) -> Self {
    let errors = value
      .errors
      .iter()
      .map(|err| Into::<Self>::into(err.clone()).0)
      .collect::<Vec<_>>();

    Value::Object(BTreeMap::from([
      ("app_id".into(), value.app_id.into()),
      ("errors".into(), Value::Array(errors)),
    ]))
    .into()
  }
}

impl From<Error> for ScriptValue {
  fn from(value: Error) -> Self {
    Value::Object(BTreeMap::from([
      ("name".into(), value.name.clone().into()),
      (
        "stacktrace".into(),
        Value::Array(
          value
            .stacktrace
            .iter()
            .map(|n| Value::Bytes(n.clone().into()))
            .collect(),
        ),
      ),
    ]))
    .into()
  }
}
