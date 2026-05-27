// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Script;
use crate::feature_flag::{FeatureFlag, FeatureFlagWrapper};
use crate::report::{ReportOutput, report_functions};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  AppMetricsT,
  ErrorT,
  FrameT,
  FrameType,
  Report as CrashReport,
  ReportT,
  ReportType,
  root_as_report,
};
use flatbuffers::FlatBufferBuilder;
use time::OffsetDateTime;
use vrl::prelude::ExpressionError;

// These tests intentionally exercise the generated flatbuffer-backed report input instead of a
// hand-rolled lookalike so the report_functions examples stay aligned with production BDRL shape.
struct PackedReport {
  bytes: Vec<u8>,
}

impl PackedReport {
  fn report(&self) -> CrashReport<'_> {
    root_as_report(&self.bytes).expect("valid report")
  }
}

fn finish_report(report_t: &ReportT) -> PackedReport {
  let mut builder = FlatBufferBuilder::new();
  let report = report_t.pack(&mut builder);
  builder.finish(report, None);

  PackedReport {
    bytes: builder.finished_data().to_vec(),
  }
}

fn packed_report(errors: &[(&str, &[&str])]) -> PackedReport {
  let mut app_metrics = AppMetricsT::default();
  app_metrics.app_id = Some("com.example.myapp".to_string());

  let mut report_t = ReportT::default();
  report_t.type_ = ReportType::JVMCrash;
  report_t.app_metrics = Some(Box::new(app_metrics));
  report_t.errors = Some(
    errors
      .iter()
      .map(|(name, frames)| {
        let mut error = ErrorT::default();
        error.name = Some((*name).to_string());
        error.stack_trace = Some(
          frames
            .iter()
            .map(|frame| {
              let mut stack_frame = FrameT::default();
              stack_frame.type_ = FrameType::JVM;
              stack_frame.symbolicated_name = Some((*frame).to_string());
              stack_frame
            })
            .collect(),
        );
        error
      })
      .collect(),
  );

  finish_report(&report_t)
}

#[test]
fn compute_interpolated_key() {
  let script = Script::new::<CrashReport<'_>>(
    "
    report_type = .type
    error_name = .errors[0].name
    frame = string!(.errors[0].stack_trace[0].symbolicated_name)
    set_grouping_key(\"{{ report_type }}-{{ error_name }}-{{ frame }}\")
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report =
    packed_report(&[("NullPointerException", &["Builder.build()", "App.start()"])]);
  let report = packed_report.report();
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some("JVMCrash-NullPointerException-Builder.build()".to_string()),
    output.grouping_hints.grouping_key
  );
}

#[test]
fn unset_grouping_key() {
  let script = Script::new::<CrashReport<'_>>(
    "
    report_type = .type
    error_name = .errors[0].name
    _key = set_grouping_key(\"{{ report_type }}-{{ error_name }}\")
    set_grouping_key(null)
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report =
    packed_report(&[("NullPointerException", &["Builder.build()", "App.start()"])]);
  let report = packed_report.report();
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(None, output.grouping_hints.grouping_key);
}

#[test]
fn abort_execution() {
  let script = Script::new::<CrashReport<'_>>(
    "
    error_name = string!(.errors[0].name)
    if contains(error_name, \"Null\") {
      abort \"skipping NPE\"
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report =
    packed_report(&[("NullPointerException", &["Builder.build()", "App.start()"])]);
  let report = packed_report.report();
  let err = script
    .run::<CrashReport<'_>, ReportOutput>(&report)
    .expect_err("aborted");
  let Some(ExpressionError::Abort { span: _, message }) = err.downcast_ref() else {
    panic!("did not abort: {err:#?}");
  };
  assert_eq!(&Some("skipping NPE".to_string()), message);
}

#[test]
fn abort_execution_without_message() {
  let script = Script::new::<CrashReport<'_>>(
    "
    error_name = string!(.errors[0].name)
    if contains(error_name, \"Null\") {
      abort
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report =
    packed_report(&[("NullPointerException", &["Builder.build()", "App.start()"])]);
  let report = packed_report.report();
  let err = script
    .run::<CrashReport<'_>, ReportOutput>(&report)
    .expect_err("aborted");
  let Some(ExpressionError::Abort { span: _, message }) = err.downcast_ref() else {
    panic!("did not abort: {err:#?}");
  };
  assert_eq!(&None, message);
}

#[test]
fn type_hints_are_working() {
  // report.type is a known string key, so we can call contains()
  Script::new::<CrashReport<'_>>(
    "
    is_mine = contains(.type, \"Crash\")
    set_grouping_key(string!(is_mine))
    ",
    report_functions(),
  )
  .expect("is ok");

  // unknown_field hasn't been declared anywhere, and needs to be type-cast
  let script = Script::new::<CrashReport<'_>>(
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
fn iterate_real_report_errors() {
  let script = Script::new::<CrashReport<'_>>(
    "
    for_each(.errors) -> |error_index, error| {
      if error_index == 1 {
        set_grouping_key(error.name)
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report = packed_report(&[
    ("NullPointerException", &["Builder.build()", "App.start()"]),
    ("SomeRxProblem", &["Layout.repaint()"]),
  ]);
  let report = packed_report.report();

  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some("SomeRxProblem".to_string()),
    output.grouping_hints.grouping_key
  );
}

#[test]
fn set_significant_frame() {
  let script = Script::new::<CrashReport<'_>>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stack_trace) -> |frame_index, frame| {
        frame_name = to_string(frame.symbolicated_name)
        if starts_with(frame_name, \"os.\") || contains(frame_name, \".rx.\") {
          set_significant_frame(error_index, frame_index, false)
        } else if starts_with(frame_name, \"com.myapp.\") {
          set_significant_frame(error_index, frame_index)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report = packed_report(&[
    (
      "NullPointerException",
      &[
        "os.Builder.build()",
        "com.myapp.Layout.repaint()",
        "com.somelib.Engine.drawScreen()",
        "com.myapp.App.new()",
      ],
    ),
    ("SomeRxProblem", &["com.myapp.rx.Layout.repaint()"]),
  ]);
  let report = packed_report.report();
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
  let script1 = Script::new::<CrashReport<'_>>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stack_trace) -> |frame_index, frame| {
        frame_name = to_string(frame.symbolicated_name)
        if starts_with(frame_name, \"com.myapp.\") {
          set_significant_frame(error_index, frame_index, true)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let script2 = Script::new::<CrashReport<'_>>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stack_trace) -> |frame_index, frame| {
        frame_name = to_string(frame.symbolicated_name)
        if starts_with(frame_name, \"os.\") || contains(frame_name, \".rx.\") {
          set_significant_frame(error_index, frame_index, false)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report = packed_report(&[
    (
      "NullPointerException",
      &[
        "os.Builder.build()",
        "com.myapp.Layout.repaint()",
        "com.somelib.Engine.drawScreen()",
        "com.myapp.App.new()",
      ],
    ),
    ("SomeRxProblem", &["com.myapp.rx.Layout.repaint()"]),
  ]);
  let report = packed_report.report();
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
  let script = Script::new::<CrashReport<'_>>(
    "
    for_each(.errors) -> |error_index, error| {
      for_each(error.stack_trace) -> |frame_index, frame| {
        frame_name = to_string(frame.symbolicated_name)
        if starts_with(frame_name, \"os.\") {
          set_significant_frame(error_index, frame_index, true)
          set_significant_frame(error_index, frame_index, null)
        }
      }
    }
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report = packed_report(&[(
    "NullPointerException",
    &[
      "os.Builder.build()",
      "com.myapp.Layout.repaint()",
      "com.somelib.Engine.drawScreen()",
      "com.myapp.App.new()",
    ],
  )]);
  let report = packed_report.report();
  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(None, output.grouping_hints.is_significant_frame(0, 0));
}

#[test]
fn emit_fields() {
  let script = Script::new::<CrashReport<'_>>(
    "
    report_type = .type
    error_name = .errors[0].name
    _a = add_field(\"some_tag\", \"{{ report_type }}-{{ error_name }}\")
    add_field(\"flag_name\", string!(.feature_flags[0].name))
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report = packed_report(&[(
    "NullPointerException",
    &[
      "os.Builder.build()",
      "com.myapp.Layout.repaint()",
      "com.somelib.Engine.drawScreen()",
      "com.myapp.App.new()",
    ],
  )]);
  let report = packed_report.report();
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
    &"JVMCrash-NullPointerException".to_string(),
    output.metrics.get("some_tag").expect("has value")
  );
}

#[test]
fn combine_script_output_fields() {
  let script1 = Script::new::<CrashReport<'_>>(
    "
    report_type = .type
    error_name = .errors[0].name
    _a = add_field(\"some_tag\", \"{{ report_type }}-{{ error_name }}\")
    add_field(\"chain_len\", string!(.feature_flags[0].name))
    ",
    report_functions(),
  )
  .expect("is ok");
  let script2 = Script::new::<CrashReport<'_>>(
    "
    _b = add_field(\"chain_len\", \"override\")
    add_field(\"new_field\", \"x\")
    ",
    report_functions(),
  )
  .expect("is ok");
  let packed_report = packed_report(&[(
    "NullPointerException",
    &[
      "os.Builder.build()",
      "com.myapp.Layout.repaint()",
      "com.somelib.Engine.drawScreen()",
      "com.myapp.App.new()",
    ],
  )]);
  let report = packed_report.report();

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
    &"JVMCrash-NullPointerException".to_string(),
    combined.metrics.get("some_tag").expect("has value")
  );
}

#[test]
fn coroutine_reason_script_matches_real_jvm_report() {
  let script = Script::new::<CrashReport<'_>>(
    r#"
    found = false
    name = ""
    for_each(.errors) -> |_i, error| {
      if !found {
        for_each(error.stack_trace) -> |_j, frame| {
          if !found {
            name = to_string(frame.symbolicated_name)
            if contains(name, "kotlinx.coroutines") {
              found = true
            }
          }
        }
      }
    }

    if found {
      add_field("coroutine_reason", name)
    }
    "#,
    report_functions(),
  )
  .expect("is ok");

  let packed_report = packed_report(&[(
    "RuntimeException",
    &[
      "java.lang.Thread.run",
      "kotlinx.coroutines.DispatchedTask.run",
      "android.os.Looper.loopOnce",
    ],
  )]);
  let report = packed_report.report();

  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some(&"kotlinx.coroutines.DispatchedTask.run".to_string()),
    output.metrics.get("coroutine_reason")
  );
}

#[test]
fn iterate_missing_errors_with_fallback_is_safe() {
  let script = Script::new::<CrashReport<'_>>(
    r#"
    saw_error = false
    for_each(.errors) -> |_i, _error| {
      saw_error = true
    }

    if !saw_error {
      add_field("status", "no-errors")
    }
    "#,
    report_functions(),
  )
  .expect("is ok");

  let mut app_metrics = AppMetricsT::default();
  app_metrics.app_id = Some("com.example.myapp".to_string());

  let mut report_t = ReportT::default();
  report_t.type_ = ReportType::JVMCrash;
  report_t.app_metrics = Some(Box::new(app_metrics));
  report_t.errors = None;

  let packed_report = finish_report(&report_t);
  let report = packed_report.report();

  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(Some(&"no-errors".to_string()), output.metrics.get("status"));
}

#[test]
fn coroutine_reason_script_skips_missing_symbolicated_name() {
  let script = Script::new::<CrashReport<'_>>(
    r#"
    found = false
    name = ""
    for_each(.errors) -> |_i, error| {
      if !found {
        for_each(error.stack_trace) -> |_j, frame| {
          if !found {
            name = to_string(frame.symbolicated_name)
            if contains(name, "kotlinx.coroutines") {
              found = true
            }
          }
        }
      }
    }

    if found {
      add_field("coroutine_reason", name)
    }
    "#,
    report_functions(),
  )
  .expect("is ok");

  let mut app_metrics = AppMetricsT::default();
  app_metrics.app_id = Some("com.example.myapp".to_string());

  let mut unnamed_frame = FrameT::default();
  unnamed_frame.type_ = FrameType::JVM;

  let mut coroutine_frame = FrameT::default();
  coroutine_frame.type_ = FrameType::JVM;
  coroutine_frame.symbolicated_name =
    Some("kotlinx.coroutines.JobSupport.completeStateFinalization".to_string());

  let mut error = ErrorT::default();
  error.name = Some("RuntimeException".to_string());
  error.stack_trace = Some(vec![unnamed_frame, coroutine_frame]);

  let mut report_t = ReportT::default();
  report_t.type_ = ReportType::JVMCrash;
  report_t.app_metrics = Some(Box::new(app_metrics));
  report_t.errors = Some(vec![error]);

  let packed_report = finish_report(&report_t);
  let report = packed_report.report();

  let output: ReportOutput = script.run(&report).expect("can run");
  assert_eq!(
    Some(&"kotlinx.coroutines.JobSupport.completeStateFinalization".to_string()),
    output.metrics.get("coroutine_reason")
  );
}

#[test]
fn override_set_value_for_script_output_grouping_key() {
  let script1 =
    Script::new::<CrashReport<'_>>("set_grouping_key(\"some value\")", report_functions())
      .expect("is ok");
  let script2 =
    Script::new::<CrashReport<'_>>("set_grouping_key(\"other value\")", report_functions())
      .expect("is ok");
  let packed_report = packed_report(&[(
    "NullPointerException",
    &[
      "os.Builder.build()",
      "com.myapp.Layout.repaint()",
      "com.somelib.Engine.drawScreen()",
      "com.myapp.App.new()",
    ],
  )]);
  let report = packed_report.report();
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
  let script1 = Script::new::<CrashReport<'_>>("", vec![]).expect("is ok");
  let script2 =
    Script::new::<CrashReport<'_>>("set_grouping_key(\"other value\")", report_functions())
      .expect("is ok");
  let packed_report = packed_report(&[(
    "NullPointerException",
    &[
      "os.Builder.build()",
      "com.myapp.Layout.repaint()",
      "com.somelib.Engine.drawScreen()",
      "com.myapp.App.new()",
    ],
  )]);
  let report = packed_report.report();
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
    Script::new::<CrashReport<'_>>("set_grouping_key(\"set value\")", report_functions())
      .expect("is ok");
  let script2 = Script::new::<CrashReport<'_>>("", vec![]).expect("is ok");
  let packed_report = packed_report(&[(
    "NullPointerException",
    &[
      "os.Builder.build()",
      "com.myapp.Layout.repaint()",
      "com.somelib.Engine.drawScreen()",
      "com.myapp.App.new()",
    ],
  )]);
  let report = packed_report.report();
  let output1: ReportOutput = script1.run(&report).expect("can run");
  let output2: ReportOutput = script2.run(&report).expect("can run");

  let combined = output1 + output2;
  assert_eq!(
    Some("set value".to_owned()),
    combined.grouping_hints.grouping_key
  );
}
