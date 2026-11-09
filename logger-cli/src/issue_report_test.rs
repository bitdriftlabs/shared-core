// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{FeatureFlagExposure, IssueKind, IssueReport, StackFrame};
use crate::types::Platform;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  FrameType,
  ReportType,
  root_as_report,
};
use time::macros::datetime;

fn anr() -> IssueReport {
  IssueReport {
    kind: IssueKind::AppNotResponding,
    platform: Platform::Android,
    app_id: "com.example.app".to_string(),
    app_version: "1.1.0".to_string(),
    app_build_id: "110".to_string(),
    name: "Application Not Responding".to_string(),
    reason: "Input dispatching timed out".to_string(),
    frames: vec![
      StackFrame {
        class_name: "com.example.PaymentClient".to_string(),
        symbol_name: "awaitAuthorization".to_string(),
        file: "PaymentClient.kt".to_string(),
        line: 88,
      },
      StackFrame {
        class_name: "com.example.CheckoutActivity".to_string(),
        symbol_name: "onConfirm".to_string(),
        file: "CheckoutActivity.kt".to_string(),
        line: 42,
      },
    ],
    feature_flags: vec![FeatureFlagExposure {
      name: "payment_sdk_v2".to_string(),
      value: Some("enabled".to_string()),
    }],
    timestamp: datetime!(2026-11-01 12:30:00.5 UTC),
  }
}

#[test]
fn encodes_kind_frames_and_timestamp() {
  let bytes = anr().to_bytes().unwrap();
  let report = root_as_report(&bytes).unwrap();

  assert_eq!(report.type_(), ReportType::AppNotResponding);
  let error = report.errors().unwrap().get(0);
  assert_eq!(error.name().unwrap(), "Application Not Responding");
  assert_eq!(error.reason().unwrap(), "Input dispatching timed out");
  let frames = error.stack_trace().unwrap();
  assert_eq!(frames.len(), 2);
  let top = frames.get(0);
  assert_eq!(top.type_(), FrameType::JVM);
  assert_eq!(top.class_name().unwrap(), "com.example.PaymentClient");
  assert_eq!(top.symbol_name().unwrap(), "awaitAuthorization");
  assert_eq!(
    top.source_file().unwrap().path().unwrap(),
    "PaymentClient.kt"
  );
  assert_eq!(top.source_file().unwrap().line(), 88);
  assert!(top.in_app());
  assert_eq!(frames.get(1).original_index(), 1);

  let thread = report.thread_details().unwrap().threads().unwrap().get(0);
  assert_eq!(thread.state().unwrap(), "BLOCKED");
  assert_eq!(thread.stack_trace().unwrap().len(), 2);

  let time = report.device_metrics().unwrap().time().unwrap();
  assert_eq!(time.seconds(), 1_793_536_200);
  assert_eq!(time.nanos(), 500_000_000);
  let flags = report.feature_flags().unwrap();
  assert_eq!(flags.get(0).name().unwrap(), "payment_sdk_v2");
  assert_eq!(flags.get(0).value().unwrap(), "enabled");
}

#[test]
fn omits_empty_stack_and_flags() {
  let bytes = IssueReport {
    kind: IssueKind::HandledError,
    frames: Vec::new(),
    feature_flags: Vec::new(),
    ..anr()
  }
  .to_bytes()
  .unwrap();
  let report = root_as_report(&bytes).unwrap();

  assert_eq!(report.type_(), ReportType::HandledError);
  assert!(report.errors().unwrap().get(0).stack_trace().is_none());
  assert!(report.feature_flags().is_none());
  let thread = report.thread_details().unwrap().threads().unwrap().get(0);
  assert_eq!(thread.state().unwrap(), "RUNNING");
}

#[test]
fn rejects_non_numeric_android_build() {
  let error = IssueReport {
    app_build_id: "1.1.0".to_string(),
    ..anr()
  }
  .to_bytes()
  .unwrap_err();
  assert_eq!(error.to_string(), "android app build ID must be an integer");
}

#[test]
fn frame_type_follows_platform_and_kind() {
  let frame_type = |platform, kind| {
    let bytes = IssueReport {
      kind,
      platform,
      ..anr()
    }
    .to_bytes()
    .unwrap();
    let report = root_as_report(&bytes).unwrap();
    report
      .errors()
      .unwrap()
      .get(0)
      .stack_trace()
      .unwrap()
      .get(0)
      .type_()
  };
  assert_eq!(
    frame_type(Platform::Android, IssueKind::NativeCrash),
    FrameType::AndroidNative
  );
  assert_eq!(
    frame_type(Platform::Android, IssueKind::JvmCrash),
    FrameType::JVM
  );
  assert_eq!(
    frame_type(Platform::Apple, IssueKind::NativeCrash),
    FrameType::DWARF
  );
}
