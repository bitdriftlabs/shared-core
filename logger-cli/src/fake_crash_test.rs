// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{build_report, write_pending_report};
use crate::cli::FakeCrashCommand;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Platform as ReportPlatform,
  root_as_report,
};
use logger_cli::types::Platform;

#[test]
fn builds_parseable_report() {
  let report = build_report(&FakeCrashCommand {
    reason: "SIGABRT".to_string(),
    detail: "generated test crash".to_string(),
    platform: Platform::Android,
    app_id: "com.example.app".to_string(),
    app_version: "2.3.4".to_string(),
    app_build_id: "42".to_string(),
    file_name: None,
    upload: false,
  })
  .unwrap();

  let parsed = root_as_report(&report).unwrap();
  assert_eq!(parsed.errors().unwrap().get(0).name().unwrap(), "SIGABRT");
  assert_eq!(
    parsed.errors().unwrap().get(0).reason().unwrap(),
    "generated test crash"
  );
  assert_eq!(
    parsed.app_metrics().unwrap().app_id().unwrap(),
    "com.example.app"
  );
  assert_eq!(
    parsed.device_metrics().unwrap().platform(),
    ReportPlatform::Android
  );
  assert_eq!(
    parsed
      .app_metrics()
      .unwrap()
      .build_number()
      .unwrap()
      .version_code(),
    42
  );
}

#[test]
fn writes_pending_fake_crash_report() {
  let sdk_directory = tempfile::tempdir().unwrap();
  let path = write_pending_report(
    sdk_directory.path(),
    &FakeCrashCommand {
      reason: "FakeCrash".to_string(),
      detail: "written to disk".to_string(),
      platform: Platform::Apple,
      app_id: "io.bitdrift.cli".to_string(),
      app_version: "1.0.0".to_string(),
      app_build_id: "10".to_string(),
      file_name: Some("queued-report".to_string()),
      upload: false,
    },
  )
  .unwrap();

  assert!(path.ends_with("queued-report.cap"));
  let report = std::fs::read(path).unwrap();
  let parsed = root_as_report(&report).unwrap();
  assert_eq!(
    parsed.device_metrics().unwrap().platform(),
    ReportPlatform::iOS
  );
  assert_eq!(
    parsed
      .app_metrics()
      .unwrap()
      .build_number()
      .unwrap()
      .cf_bundle_version()
      .unwrap(),
    "10"
  );
}
