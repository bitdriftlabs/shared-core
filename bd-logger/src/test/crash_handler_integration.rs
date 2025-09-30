// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::Setup;
use crate::logger::{Block, CaptureSession, ReportProcessingSession};
use crate::test::setup::SetupOptions;
use assert_matches::assert_matches;
use bd_log_primitives::LogType;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::test_api_server::log_upload::LogUpload;
use itertools::Itertools as _;
use std::collections::HashSet;
use std::sync::Arc;
use time::ext::NumericalStdDuration;
use time::macros::datetime;

// empty fbs format report
#[rustfmt::skip]
const CRASH_CONTENTS: &str = "\x14\x00\x00\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x0e\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00crash1\x00\x00";

#[test]
fn crash_reports_artifact_upload() {
  let timestamp = datetime!(2021-01-01 00:00:00 UTC);

  let (directory, initial_session_id) = {
    let setup = Setup::new_with_options(SetupOptions {
      disk_storage: true,
      metadata_provider: Arc::new(LogMetadata {
        timestamp: time::OffsetDateTime::now_utc().into(),
        ootb_fields: [("_ootb_field".into(), "ootb".into())].into(),
        custom_fields: [("custom".into(), "custom".into())].into(),
      }),
      ..Default::default()
    });

    // Log one log to trigger a global state update, blocking to make sure it gets processed.
    setup.logger_handle.log(
      0,
      LogType::Normal,
      "".into(),
      [].into(),
      [].into(),
      None,
      Block::Yes(5.std_seconds()),
      &CaptureSession::default(),
    );

    std::fs::create_dir_all(setup.sdk_directory.path().join("reports/new")).unwrap();

    std::fs::write(
      setup.sdk_directory.path().join("reports/new/crash1.cap"),
      CRASH_CONTENTS,
    )
    .unwrap();

    (
      setup.sdk_directory.clone(),
      setup.logger.new_logger_handle().session_id(),
    )
  };

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory,
    disk_storage: true,
    ..Default::default()
  });

  assert_ne!(
    initial_session_id,
    setup.logger.new_logger_handle().session_id()
  );

  setup.configure_stream_all_logs();
  setup.upload_individual_logs();
  setup.upload_crash_reports(ReportProcessingSession::PreviousRun);

  let uploads: Vec<LogUpload> = vec![setup.server.blocking_next_log_upload().unwrap()];

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let crash = logs
    .iter()
    .find(|log| log.field("_app_exit_info") == "crash1")
    .unwrap();
  assert_eq!(crash.message(), "AppExit");
  assert_eq!(crash.session_id(), initial_session_id);
  assert_ne!(crash.timestamp(), timestamp);
  assert_eq!(crash.field("_ootb_field"), "ootb");
  assert!(!crash.has_field("_crash_artifact"));
  assert!(crash.has_field("custom"));
  let crash1_uuid = crash.field("_crash_artifact_id");

  let mut remaining_uploads: HashSet<_> = [crash1_uuid].into();
  // Verify that out of band uploads happen.
  for _ in 0 .. 1 {
    // Verify that out of band uploads happen.
    let request = setup.server.blocking_next_artifact_intent().unwrap();
    let uuid = request.artifact_id;

    let request = setup.server.blocking_next_artifact_upload().unwrap();
    assert_eq!(request.artifact_id, uuid);
    assert_eq!(request.contents, CRASH_CONTENTS.as_bytes());
    remaining_uploads.remove(uuid.as_str());
  }

  assert!(
    remaining_uploads.is_empty(),
    "all uploads should have been seen"
  );
}

#[test]
fn crash_reports_feature_flags() {
  let timestamp = datetime!(2021-01-01 00:00:00 UTC);

  let (directory, initial_session_id) = {
    let setup = Setup::new_with_options(SetupOptions {
      disk_storage: true,
      metadata_provider: Arc::new(LogMetadata {
        timestamp: time::OffsetDateTime::now_utc().into(),
        ootb_fields: [("_ootb_field".into(), "ootb".into())].into(),
        custom_fields: [("custom".into(), "custom".into())].into(),
      }),
      ..Default::default()
    });

    setup.logger_handle.set_feature_flag("flag_name", None);

    setup
      .logger_handle
      .set_feature_flag("flag_with_variant", Some("variant".to_string()));

    // Log one log to trigger a global state update, blocking to make sure it gets processed.
    setup.logger_handle.log(
      0,
      LogType::Normal,
      "".into(),
      [].into(),
      [].into(),
      None,
      Block::Yes(5.std_seconds()),
      &CaptureSession::default(),
    );

    std::fs::create_dir_all(setup.sdk_directory.path().join("reports/new")).unwrap();

    std::fs::write(
      setup.sdk_directory.path().join("reports/new/crash1.cap"),
      CRASH_CONTENTS,
    )
    .unwrap();

    (
      setup.sdk_directory.clone(),
      setup.logger.new_logger_handle().session_id(),
    )
  };

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory,
    disk_storage: true,
    ..Default::default()
  });

  assert_ne!(
    initial_session_id,
    setup.logger.new_logger_handle().session_id()
  );

  setup.configure_stream_all_logs();
  setup.upload_individual_logs();
  setup.upload_crash_reports(ReportProcessingSession::PreviousRun);

  let uploads: Vec<LogUpload> = vec![setup.server.blocking_next_log_upload().unwrap()];

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let crash_log = logs
    .iter()
    .find(|log| log.field("_app_exit_info") == "crash1")
    .unwrap();
  assert_eq!(crash_log.message(), "AppExit");
  assert_eq!(crash_log.session_id(), initial_session_id);
  assert_ne!(crash_log.timestamp(), timestamp);
  assert_eq!(crash_log.field("_ootb_field"), "ootb");
  assert!(!crash_log.has_field("_crash_artifact"));
  assert!(crash_log.has_field("custom"));
  let crash1_uuid = crash_log.field("_crash_artifact_id");

  let mut remaining_uploads: HashSet<_> = [crash1_uuid].into();
  // Verify that out of band uploads happen.
  for _ in 0 .. 1 {
    // Verify that out of band uploads happen.
    let request = setup.server.blocking_next_artifact_intent().unwrap();
    let uuid = request.artifact_id;

    let request = setup.server.blocking_next_artifact_upload().unwrap();
    assert_eq!(request.artifact_id, uuid);
    assert_eq!(request.contents, CRASH_CONTENTS.as_bytes());
    let mut feature_flags = request
      .feature_flags
      .iter()
      .sorted_by(|a, b| a.name.cmp(&b.name));
    assert_matches!(feature_flags.next(), Some(flag) => {
      assert_eq!(flag.name, "flag_name");
        assert!(flag.variant.is_none());
    });
    assert_matches!(feature_flags.next(), Some(flag) => {
      assert_eq!(flag.name, "flag_with_variant");
        assert_eq!(flag.variant.as_deref(), Some("variant"));
    });
    remaining_uploads.remove(uuid.as_str());
  }

  assert!(
    remaining_uploads.is_empty(),
    "all uploads should have been seen"
  );
}
