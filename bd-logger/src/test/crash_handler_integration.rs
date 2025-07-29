// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::Setup;
use crate::logger::{Block, CaptureSession};
use crate::test::setup::SetupOptions;
use bd_log_primitives::LogType;
use bd_runtime::runtime::{FeatureFlag, artifact_upload};
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_test_helpers::test_api_server::StreamAction;
use bd_test_helpers::test_api_server::log_upload::LogUpload;
use itertools::Itertools as _;
use std::collections::HashSet;
use std::sync::Arc;
use time::ext::NumericalDuration;
use time::macros::datetime;

// empty fbs format report
#[rustfmt::skip]
const CRASH_CONTENTS: &str = "\x14\x00\x00\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x0e\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00crash1\x00\x00";

#[test]
fn crash_reports() {
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

    // Log one log to trigger a global state update.
    setup.logger_handle.log_sdk_start([].into(), 1.seconds());

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

  setup.configure_default_buffers();
  setup.upload_individual_logs();

  let upload_intent = setup.server.next_log_intent().unwrap();
  assert_eq!("crash_handler", upload_intent.explicit_session_capture().id);

  let uploads: Vec<LogUpload> = vec![setup.server.blocking_next_log_upload().unwrap()];

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let Some(crash1) = logs
    .iter()
    .find(|log| log.field("_app_exit_info") == "crash1")
  else {
    panic!("no crash found with reason!");
  };
  assert_eq!(crash1.message(), "AppExit");
  assert_eq!(crash1.session_id(), initial_session_id);
  assert_ne!(crash1.timestamp(), timestamp);
  assert_eq!(crash1.field("_ootb_field"), "ootb");
  let artifact = crash1.binary_field("_crash_artifact");
  assert_eq!(artifact, CRASH_CONTENTS.as_bytes());
  assert!(!crash1.has_field("custom"));
}

#[test]
fn crash_reports_artifact_upload() {
  let timestamp = datetime!(2021-01-01 00:00:00 UTC);

  let (directory, initial_session_id) = {
    let mut setup = Setup::new_with_options(SetupOptions {
      disk_storage: true,
      metadata_provider: Arc::new(LogMetadata {
        timestamp: time::OffsetDateTime::now_utc().into(),
        ootb_fields: [("_ootb_field".into(), "ootb".into())].into(),
        custom_fields: [("custom".into(), "custom".into())].into(),
      }),
      ..Default::default()
    });

    setup
      .current_api_stream()
      .blocking_stream_action(StreamAction::SendRuntime(make_update(
        Setup::get_default_runtime_values()
          .into_iter()
          .chain(std::iter::once((
            artifact_upload::Enabled::path(),
            ValueKind::Bool(true),
          )))
          .collect(),
        "nonce".to_string(),
      )));
    setup.server.blocking_next_runtime_ack();

    // Log one log to trigger a global state update, blocking to make sure it gets processed.
    setup.logger_handle.log(
      0,
      LogType::Normal,
      "".into(),
      [].into(),
      [].into(),
      None,
      Block::No,
      CaptureSession::default(),
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

  let uploads: Vec<LogUpload> = vec![setup.server.blocking_next_log_upload().unwrap()];

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let crash1 = logs
    .iter()
    .find(|log| log.field("_app_exit_info") == "crash1")
    .unwrap();
  assert_eq!(crash1.message(), "AppExit");
  assert_eq!(crash1.session_id(), initial_session_id);
  assert_ne!(crash1.timestamp(), timestamp);
  assert_eq!(crash1.field("_ootb_field"), "ootb");
  let crash1_uuid = crash1.field("_crash_artifact_id");
  assert!(!crash1.has_field("custom"));

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
