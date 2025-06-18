// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::Setup;
use crate::logger::{Block, CaptureSession};
use crate::test::setup::SetupOptions;
use crate::wait_for;
use bd_log_primitives::LogType;
use bd_runtime::runtime::crash_handling::CrashDirectories;
use bd_runtime::runtime::{artifact_upload, FeatureFlag};
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::runtime::{make_simple_update, make_update, ValueKind};
use bd_test_helpers::test_api_server::log_upload::LogUpload;
use bd_test_helpers::test_api_server::StreamAction;
use itertools::Itertools as _;
use std::collections::HashSet;
use std::sync::Arc;
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::macros::datetime;

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
      setup.sdk_directory.path().join("reports/reason_inference"),
      "crash",
    )
    .unwrap();

    std::fs::write(
      setup.sdk_directory.path().join("reports/new/crash1.json"),
      "{\"crash\": \"crash1\"}",
    )
    .unwrap();

    std::fs::write(
      setup.sdk_directory.path().join(format!(
        "reports/new/{}_crash2.json",
        timestamp.unix_timestamp_nanos() / 1_000_000
      )),
      "{\"crash\": \"crash2\"}",
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

  // Sometimes we get the uploads split up between multiple payloads, so collect all the logs from
  // any number of uploads.
  let mut uploads: Vec<LogUpload> = vec![];
  while uploads
    .iter()
    .map(|upload| upload.logs().len())
    .sum::<usize>()
    < 2
  {
    uploads.push(setup.server.blocking_next_log_upload().unwrap());
  }

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let crash1 = logs
    .iter()
    .find(|log| log.field("_crash_reason") == "crash1")
    .unwrap();
  assert_eq!(crash1.message(), "App crashed");
  assert_eq!(crash1.session_id(), initial_session_id);
  assert_ne!(crash1.timestamp(), timestamp);
  assert_eq!(crash1.field("_ootb_field"), "ootb");
  let artifact = crash1.binary_field("_crash_artifact");
  assert_eq!(artifact, b"{\"crash\": \"crash1\"}");
  assert!(!crash1.has_field("custom"));

  let crash2 = logs
    .iter()
    .find(|log| log.field("_crash_reason") == "crash2")
    .unwrap();
  assert_eq!(crash2.message(), "App crashed");
  assert_eq!(crash2.session_id(), initial_session_id);
  assert_eq!(crash2.timestamp(), timestamp);
  assert_eq!(crash2.field("_ootb_field"), "ootb");
  let artifact = crash2.binary_field("_crash_artifact");
  assert_eq!(artifact, b"{\"crash\": \"crash2\"}");
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
      setup.sdk_directory.path().join("reports/reason_inference"),
      "crash",
    )
    .unwrap();

    std::fs::write(
      setup.sdk_directory.path().join("reports/new/crash1.json"),
      "{\"crash\": \"crash1\"}",
    )
    .unwrap();

    std::fs::write(
      setup.sdk_directory.path().join(format!(
        "reports/new/{}_crash2.json",
        timestamp.unix_timestamp_nanos() / 1_000_000
      )),
      "{\"crash\": \"crash2\"}",
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

  // Sometimes we get the uploads split up between multiple payloads, so collect all the logs from
  // any number of uploads.
  let mut uploads: Vec<LogUpload> = vec![];
  while uploads
    .iter()
    .map(|upload| upload.logs().len())
    .sum::<usize>()
    < 2
  {
    uploads.push(setup.server.blocking_next_log_upload().unwrap());
  }

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let crash1 = logs
    .iter()
    .find(|log| log.field("_crash_reason") == "crash1")
    .unwrap();
  assert_eq!(crash1.message(), "App crashed");
  assert_eq!(crash1.session_id(), initial_session_id);
  assert_ne!(crash1.timestamp(), timestamp);
  assert_eq!(crash1.field("_ootb_field"), "ootb");
  let crash1_uuid = crash1.field("_crash_artifact_id");
  assert!(!crash1.has_field("custom"));

  let crash2 = logs
    .iter()
    .find(|log| log.field("_crash_reason") == "crash2")
    .unwrap();
  assert_eq!(crash2.message(), "App crashed");
  assert_eq!(crash2.session_id(), initial_session_id);
  assert_eq!(crash2.timestamp(), timestamp);
  assert_eq!(crash2.field("_ootb_field"), "ootb");
  let crash2_uuid = crash2.field("_crash_artifact_id");
  assert!(!crash1.has_field("custom"));

  let mut remaining_uploads: HashSet<_> = [crash1_uuid, crash2_uuid].into();
  // Verify that out of band uploads happen.
  for _ in 0 .. 2 {
    // Verify that out of band uploads happen.
    let request = setup.server.blocking_next_artifact_intent().unwrap();
    let uuid = request.artifact_id;

    let request = setup.server.blocking_next_artifact_upload().unwrap();
    assert_eq!(request.artifact_id, uuid);
    assert_eq!(
      request.contents,
      if uuid == crash1_uuid {
        b"{\"crash\": \"crash1\"}"
      } else {
        b"{\"crash\": \"crash2\"}"
      }
    );
    remaining_uploads.remove(uuid.as_str());
  }

  assert!(
    remaining_uploads.is_empty(),
    "all uploads should have been seen"
  );
}

#[test]
fn crash_directories_configuration() {
  let mut setup = Setup::new();

  setup.current_api_stream().blocking_stream_action(
    bd_test_helpers::test_api_server::StreamAction::SendRuntime(make_simple_update(vec![(
      CrashDirectories::path(),
      ValueKind::String("a:b".to_string()),
    )])),
  );
  setup.server.blocking_next_runtime_ack();

  wait_for!(
    std::fs::read(setup.sdk_directory.path().join("reports/config")).unwrap_or_default() == b"a:b"
  );

  setup.current_api_stream().blocking_stream_action(
    bd_test_helpers::test_api_server::StreamAction::SendRuntime(make_simple_update(vec![])),
  );
  setup.server.blocking_next_runtime_ack();

  wait_for!(
    !std::fs::exists(setup.sdk_directory.path().join("reports/config")).unwrap_or_default()
  );
}
