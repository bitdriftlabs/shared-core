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
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::FeatureFlag as _;
use bd_test_helpers::config_helper::configuration_update;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::runtime::ValueKind;
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
#[allow(unused_variables)]
fn crash_report_upload() {
  let timestamp = datetime!(2021-01-01 00:00:00 UTC);

  // In order to get persistent storage enabled, we need to send a runtime update first.
  let directory = {
    let setup = Setup::new_with_options(SetupOptions {
      disk_storage: true,
      metadata_provider: Arc::new(LogMetadata {
        timestamp: time::OffsetDateTime::now_utc().into(),
        ootb_fields: [("_ootb_field".into(), "ootb".into())].into(),
        custom_fields: [("custom".into(), "custom".into())].into(),
      }),
      extra_runtime_values: vec![(
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      )],
      ..Default::default()
    });
    setup.sdk_directory.clone()
  };

  let initial_session_id = {
    let mut setup = Setup::new_with_options(SetupOptions {
      disk_storage: true,
      metadata_provider: Arc::new(LogMetadata {
        timestamp: time::OffsetDateTime::now_utc().into(),
        ootb_fields: [("_ootb_field".into(), "ootb".into())].into(),
        custom_fields: [("custom".into(), "custom".into())].into(),
      }),
      sdk_directory: directory.clone(),
      extra_runtime_values: vec![(
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      )],
      ..Default::default()
    });

    setup
      .logger_handle
      .set_feature_flag_exposure("flag_name".to_string(), None);

    setup
      .logger_handle
      .set_feature_flag_exposure("flag_with_variant".to_string(), Some("variant".to_string()));

    // Trigger config initialization so pre-config buffer (including feature flags) gets replayed
    // Use an empty configuration to avoid setting up any buffers or generating extra logs
    setup.send_configuration_update(configuration_update("", StateOfTheWorld::default()));

    // Flush state to ensure feature flags are persisted before writing the crash report
    setup.logger_handle.flush_state(Block::Yes(5.std_seconds()));

    // Log one log to trigger a global state update, blocking to make sure it gets processed.
    setup.logger_handle.log(
      0,
      LogType::NORMAL,
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

    setup.logger.new_logger_handle().session_id()
  };

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory,
    disk_storage: true,
    extra_runtime_values: vec![(
      bd_runtime::runtime::state::UsePersistentStorage::path(),
      ValueKind::Bool(true),
    )],
    ..Default::default()
  });

  assert_ne!(
    initial_session_id,
    setup.logger.new_logger_handle().session_id()
  );

  setup.configure_stream_all_logs();
  setup.upload_individual_logs();
  setup.upload_crash_reports(ReportProcessingSession::PreviousRun);

  // Collect all log uploads - crash reports generate separate uploads
  let mut uploads: Vec<LogUpload> = vec![];
  for _ in 0..5 {
    if let Some(upload) = setup.server.blocking_next_log_upload() {
      uploads.push(upload);
    } else {
      break;
    }
  }

  let logs = uploads
    .iter()
    .flat_map(bd_test_helpers::test_api_server::log_upload::LogUpload::logs)
    .collect_vec();

  let crash = logs
    .iter()
    .find(|log| log.has_field("_app_exit_info") && log.field("_app_exit_info") == "crash1")
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
