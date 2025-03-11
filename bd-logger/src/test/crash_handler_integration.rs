// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::Setup;
use crate::test::setup::SetupOptions;
use crate::wait_for;
use assert_matches::assert_matches;
use bd_runtime::runtime::crash_handling::CrashDirectories;
use bd_runtime::runtime::FeatureFlag;
use bd_test_helpers::runtime::{make_simple_update, ValueKind};
use time::ext::{NumericalDuration, NumericalStdDuration};

#[test]
fn crash_reports() {
  let (directory, initial_session_id) = {
    let setup = Setup::new_with_options(SetupOptions {
      disk_storage: true,
      ..Default::default()
    });

    std::fs::create_dir_all(setup.sdk_directory.path().join("reports/new")).unwrap();

    std::fs::write(
      setup.sdk_directory.path().join("reports/new/crash1.txt"),
      "crash1",
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

  assert_matches!(setup.server.blocking_next_log_upload(), Some(upload) => {
    assert_eq!(upload.logs().len(), 1);
    assert_eq!(upload.logs()[0].message(), "App crashed");
    assert_eq!(upload.logs()[0].message(), "App crashed");
    assert_eq!(upload.logs()[0].binary_field("_crash_artifact"), b"crash1");
    assert_eq!(upload.logs()[0].session_id(), initial_session_id);
  });
}

#[test]
fn crash_directories_configuration() {
  let mut setup = Setup::new();

  setup.current_api_stream.blocking_stream_action(
    bd_test_helpers::test_api_server::StreamAction::SendRuntime(make_simple_update(vec![(
      CrashDirectories::path(),
      ValueKind::String("a:b".to_string()),
    )])),
  );
  setup.server.blocking_next_runtime_ack();

  wait_for!(std::fs::exists(setup.sdk_directory.path().join("reports/config")).unwrap_or_default());

  assert_eq!(
    std::fs::read(setup.sdk_directory.path().join("reports/config")).unwrap(),
    b"a:b"
  );

  setup.current_api_stream.blocking_stream_action(
    bd_test_helpers::test_api_server::StreamAction::SendRuntime(make_simple_update(vec![])),
  );
  setup.server.blocking_next_runtime_ack();

  wait_for!(
    !std::fs::exists(setup.sdk_directory.path().join("reports/config")).unwrap_or_default()
  );
}
