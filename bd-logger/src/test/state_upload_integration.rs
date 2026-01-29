// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::setup::{Setup, SetupOptions};
use crate::log_level;
use bd_log_matcher::builder::message_equals;
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::config::v1::config::{BufferConfigList, buffer_config};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::FeatureFlag as _;
use bd_test_helpers::config_helper::{
  ConfigurationUpdateParts,
  configuration_update,
  configuration_update_from_parts,
  default_buffer_config,
  make_buffer_matcher_matching_everything,
  make_workflow_config_flushing_buffer,
};
use bd_test_helpers::runtime::ValueKind;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn continuous_buffer_creates_and_uploads_state_snapshot() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory: sdk_directory.clone(),
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
        ValueKind::Int(1),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.send_configuration_update(configuration_update(
    "",
    StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList {
        buffer_config: vec![default_buffer_config(
          buffer_config::Type::CONTINUOUS,
          make_buffer_matcher_matching_everything().into(),
        )],
        ..Default::default()
      })
      .into(),
      ..Default::default()
    },
  ));

  setup
    .logger_handle
    .set_feature_flag_exposure("test_flag".to_string(), Some("variant_a".to_string()));
  setup
    .logger_handle
    .set_feature_flag_exposure("another_flag".to_string(), Some("variant_b".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "test message".into(),
    [].into(),
    [].into(),
    None,
  );

  let log_upload = setup.server.blocking_next_log_upload();
  assert!(log_upload.is_some(), "expected log upload");

  let timeout = std::time::Duration::from_secs(2);
  let start = std::time::Instant::now();

  while start.elapsed() < timeout {
    if let Some(upload) = setup.server.blocking_next_artifact_upload() {
      assert!(
        !upload.contents.is_empty(),
        "state snapshot should have content"
      );

      let snapshots_dir = sdk_directory.path().join("state/snapshots");
      if snapshots_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&snapshots_dir)
          .unwrap()
          .filter_map(Result::ok)
          .collect();
        assert!(
          !entries.is_empty(),
          "snapshot files should exist in state/snapshots/"
        );
      }
      return;
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
  }
}

#[test]
fn trigger_buffer_flush_creates_snapshot() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory: sdk_directory.clone(),
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![default_buffer_config(
        buffer_config::Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: make_workflow_config_flushing_buffer("default", message_equals("flush")),
      ..Default::default()
    },
  ));

  setup
    .logger_handle
    .set_feature_flag_exposure("trigger_flag".to_string(), Some("enabled".to_string()));

  for i in 0 .. 3 {
    setup.log(
      log_level::INFO,
      LogType::NORMAL,
      format!("trigger log {i}").into(),
      [].into(),
      [].into(),
      None,
    );
  }

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "flush".into(),
    [].into(),
    [].into(),
    None,
  );

  let log_upload = setup.server.blocking_next_log_upload();
  assert!(
    log_upload.is_some(),
    "expected log upload from trigger buffer flush"
  );

  let upload = log_upload.unwrap();
  let logs = upload.logs();
  assert!(!logs.is_empty(), "expected logs in upload");

  let timeout = std::time::Duration::from_secs(2);
  let start = std::time::Instant::now();

  while start.elapsed() < timeout {
    if let Some(artifact) = setup.server.blocking_next_artifact_upload() {
      assert!(
        !artifact.contents.is_empty(),
        "state snapshot should have content"
      );

      let snapshots_dir = sdk_directory.path().join("state/snapshots");
      if snapshots_dir.exists() {
        let snapshot_files: Vec<_> = std::fs::read_dir(&snapshots_dir)
          .unwrap()
          .filter_map(Result::ok)
          .filter(|e| e.path().extension().is_some_and(|ext| ext == "zz"))
          .collect();
        assert!(
          !snapshot_files.is_empty(),
          "snapshot .zz files should exist after trigger flush"
        );
      }
      return;
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
  }

  panic!("expected state snapshot upload within timeout");
}

#[test]
fn trigger_buffer_with_multiple_flushes_uploads_state_once() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory,
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.send_configuration_update(configuration_update_from_parts(
    "",
    ConfigurationUpdateParts {
      buffer_config: vec![default_buffer_config(
        buffer_config::Type::TRIGGER,
        make_buffer_matcher_matching_everything().into(),
      )],
      workflows: make_workflow_config_flushing_buffer("default", message_equals("flush")),
      ..Default::default()
    },
  ));

  setup
    .logger_handle
    .set_feature_flag_exposure("multi_flush_flag".to_string(), Some("value".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "first batch log".into(),
    [].into(),
    [].into(),
    None,
  );
  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "flush".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut first_flush_artifacts = 0;
  let timeout = std::time::Duration::from_millis(500);
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      first_flush_artifacts += 1;
    } else {
      break;
    }
  }

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "second batch log".into(),
    [].into(),
    [].into(),
    None,
  );
  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "flush".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut second_flush_artifacts = 0;
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      second_flush_artifacts += 1;
    } else {
      break;
    }
  }

  assert!(
    second_flush_artifacts <= first_flush_artifacts,
    "second flush ({second_flush_artifacts}) should not upload more state than first \
     ({first_flush_artifacts})"
  );
}

#[test]
fn state_correlator_prevents_duplicate_uploads() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory,
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
        ValueKind::Int(1),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.configure_stream_all_logs();

  setup
    .logger_handle
    .set_feature_flag_exposure("dup_test_flag".to_string(), Some("value".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "first log".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut first_batch_artifacts = 0;
  let timeout = std::time::Duration::from_millis(500);
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      first_batch_artifacts += 1;
    } else {
      break;
    }
  }

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "second log".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut second_batch_artifacts = 0;
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      second_batch_artifacts += 1;
    } else {
      break;
    }
  }

  assert!(
    second_batch_artifacts <= first_batch_artifacts,
    "second batch ({second_batch_artifacts}) should not upload more state than first batch \
     ({first_batch_artifacts})"
  );
}

#[test]
fn new_state_changes_trigger_new_snapshot() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory,
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
        ValueKind::Int(1),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.configure_stream_all_logs();

  setup
    .logger_handle
    .set_feature_flag_exposure("flag_v1".to_string(), Some("value1".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "log after first state".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let timeout = std::time::Duration::from_millis(500);
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_none() {
      break;
    }
  }

  setup
    .logger_handle
    .set_feature_flag_exposure("flag_v2".to_string(), Some("value2".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "log after second state".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();
}

#[test]
fn continuous_streaming_uploads_state_with_first_batch() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory: sdk_directory.clone(),
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
        ValueKind::Int(2),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.configure_stream_all_logs();

  setup
    .logger_handle
    .set_feature_flag_exposure("streaming_flag".to_string(), Some("active".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "streaming log 1".into(),
    [].into(),
    [].into(),
    None,
  );
  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "streaming log 2".into(),
    [].into(),
    [].into(),
    None,
  );

  let log_upload = setup.server.blocking_next_log_upload();
  assert!(log_upload.is_some(), "expected log upload");

  let timeout = std::time::Duration::from_secs(2);
  let start = std::time::Instant::now();
  let mut found_artifact = false;

  while start.elapsed() < timeout {
    if let Some(artifact) = setup.server.blocking_next_artifact_upload() {
      assert!(
        !artifact.contents.is_empty(),
        "state snapshot should have content"
      );
      found_artifact = true;

      let snapshots_dir = sdk_directory.path().join("state/snapshots");
      if snapshots_dir.exists() {
        let snapshot_files: Vec<_> = std::fs::read_dir(&snapshots_dir)
          .unwrap()
          .filter_map(Result::ok)
          .filter(|e| e.path().extension().is_some_and(|ext| ext == "zz"))
          .collect();
        assert!(
          !snapshot_files.is_empty(),
          "snapshot .zz files should exist for continuous streaming"
        );
      }
      break;
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
  }

  assert!(
    found_artifact,
    "expected artifact upload for state snapshot with continuous streaming"
  );
}

#[test]
fn continuous_streaming_multiple_batches_single_state_upload() {
  let sdk_directory = Arc::new(TempDir::with_prefix("sdk").unwrap());

  let mut setup = Setup::new_with_cached_runtime(SetupOptions {
    sdk_directory,
    disk_storage: true,
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::state::UsePersistentStorage::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
        ValueKind::Int(1),
      ),
      (
        bd_runtime::runtime::state::SnapshotCreationIntervalMs::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });

  setup.configure_stream_all_logs();

  setup
    .logger_handle
    .set_feature_flag_exposure("batch_flag".to_string(), Some("test".to_string()));

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "batch 1 log".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut first_batch_artifacts = 0;
  let timeout = std::time::Duration::from_millis(500);
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      first_batch_artifacts += 1;
    } else {
      break;
    }
  }

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "batch 2 log".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut second_batch_artifacts = 0;
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      second_batch_artifacts += 1;
    } else {
      break;
    }
  }

  setup.log(
    log_level::INFO,
    LogType::NORMAL,
    "batch 3 log".into(),
    [].into(),
    [].into(),
    None,
  );

  let _ = setup.server.blocking_next_log_upload();

  let mut third_batch_artifacts = 0;
  let start = std::time::Instant::now();
  while start.elapsed() < timeout {
    if setup.server.blocking_next_artifact_upload().is_some() {
      third_batch_artifacts += 1;
    } else {
      break;
    }
  }

  assert!(
    second_batch_artifacts <= first_batch_artifacts
      && third_batch_artifacts <= first_batch_artifacts,
    "subsequent batches should not upload more state than first batch \
     (first={first_batch_artifacts}, second={second_batch_artifacts}, \
     third={third_batch_artifacts})"
  );
}
