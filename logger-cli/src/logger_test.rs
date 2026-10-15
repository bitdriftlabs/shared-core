// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{
  SessionStrategyConfig,
  make_session_strategy,
  make_session_strategy_with_time_provider,
};
use crate::metadata::Metadata;
use crate::storage::SQLiteStorage;
use bd_api::{Metadata as ApiMetadata, Platform};
use bd_device::Device;
use bd_key_value::Store;
use bd_time::TestTimeProvider;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::{Builder, TempDir};
use time::{Duration, OffsetDateTime};

struct TestSdkDirectory {
  temp_dir: TempDir,
}

impl TestSdkDirectory {
  fn new() -> Self {
    let temp_root = std::env::current_dir().unwrap().join(".tmp");
    fs::create_dir_all(&temp_root).unwrap();

    // Keep test scratch space inside the workspace, but let tempfile provide a collision-resistant
    // per-test directory for the concurrently executed restart tests.
    let temp_dir = Builder::new()
      .prefix("logger-cli-session-")
      .tempdir_in(temp_root)
      .unwrap();

    Self { temp_dir }
  }

  fn path(&self) -> &Path {
    self.temp_dir.path()
  }
}

#[test]
fn command_artifact_metadata_includes_platform_build_number() {
  let sdk_directory = TestSdkDirectory::new();
  let storage = SQLiteStorage::new(&sdk_directory.path().join("defaults.db"));
  let device = Arc::new(Device::new(Arc::new(Store::new(Box::new(storage)))));
  for (platform, build_key) in [
    (Platform::Android, "_app_version_code"),
    (Platform::Apple, "_build_number"),
  ] {
    let metadata = Metadata {
      app_id: Some("io.bitdrift.cli.workflowperfetto".to_string()),
      app_version: Some("1.0.0".to_string()),
      app_version_code: "42".to_string(),
      platform,
      device: device.clone(),
      os_version: None,
      manufacturer: None,
      model: "test".to_string(),
    };

    assert_eq!(
      metadata.command_artifact_metadata(),
      HashMap::from([
        ("platform".to_string(), platform.as_str().to_string()),
        (
          "app_id".to_string(),
          "io.bitdrift.cli.workflowperfetto".to_string()
        ),
        ("app_version".to_string(), "1.0.0".to_string()),
        (build_key.to_string(), "42".to_string()),
      ]),
    );
  }
}

#[tokio::test]
async fn fixed_sessions_do_not_persist_across_restarts() {
  let sdk_directory = TestSdkDirectory::new();
  let config = SessionStrategyConfig::Fixed;

  let (first_strategy, first_worker) =
    make_session_strategy(sdk_directory.path(), &config).into_parts();
  let first_session_id = first_strategy.session_id().unwrap();
  bd_session::test::flush(first_strategy.clone(), first_worker).await;
  drop(first_strategy);

  let restarted_strategy = make_session_strategy(sdk_directory.path(), &config).strategy();
  let restarted_session_id = restarted_strategy.session_id().unwrap();

  assert_ne!(first_session_id, restarted_session_id);
  assert_eq!(
    Some(first_session_id),
    restarted_strategy.previous_process_session_id()
  );
}

#[tokio::test]
async fn activity_based_sessions_persist_across_restarts_within_threshold() {
  let sdk_directory = TestSdkDirectory::new();
  let now = OffsetDateTime::now_utc();
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let config = SessionStrategyConfig::ActivityBased {
    inactivity_threshold_mins: 30,
  };

  let (first_strategy, first_worker) =
    make_session_strategy_with_time_provider(sdk_directory.path(), &config, time_provider.clone())
      .into_parts();
  let first_session_id = first_strategy.session_id().unwrap();
  bd_session::test::flush(first_strategy.clone(), first_worker).await;
  drop(first_strategy);

  time_provider.advance(Duration::minutes(5));

  let restarted_strategy =
    make_session_strategy_with_time_provider(sdk_directory.path(), &config, time_provider)
      .strategy();
  let restarted_session_id = restarted_strategy.session_id().unwrap();

  assert_eq!(first_session_id, restarted_session_id);
  assert_eq!(
    Some(first_session_id),
    restarted_strategy.previous_process_session_id()
  );
}
