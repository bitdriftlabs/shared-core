// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{
  SessionStrategyConfig,
  make_session_strategy,
  make_session_strategy_with_time_provider,
};
use bd_time::TestTimeProvider;
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

#[tokio::test]
async fn fixed_sessions_do_not_persist_across_restarts() {
  let sdk_directory = TestSdkDirectory::new();
  let config = SessionStrategyConfig::Fixed;

  let first_strategy = make_session_strategy(sdk_directory.path(), &config);
  let first_session_id = first_strategy.session_id().await.unwrap();
  first_strategy.flush().await;
  drop(first_strategy);

  let restarted_strategy = make_session_strategy(sdk_directory.path(), &config);
  let restarted_session_id = restarted_strategy.session_id().await.unwrap();

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

  let first_strategy =
    make_session_strategy_with_time_provider(sdk_directory.path(), &config, time_provider.clone());
  let first_session_id = first_strategy.session_id().await.unwrap();
  first_strategy.flush().await;
  drop(first_strategy);

  time_provider.advance(Duration::minutes(5));

  let restarted_strategy =
    make_session_strategy_with_time_provider(sdk_directory.path(), &config, time_provider);
  let restarted_session_id = restarted_strategy.session_id().await.unwrap();

  assert_eq!(first_session_id, restarted_session_id);
  assert_eq!(
    Some(first_session_id),
    restarted_strategy.previous_process_session_id()
  );
}
