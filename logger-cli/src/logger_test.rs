// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::make_session_strategy;
use std::fs;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

struct TestSdkDirectory {
  path: PathBuf,
}

impl TestSdkDirectory {
  fn new() -> Self {
    let path = std::env::current_dir().unwrap().join(".tmp").join(format!(
      "logger-cli-session-{}",
      OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    Self { path }
  }

  fn path(&self) -> &Path {
    &self.path
  }
}

impl Drop for TestSdkDirectory {
  fn drop(&mut self) {
    let _ignored = fs::remove_dir_all(&self.path);
  }
}

#[tokio::test]
async fn fixed_sessions_do_not_persist_across_restarts() {
  let sdk_directory = TestSdkDirectory::new();

  let first_strategy = make_session_strategy(sdk_directory.path());
  let first_session_id = first_strategy.session_id().await.unwrap();
  first_strategy.flush().await;
  drop(first_strategy);

  let restarted_strategy = make_session_strategy(sdk_directory.path());
  let restarted_session_id = restarted_strategy.session_id().await.unwrap();

  assert_ne!(first_session_id, restarted_session_id);
  assert_eq!(
    Some(first_session_id),
    restarted_strategy.previous_process_session_id()
  );
}
