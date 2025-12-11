// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::{InitParams, LoggerBuilder};
use bd_device::Store;
use bd_noop_network::NoopNetwork;
use bd_session::fixed::{Strategy, UUIDCallbacks};
use bd_test_helpers::metadata::EmptyMetadata;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::session::in_memory_store;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// Helper to create minimal init params for testing directory locking
fn create_init_params(sdk_directory: &TempDir) -> InitParams {
  let session_store = in_memory_store();
  let device_store = Arc::new(Store::new(Box::new(
    bd_test_helpers::session::InMemoryStorage::default(),
  )));

  InitParams {
    sdk_directory: sdk_directory.path().into(),
    api_key: "test-api-key".to_string(),
    session_strategy: Arc::new(bd_session::Strategy::Fixed(Strategy::new(
      session_store,
      Arc::new(UUIDCallbacks),
    ))),
    metadata_provider: Arc::new(LogMetadata::default()),
    resource_utilization_target: Box::new(EmptyTarget),
    session_replay_target: Box::new(bd_test_helpers::session_replay::NoOpTarget),
    events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
    device: Arc::new(bd_device::Device::new(device_store.clone())),
    store: device_store,
    network: Box::new(NoopNetwork),
    static_metadata: Arc::new(EmptyMetadata),
    start_in_sleep_mode: false,
  }
}

#[tokio::test]
async fn multiple_loggers_same_directory_second_becomes_noop() {
  // Create a shared SDK directory
  let shared_sdk_dir = TempDir::new().unwrap();

  // First logger should successfully acquire the lock
  let params1 = create_init_params(&shared_sdk_dir);
  let (_logger1, _tx1, future1, _flush1) = LoggerBuilder::new(params1).build().unwrap();

  // Spawn the first logger's event loop
  let handle1 = tokio::spawn(future1);

  // Give it a moment to acquire the lock
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Second logger with the same directory should fail to acquire lock and become no-op
  let params2 = create_init_params(&shared_sdk_dir);
  let (_logger2, _tx2, future2, _flush2) = LoggerBuilder::new(params2).build().unwrap();

  // Spawn the second logger's event loop - it should return immediately (no-op mode)
  let handle2 = tokio::spawn(future2);

  // The second logger's future should complete quickly because it couldn't acquire the lock
  let result = tokio::time::timeout(Duration::from_millis(500), handle2).await;
  assert!(
    result.is_ok(),
    "Second logger should exit quickly in no-op mode"
  );

  // Clean up first logger
  handle1.abort();
}

#[tokio::test]
async fn logger_releases_lock_on_shutdown() {
  let shared_sdk_dir = TempDir::new().unwrap();

  // First logger acquires the lock
  {
    let params1 = create_init_params(&shared_sdk_dir);
    let (_logger1, _tx1, future1, _flush1) = LoggerBuilder::new(params1).build().unwrap();

    let handle1 = tokio::spawn(future1);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Abort the first logger - this should release the lock when the future is dropped
    handle1.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle1).await;
  }

  // Give OS a moment to fully release the lock
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Second logger should now be able to acquire the lock successfully
  let params2 = create_init_params(&shared_sdk_dir);
  let (_logger2, _tx2, future2, _flush2) = LoggerBuilder::new(params2).build().unwrap();

  let handle2 = tokio::spawn(future2);

  // Give it time to acquire the lock and start running
  tokio::time::sleep(Duration::from_millis(200)).await;

  // The second logger should still be running (not in no-op mode)
  // If it was in no-op mode, the future would have completed immediately
  assert!(
    !handle2.is_finished(),
    "Second logger should be running normally"
  );

  // Clean up
  handle2.abort();
}

#[tokio::test]
async fn concurrent_logger_initialization() {
  let shared_sdk_dir = TempDir::new().unwrap();

  // Build two loggers concurrently
  let params1 = create_init_params(&shared_sdk_dir);
  let params2 = create_init_params(&shared_sdk_dir);

  let (logger1, _tx1, future1, _flush1) = LoggerBuilder::new(params1).build().unwrap();
  let (logger2, _tx2, future2, _flush2) = LoggerBuilder::new(params2).build().unwrap();

  // Spawn both event loops - one should get the lock, one should go no-op
  let handle1 = tokio::spawn(future1);
  let handle2 = tokio::spawn(future2);

  // Wait a bit for lock acquisition to happen
  tokio::time::sleep(Duration::from_millis(200)).await;

  // One handle should be finished (no-op mode), one should still be running
  let finished_count = [handle1.is_finished(), handle2.is_finished()]
    .iter()
    .filter(|&&x| x)
    .count();

  assert_eq!(
    finished_count, 1,
    "Exactly one logger should be in no-op mode (finished)"
  );

  // Clean up both loggers
  drop(logger1);
  drop(logger2);
  handle1.abort();
  handle2.abort();
}
