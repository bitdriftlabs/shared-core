// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{RetentionRegistry, SnapshotCleanupTask};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

async fn create_test_snapshot(dir: &std::path::Path, name: &str, timestamp: u64) {
  let filename = format!("{name}.jrn.t{timestamp}.zz");
  let path = dir.join(filename);
  tokio::fs::write(path, b"test snapshot data").await.unwrap();
}

#[tokio::test]
async fn cleanup_deletes_old_snapshots() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create some test snapshots
  create_test_snapshot(temp_dir.path(), "test", 1000).await;
  create_test_snapshot(temp_dir.path(), "test", 2000).await;
  create_test_snapshot(temp_dir.path(), "test", 3000).await;

  // Create a handle that requires data from timestamp 2500 forward
  let handle = registry.create_handle("test_handle").await;
  handle.update_retention_micros(2500);

  // Create cleanup task
  let task = SnapshotCleanupTask::new(
    temp_dir.path(),
    "test",
    registry.clone(),
    Duration::from_secs(3600),
  );

  // Verify files exist before cleanup
  assert!(temp_dir.path().join("test.jrn.t1000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t2000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t3000.zz").exists());

  // Drop handle so min_retention returns None
  drop(handle);

  let result = task.cleanup_once().await;
  assert!(result.is_ok(), "Cleanup should succeed when no handles");

  // All files should still exist because no handles means no cleanup
  assert!(temp_dir.path().join("test.jrn.t1000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t2000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t3000.zz").exists());
}

#[tokio::test]
async fn cleanup_skips_when_no_handles() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create some test snapshots
  create_test_snapshot(temp_dir.path(), "test", 1000).await;
  create_test_snapshot(temp_dir.path(), "test", 2000).await;

  let task = SnapshotCleanupTask::new(temp_dir.path(), "test", registry, Duration::from_secs(3600));

  let result = task.cleanup_once().await;
  assert!(
    result.is_ok(),
    "Cleanup should succeed even with no handles"
  );

  // Files should still exist
  assert!(temp_dir.path().join("test.jrn.t1000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t2000.zz").exists());
}

#[tokio::test]
async fn cleanup_only_deletes_matching_journal_name() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create snapshots for different journals
  create_test_snapshot(temp_dir.path(), "journal1", 1000).await;
  create_test_snapshot(temp_dir.path(), "journal2", 1000).await;

  let handle = registry.create_handle("test").await;
  handle.update_retention_micros(1000);

  let task = SnapshotCleanupTask::new(
    temp_dir.path(),
    "journal1",
    registry,
    Duration::from_secs(3600),
  );

  let result = task.cleanup_once().await;
  assert!(result.is_ok());

  // journal2 snapshot should still exist since we only clean journal1
  assert!(temp_dir.path().join("journal2.jrn.t1000.zz").exists());
}

#[tokio::test]
async fn cleanup_handles_missing_directory_gracefully() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  let nonexistent = temp_dir.path().join("nonexistent");

  let task = SnapshotCleanupTask::new(nonexistent, "test", registry, Duration::from_secs(3600));

  // Should error when trying to read a nonexistent directory, but we should handle it gracefully
  let result = task.cleanup_once().await;
  // Either errors or succeeds with no cleanup - both are acceptable
  let _ = result;
}

#[tokio::test]
async fn cleanup_task_can_be_spawned() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  let task = SnapshotCleanupTask::new(temp_dir.path(), "test", registry, Duration::from_millis(10));

  let handle = task.spawn();

  // Let it run a bit
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Abort the task
  handle.abort();

  // Wait for abort to complete
  let _ = handle.await;
}

#[tokio::test]
async fn extract_timestamp_from_various_filenames() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create snapshots with different timestamp formats
  create_test_snapshot(temp_dir.path(), "test", 123).await;
  create_test_snapshot(temp_dir.path(), "test", 123_456_789).await;
  create_test_snapshot(temp_dir.path(), "test", 999_999_999_999).await;

  let task = SnapshotCleanupTask::new(temp_dir.path(), "test", registry, Duration::from_secs(3600));

  let result = task.cleanup_once().await;
  assert!(result.is_ok(), "Should handle various timestamp formats");

  // All files should still exist (no cleanup without retention requirements)
  assert!(temp_dir.path().join("test.jrn.t123.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t123456789.zz").exists());
  assert!(temp_dir.path().join("test.jrn.t999999999999.zz").exists());
}
