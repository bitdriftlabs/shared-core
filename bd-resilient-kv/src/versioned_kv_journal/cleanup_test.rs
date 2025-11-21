// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{RetentionRegistry, cleanup_old_snapshots};
use std::sync::Arc;
use tempfile::TempDir;

async fn create_test_snapshot(dir: &std::path::Path, name: &str, generation: u64, timestamp: u64) {
  let filename = format!("{name}.jrn.g{generation}.t{timestamp}.zz");
  let path = dir.join(filename);
  // Create parent directory if it doesn't exist
  if let Some(parent) = path.parent() {
    tokio::fs::create_dir_all(parent).await.unwrap();
  }
  tokio::fs::write(path, b"test snapshot data").await.unwrap();
}

#[tokio::test]
async fn cleanup_deletes_old_snapshots() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create some test snapshots with different timestamps
  create_test_snapshot(temp_dir.path(), "test", 0, 1000).await;
  create_test_snapshot(temp_dir.path(), "test", 1, 2000).await;
  create_test_snapshot(temp_dir.path(), "test", 2, 3000).await;

  // Create a handle that requires data from timestamp 2500 forward
  let handle = registry.create_handle().await;
  handle.update_retention_micros(2500);

  // Verify files exist before cleanup
  assert!(temp_dir.path().join("test.jrn.g0.t1000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.g1.t2000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.g2.t3000.zz").exists());

  // Run cleanup
  let result = cleanup_old_snapshots(temp_dir.path(), "test", &registry).await;
  assert!(result.is_ok(), "Cleanup should succeed");

  // Old snapshots (timestamp < 2500) should be deleted
  assert!(!temp_dir.path().join("test.jrn.g0.t1000.zz").exists());
  assert!(!temp_dir.path().join("test.jrn.g1.t2000.zz").exists());

  // New snapshot (timestamp >= 2500) should still exist
  assert!(temp_dir.path().join("test.jrn.g2.t3000.zz").exists());
}

#[tokio::test]
async fn cleanup_skips_when_no_handles() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create some test snapshots
  create_test_snapshot(temp_dir.path(), "test", 0, 1000).await;
  create_test_snapshot(temp_dir.path(), "test", 1, 2000).await;

  // Run cleanup without any handles
  let result = cleanup_old_snapshots(temp_dir.path(), "test", &registry).await;
  assert!(
    result.is_ok(),
    "Cleanup should succeed even with no handles"
  );

  // Files should still exist because no handles means no cleanup
  assert!(temp_dir.path().join("test.jrn.g0.t1000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.g1.t2000.zz").exists());
}

#[tokio::test]
async fn cleanup_only_deletes_matching_journal_name() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create snapshots for different journals
  create_test_snapshot(temp_dir.path(), "journal1", 0, 1000).await;
  create_test_snapshot(temp_dir.path(), "journal2", 0, 1000).await;

  let handle = registry.create_handle().await;
  handle.update_retention_micros(2000); // Delete anything older than 2000

  // Run cleanup for journal1 only
  let result = cleanup_old_snapshots(temp_dir.path(), "journal1", &registry).await;
  assert!(result.is_ok());

  // journal1 snapshot should be deleted (timestamp 1000 < 2000)
  assert!(!temp_dir.path().join("journal1.jrn.g0.t1000.zz").exists());

  // journal2 snapshot should still exist (different journal name)
  assert!(temp_dir.path().join("journal2.jrn.g0.t1000.zz").exists());
}

#[tokio::test]
async fn cleanup_handles_missing_directory_gracefully() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  let nonexistent = temp_dir.path().join("nonexistent");

  // Create a handle so cleanup actually tries to run
  let _handle = registry.create_handle().await;

  // Should error when trying to read a nonexistent directory
  let result = cleanup_old_snapshots(&nonexistent, "test", &registry).await;
  assert!(result.is_err(), "Should error for nonexistent directory");
}

#[tokio::test]
async fn cleanup_respects_zero_retention() {
  let temp_dir = TempDir::new().unwrap();
  let registry = Arc::new(RetentionRegistry::new());

  // Create some test snapshots
  create_test_snapshot(temp_dir.path(), "test", 0, 1000).await;
  create_test_snapshot(temp_dir.path(), "test", 1, 2000).await;

  // Create handle with retention timestamp 0 (retain all)
  let handle = registry.create_handle().await;
  handle.update_retention_micros(0);

  let result = cleanup_old_snapshots(temp_dir.path(), "test", &registry).await;
  assert!(result.is_ok());

  // All files should still exist (retention timestamp 0 means keep everything)
  assert!(temp_dir.path().join("test.jrn.g0.t1000.zz").exists());
  assert!(temp_dir.path().join("test.jrn.g1.t2000.zz").exists());
}
