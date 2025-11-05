// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::snapshot_cleanup::{SnapshotCleanup, SnapshotInfo};
use std::fs;
use tempfile::TempDir;

fn create_mock_snapshot(dir: &TempDir, base_name: &str, version: u64, size: usize) -> SnapshotInfo {
  let filename = format!("{}.v{}.zz", base_name, version);
  let path = dir.path().join(&filename);

  // Create file with specified size
  let data = vec![0u8; size];
  fs::write(&path, data).unwrap();

  SnapshotInfo {
    path,
    version,
    size_bytes: size as u64,
  }
}

#[test]
fn list_snapshots_empty_directory() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();
  let snapshots = cleanup.list_snapshots().unwrap();

  assert!(snapshots.is_empty());
}

#[test]
fn list_snapshots_with_multiple_versions() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  // Create snapshots with different versions
  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);
  create_mock_snapshot(&temp_dir, "test.jrn", 1500, 150);
  create_mock_snapshot(&temp_dir, "test.jrn", 3000, 300);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();
  let snapshots = cleanup.list_snapshots().unwrap();

  assert_eq!(snapshots.len(), 4);
  // Should be sorted by version
  assert_eq!(snapshots[0].version, 1000);
  assert_eq!(snapshots[1].version, 1500);
  assert_eq!(snapshots[2].version, 2000);
  assert_eq!(snapshots[3].version, 3000);

  // Verify sizes
  assert_eq!(snapshots[0].size_bytes, 100);
  assert_eq!(snapshots[1].size_bytes, 150);
  assert_eq!(snapshots[2].size_bytes, 200);
  assert_eq!(snapshots[3].size_bytes, 300);
}

#[test]
fn list_snapshots_ignores_other_files() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  // Create valid snapshots
  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);

  // Create files that should be ignored
  fs::write(temp_dir.path().join("test.jrn"), b"active journal").unwrap();
  fs::write(temp_dir.path().join("other.jrn.v1000.zz"), b"other journal").unwrap();
  fs::write(temp_dir.path().join("test.jrn.v1000"), b"uncompressed").unwrap();
  fs::write(temp_dir.path().join("test.jrn.backup"), b"backup").unwrap();

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();
  let snapshots = cleanup.list_snapshots().unwrap();

  assert_eq!(snapshots.len(), 2);
  assert_eq!(snapshots[0].version, 1000);
  assert_eq!(snapshots[1].version, 2000);
}

#[test]
fn cleanup_before_version_removes_old_snapshots() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);
  create_mock_snapshot(&temp_dir, "test.jrn", 3000, 300);
  create_mock_snapshot(&temp_dir, "test.jrn", 4000, 400);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  // Remove snapshots before version 3000 (keep 3000 and 4000)
  let removed = cleanup.cleanup_before_version(3000).unwrap();

  assert_eq!(removed.len(), 2);
  assert_eq!(removed[0].version, 1000);
  assert_eq!(removed[1].version, 2000);

  // Verify remaining snapshots
  let remaining = cleanup.list_snapshots().unwrap();
  assert_eq!(remaining.len(), 2);
  assert_eq!(remaining[0].version, 3000);
  assert_eq!(remaining[1].version, 4000);

  // Verify files are actually deleted
  assert!(!removed[0].path.exists());
  assert!(!removed[1].path.exists());
  assert!(remaining[0].path.exists());
  assert!(remaining[1].path.exists());
}

#[test]
fn cleanup_before_version_keeps_all_if_min_version_too_low() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  // Min version is lower than all snapshots
  let removed = cleanup.cleanup_before_version(500).unwrap();

  assert!(removed.is_empty());

  let remaining = cleanup.list_snapshots().unwrap();
  assert_eq!(remaining.len(), 2);
}

#[test]
fn cleanup_before_version_removes_all_if_min_version_too_high() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  // Min version is higher than all snapshots
  let removed = cleanup.cleanup_before_version(5000).unwrap();

  assert_eq!(removed.len(), 2);

  let remaining = cleanup.list_snapshots().unwrap();
  assert!(remaining.is_empty());
}

#[test]
fn cleanup_keep_recent_removes_old_snapshots() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);
  create_mock_snapshot(&temp_dir, "test.jrn", 3000, 300);
  create_mock_snapshot(&temp_dir, "test.jrn", 4000, 400);
  create_mock_snapshot(&temp_dir, "test.jrn", 5000, 500);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  // Keep only the 2 most recent snapshots
  let removed = cleanup.cleanup_keep_recent(2).unwrap();

  assert_eq!(removed.len(), 3);
  assert_eq!(removed[0].version, 1000);
  assert_eq!(removed[1].version, 2000);
  assert_eq!(removed[2].version, 3000);

  // Verify remaining snapshots
  let remaining = cleanup.list_snapshots().unwrap();
  assert_eq!(remaining.len(), 2);
  assert_eq!(remaining[0].version, 4000);
  assert_eq!(remaining[1].version, 5000);
}

#[test]
fn cleanup_keep_recent_keeps_all_if_count_too_high() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  // Keep count is higher than total snapshots
  let removed = cleanup.cleanup_keep_recent(5).unwrap();

  assert!(removed.is_empty());

  let remaining = cleanup.list_snapshots().unwrap();
  assert_eq!(remaining.len(), 2);
}

#[test]
fn cleanup_keep_recent_with_zero_removes_all() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  let removed = cleanup.cleanup_keep_recent(0).unwrap();

  assert_eq!(removed.len(), 2);

  let remaining = cleanup.list_snapshots().unwrap();
  assert!(remaining.is_empty());
}

#[test]
fn total_snapshot_size_calculates_correctly() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 250);
  create_mock_snapshot(&temp_dir, "test.jrn", 3000, 150);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();
  let total_size = cleanup.total_snapshot_size().unwrap();

  assert_eq!(total_size, 500); // 100 + 250 + 150
}

#[test]
fn total_snapshot_size_returns_zero_for_empty() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();
  let total_size = cleanup.total_snapshot_size().unwrap();

  assert_eq!(total_size, 0);
}

#[test]
fn oldest_and_newest_snapshot_versions() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  create_mock_snapshot(&temp_dir, "test.jrn", 2000, 200);
  create_mock_snapshot(&temp_dir, "test.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "test.jrn", 3000, 300);

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  assert_eq!(cleanup.oldest_snapshot_version().unwrap(), Some(1000));
  assert_eq!(cleanup.newest_snapshot_version().unwrap(), Some(3000));
}

#[test]
fn oldest_and_newest_return_none_for_empty() {
  let temp_dir = TempDir::new().unwrap();
  let journal_path = temp_dir.path().join("test.jrn");

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();

  assert_eq!(cleanup.oldest_snapshot_version().unwrap(), None);
  assert_eq!(cleanup.newest_snapshot_version().unwrap(), None);
}

#[test]
fn works_with_subdirectory_paths() {
  let temp_dir = TempDir::new().unwrap();
  let subdir = temp_dir.path().join("data");
  fs::create_dir(&subdir).unwrap();

  let journal_path = subdir.join("store.jrn");

  // Create snapshots in subdirectory
  let filename1 = format!("store.jrn.v{}.zz", 1000);
  let path1 = subdir.join(&filename1);
  fs::write(&path1, vec![0u8; 100]).unwrap();

  let filename2 = format!("store.jrn.v{}.zz", 2000);
  let path2 = subdir.join(&filename2);
  fs::write(&path2, vec![0u8; 200]).unwrap();

  let cleanup = SnapshotCleanup::new(&journal_path).unwrap();
  let snapshots = cleanup.list_snapshots().unwrap();

  assert_eq!(snapshots.len(), 2);
  assert_eq!(snapshots[0].version, 1000);
  assert_eq!(snapshots[1].version, 2000);
}

#[test]
fn cleanup_with_different_base_names() {
  let temp_dir = TempDir::new().unwrap();

  // Create snapshots for different journals
  create_mock_snapshot(&temp_dir, "journal_a.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "journal_a.jrn", 2000, 200);
  create_mock_snapshot(&temp_dir, "journal_b.jrn", 1000, 100);
  create_mock_snapshot(&temp_dir, "journal_b.jrn", 2000, 200);

  // Cleanup for journal_a should only see journal_a snapshots
  let cleanup_a = SnapshotCleanup::new(temp_dir.path().join("journal_a.jrn")).unwrap();
  let snapshots_a = cleanup_a.list_snapshots().unwrap();

  assert_eq!(snapshots_a.len(), 2);
  assert!(
    snapshots_a
      .iter()
      .all(|s| s.path.to_string_lossy().contains("journal_a"))
  );

  // Cleanup for journal_b should only see journal_b snapshots
  let cleanup_b = SnapshotCleanup::new(temp_dir.path().join("journal_b.jrn")).unwrap();
  let snapshots_b = cleanup_b.list_snapshots().unwrap();

  assert_eq!(snapshots_b.len(), 2);
  assert!(
    snapshots_b
      .iter()
      .all(|s| s.path.to_string_lossy().contains("journal_b"))
  );
}
