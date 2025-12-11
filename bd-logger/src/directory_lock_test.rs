// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::DirectoryLock;
use tempfile::TempDir;

#[tokio::test]
async fn acquires_lock_on_new_directory() {
  let temp_dir = TempDir::new().unwrap();
  let lock = DirectoryLock::try_acquire(temp_dir.path().to_path_buf())
    .await
    .unwrap();

  // Verify lock file was created
  let lock_file = temp_dir.path().join(".bitdrift_lock");
  assert!(lock_file.exists());

  drop(lock);
}

#[tokio::test]
async fn creates_directory_if_not_exists() {
  let temp_dir = TempDir::new().unwrap();
  let subdir = temp_dir.path().join("does_not_exist");

  // Directory doesn't exist yet
  assert!(!subdir.exists());

  let _lock = DirectoryLock::try_acquire(subdir.clone()).await.unwrap();

  // Directory should now exist
  assert!(subdir.exists());
}

#[tokio::test]
async fn lock_released_on_drop() {
  let temp_dir = TempDir::new().unwrap();

  {
    let _lock = DirectoryLock::try_acquire(temp_dir.path().to_path_buf())
      .await
      .unwrap();
    // Lock is held here
  } // Lock is dropped here

  // Should be able to acquire the lock again
  let _lock2 = DirectoryLock::try_acquire(temp_dir.path().to_path_buf())
    .await
    .unwrap();
}

#[tokio::test]
async fn second_lock_fails_while_first_is_held() {
  let temp_dir = TempDir::new().unwrap();

  let _lock1 = DirectoryLock::try_acquire(temp_dir.path().to_path_buf())
    .await
    .unwrap();

  // Second lock should fail
  let result = DirectoryLock::try_acquire(temp_dir.path().to_path_buf()).await;
  assert!(result.is_err());

  // Error should mention lock acquisition failure
  let err = result.unwrap_err();
  assert!(err.to_string().contains("failed to acquire lock"));
}

#[tokio::test]
async fn reuses_existing_lock_file() {
  let temp_dir = TempDir::new().unwrap();

  // Create lock file first
  let lock_file_path = temp_dir.path().join(".bitdrift_lock");
  std::fs::write(&lock_file_path, "").unwrap();

  // Should reuse the existing file, not fail
  let _lock = DirectoryLock::try_acquire(temp_dir.path().to_path_buf())
    .await
    .unwrap();

  assert!(lock_file_path.exists());
}
