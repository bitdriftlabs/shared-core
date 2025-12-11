// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./directory_lock_test.rs"]
mod tests;

use anyhow::Context;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// A lock on a directory to ensure exclusive access across multiple processes.
///
/// The lock is implemented using a lock file with flock-based file locking, which is released
/// automatically when the `DirectoryLock` is dropped.
#[derive(Debug)]
pub struct DirectoryLock {
  // The lock file handle must be kept alive for the duration of the lock
  _lock_file: File,
}

impl DirectoryLock {
  const LOCK_FILE_NAME: &'static str = ".bitdrift_lock";

  /// Attempts to acquire an exclusive lock on the specified directory asynchronously.
  ///
  /// This function performs blocking I/O operations on a separate thread pool to avoid
  /// blocking the async runtime. It will:
  /// 1. Create the directory if it doesn't exist (mkdir -p behavior)
  /// 2. Create a lock file in the directory if it doesn't exist
  /// 3. Attempt to acquire an exclusive, non-blocking lock on the file
  ///
  /// # Errors
  /// Returns an error if:
  /// - The directory cannot be created
  /// - The lock file cannot be opened/created
  /// - Another process already holds the lock
  /// - The blocking task fails to spawn
  pub async fn try_acquire(directory: PathBuf) -> anyhow::Result<Self> {
    tokio::task::spawn_blocking(move || Self::try_acquire_blocking(&directory))
      .await
      .context("spawn_blocking failed")?
  }

  /// Synchronous version of `try_acquire` for use in non-async contexts.
  ///
  /// # Implementation Notes
  ///
  /// This uses flock-based file locking which has the following properties:
  /// - Locks are associated with the file inode, not the file path
  /// - Multiple processes opening the same file will reference the same inode
  /// - If the file is deleted while locked, a new process can create a new file with a different
  ///   inode, potentially acquiring a separate lock. However, this is acceptable because:
  ///   1. The lock file is hidden and in an app-specific directory
  ///   2. No legitimate process should delete it during normal operation
  ///   3. The lock is advisory and primarily prevents accidental misuse
  ///
  /// # Errors
  /// Returns an error if the directory cannot be created, the lock file cannot be opened,
  /// or the lock cannot be acquired.
  fn try_acquire_blocking(directory: &Path) -> anyhow::Result<Self> {
    std::fs::create_dir_all(directory)
      .with_context(|| format!("failed to create directory: {}", directory.display()))?;

    let lock_file_path = directory.join(Self::LOCK_FILE_NAME);

    // Open or create the lock file. The O_CREAT flag ensures that:
    // - If the file doesn't exist, it's created atomically
    // - If multiple processes try to create it simultaneously, they all get the same inode
    // - This prevents race conditions where different processes get different inodes
    let lock_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(&lock_file_path)
      .with_context(|| format!("failed to open lock file: {}", lock_file_path.display()))?;

    // Try to acquire an exclusive, non-blocking lock on the file.
    // This will fail immediately if another process holds the lock on the same inode.
    lock_file
      .try_lock_exclusive()
      .with_context(|| format!("failed to acquire lock on: {}", lock_file_path.display()))?;

    Ok(Self {
      _lock_file: lock_file,
    })
  }
}
