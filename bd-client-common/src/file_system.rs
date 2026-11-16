// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! SDK-rooted filesystem access and explicit file ownership.
//!
//! Atomic visibility and durability are separate operations: a rename publishes a file, while
//! syncing the file and parent directory makes that publication crash-safe. Callers must retain
//! published payloads when the final sync fails, because another reader may already own them.
//! `OwnedFile` is the complementary producer-side guard: it removes a handed-over absolute path,
//! with synchronous cleanup as a fallback, or relinquishes it after a move.

use async_trait::async_trait;
use std::path::{Path, PathBuf};

//
// OwnedFile
//

/// Owns cleanup of a handed-over absolute file, including synchronous cleanup on drop.
///
/// Relative paths remain the consumer's responsibility because only that consumer knows its root.
/// A moved file must be disarmed immediately: retaining the old path could delete a replacement
/// created there after ownership has moved into SDK storage.
#[derive(Debug)]
pub struct OwnedFile(Option<PathBuf>);

impl OwnedFile {
  #[must_use]
  pub fn new(path: &Path) -> Option<Self> {
    path.is_absolute().then(|| Self(Some(path.to_owned())))
  }

  pub fn disarm(mut self) {
    self.0 = None;
  }

  /// Removes the file off the async executor, retaining cleanup ownership if awaiting is cancelled.
  ///
  /// The worker disarms the fallback before attempting removal, even when removal fails, so a
  /// later drop cannot unlink a replacement at the same path.
  pub async fn remove(self) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
      let mut owned = self;
      if let Some(path) = owned.0.take() {
        match std::fs::remove_file(path) {
          Ok(()) => {},
          Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
          Err(error) => return Err(error),
        }
      }
      Ok(())
    })
    .await??;
    Ok(())
  }
}

impl Drop for OwnedFile {
  fn drop(&mut self) {
    if let Some(path) = self.0.take()
      && let Err(error) = std::fs::remove_file(&path)
      && error.kind() != std::io::ErrorKind::NotFound
    {
      log::debug!("failed to remove owned file {}: {error}", path.display());
    }
  }
}

//
// FileSystem
//

/// A filesystem scoped to the SDK directory. This allows for mocking and abstracting away the
/// relative root of the data directory used by the SDK.
///
/// Hard links are not supported for Android SDK storage, so this trait must not rely on them.
#[async_trait]
pub trait FileSystem: Send + Sync {
  async fn exists(&self, path: &Path) -> anyhow::Result<bool>;

  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>>;

  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>>;

  async fn open_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File>;

  async fn open_regular_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File>;

  /// Available payload space; callers must additionally reserve metadata/free-space headroom.
  async fn available_space(&self, directory: &Path) -> anyhow::Result<u64>;

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()>;

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File>;

  /// Deletes the file if it exists.
  async fn delete_file(&self, path: &Path) -> anyhow::Result<()>;

  /// Renames/moves a file relative to the SDK root.
  async fn rename_file(&self, from: &Path, to: &Path) -> anyhow::Result<()>;

  /// Ensures a file and its directory entry survive a crash before acknowledging a lease.
  async fn sync_file_and_parent(&self, path: &Path) -> anyhow::Result<()>;

  /// Syncs file contents without syncing its parent; callers must sync the directory before commit.
  async fn sync_file(&self, path: &Path) -> anyhow::Result<()> {
    Ok(self.open_file(path).await?.sync_all().await?)
  }

  /// Syncs directory entries after all files in a staged artifact have been synced.
  async fn sync_directory(&self, path: &Path) -> anyhow::Result<()> {
    Ok(self.open_file(path).await?.sync_all().await?)
  }

  /// Deletes the directory if it exists.
  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()>;

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()>;
}

pub async fn open_regular_file_async(path: &Path) -> std::io::Result<tokio::fs::File> {
  if !tokio::fs::symlink_metadata(path).await?.is_file() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "attachment must be a regular file",
    ));
  }
  let mut options = tokio::fs::OpenOptions::new();
  options.read(true);
  #[cfg(unix)]
  options.custom_flags(libc::O_NOFOLLOW);
  let file = options.open(path).await?;
  if !file.metadata().await?.is_file() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "attachment must be a regular file",
    ));
  }
  Ok(file)
}

/// Writes a file through a sibling temporary path so readers never observe a partial write.
pub async fn write_file_atomic(path: &Path, data: &[u8]) -> anyhow::Result<()> {
  if let Some(parent) = path.parent() {
    tokio::fs::create_dir_all(parent).await?;
  }

  let tmp_path = path.with_extension("tmp");
  tokio::fs::write(&tmp_path, data).await?;
  tokio::fs::rename(&tmp_path, path).await?;
  Ok(())
}

pub async fn sync_file_and_parent(path: &Path) -> anyhow::Result<()> {
  tokio::fs::File::open(path).await?.sync_all().await?;
  if let Some(parent) = path.parent() {
    tokio::fs::File::open(parent).await?.sync_all().await?;
  }
  Ok(())
}

/// Delete a file if it exists, treating a not found error as success.
pub async fn delete_file_if_exists_async(path: &Path) -> anyhow::Result<()> {
  match tokio::fs::remove_file(path).await {
    Ok(()) => Ok(()),
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
    Err(e) => Err(e.into()),
  }
}

/// Delete a directory tree while treating an absent directory as success.
pub async fn remove_dir_if_exists_async(path: &Path) -> anyhow::Result<()> {
  match tokio::fs::remove_dir_all(path).await {
    Ok(()) => Ok(()),
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
    Err(e) => Err(e.into()),
  }
}

//
// RealFileSystem
//

/// The real filesystem implementation which delegates to `tokio::fs`, joining the relative paths
/// provided in the calls with the SDK directory.
pub struct RealFileSystem {
  directory: PathBuf,
}

impl RealFileSystem {
  #[must_use]
  pub const fn new(directory: PathBuf) -> Self {
    Self { directory }
  }
}

#[async_trait]
impl FileSystem for RealFileSystem {
  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>> {
    let mut files = Vec::new();

    let mut entries = tokio::fs::read_dir(self.directory.join(directory)).await?;

    while let Some(entry) = entries.next_entry().await? {
      let file_type = entry.file_type().await?;

      if file_type.is_file()
        || (file_type.is_symlink() && tokio::fs::metadata(entry.path()).await?.is_file())
      {
        files.push(entry.path().to_string_lossy().to_string());
      }
    }

    Ok(files)
  }

  async fn exists(&self, path: &Path) -> anyhow::Result<bool> {
    match tokio::fs::metadata(self.directory.join(path)).await {
      Ok(_) => Ok(true),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
      Err(e) => Err(e.into()),
    }
  }

  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>> {
    Ok(tokio::fs::read(self.directory.join(path)).await?)
  }

  async fn open_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    Ok(tokio::fs::File::open(self.directory.join(path)).await?)
  }

  async fn open_regular_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    Ok(open_regular_file_async(&self.directory.join(path)).await?)
  }

  async fn available_space(&self, directory: &Path) -> anyhow::Result<u64> {
    let directory = self.directory.join(directory);
    Ok(tokio::task::spawn_blocking(move || fs2::available_space(directory)).await??)
  }

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    Ok(tokio::fs::write(self.directory.join(path), data).await?)
  }

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    Ok(tokio::fs::File::create(self.directory.join(path)).await?)
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    delete_file_if_exists_async(&self.directory.join(path)).await
  }

  async fn rename_file(&self, from: &Path, to: &Path) -> anyhow::Result<()> {
    Ok(tokio::fs::rename(self.directory.join(from), self.directory.join(to)).await?)
  }

  async fn sync_file_and_parent(&self, path: &Path) -> anyhow::Result<()> {
    sync_file_and_parent(&self.directory.join(path)).await
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    remove_dir_if_exists_async(&self.directory.join(path)).await
  }

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()> {
    Ok(tokio::fs::create_dir(self.directory.join(path)).await?)
  }
}
