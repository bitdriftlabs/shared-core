// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use async_trait::async_trait;
use std::path::{Path, PathBuf};

//
// FileSystem
//

/// A filesystem scoped to the SDK directory. This allows for mocking and abstracting away the
/// relative root of the data directory used by the SDK.
#[async_trait]
pub trait FileSystem: Send + Sync {
  async fn exists(&self, path: &Path) -> anyhow::Result<bool>;

  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>>;

  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>>;

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()>;

  /// Deletes the file if it exists.
  async fn delete_file(&self, path: &Path) -> anyhow::Result<()>;
  /// Deletes the file if it exists.
  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()>;

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()>;
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
      let path = entry.path();

      if path.is_file() {
        files.push(path.to_string_lossy().to_string());
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

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    Ok(tokio::fs::write(self.directory.join(path), data).await?)
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(self.directory.join(path)).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_dir_all(self.directory.join(path)).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()> {
    Ok(tokio::fs::create_dir(self.directory.join(path)).await?)
  }
}
