// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncWriteExt;

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

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File>;

  /// Deletes the file if it exists.
  async fn delete_file(&self, path: &Path) -> anyhow::Result<()>;

  /// Deletes the directory if it exists.
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

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    Ok(tokio::fs::File::create(self.directory.join(path)).await?)
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

//
// TestFileSystem
//

/// An in-memory test implementation of a file system, meant to somewhat mimic the behavior of a
/// real filesystem.
pub struct TestFileSystem {
  directory: tempfile::TempDir,
  pub disk_full: AtomicBool,
}

#[async_trait]
impl FileSystem for TestFileSystem {
  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>> {
    let mut files = Vec::new();

    walkdir::WalkDir::new(self.directory.path().join(directory))
      .into_iter()
      .filter_map(std::result::Result::ok)
      .filter(|e| e.file_type().is_file())
      .for_each(|e| {
        files.push(e.path().to_string_lossy().to_string());
      });

    Ok(files)
  }

  async fn exists(&self, path: &Path) -> anyhow::Result<bool> {
    Ok(self.directory.path().join(path).exists())
  }

  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>> {
    let file_path = self.directory.path().join(path);

    tokio::fs::read(&file_path)
      .await
      .map_err(|e| anyhow!("failed to read file {}: {}", file_path.display(), e))
  }

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    if self.disk_full.load(Ordering::Relaxed) {
      anyhow::bail!("disk full");
    }

    let file_path = self.directory.path().join(path);
    let mut file = tokio::fs::File::create(&file_path)
      .await
      .map_err(|e| anyhow!("failed to create file {}: {}", file_path.display(), e))?;
    file
      .write_all(data)
      .await
      .map_err(|e| anyhow!("failed to write file {}: {}", file_path.display(), e))?;
    file.sync_all().await.unwrap();

    Ok(())
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    let file_path = self.directory.path().join(path);
    if !file_path.exists() {
      return Ok(());
    }

    tokio::fs::remove_file(&file_path)
      .await
      .map_err(|e| anyhow!("failed to delete file {}: {}", file_path.display(), e))?;

    Ok(())
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    let dir_path = self.directory.path().join(path);
    if !dir_path.exists() {
      return Ok(());
    }

    tokio::fs::remove_dir_all(&dir_path)
      .await
      .map_err(|e| anyhow!("failed to remove directory {}: {}", dir_path.display(), e))?;

    Ok(())
  }

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()> {
    let dir_path = self.directory.path().join(path);
    if dir_path.exists() {
      return Ok(());
    }
    tokio::fs::create_dir_all(&dir_path)
      .await
      .map_err(|e| anyhow!("failed to create directory {}: {}", dir_path.display(), e))?;

    Ok(())
  }

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    if self.disk_full.load(Ordering::Relaxed) {
      anyhow::bail!("disk full");
    }

    let file_path = self.directory.path().join(path);
    tokio::fs::File::create(&file_path)
      .await
      .map_err(|e| anyhow!("failed to create file {}: {}", file_path.display(), e))
  }
}

impl Default for TestFileSystem {
  fn default() -> Self {
    Self::new()
  }
}

impl TestFileSystem {
  #[must_use]
  pub fn new() -> Self {
    Self {
      directory: tempfile::tempdir().expect("failed to create temp dir"),
      disk_full: AtomicBool::new(false),
    }
  }

  pub fn files(&self) -> HashMap<String, Vec<u8>> {
    walkdir::WalkDir::new(self.directory.path())
      .into_iter()
      .filter_map(std::result::Result::ok)
      .filter(|e| e.file_type().is_file())
      .map(|e| {
        let data = std::fs::read(e.path()).expect("failed to read file");

        let path = e
          .path()
          .strip_prefix(self.directory.path())
          .expect("failed to strip prefix");
        (Self::path_as_str(path), data)
      })
      .collect()
  }

  fn path_as_str(path: impl AsRef<Path>) -> String {
    path.as_ref().as_os_str().to_str().unwrap().to_string()
  }
}
