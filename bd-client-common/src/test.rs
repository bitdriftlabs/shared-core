// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::panic, clippy::unwrap_used)]

use crate::file_system::FileSystem;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncWriteExt as _;

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
      .map_err(|e| anyhow::anyhow!("failed to read file {}: {}", file_path.display(), e))
  }

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    if self.disk_full.load(Ordering::Relaxed) {
      anyhow::bail!("disk full");
    }

    let file_path = self.directory.path().join(path);
    let mut file = tokio::fs::File::create(&file_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to create file {}: {}", file_path.display(), e))?;
    file
      .write_all(data)
      .await
      .map_err(|e| anyhow::anyhow!("failed to write file {}: {}", file_path.display(), e))?;
    file.sync_all().await?;

    Ok(())
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    let file_path = self.directory.path().join(path);
    if !file_path.exists() {
      return Ok(());
    }

    tokio::fs::remove_file(&file_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to delete file {}: {}", file_path.display(), e))?;

    Ok(())
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    let dir_path = self.directory.path().join(path);
    if !dir_path.exists() {
      return Ok(());
    }

    tokio::fs::remove_dir_all(&dir_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to remove directory {}: {}", dir_path.display(), e))?;

    Ok(())
  }

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()> {
    let dir_path = self.directory.path().join(path);
    if dir_path.exists() {
      return Ok(());
    }
    tokio::fs::create_dir_all(&dir_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to create directory {}: {}", dir_path.display(), e))?;

    Ok(())
  }

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    if self.disk_full.load(Ordering::Relaxed) {
      anyhow::bail!("disk full");
    }

    let file_path = self.directory.path().join(path);
    tokio::fs::File::create(&file_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to create file {}: {}", file_path.display(), e))
  }
}

impl TestFileSystem {
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
    path
      .as_ref()
      .as_os_str()
      .to_str()
      .unwrap_or_default()
      .to_string()
  }
}
