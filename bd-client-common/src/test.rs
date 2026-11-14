// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use crate::file_system::FileSystem;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::oneshot;

//
// TestFileSystem
//

/// A temporary-directory filesystem with deterministic storage fault injection.
pub struct TestFileSystem {
  directory: tempfile::TempDir,
  pub disk_full: AtomicBool,
  pub available_bytes: Mutex<Option<u64>>,
  pub fail_deletes: AtomicBool,
  pub fail_next_sync: AtomicBool,
  pub fail_next_rename: AtomicBool,
  pub list_calls: AtomicUsize,
  pub delete_calls: AtomicUsize,
  pub file_sync_calls: AtomicUsize,
  pub directory_sync_calls: AtomicUsize,
  fail_sync_path: Mutex<Option<PathBuf>>,
  sync_gate: Mutex<Option<(PathBuf, oneshot::Sender<oneshot::Sender<()>>)>>,
}

#[async_trait]
impl FileSystem for TestFileSystem {
  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>> {
    self.list_calls.fetch_add(1, Ordering::Relaxed);
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

  async fn open_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    let file_path = self.directory.path().join(path);
    tokio::fs::File::open(&file_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to open file {}: {}", file_path.display(), e))
  }

  async fn open_regular_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    Ok(crate::file_system::open_regular_file_async(&self.directory.path().join(path)).await?)
  }

  async fn available_space(&self, directory: &Path) -> anyhow::Result<u64> {
    if self.disk_full.load(Ordering::Relaxed) {
      return Ok(0);
    }
    if let Some(bytes) = *self.available_bytes.lock() {
      return Ok(bytes);
    }
    Ok(fs2::available_space(self.directory.path().join(directory))?)
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
    self.delete_calls.fetch_add(1, Ordering::Relaxed);
    let file_path = self.directory.path().join(path);
    if !file_path.exists() {
      return Ok(());
    }
    if self.fail_deletes.load(Ordering::Relaxed) {
      anyhow::bail!("injected delete failure");
    }

    tokio::fs::remove_file(&file_path)
      .await
      .map_err(|e| anyhow::anyhow!("failed to delete file {}: {}", file_path.display(), e))?;

    Ok(())
  }

  async fn rename_file(&self, from: &Path, to: &Path) -> anyhow::Result<()> {
    if self.fail_next_rename.swap(false, Ordering::Relaxed) {
      anyhow::bail!("injected rename failure");
    }
    let from_path = self.directory.path().join(from);
    let to_path = self.directory.path().join(to);

    tokio::fs::rename(&from_path, &to_path).await.map_err(|e| {
      anyhow::anyhow!(
        "failed to rename file {} to {}: {}",
        from_path.display(),
        to_path.display(),
        e
      )
    })?;

    Ok(())
  }

  async fn sync_file_and_parent(&self, path: &Path) -> anyhow::Result<()> {
    self.sync_file(path).await?;
    if let Some(parent) = path.parent() {
      self.sync_directory(parent).await?;
    }
    Ok(())
  }

  async fn sync_file(&self, path: &Path) -> anyhow::Result<()> {
    self.file_sync_calls.fetch_add(1, Ordering::Relaxed);
    self.before_sync(path).await?;
    Ok(self.open_file(path).await?.sync_all().await?)
  }

  async fn sync_directory(&self, path: &Path) -> anyhow::Result<()> {
    self.directory_sync_calls.fetch_add(1, Ordering::Relaxed);
    self.before_sync(path).await?;
    Ok(self.open_file(path).await?.sync_all().await?)
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

#[async_trait]
impl FileSystem for Arc<TestFileSystem> {
  async fn exists(&self, path: &Path) -> anyhow::Result<bool> {
    self.as_ref().exists(path).await
  }

  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>> {
    self.as_ref().list_files(directory).await
  }

  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>> {
    self.as_ref().read_file(path).await
  }

  async fn open_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    self.as_ref().open_file(path).await
  }

  async fn open_regular_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    self.as_ref().open_regular_file(path).await
  }

  async fn available_space(&self, directory: &Path) -> anyhow::Result<u64> {
    self.as_ref().available_space(directory).await
  }

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    self.as_ref().write_file(path, data).await
  }

  async fn create_file(&self, path: &Path) -> anyhow::Result<tokio::fs::File> {
    self.as_ref().create_file(path).await
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    self.as_ref().delete_file(path).await
  }

  async fn rename_file(&self, from: &Path, to: &Path) -> anyhow::Result<()> {
    self.as_ref().rename_file(from, to).await
  }

  async fn sync_file_and_parent(&self, path: &Path) -> anyhow::Result<()> {
    self.as_ref().sync_file_and_parent(path).await
  }

  async fn sync_file(&self, path: &Path) -> anyhow::Result<()> {
    self.as_ref().sync_file(path).await
  }

  async fn sync_directory(&self, path: &Path) -> anyhow::Result<()> {
    self.as_ref().sync_directory(path).await
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    self.as_ref().remove_dir(path).await
  }

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()> {
    self.as_ref().create_dir(path).await
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
      available_bytes: Mutex::new(None),
      fail_deletes: AtomicBool::new(false),
      fail_next_sync: AtomicBool::new(false),
      fail_next_rename: AtomicBool::new(false),
      list_calls: AtomicUsize::new(0),
      delete_calls: AtomicUsize::new(0),
      file_sync_calls: AtomicUsize::new(0),
      directory_sync_calls: AtomicUsize::new(0),
      fail_sync_path: Mutex::new(None),
      sync_gate: Mutex::new(None),
    }
  }

  pub fn directory(&self) -> &Path {
    self.directory.path()
  }

  pub fn fail_next_sync_at(&self, path: PathBuf) {
    assert!(self.fail_sync_path.lock().replace(path).is_none());
  }

  pub fn gate_next_sync(&self, path: PathBuf) -> oneshot::Receiver<oneshot::Sender<()>> {
    let (gate_tx, gate_rx) = oneshot::channel();
    assert!(self.sync_gate.lock().replace((path, gate_tx)).is_none());
    gate_rx
  }

  async fn before_sync(&self, path: &Path) -> anyhow::Result<()> {
    let gate = {
      let mut gate = self.sync_gate.lock();
      if gate.as_ref().is_some_and(|(target, _)| target == path) {
        gate.take().map(|(_, gate)| gate)
      } else {
        None
      }
    };
    if let Some(gate) = gate {
      let (resume_tx, resume_rx) = oneshot::channel();
      gate
        .send(resume_tx)
        .map_err(|_| anyhow::anyhow!("sync gate observer was dropped"))?;
      resume_rx.await?;
    }
    let fail_at_path = {
      let mut target = self.fail_sync_path.lock();
      if target.as_deref() == Some(path) {
        target.take();
        true
      } else {
        false
      }
    };
    if self.fail_next_sync.swap(false, Ordering::Relaxed) || fail_at_path {
      anyhow::bail!("injected sync failure");
    }
    Ok(())
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
