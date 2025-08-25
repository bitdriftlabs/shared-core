// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::panic, clippy::unwrap_used)]

use crate::file_system::FileSystem;
use anyhow::anyhow;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

//
// TestFileSystem
//

/// An in-memory test implementation of a file system, meant to somewhat mimic the behavior of a
/// real filesystem.
pub struct TestFileSystem {
  files: Mutex<HashMap<String, Vec<u8>>>,
  pub disk_full: AtomicBool,
}

#[async_trait]
impl FileSystem for TestFileSystem {
  async fn list_files(&self, directory: &Path) -> anyhow::Result<Vec<String>> {
    let l = self.files.lock();

    let mut files = Vec::new();

    for (k, _) in l.iter() {
      if k.starts_with(directory.to_str().unwrap()) {
        files.push(k.clone());
      }
    }

    Ok(files)
  }

  async fn exists(&self, path: &Path) -> anyhow::Result<bool> {
    let l = self.files.lock();

    Ok(l.contains_key(&Self::path_as_str(path)))
  }

  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>> {
    let l = self.files.lock();

    l.get(&Self::path_as_str(path))
      .cloned()
      .ok_or_else(|| anyhow!("not found"))
  }

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    if self.disk_full.load(Ordering::Relaxed) {
      anyhow::bail!("disk full");
    }

    self
      .files
      .lock()
      .insert(Self::path_as_str(path), data.as_ref().to_vec());

    Ok(())
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    self.files.lock().remove(&Self::path_as_str(path));

    Ok(())
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    self
      .files
      .lock()
      .retain(|k, _| !k.starts_with(path.to_str().unwrap()));

    Ok(())
  }

  async fn create_dir(&self, _path: &Path) -> anyhow::Result<()> {
    // Technically we should only allow creating files if the directory already exists, but we
    // ignore that for now.
    Ok(())
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
      files: Mutex::new(HashMap::new()),
      disk_full: AtomicBool::new(false),
    }
  }

  pub fn files(&self) -> HashMap<String, Vec<u8>> {
    self.files.lock().clone()
  }

  fn path_as_str(path: impl AsRef<Path>) -> String {
    path.as_ref().as_os_str().to_str().unwrap().to_string()
  }
}
