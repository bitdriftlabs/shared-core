use anyhow::anyhow;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

//
// FileSystem
//

/// A filesystem scoped to the SDK directory. This allows for mocking and abstracting away the
/// relative root of the data directory used by the SDK.
#[async_trait]
pub trait FileSystem: Send + Sync {
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

//
// TestFileSystem
//

/// An in-memory test implementation of a file system, meant to somewhat mimic the behavior of a
/// real filesystem.
pub struct TestFileSystem {
  files: Mutex<HashMap<String, Vec<u8>>>,
  disk_full: AtomicBool,
}

#[async_trait]
impl FileSystem for TestFileSystem {
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

impl TestFileSystem {
  pub fn new() -> Self {
    Self {
      files: Mutex::new(HashMap::new()),
      disk_full: AtomicBool::new(false),
    }
  }

  fn path_as_str(path: impl AsRef<Path>) -> String {
    path.as_ref().as_os_str().to_str().unwrap().to_string()
  }
}
