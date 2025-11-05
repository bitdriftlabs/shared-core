// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::path::{Path, PathBuf};

/// Information about an archived journal snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
  /// Full path to the snapshot file
  pub path: PathBuf,
  /// Version number extracted from the snapshot filename
  pub version: u64,
  /// File size in bytes
  pub size_bytes: u64,
}

/// A utility for managing cleanup of archived journal snapshots.
///
/// `SnapshotCleanup` provides functionality to discover and remove old archived journals
/// based on version thresholds. This is useful for managing disk space when your system
/// determines how far back in history you need to maintain snapshots.
///
/// Archived journals follow the naming pattern: `{base_name}.v{version}.zz`
/// For example: `my_store.jrn.v1000.zz`, `my_store.jrn.v2000.zz`
///
/// # Example
/// ```ignore
/// use bd_resilient_kv::SnapshotCleanup;
///
/// // Create cleanup utility for a journal
/// let cleanup = SnapshotCleanup::new("my_store.jrn")?;
///
/// // List all archived snapshots
/// let snapshots = cleanup.list_snapshots()?;
/// for snapshot in &snapshots {
///     println!("Version: {}, Size: {} bytes", snapshot.version, snapshot.size_bytes);
/// }
///
/// // Remove snapshots older than version 5000
/// let removed = cleanup.cleanup_before_version(5000)?;
/// println!("Removed {} snapshots", removed.len());
/// ```
pub struct SnapshotCleanup {
  directory: PathBuf,
  base_filename: String,
}

impl SnapshotCleanup {
  /// Create a new `SnapshotCleanup` utility for the given journal path.
  ///
  /// The journal path should be the same path used to create the `VersionedKVStore`.
  /// For example, if you created your store with `"my_store.jrn"`, pass the same path here.
  ///
  /// # Arguments
  /// * `journal_path` - Path to the journal file (e.g., "`my_store.jrn`")
  ///
  /// # Errors
  /// Returns an error if the path is invalid or cannot be canonicalized.
  pub fn new<P: AsRef<Path>>(journal_path: P) -> anyhow::Result<Self> {
    let path = journal_path.as_ref();

    let directory = path
      .parent()
      .map_or_else(|| PathBuf::from("."), std::path::Path::to_path_buf);

    let base_filename = path
      .file_name()
      .ok_or_else(|| anyhow::anyhow!("Invalid journal path: no filename"))?
      .to_string_lossy()
      .to_string();

    Ok(Self {
      directory,
      base_filename,
    })
  }

  /// List all archived snapshots for this journal.
  ///
  /// Returns a vector of `SnapshotInfo` containing details about each archived journal,
  /// sorted by version number in ascending order.
  ///
  /// # Errors
  /// Returns an error if the directory cannot be read or if file metadata cannot be accessed.
  pub async fn list_snapshots(&self) -> anyhow::Result<Vec<SnapshotInfo>> {
    let mut snapshots = Vec::new();

    // Read directory entries
    if !self.directory.exists() {
      return Ok(snapshots);
    }

    let mut entries = tokio::fs::read_dir(&self.directory).await?;

    while let Some(entry) = entries.next_entry().await? {
      let path = entry.path();

      // Check if this is an archived snapshot for our journal
      if let Some(version) = self.extract_version_from_path(&path) {
        let metadata = entry.metadata().await?;
        snapshots.push(SnapshotInfo {
          path: path.clone(),
          version,
          size_bytes: metadata.len(),
        });
      }
    }

    // Sort by version number
    snapshots.sort_by_key(|s| s.version);

    Ok(snapshots)
  }

  /// Remove all archived snapshots with versions strictly less than the specified version.
  ///
  /// This keeps snapshots at or after the minimum version, removing only older ones.
  ///
  /// # Arguments
  /// * `min_version` - Minimum version to keep (exclusive). Snapshots with versions less than this
  ///   will be removed.
  ///
  /// # Returns
  /// Returns a vector of `SnapshotInfo` for the snapshots that were removed.
  ///
  /// # Errors
  /// Returns an error if any snapshot cannot be removed. If an error occurs while removing
  /// a snapshot, the operation stops and returns the error. Some snapshots may have been
  /// removed before the error occurred.
  ///
  /// # Example
  /// ```ignore
  /// // Keep snapshots at version 5000 and later, remove older ones
  /// let removed = cleanup.cleanup_before_version(5000)?;
  /// ```
  pub async fn cleanup_before_version(
    &self,
    min_version: u64,
  ) -> anyhow::Result<Vec<SnapshotInfo>> {
    let snapshots = self.list_snapshots().await?;
    let mut removed = Vec::new();

    for snapshot in snapshots {
      if snapshot.version < min_version {
        tokio::fs::remove_file(&snapshot.path).await?;
        removed.push(snapshot);
      }
    }

    Ok(removed)
  }

  /// Remove all archived snapshots except the most recent N versions.
  ///
  /// This keeps the N newest snapshots and removes all older ones.
  ///
  /// # Arguments
  /// * `keep_count` - Number of most recent snapshots to keep
  ///
  /// # Returns
  /// Returns a vector of `SnapshotInfo` for the snapshots that were removed.
  ///
  /// # Errors
  /// Returns an error if any snapshot cannot be removed.
  ///
  /// # Example
  /// ```ignore
  /// // Keep only the 5 most recent snapshots
  /// let removed = cleanup.cleanup_keep_recent(5)?;
  /// ```
  pub async fn cleanup_keep_recent(&self, keep_count: usize) -> anyhow::Result<Vec<SnapshotInfo>> {
    let mut snapshots = self.list_snapshots().await?;

    if snapshots.len() <= keep_count {
      return Ok(Vec::new());
    }

    // Sort by version descending to get most recent first
    snapshots.sort_by_key(|s| std::cmp::Reverse(s.version));

    // Remove all except the most recent keep_count
    let mut removed = Vec::new();
    for snapshot in snapshots.into_iter().skip(keep_count) {
      tokio::fs::remove_file(&snapshot.path).await?;
      removed.push(snapshot);
    }

    // Sort removed list by version ascending for consistency
    removed.sort_by_key(|s| s.version);

    Ok(removed)
  }

  /// Calculate the total disk space used by all archived snapshots.
  ///
  /// # Errors
  /// Returns an error if snapshots cannot be listed.
  pub async fn total_snapshot_size(&self) -> anyhow::Result<u64> {
    let snapshots = self.list_snapshots().await?;
    Ok(snapshots.iter().map(|s| s.size_bytes).sum())
  }

  /// Get the oldest snapshot version.
  ///
  /// Returns `None` if there are no archived snapshots.
  ///
  /// # Errors
  /// Returns an error if snapshots cannot be listed.
  pub async fn oldest_snapshot_version(&self) -> anyhow::Result<Option<u64>> {
    let snapshots = self.list_snapshots().await?;
    Ok(snapshots.first().map(|s| s.version))
  }

  /// Get the newest snapshot version.
  ///
  /// Returns `None` if there are no archived snapshots.
  ///
  /// # Errors
  /// Returns an error if snapshots cannot be listed.
  pub async fn newest_snapshot_version(&self) -> anyhow::Result<Option<u64>> {
    let snapshots = self.list_snapshots().await?;
    Ok(snapshots.last().map(|s| s.version))
  }

  /// Extract version number from an archived journal path.
  ///
  /// Returns `Some(version)` if the path matches the pattern `{base_name}.v{version}.zz`,
  /// otherwise returns `None`.
  fn extract_version_from_path(&self, path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_string_lossy();

    // Check if filename starts with our base filename
    if !filename.starts_with(&self.base_filename) {
      return None;
    }

    // Pattern: {base_filename}.v{version}.zz
    let suffix = filename.strip_prefix(&self.base_filename)?;

    // Should start with ".v"
    let version_part = suffix.strip_prefix(".v")?;

    // Should end with ".zz"
    let version_str = version_part.strip_suffix(".zz")?;

    // Parse version number
    version_str.parse::<u64>().ok()
  }
}
