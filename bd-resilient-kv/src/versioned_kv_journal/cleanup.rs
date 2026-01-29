// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./cleanup_test.rs"]
mod tests;

use super::filename::SnapshotFilename;
use super::retention::RetentionRegistry;
use bd_error_reporter::reporter::handle_unexpected;
use std::path::{Path, PathBuf};

/// Cleans up old snapshot files based on retention requirements.
///
/// This function is called during journal rotation to delete archived journal snapshots
/// that are older than the minimum retention timestamp required by any registered handle.
///
/// If no retention handles are registered, no cleanup is performed (snapshots are kept).
pub async fn cleanup_old_snapshots(
  directory: &Path,
  registry: &RetentionRegistry,
) -> anyhow::Result<()> {
  // Get minimum retention timestamp across all subsystems
  let Some(min_retention) = registry.min_retention_timestamp().await else {
    log::debug!("No retention handles registered, skipping cleanup");
    return Ok(());
  };

  log::debug!("Running snapshot cleanup with min_retention={min_retention}");

  // Find all archived snapshots
  let snapshots = find_archived_snapshots(directory).await?;

  let mut deleted_count = 0;
  let mut kept_count = 0;

  for (path, timestamp) in snapshots {
    if timestamp < min_retention {
      log::debug!(
        "Deleting snapshot {} (timestamp={} < min_retention={})",
        path.display(),
        timestamp,
        min_retention
      );

      handle_unexpected(
        tokio::fs::remove_file(&path)
          .await
          .map(|()| deleted_count += 1),
        "snapshot deletion",
      );
    } else {
      kept_count += 1;
    }
  }

  if deleted_count > 0 {
    log::debug!("Snapshot cleanup complete: deleted {deleted_count}, kept {kept_count}");
  }

  Ok(())
}

/// Finds all archived snapshot files and extracts their timestamps.
///
/// Since snapshots are stored in a dedicated directory for this journal, all `.zz` files
/// are assumed to be snapshots belonging to this journal.
/// Returns a vector of (path, timestamp) tuples sorted by timestamp.
async fn find_archived_snapshots(directory: &Path) -> anyhow::Result<Vec<(PathBuf, u64)>> {
  let mut entries = tokio::fs::read_dir(directory).await?;
  let mut snapshots = Vec::new();

  while let Some(entry) = entries.next_entry().await? {
    let path = entry.path();

    if let Some(filename) = path.file_name().and_then(|f| f.to_str())
      && let Some(timestamp) = SnapshotFilename::extract_timestamp(filename)
    {
      snapshots.push((path, timestamp));
    }
  }

  snapshots.sort_by_key(|(_, ts)| *ts);
  Ok(snapshots)
}
