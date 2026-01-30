// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./cleanup_test.rs"]
mod tests;

use super::retention::{RetentionHandle, RetentionRegistry};
use bd_error_reporter::reporter::handle_unexpected;
use std::path::{Path, PathBuf};

/// Cleans up old snapshot files based on retention requirements.
///
/// This function is called during journal rotation to delete archived journal snapshots
/// that are older than the minimum retention timestamp required by any registered handle.
///
/// If no retention handles are registered and no snapshot limit is set, no cleanup is performed
/// (snapshots are kept).
pub async fn cleanup_old_snapshots(
  directory: &Path,
  registry: &RetentionRegistry,
) -> anyhow::Result<()> {
  if !registry.snapshots_enabled() {
    log::debug!("Snapshotting disabled, skipping cleanup");
    return Ok(());
  }

  // Get minimum retention timestamp across all subsystems
  let min_retention = registry.min_retention_timestamp().await;
  let max_snapshot_count = registry.max_snapshot_count();
  if min_retention.is_none() && max_snapshot_count.is_none() {
    log::debug!("No retention handles or snapshot limit, skipping cleanup");
    return Ok(());
  }

  log::debug!(
    "Running snapshot cleanup with min_retention={min_retention:?}, \
     max_snapshot_count={max_snapshot_count:?}"
  );

  // Find all archived snapshots
  let snapshots = find_archived_snapshots(directory).await?;

  let mut deleted_count = 0;
  let mut kept_count = 0;

  let mut kept_snapshots = Vec::new();
  for (path, timestamp) in snapshots {
    let should_delete = match min_retention {
      Some(RetentionHandle::NO_RETENTION_REQUIREMENT) => true,
      None | Some(0) => false,
      Some(min_retention) => timestamp < min_retention,
    };

    if should_delete {
      log::debug!(
        "Deleting snapshot {} (timestamp={} < min_retention={:?})",
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
      kept_snapshots.push((path, timestamp));
    }
  }

  let keep_newest_at_most = max_snapshot_count.unwrap_or(usize::MAX);
  if kept_snapshots.len() > keep_newest_at_most {
    let overflow = kept_snapshots.len() - keep_newest_at_most;
    for (path, _timestamp) in kept_snapshots.iter().take(overflow) {
      log::debug!("Deleting snapshot {} (exceeds max snapshot count)", path.display());
      handle_unexpected(
        tokio::fs::remove_file(path)
          .await
          .map(|()| deleted_count += 1),
        "snapshot deletion",
      );
    }
    kept_count += keep_newest_at_most;
  } else {
    kept_count += kept_snapshots.len();
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

    // All .zz files in the snapshots directory belong to this journal
    if let Some(filename) = path.file_name().and_then(|f| f.to_str())
      && std::path::Path::new(filename)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zz"))
      && let Some(timestamp) = extract_timestamp_from_filename(filename)
    {
      snapshots.push((path, timestamp));
    }
  }

  snapshots.sort_by_key(|(_, ts)| *ts);
  Ok(snapshots)
}

/// Extracts the timestamp from an archived journal filename.
///
/// Expected format: `{name}.jrn.g{generation}.t{timestamp}.zz`
fn extract_timestamp_from_filename(filename: &str) -> Option<u64> {
  filename
    .split('.')
    .find(|part| {
      part.starts_with('t') && part.len() > 1 && part[1 ..].chars().all(|c| c.is_ascii_digit())
    })
    .and_then(|part| part.strip_prefix('t'))
    .and_then(|ts| ts.parse::<u64>().ok())
}
