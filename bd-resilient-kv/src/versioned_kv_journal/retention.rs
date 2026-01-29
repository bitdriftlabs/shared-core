// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./retention_test.rs"]
mod tests;

use bd_runtime::runtime::IntWatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;

/// A handle that declares a retention requirement for snapshot data.
///
/// Each subsystem that needs historical data should hold one of these handles
/// and update its `retain_from` timestamp as its requirements change.
///
/// When dropped, the handle automatically releases its retention requirement by
/// setting the retention timestamp to "now".
#[derive(Clone)]
pub struct RetentionHandle {
  /// The timestamp from which data must be retained (inclusive).
  /// Data older than this timestamp may be eligible for cleanup.
  ///
  /// Stored as microseconds since epoch to match the journal timestamp format.
  retain_from: Arc<AtomicU64>,
}


impl RetentionHandle {
  pub const NO_RETENTION_REQUIREMENT: u64 = u64::MAX;

  /// Updates the retention requirement to retain data from the given timestamp (in microseconds).
  ///
  /// The timestamp should be in microseconds since UNIX epoch to match the journal timestamp
  /// format.
  pub fn update_retention_micros(&self, timestamp_micros: u64) {
    self.retain_from.store(timestamp_micros, Ordering::Relaxed);
  }

  /// Gets the current retention timestamp in microseconds.
  #[must_use]
  pub fn get_retention(&self) -> u64 {
    self.retain_from.load(Ordering::Relaxed)
  }
}

/// Registry for retention handles and cleanup coordination.
///
/// The registry tracks all active retention requirements and can compute the minimum
/// retention timestamp needed across all registered handles.
pub struct RetentionRegistry {
  handles: Arc<RwLock<Vec<Weak<AtomicU64>>>>,
  max_snapshot_count_watch: IntWatch<bd_runtime::runtime::state::MaxSnapshotCount>,
}

impl RetentionRegistry {
  #[must_use]
  pub fn new(
    max_snapshot_count_watch: IntWatch<bd_runtime::runtime::state::MaxSnapshotCount>,
  ) -> Self {
    Self {
      handles: Arc::new(RwLock::new(Vec::new())),
      max_snapshot_count_watch,
    }
  }

  /// Returns the maximum number of snapshots to retain, if configured.
  #[must_use]
  pub fn max_snapshot_count(&self) -> Option<usize> {
    let value = usize::try_from(*self.max_snapshot_count_watch.read()).ok()?;
    if value == 0 { None } else { Some(value) }
  }

  /// Creates a new retention handle.
  ///
  /// The handle starts with a sentinel indicating no retention requirement until initialized.
  pub async fn create_handle(&self) -> RetentionHandle {
    let retain_from = Arc::new(AtomicU64::new(RetentionHandle::NO_RETENTION_REQUIREMENT));

    // Store weak reference to the Arc<AtomicU64> so dropped handles are automatically cleaned up
    self
      .handles
      .write()
      .await
      .push(Arc::downgrade(&retain_from));

    RetentionHandle { retain_from }
  }

  /// Computes the minimum retention timestamp across all registered handles.
  ///
  /// This represents the earliest point in time that any subsystem still needs.
  /// Snapshots older than this can be safely deleted.
  ///
  /// Returns `None` if no handles are registered or all handles have been dropped.
  pub async fn min_retention_timestamp(&self) -> Option<u64> {
    let mut handles = self.handles.write().await;

    // Clean up dropped handles
    handles.retain(|weak| weak.strong_count() > 0);

    if handles.is_empty() {
      return None;
    }

    let mut min_retention: Option<u64> = None;
    let mut has_handles = false;
    for handle in handles.iter().filter_map(std::sync::Weak::upgrade) {
      has_handles = true;
      let retention = handle.load(Ordering::Relaxed);
      if retention == RetentionHandle::NO_RETENTION_REQUIREMENT {
        continue;
      }
      min_retention = Some(min_retention.map_or(retention, |min| min.min(retention)));
    }

    if min_retention.is_some() {
      return min_retention;
    }

    has_handles.then_some(RetentionHandle::NO_RETENTION_REQUIREMENT)
  }
}
