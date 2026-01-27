// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./retention_test.rs"]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::RwLock;

/// A handle that declares a retention requirement for snapshot data.
///
/// Each subsystem that needs historical data should hold one of these handles
/// and update its `retain_from` timestamp as its requirements change.
///
/// When dropped, the handle automatically releases its retention requirement by
/// setting the retention timestamp to "now".
pub struct RetentionHandle {
  /// The timestamp from which data must be retained (inclusive).
  /// Data older than this timestamp may be eligible for cleanup.
  ///
  /// Stored as microseconds since epoch to match the journal timestamp format.
  retain_from: Arc<AtomicU64>,
}


impl RetentionHandle {
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
  max_snapshot_count: Arc<AtomicUsize>,
}

impl Default for RetentionRegistry {
  fn default() -> Self {
    Self::new()
  }
}

impl RetentionRegistry {
  #[must_use]
  pub fn new() -> Self {
    Self {
      handles: Arc::new(RwLock::new(Vec::new())),
      max_snapshot_count: Arc::new(AtomicUsize::new(0)),
    }
  }

  /// Sets the maximum number of snapshots to retain.
  ///
  /// A value of `None` means no count-based limit is applied.
  pub fn set_max_snapshot_count(&self, max_snapshot_count: Option<usize>) {
    let value = max_snapshot_count.unwrap_or(0);
    self.max_snapshot_count.store(value, Ordering::Relaxed);
  }

  /// Returns the maximum number of snapshots to retain, if configured.
  #[must_use]
  pub fn max_snapshot_count(&self) -> Option<usize> {
    let value = self.max_snapshot_count.load(Ordering::Relaxed);
    if value == 0 { None } else { Some(value) }
  }

  /// Creates a new retention handle.
  ///
  /// The handle starts with a retention timestamp of 0 (epoch), meaning it initially
  /// requires all historical data to be retained.
  pub async fn create_handle(&self) -> RetentionHandle {
    let retain_from = Arc::new(AtomicU64::new(0)); // Start with "retain everything"

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

    // Collect all valid handles and find minimum
    handles
      .iter()
      .filter_map(std::sync::Weak::upgrade)
      .map(|atomic| atomic.load(Ordering::Relaxed))
      .min()
  }
}
