// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./retention_test.rs"]
mod tests;

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

  /// Human-readable name for debugging/logging
  name: String,
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

  /// Human-readable name for this handle.
  #[must_use]
  pub fn name(&self) -> &str {
    &self.name
  }
}

impl Drop for RetentionHandle {
  fn drop(&mut self) {
    // When the handle is dropped, we don't need to do anything special.
    // The Arc<AtomicU64> will be dropped when all RetentionHandles referencing it are dropped,
    // and the registry will clean up the weak reference automatically.
  }
}

/// Registry for retention handles and cleanup coordination.
///
/// The registry tracks all active retention requirements and can compute the minimum
/// retention timestamp needed across all registered handles.
pub struct RetentionRegistry {
  handles: Arc<RwLock<Vec<Weak<AtomicU64>>>>,
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
    }
  }

  /// Creates a new retention handle with the given name.
  ///
  /// The handle starts with a retention timestamp of 0 (epoch), meaning it initially
  /// requires all historical data to be retained.
  pub async fn create_handle(&self, name: impl Into<String>) -> RetentionHandle {
    let retain_from = Arc::new(AtomicU64::new(0)); // Start with "retain everything"

    // Store weak reference to the Arc<AtomicU64> so dropped handles are automatically cleaned up
    self
      .handles
      .write()
      .await
      .push(Arc::downgrade(&retain_from));

    RetentionHandle {
      retain_from,
      name: name.into(),
    }
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

  /// Returns debug information about current retention requirements.
  ///
  /// This is useful for debugging and monitoring retention requirements.
  pub async fn debug_info(&self) -> Vec<(String, u64)> {
    let mut handles = self.handles.write().await;

    // Clean up dropped handles
    handles.retain(|weak| weak.strong_count() > 0);

    // We can't easily get the names since we only store Weak<AtomicU64>
    // Return just the retention timestamps
    handles
      .iter()
      .filter_map(std::sync::Weak::upgrade)
      .enumerate()
      .map(|(i, atomic)| (format!("handle_{i}"), atomic.load(Ordering::Relaxed)))
      .collect()
  }
}
