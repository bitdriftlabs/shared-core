// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./kv_test.rs"]
mod kv_test;

pub mod in_memory;
pub mod memmapped;

use bd_bonjson::Value;
use std::collections::HashMap;

/// Callback function type for high water mark notifications.
/// 
/// Called when the buffer usage exceeds the high water mark threshold.
/// Parameters: (current_position, buffer_size, high_water_mark_position)
pub type HighWaterMarkCallback = fn(usize, usize, usize);

/// Trait for crash-resilient key-value stores that can be recovered even if writing is interrupted.
pub trait ResilientKv {
  /// Get the current high water mark position.
  fn high_water_mark(&self) -> usize;

  /// Check if the high water mark has been triggered.
  fn is_high_water_mark_triggered(&self) -> bool;

  /// Get the current buffer usage as a percentage (0.0 to 1.0).
  fn buffer_usage_ratio(&self) -> f32;

  /// Set key to value in this kv store.
  ///
  /// This will create a new journal entry.
  ///
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()>;

  /// Delete a key from this kv store.
  ///
  /// This will create a new journal entry.
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn delete(&mut self, key: &str) -> anyhow::Result<()>;

  /// Get the current state of the kv store as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>>;
}

// Re-export the in-memory implementation for convenience
pub use in_memory::InMemoryResilientKv;
pub use memmapped::MemMappedResilientKv;
