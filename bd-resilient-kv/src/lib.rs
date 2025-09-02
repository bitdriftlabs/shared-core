// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./kv_test.rs"]
mod kv_test;

#[cfg(test)]
#[path = "./memmapped_test.rs"]
mod memmapped_test;

#[cfg(test)]
#[path = "./double_buffered_test.rs"]
mod double_buffered_test;

#[cfg(test)]
#[path = "./double_buffered_memmapped_new_test.rs"]
mod double_buffered_memmapped_test;

#[cfg(test)]
#[path = "./double_buffered_selection_test.rs"]
mod double_buffered_selection_test;

#[cfg(test)]
#[path = "./error_handling_test.rs"]
mod error_handling_test;

#[cfg(test)]
#[path = "./concurrency_test.rs"]
mod concurrency_test;

#[cfg(test)]
#[path = "./boundary_test.rs"]
mod boundary_test;

pub mod in_memory;
pub mod memmapped;
pub mod double_buffered;

use bd_bonjson::Value;
use std::collections::HashMap;

/// Callback function type for high water mark notifications.
/// 
/// Called when a buffer usage exceeds the high water mark threshold.
/// Parameters: (current_position, buffer_size, high_water_mark_position)
pub type HighWaterMarkCallback = fn(usize, usize, usize);

/// Trait for a key-value journaling system whose data can be recovered up to the last successful write checkpoint.
pub trait KVJournal {
  /// Get the current high water mark position.
  fn high_water_mark(&self) -> usize;

  /// Check if the high water mark has been triggered.
  fn is_high_water_mark_triggered(&self) -> bool;

  /// Get the current buffer usage as a percentage (0.0 to 1.0).
  fn buffer_usage_ratio(&self) -> f32;

  /// Get the time when the journal was initialized (nanoseconds since UNIX epoch).
  ///
  /// # Errors
  /// Returns an error if the initialization timestamp cannot be retrieved.
  fn get_init_time(&mut self) -> anyhow::Result<u64>;

  /// Generate a new journal entry recording the setting of a key to a value.
  ///
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()>;

  /// Generate a new journal entry recording the deletion of a key.
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn delete(&mut self, key: &str) -> anyhow::Result<()>;

  /// Get the current state of the journal as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>>;

  /// Reinitialize this journal using the data from another journal.
  ///
  /// This method clears the current contents and populates this journal with all
  /// key-value pairs from the other journal. The high water mark is not affected.
  ///
  /// # Errors
  /// Returns an error if the other journal cannot be read or if writing to this journal fails.
  fn reinit_from(&mut self, other: &mut dyn KVJournal) -> anyhow::Result<()>;
}

// Re-export the in-memory implementation for convenience
pub use in_memory::InMemoryKVJournal;
pub use memmapped::MemMappedKVJournal;
pub use double_buffered::DoubleBufferedKVJournal;
