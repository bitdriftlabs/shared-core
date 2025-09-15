// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Feature flags management with persistent storage.
//!
//! This crate provides a feature flag system that stores flags persistently using the
//! `bd-resilient-kv` key-value store. Feature flags consist of a name (key) and an optional
//! variant string, along with a timestamp indicating when the flag was last updated.
//!
//! # Storage Format
//!
//! Feature flags are stored in the underlying key-value store as BONJSON objects with the following
//! structure:
//! - Key: The feature flag name (String)
//! - Value: Object containing:
//!   - `"v"`: The variant string (empty string if no variant)
//!   - `"t"`: Unix timestamp in nanoseconds when the flag was last updated
//!
//! The key-value store will write to two files with the same basename, and extensions "jrna" and
//! "jrnb"

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use bd_bonjson::Value;
use bd_resilient_kv::KVStore;
use std::collections::HashMap;
use std::path::Path;
use time::OffsetDateTime;

#[cfg(test)]
#[path = "./feature_flags_test.rs"]
mod feature_flags_test;

const VARIANT_KEY: &str = "v";
const TIMESTAMP_KEY: &str = "t";
const DEFAULT_HIGH_WATER_MARK_RATIO: f32 = 0.8;

/// A feature flag containing an optional variant and timestamp.
///
/// Each feature flag consists of:
/// - An optional variant string that can be used to configure behavior
/// - A timestamp indicating when the flag was last updated (in nanoseconds since Unix epoch)
#[derive(Debug)]
pub struct FeatureFlag {
  /// The variant string for this feature flag.
  ///
  /// - `Some(variant)` indicates the flag is enabled with the specified variant
  /// - `None` indicates the flag is enabled without a specific variant
  pub variant: Option<String>,

  /// Unix timestamp in nanoseconds when this flag was last updated.
  pub timestamp: u64,
}

/// A feature flags manager with persistent storage.
///
/// `FeatureFlags` provides a way to manage feature flags that are persisted to disk
/// using a resilient key-value store. All flags are cached in memory for fast access
/// and automatically loaded from storage when the instance is created.
pub struct FeatureFlags {
  flags_store: KVStore,
  flags_cache: HashMap<String, FeatureFlag>,
}

fn get_current_timestamp() -> u64 {
  // OffsetDateTime::now_utc() returns the current UTC time
  // unix_timestamp_nanos() returns i128, so we clamp to u64 (0 if negative)
  let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
  if nanos < 0 {
    0
  } else {
    nanos as u64
  }
}

fn generate_initial_cache(store: &KVStore) -> HashMap<String, FeatureFlag> {
  let mut flags_cache = HashMap::new();
  for (key, value) in store.as_hashmap() {
    // Note: We discard any malformed entries
    if let Value::Object(obj) = value {
      let variant = match obj.get(VARIANT_KEY) {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::String(_)) => None,
        _ => continue,
      };

      let timestamp = match obj.get(TIMESTAMP_KEY) {
        Some(Value::Unsigned(t)) => *t,
        #[allow(clippy::cast_sign_loss)]
        Some(Value::Signed(t)) if *t >= 0 => *t as u64,
        _ => continue,
      };

      flags_cache.insert(key.clone(), FeatureFlag { variant, timestamp });
    }
  }
  flags_cache
}

impl FeatureFlags {
  /// Creates a new `FeatureFlags` instance with persistent storage.
  ///
  /// This constructor creates a new feature flags manager backed by a resilient key-value store
  /// at the specified path. Any existing flags stored at this location will be automatically
  /// loaded into the in-memory cache.
  ///
  /// # Arguments
  ///
  /// * `base_path` - The filesystem path to the file basename where feature flags will be stored.
  ///   Two files will be created with extensions "jrna" and "jrnb"
  /// * `buffer_size` - The size of the underlying storage buffer in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0-1.0) for buffer high water mark. When the
  ///   buffer reaches this percentage full, older data may be compacted. If `None`, a default value
  ///   of 0.8 will be used.
  ///
  /// # Returns
  ///
  /// Returns `Ok(FeatureFlags)` on success, or an error if the storage cannot be initialized.
  ///
  /// # Errors
  ///
  /// This function will return an error if:
  /// - The storage directory cannot be created or accessed
  /// - Insufficient permissions to access the storage location
  /// - Backing storage files cannot be written to
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let flags_store = KVStore::new(
      base_path,
      buffer_size,
      high_water_mark_ratio.or(Some(DEFAULT_HIGH_WATER_MARK_RATIO)),
      None,
    )?;
    let flags_cache = generate_initial_cache(&flags_store);
    Ok(Self {
      flags_store,
      flags_cache,
    })
  }

  /// Retrieves a feature flag by name.
  ///
  /// Returns a reference to the feature flag if it exists, or `None` if no flag
  /// with the given name is found.
  ///
  /// # Arguments
  ///
  /// * `key` - The name of the feature flag to retrieve
  #[must_use]
  pub fn get(&self, key: &str) -> Option<&FeatureFlag> {
    self.flags_cache.get(key)
  }

  /// Synchronizes in-memory changes to persistent storage.
  ///
  /// This method ensures that all feature flag changes made since the last sync
  /// are written to disk. While changes are typically persisted automatically,
  /// calling this method guarantees that data is flushed to storage immediately.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on successful synchronization, or an error if the write operation fails.
  ///
  /// # Errors
  ///
  /// This function will return an error if the underlying storage system
  /// cannot write data to disk due to I/O errors, insufficient disk space,
  /// or permission issues.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.flags_store.sync()
  }

  /// Sets or updates a feature flag.
  ///
  /// Creates a new feature flag with the given name and variant, or updates an existing flag.
  /// The flag is immediately stored both in memory and in persistent storage, and receives
  /// a timestamp indicating when it was last modified.
  ///
  /// # Arguments
  ///
  /// * `key` - The name of the feature flag to set or update
  /// * `variant` - The variant value for the flag:
  ///   - `Some(string)` sets the flag with the specified variant
  ///   - `None` sets the flag without a variant (simple boolean-style flag)
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on success, or an error if the flag cannot be stored.
  ///
  /// # Errors
  ///
  /// This function will return an error if the flag cannot be written to persistent
  /// storage due to I/O errors, insufficient disk space, or permission issues.
  pub fn set(&mut self, key: &str, variant: Option<&str>) -> anyhow::Result<()> {
    let timestamp = get_current_timestamp();
    self.flags_cache.insert(
      key.to_string(),
      FeatureFlag {
        variant: variant.map(String::from),
        timestamp,
      },
    );

    let value = HashMap::from([
      (
        VARIANT_KEY.to_string(),
        Value::String(variant.unwrap_or_default().to_string()),
      ),
      (TIMESTAMP_KEY.to_string(), Value::Unsigned(timestamp)),
    ]);
    self
      .flags_store
      .insert(key.to_string(), Value::Object(value))?;

    Ok(())
  }

  /// Removes all feature flags from both memory and persistent storage.
  ///
  /// This method deletes all feature flags, clearing both the in-memory cache
  /// and the persistent storage. This operation cannot be undone.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on success, or an error if the storage cannot be cleared.
  ///
  /// # Errors
  ///
  /// This function will return an error if the underlying storage cannot be
  /// cleared due to I/O errors or permission issues.
  pub fn clear(&mut self) -> anyhow::Result<()> {
    self.flags_cache.clear();
    self.flags_store.clear()?;
    Ok(())
  }

  /// Returns a reference to the internal `HashMap` containing all feature flags.
  ///
  /// This method provides read-only access to all feature flags as a standard
  /// Rust `HashMap`. This is useful for iterating over all flags or performing
  /// bulk operations.
  ///
  /// # Returns
  ///
  /// A reference to the internal `HashMap<String, FeatureFlag>` containing all flags.
  #[must_use]
  pub fn as_hashmap(&self) -> &HashMap<String, FeatureFlag> {
    &self.flags_cache
  }
}
