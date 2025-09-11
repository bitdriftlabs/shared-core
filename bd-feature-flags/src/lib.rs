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
use std::time::{SystemTime, UNIX_EPOCH};

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
///
/// # Examples
///
/// ## Basic usage
///
/// ```no_run
/// use bd_feature_flags::FeatureFlags;
/// use tempfile::TempDir;
///
/// # fn main() -> anyhow::Result<()> {
/// let temp_dir = TempDir::new()?;
/// let mut flags = FeatureFlags::new(temp_dir.path(), 1024, None)?;
///
/// // Set a feature flag with a variant
/// flags.set("feature_a".to_string(), Some("blue".to_string()))?;
///
/// // Set a simple boolean-style flag
/// flags.set("feature_b".to_string(), None)?;
///
/// // Check if a flag exists and get its value
/// if let Some(flag) = flags.get("feature_a") {
///   println!("Feature A variant: {:?}", flag.variant);
/// }
///
/// // Get all flags as a HashMap for iteration
/// for (name, flag) in flags.as_hashmap() {
///   println!("Flag '{}' has variant: {:?}", name, flag.variant);
/// }
/// # Ok(())
/// # }
/// ```
///
/// ## Persistence across instances
///
/// ```no_run
/// use bd_feature_flags::FeatureFlags;
/// use tempfile::TempDir;
///
/// # fn main() -> anyhow::Result<()> {
/// let temp_dir = TempDir::new()?;
/// let temp_path = temp_dir.path();
///
/// // Create flags in one instance
/// {
///   let mut flags = FeatureFlags::new(temp_path, 1024, None)?;
///   flags.set("persistent_flag".to_string(), Some("value".to_string()))?;
///   flags.sync()?; // Ensure data is written to disk
/// }
///
/// // Load flags in a new instance
/// let flags = FeatureFlags::new(temp_path, 1024, None)?;
/// assert!(flags.get("persistent_flag").is_some());
/// # Ok(())
/// # }
/// ```
pub struct FeatureFlags {
  flags_store: KVStore,
  flags_cache: HashMap<String, FeatureFlag>,
}

fn get_current_timestamp() -> u64 {
  u64::try_from(
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos(),
  )
  .unwrap_or(0)
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
  /// # Examples
  ///
  /// ```no_run
  /// use bd_feature_flags::FeatureFlags;
  /// use tempfile::TempDir;
  ///
  /// # fn main() -> anyhow::Result<()> {
  /// let temp_dir = TempDir::new()?;
  ///
  /// // Create with default high water mark
  /// let flags1 = FeatureFlags::new(temp_dir.path(), 1024, None)?;
  ///
  /// // Create with custom high water mark
  /// let flags2 = FeatureFlags::new(temp_dir.path().join("flags2"), 2048, Some(0.8))?;
  /// # Ok(())
  /// # }
  /// ```
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
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use bd_feature_flags::FeatureFlags;
  /// use tempfile::TempDir;
  ///
  /// # fn main() -> anyhow::Result<()> {
  /// let temp_dir = TempDir::new()?;
  /// let mut flags = FeatureFlags::new(temp_dir.path(), 1024, None)?;
  /// flags.set("my_feature".to_string(), Some("variant_a".to_string()))?;
  ///
  /// if let Some(flag) = flags.get("my_feature") {
  ///   println!("Found flag with variant: {:?}", flag.variant);
  /// } else {
  ///   println!("Flag not found");
  /// }
  ///
  /// // Non-existent flags return None
  /// assert!(flags.get("nonexistent_flag").is_none());
  /// # Ok(())
  /// # }
  /// ```
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
  /// # Examples
  ///
  /// ```no_run
  /// use bd_feature_flags::FeatureFlags;
  /// use tempfile::TempDir;
  ///
  /// # fn main() -> anyhow::Result<()> {
  /// let temp_dir = TempDir::new()?;
  /// let mut flags = FeatureFlags::new(temp_dir.path(), 1024, None)?;
  ///
  /// flags.set(
  ///   "critical_flag".to_string(),
  ///   Some("important_value".to_string()),
  /// )?;
  ///
  /// // Ensure the flag is persisted immediately
  /// flags.sync()?;
  /// # Ok(())
  /// # }
  /// ```
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
  /// # Examples
  ///
  /// ```no_run
  /// use bd_feature_flags::FeatureFlags;
  /// use tempfile::TempDir;
  ///
  /// # fn main() -> anyhow::Result<()> {
  /// let temp_dir = TempDir::new()?;
  /// let mut flags = FeatureFlags::new(temp_dir.path(), 1024, None)?;
  ///
  /// // Set a flag with a variant
  /// flags.set("color_scheme".to_string(), Some("dark".to_string()))?;
  ///
  /// // Set a simple boolean-style flag
  /// flags.set("debug_mode".to_string(), None)?;
  ///
  /// // Update an existing flag
  /// flags.set("color_scheme".to_string(), Some("light".to_string()))?;
  ///
  /// // Empty string variant
  /// flags.set("empty_variant".to_string(), Some(String::new()))?;
  /// # Ok(())
  /// # }
  /// ```
  ///
  /// # Errors
  ///
  /// This function will return an error if the flag cannot be written to persistent
  /// storage due to I/O errors, insufficient disk space, or permission issues.
  pub fn set(&mut self, key: String, variant: Option<String>) -> anyhow::Result<()> {
    let timestamp = get_current_timestamp();
    self.flags_cache.insert(
      key.clone(),
      FeatureFlag {
        variant: variant.clone(),
        timestamp,
      },
    );

    let value = HashMap::from([
      (
        VARIANT_KEY.to_string(),
        Value::String(variant.unwrap_or_default()),
      ),
      (TIMESTAMP_KEY.to_string(), Value::Unsigned(timestamp)),
    ]);
    self.flags_store.insert(key, Value::Object(value))?;

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
  /// # Examples
  ///
  /// ```no_run
  /// use bd_feature_flags::FeatureFlags;
  /// use tempfile::TempDir;
  ///
  /// # fn main() -> anyhow::Result<()> {
  /// let temp_dir = TempDir::new()?;
  /// let mut flags = FeatureFlags::new(temp_dir.path(), 1024, None)?;
  ///
  /// // Add some flags
  /// flags.set("flag1".to_string(), Some("value1".to_string()))?;
  /// flags.set("flag2".to_string(), None)?;
  /// assert_eq!(flags.as_hashmap().len(), 2);
  ///
  /// // Clear all flags
  /// flags.clear()?;
  /// assert_eq!(flags.as_hashmap().len(), 0);
  /// # Ok(())
  /// # }
  /// ```
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
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use bd_feature_flags::FeatureFlags;
  /// use tempfile::TempDir;
  ///
  /// # fn main() -> anyhow::Result<()> {
  /// let temp_dir = TempDir::new()?;
  /// let mut flags = FeatureFlags::new(temp_dir.path(), 1024, None)?;
  ///
  /// flags.set("flag1".to_string(), Some("variant1".to_string()))?;
  /// flags.set("flag2".to_string(), None)?;
  ///
  /// // Iterate over all flags
  /// for (name, flag) in flags.as_hashmap() {
  ///   println!(
  ///     "Flag '{}' has variant: {:?} (updated at {})",
  ///     name, flag.variant, flag.timestamp
  ///   );
  /// }
  ///
  /// // Check total number of flags
  /// println!("Total flags: {}", flags.as_hashmap().len());
  ///
  /// // Check if specific flag exists without borrowing
  /// if flags.as_hashmap().contains_key("flag1") {
  ///   println!("flag1 exists");
  /// }
  /// # Ok(())
  /// # }
  /// ```
  #[must_use]
  pub fn as_hashmap(&self) -> &HashMap<String, FeatureFlag> {
    &self.flags_cache
  }
}
