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
use bd_client_common::error::InvariantError;
use bd_resilient_kv::KVStore;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

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
/// - A timestamp indicating when the flag was last updated
#[derive(Debug)]
pub struct FeatureFlag {
  /// The variant string for this feature flag.
  ///
  /// - `Some(variant)` indicates the flag is enabled with the specified variant
  /// - `None` indicates the flag is enabled without a specific variant
  pub variant: Option<String>,

  /// Timestamp when this flag was last updated.
  pub timestamp: time::OffsetDateTime,
}

impl FeatureFlag {
  /// Creates a new `FeatureFlag` with the given variant and optional timestamp.
  ///
  /// Validates the variant (rejecting empty strings) and uses the current time
  /// if no timestamp is provided.
  ///
  /// Returns an error if an empty string is provided as the variant.
  pub fn new(
    variant: Option<&str>,
    timestamp: Option<time::OffsetDateTime>,
  ) -> anyhow::Result<Self> {
    // Validate the variant - reject empty strings
    let validated_variant = match variant {
      Some("") => return Err(InvariantError::Invariant.into()),
      Some(s) => Some(s.to_string()),
      None => None,
    };

    Ok(Self {
      variant: validated_variant,
      timestamp: timestamp.unwrap_or_else(time::OffsetDateTime::now_utc),
    })
  }

  /// Creates a new `FeatureFlag` from a BONJSON Value.
  ///
  /// Returns `None` if the value is not a valid feature flag object.
  #[must_use]
  pub fn from_value(value: &Value) -> Option<Self> {
    if let Value::Object(obj) = value {
      let variant = match obj.get(VARIANT_KEY) {
        Some(Value::String(s)) => {
          if s.is_empty() {
            None
          } else {
            Some(s.to_string())
          }
        },
        _ => return None,
      };

      let timestamp = match obj.get(TIMESTAMP_KEY) {
        Some(value) => {
          let timestamp_nanos = match value {
            Value::Unsigned(t) => *t,
            Value::Signed(t) if *t >= 0 => u64::try_from(*t).ok()?,
            _ => return None,
          };
          time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_nanos)).ok()?
        },
        _ => return None,
      };

      Some(Self { variant, timestamp })
    } else {
      None
    }
  }

  /// Converts a `FeatureFlag` to a BONJSON Value.
  pub fn to_value(&self) -> Value {
    let storage_variant = self
      .variant
      .as_ref()
      .map_or_else(String::new, std::clone::Clone::clone);

    let timestamp_nanos = u64::try_from(self.timestamp.unix_timestamp_nanos()).unwrap_or(0);

    let obj = HashMap::from([
      (VARIANT_KEY.to_string(), Value::String(storage_variant)),
      (TIMESTAMP_KEY.to_string(), Value::Unsigned(timestamp_nanos)),
    ]);
    Value::Object(obj)
  }
}

/// A feature flags manager with persistent storage.
///
/// `FeatureFlags` provides a way to manage feature flags that are persisted to disk
/// using a resilient key-value store. All flags are stored directly in the persistent
/// storage and converted on demand.
pub struct FeatureFlags {
  flags_store: KVStore,
}

impl FeatureFlags {
  /// Creates a new `FeatureFlags` instance with persistent storage.
  ///
  /// This constructor creates a new feature flags manager backed by a resilient key-value store
  /// at the specified path. Any existing flags stored at this location will be automatically
  /// loaded and made available for retrieval.
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
      Some(high_water_mark_ratio.unwrap_or(DEFAULT_HIGH_WATER_MARK_RATIO)),
      None,
    )?;
    Ok(Self { flags_store })
  }

  /// Retrieves a feature flag by name.
  ///
  /// Returns the feature flag if it exists, or `None` if no flag
  /// with the given name is found.
  ///
  /// # Arguments
  ///
  /// * `key` - The name of the feature flag to retrieve
  #[must_use]
  pub fn get(&self, key: &str) -> Option<FeatureFlag> {
    let value = self.flags_store.get(key)?;
    FeatureFlag::from_value(value)
  }

  /// Sets or updates a feature flag.
  ///
  /// Creates a new feature flag with the given name and variant, or updates an existing flag.
  /// The flag is immediately stored in persistent storage and receives
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
  /// This function will return an error if:
  /// - An empty string is provided as the variant (validation error)
  /// - The flag cannot be written to persistent storage due to I/O errors, insufficient disk space,
  ///   or permission issues
  pub fn set(&mut self, key: &str, variant: Option<&str>) -> anyhow::Result<()> {
    let feature_flag = FeatureFlag::new(variant, None)?;
    let value = feature_flag.to_value();
    self.flags_store.insert(key.to_string(), value)?;
    Ok(())
  }

  /// Removes all feature flags from persistent storage.
  ///
  /// This method deletes all feature flags, clearing the persistent storage.
  /// This operation cannot be undone.
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
    self.flags_store.clear()?;
    Ok(())
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

  /// Returns a `HashMap` containing all feature flags.
  ///
  /// This method provides access to all feature flags as a standard
  /// Rust `HashMap`. This is useful for iterating over all flags or performing
  /// bulk operations. The `HashMap` is generated on-demand from the persistent storage.
  ///
  /// # Returns
  ///
  /// A `HashMap<String, FeatureFlag>` containing all flags.
  ///
  /// TODO: Make an iterator instead
  #[must_use]
  pub fn as_hashmap(&self) -> HashMap<String, FeatureFlag> {
    let mut flags = HashMap::new();
    for (key, value) in self.flags_store.as_hashmap() {
      if let Some(feature_flag) = FeatureFlag::from_value(value) {
        flags.insert(key.clone(), feature_flag);
      }
    }
    flags
  }
}

pub struct FeatureFlagsManager {
  current_path: PathBuf,
  previous_path: PathBuf,
  file_size: usize,
  high_water_mark_ratio: f32,
}

impl FeatureFlagsManager {
  #[must_use]
  pub fn new(sdk_path: &Path, file_size: usize, high_water_mark_ratio: f32) -> Self {
    Self {
      current_path: sdk_path.join("feature_flags_current"),
      previous_path: sdk_path.join("feature_flags_previous"),
      file_size,
      high_water_mark_ratio,
    }
  }

  pub fn backup_previous(&self) -> anyhow::Result<()> {
    if self.previous_path.exists() {
      std::fs::remove_file(&self.previous_path)?;
    }
    if self.current_path.exists() {
      std::fs::rename(&self.current_path, &self.previous_path)?;
    }
    Ok(())
  }

  pub fn current_feature_flags(&self) -> anyhow::Result<FeatureFlags> {
    FeatureFlags::new(&self.current_path, self.file_size, Some(self.high_water_mark_ratio))
  }

  pub fn previous_feature_flags(&self) -> anyhow::Result<FeatureFlags> {
    FeatureFlags::new(&self.previous_path, self.file_size, Some(self.high_water_mark_ratio))
  }
}
