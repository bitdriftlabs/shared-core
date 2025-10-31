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
use std::path::{Path, PathBuf};

#[cfg(test)]
#[path = "./feature_flags_test.rs"]
mod feature_flags_test;

pub mod test;

const VARIANT_KEY: &str = "v";
const TIMESTAMP_KEY: &str = "t";
const DEFAULT_HIGH_WATER_MARK_RATIO: f32 = 0.8;

/// A feature flag containing an optional variant and timestamp.
///
/// Each flag consists of:
/// - An optional variant string that can be used to configure behavior
/// - A timestamp indicating when the flag was last updated
#[derive(Debug)]
pub struct Flag<'a> {
  /// The variant string for this flag.
  ///
  /// - `Some(variant)` indicates the flag is enabled with the specified variant
  /// - `None` indicates the flag is enabled without a specific variant
  pub variant: Option<&'a str>,

  /// Timestamp when this flag was last updated.
  pub timestamp: time::OffsetDateTime,
}

impl<'a> Flag<'a> {
  /// Creates a new `Flag` with the given variant and optional timestamp.
  ///
  /// Uses the current time if no timestamp is provided.
  ///
  /// If variant is `Some("")`, it is treated as `None`.
  pub fn new(variant: Option<&'a str>, timestamp: Option<time::OffsetDateTime>) -> Self {
    // Validate the variant - reject empty strings
    let validated_variant = match variant {
      Some(s) if s.is_empty() => None,
      None => None,
      Some(s) => Some(s),
    };

    Self {
      variant: validated_variant,
      timestamp: timestamp.unwrap_or_else(time::OffsetDateTime::now_utc),
    }
  }

  /// Creates a new `Flag` from a BONJSON Value.
  ///
  /// Returns `None` if the value is not a valid feature flag object.
  #[must_use]
  pub fn from_value(value: &'a Value) -> Option<Self> {
    // Handle both Object and KVVec types for backward compatibility
    let get_field = |key: &str| -> Option<&Value> {
      match value {
        Value::Object(obj) => obj.get(key),
        // Although this should never happen because the deserializer will always decode as an
        // `Object`, `KVVec` is technically possible so we handle it.
        Value::KVVec(kv_vec) => kv_vec.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
      }
    };

    let variant = match get_field(VARIANT_KEY) {
      Some(Value::String(s)) => {
        if s.is_empty() {
          None
        } else {
          Some(s.as_ref())
        }
      },
      _ => return None,
    };

    let timestamp = match get_field(TIMESTAMP_KEY) {
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
  }

  /// Converts a `Flag` to a BONJSON Value.
  pub fn to_value(&self) -> Value {
    let storage_variant = self
      .variant
      .as_ref()
      .map_or_else(String::new, ToString::to_string);

    let timestamp_nanos = u64::try_from(self.timestamp.unix_timestamp_nanos()).unwrap_or(0);

    let kv_vec = vec![
      (VARIANT_KEY.to_string(), Value::String(storage_variant)),
      (TIMESTAMP_KEY.to_string(), Value::Unsigned(timestamp_nanos)),
    ];
    Value::KVVec(kv_vec)
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
    )?;
    Ok(Self { flags_store })
  }

  /// Opens an existing `FeatureFlags` instance from pre-existing storage files.
  ///
  /// Unlike `new()`, this constructor requires both journal files to exist and will fail if either
  /// is missing. This is useful when you want to ensure you're loading from an existing feature
  /// flags store rather than creating a new one.
  ///
  /// # Arguments
  ///
  /// * `base_path` - The filesystem path to the file basename where feature flags are stored. Two
  ///   files with extensions "jrna" and "jrnb" must already exist at this location.
  /// * `buffer_size` - The size of the underlying storage buffer in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0-1.0) for buffer high water mark. When the
  ///   buffer reaches this percentage full, older data may be compacted. If `None`, a default value
  ///   of 0.8 will be used.
  ///
  /// # Returns
  ///
  /// Returns `Ok(FeatureFlags)` on success, or an error if the storage files don't exist or
  /// cannot be opened.
  ///
  /// # Errors
  ///
  /// This function will return an error if:
  /// - Either of the required journal files does not exist
  /// - The storage files cannot be opened or read
  /// - The storage files contain invalid data
  /// - Insufficient permissions to access the storage location
  pub fn open_existing<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let flags_store = KVStore::open_existing(
      base_path,
      buffer_size,
      Some(high_water_mark_ratio.unwrap_or(DEFAULT_HIGH_WATER_MARK_RATIO)),
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
  pub fn get(&self, key: &str) -> Option<Flag<'_>> {
    let value = self.flags_store.get(key)?;
    Flag::from_value(value)
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
  ///   - `Some("")` treats the variant as `None`
  ///   - `None` sets the flag without a variant (simple boolean-style flag)
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on success, or an error if the flag cannot be stored.
  ///
  /// # Errors
  ///
  /// This function will return an error if:
  /// - The flag cannot be written to persistent storage due to I/O errors, insufficient disk space,
  ///   or permission issues
  pub fn set(&mut self, key: String, variant: Option<&str>) -> anyhow::Result<()> {
    let feature_flag = Flag::new(variant, None);
    let value = feature_flag.to_value();
    self.flags_store.insert(key, value)?;
    Ok(())
  }

  /// Sets or updates multiple feature flags in a single operation.
  ///
  /// Creates or updates multiple feature flags with their respective variants. All flags are
  /// immediately stored in persistent storage and receive timestamps indicating when they were
  /// last modified. This method converts the input into a Vec of (key, value) pairs for
  /// efficient batch processing by the underlying KV store.
  ///
  /// # Arguments
  ///
  /// * `flags` - A vector of tuples containing flag names and their variants:
  ///   - `Some(string)` sets the flag with the specified variant
  ///   - `Some("")` treats the variant as `None`
  ///   - `None` sets the flag without a variant (simple boolean-style flag)
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on success, or an error if any flag cannot be stored.
  ///
  /// # Errors
  ///
  /// This function will return an error if:
  /// - Any flag cannot be written to persistent storage due to I/O errors, insufficient disk space,
  ///   or permission issues
  /// - If an error occurs, no flags will be written.
  pub fn set_multiple(
    &mut self,
    flags: Vec<(String, Option<impl AsRef<str>>)>,
  ) -> anyhow::Result<()> {
    // Convert the input vector to Vec format for the KV store
    let now = time::OffsetDateTime::now_utc();
    let kv_entries: Vec<(String, bd_bonjson::Value)> = flags
      .into_iter()
      .map(|(key, variant)| {
        let feature_flag = Flag::new(variant.as_ref().map(std::convert::AsRef::as_ref), Some(now));
        let value = feature_flag.to_value();
        Ok((key, value))
      })
      .collect::<anyhow::Result<Vec<_>>>()?;

    self.flags_store.insert_multiple(&kv_entries)?;
    Ok(())
  }

  /// Removes a feature flag.
  ///
  /// # Arguments
  ///
  /// * `key` - The name of the feature flag to remove
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on success, or an error if the flag cannot be removed.
  ///
  /// # Errors
  ///
  /// This function will return an error if:
  /// - The flag cannot be removed due to I/O errors, insufficient disk space, or permission issues
  pub fn remove(&mut self, key: &str) -> anyhow::Result<()> {
    self.flags_store.remove(key)?;
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

  /// Returns an iterator over all feature flags.
  ///
  /// This method provides an iterator that yields `(String, FeatureFlag)` pairs for all
  /// valid feature flags in the store. Invalid entries are filtered out during iteration.
  ///
  /// # Returns
  ///
  /// An iterator over `(String, FeatureFlag)` pairs.
  pub fn iter(&self) -> impl Iterator<Item = (&str, Flag<'_>)> + '_ {
    self
      .flags_store
      .as_hashmap()
      .iter()
      .filter_map(|(key, value)| {
        Flag::from_value(value).map(|feature_flag| (key.as_str(), feature_flag))
      })
  }
}

/// Builds `FeatureFlags` objects, and manages on-disk data for current and previous runs.
#[derive(Clone)]
pub struct FeatureFlagsBuilder {
  current_path: PathBuf,
  previous_path: PathBuf,
  file_size: usize,
  high_water_mark_ratio: f32,
}

impl FeatureFlagsBuilder {
  #[must_use]
  pub fn new(sdk_path: &Path, file_size: usize, high_water_mark_ratio: f32) -> Self {
    Self {
      current_path: sdk_path.join("feature_flags_current"),
      previous_path: sdk_path.join("feature_flags_previous"),
      file_size,
      high_water_mark_ratio,
    }
  }

  fn replace_file(from: &Path, to: &Path) {
    // These should never fail unless there's a serious filesystem issue.
    match std::fs::remove_file(to) {
      Ok(()) => {},
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
      Err(e) => {
        log::warn!("failed to remove existing file {}: {e}", to.display());
      },
    }

    match std::fs::rename(from, to) {
      Ok(()) => {},
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
      Err(e) => {
        log::warn!(
          "failed to rename file {} to {}: {e}",
          from.display(),
          to.display()
        );
      },
    }
  }

  /// Backup the current feature flags.
  /// This should be called only once, on relaunch, before getting current or previous feature
  /// flags.
  pub fn backup_previous(&self) {
    Self::replace_file(
      &self.current_path.with_extension("jrna"),
      &self.previous_path.with_extension("jrna"),
    );
    Self::replace_file(
      &self.current_path.with_extension("jrnb"),
      &self.previous_path.with_extension("jrnb"),
    );
  }

  pub fn current_feature_flags(&self) -> anyhow::Result<FeatureFlags> {
    FeatureFlags::new(
      &self.current_path,
      self.file_size,
      Some(self.high_water_mark_ratio),
    )
  }

  pub fn previous_feature_flags(&self) -> anyhow::Result<FeatureFlags> {
    FeatureFlags::open_existing(
      &self.previous_path,
      self.file_size,
      Some(self.high_water_mark_ratio),
    )
  }
}
