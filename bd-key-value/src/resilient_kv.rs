// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use crate::Storage;
use bd_bonjson::Value;
use bd_resilient_kv::KVStore;
use parking_lot::Mutex;
use std::path::Path;
use std::sync::Arc;

#[cfg(test)]
#[path = "resilient_kv_test.rs"]
mod tests;

/// A thread-safe wrapper around `KVStore` that implements the `Storage` trait.
///
/// This container handles the mutability and concurrency concerns that arise from
/// the mismatch between `KVStore`'s mutable operations and the `Storage` trait's
/// immutable interface. It provides thread-safe access to the underlying `KVStore`
/// through interior mutability via a `Mutex`.
pub struct ResilientKvStorage {
  store: Arc<Mutex<KVStore>>,
}

impl ResilientKvStorage {
  /// Create a new `ResilientKvStorage` with the specified configuration.
  ///
  /// This is a convenience wrapper around `KVStore::new` that automatically
  /// provides the necessary thread-safety wrapper.
  ///
  /// # Arguments
  /// * `base_path` - Base path for the journal files (extensions will be added automatically)
  /// * `buffer_size` - Size in bytes for each journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if the underlying `KVStore` cannot be created/opened.
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let kv_store = KVStore::new(base_path, buffer_size, high_water_mark_ratio)?;
    Ok(Self {
      store: Arc::new(Mutex::new(kv_store)),
    })
  }

  fn with_store<F, R>(&self, f: F) -> anyhow::Result<R>
  where
    F: FnOnce(&KVStore) -> anyhow::Result<R>,
  {
    let store = self.store.lock();
    f(&store)
  }

  fn with_store_mut<F, R>(&self, f: F) -> anyhow::Result<R>
  where
    F: FnOnce(&mut KVStore) -> anyhow::Result<R>,
  {
    let mut store = self.store.lock();
    f(&mut store)
  }
}

impl Storage for ResilientKvStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    self.with_store_mut(|store| {
      store.insert(key.to_string(), Value::String(value.to_string()))?;
      Ok(())
    })
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    self.with_store(|store| match store.get(key) {
      Some(value) => match value {
        Value::String(s) => Ok(Some(s.clone())),
        _ => anyhow::bail!("Value at key '{key}' is not a string"),
      },
      None => Ok(None),
    })
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.with_store_mut(|store| {
      store.remove(key)?;
      Ok(())
    })
  }
}
