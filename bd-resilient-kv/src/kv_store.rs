// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::{DoubleBufferedKVJournal, KVJournal, MemMappedKVJournal};
use ahash::AHashMap;
use bd_bonjson::Value;
use bd_client_common::error::InvariantError;
use once_cell::sync::OnceCell;
use std::path::{Path, PathBuf};

/// A persistent key-value store that provides HashMap-like semantics.
///
/// `KVStore` is backed by a `DoubleBufferedKVJournal` using two `MemMappedKVJournal` instances
/// for crash-resilient storage with automatic compression and high water mark management.
///
/// For performance optimization, `KVStore` maintains an in-memory cache of the key-value data
/// to provide O(1) read operations and avoid expensive journal decoding on every access.
/// The cache is always kept in sync with the underlying journal state.
///
/// The store automatically manages two journal files with extensions ".jrna" and ".jrnb"
/// based on the provided base path.
#[derive(Debug)]
pub struct KVStore {
  journal: DoubleBufferedKVJournal<MemMappedKVJournal, MemMappedKVJournal>,
  cached_map: AHashMap<String, Value>,
}

/// Create or open a single journal file, handling resizing as needed.
fn open_or_create_journal<P: AsRef<Path>>(
  file_path: P,
  target_size: usize,
  high_water_mark_ratio: Option<f32>,
) -> anyhow::Result<MemMappedKVJournal> {
  let path = file_path.as_ref();

  // Try to open with existing data first
  MemMappedKVJournal::from_file(path, target_size, high_water_mark_ratio).or_else(|_| {
    // Data is corrupt or unreadable, create fresh
    MemMappedKVJournal::new(path, target_size, high_water_mark_ratio)
  })
}

impl KVStore {
  /// Create a new `KVStore` with the specified base path and buffer size.
  ///
  /// The actual journal files will be created/opened with extensions ".jrna" and ".jrnb".
  /// If the files already exist, they will be loaded with their existing contents.
  /// If the specified size is larger than existing files, they will be resized while preserving
  /// data. If the specified size is smaller and the existing data doesn't fit, fresh journals
  /// will be created.
  ///
  /// # Arguments
  /// * `base_path` - Base path for the journal files (extensions will be added automatically)
  /// * `buffer_size` - Size in bytes for each journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the journal files cannot be created/opened or if initialization fails.
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let base = base_path.as_ref();
    let file_a = base.with_extension("jrna");
    let file_b = base.with_extension("jrnb");

    // Try to create/open the journal files
    let journal_a = open_or_create_journal(&file_a, buffer_size, high_water_mark_ratio)?;
    let journal_b = open_or_create_journal(&file_b, buffer_size, high_water_mark_ratio)?;

    let journal = DoubleBufferedKVJournal::new(journal_a, journal_b)?;
    let cached_map = journal.as_hashmap()?;

    Ok(Self {
      journal,
      cached_map,
    })
  }

  /// Get a value by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get(&self, key: &str) -> Option<&Value> {
    self.cached_map.get(key)
  }

  /// Insert a value for a key, returning the previous value if it existed.
  ///
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// Returns an error if the value cannot be written to the journal.
  pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<Option<Value>> {
    let old_value = self.get(&key).cloned();
    if matches!(value, Value::Null) {
      // Inserting null is equivalent to deletion
      if old_value.is_some() {
        self.journal.delete(&key)?;
        self.cached_map.remove(&key);
      }
    } else {
      self.journal.set(&key, &value)?;
      self.cached_map.insert(key, value);
    }
    Ok(old_value)
  }

  /// Insert multiple key-value pairs in a single operation.
  ///
  /// This method efficiently inserts multiple key-value pairs by using the underlying
  /// journal's batch operation capability. For each key-value pair:
  /// - Inserting `Value::Null` is equivalent to removing the key
  /// - Other values are inserted normally
  ///
  /// # Arguments
  /// * `entries` - A slice of key-value pairs to be inserted
  ///
  /// # Errors
  /// Returns an error if any value cannot be written to the journal. If an error occurs,
  /// no entries will be written.
  pub fn insert_multiple(&mut self, entries: &[(String, Value)]) -> anyhow::Result<()> {
    self.journal.set_multiple(entries)?;

    // Update the cache to reflect all changes
    for (key, value) in entries {
      if matches!(value, Value::Null) {
        self.cached_map.remove(key);
      } else {
        self.cached_map.insert(key.clone(), value.clone());
      }
    }

    Ok(())
  }

  /// Remove a key and return its value if it existed.
  ///
  /// # Errors
  /// Returns an error if the deletion cannot be written to the journal.
  pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<Value>> {
    let old_value = self.get(key).cloned();
    if old_value.is_some() {
      self.journal.delete(key)?;
      self.cached_map.remove(key);
    }
    Ok(old_value)
  }

  /// Check if the store contains a key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn contains_key(&self, key: &str) -> bool {
    self.cached_map.contains_key(key)
  }

  /// Get the number of key-value pairs in the store.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn len(&self) -> usize {
    self.cached_map.len()
  }

  /// Check if the store is empty.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Clear all key-value pairs from the store.
  ///
  /// # Errors
  /// Returns an error if the clearing operation fails.
  pub fn clear(&mut self) -> anyhow::Result<()> {
    self.journal.clear()?;
    self.cached_map.clear();
    Ok(())
  }

  /// Get a reference to the current hash map
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn as_hashmap(&self) -> &AHashMap<String, Value> {
    &self.cached_map
  }

  /// Force compression of the underlying journals.
  ///
  /// This operation reinitializes the inactive journal from the active one and switches to it,
  /// which can help reduce fragmentation and optimize storage space.
  ///
  /// # Errors
  /// Returns an error if the compression operation fails.
  pub fn compress(&mut self) -> anyhow::Result<()> {
    self.journal.compress()?;
    // No need to refresh cache - compression doesn't change the data,
    // just reorganizes storage for better performance
    Ok(())
  }

  /// Synchronize changes to disk for both journal files.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.journal.sync()
  }

  /// Get the current buffer usage ratio (0.0 to 1.0).
  #[must_use]
  pub fn buffer_usage_ratio(&self) -> f32 {
    self.journal.buffer_usage_ratio()
  }

  /// Check if the high water mark has been triggered.
  #[must_use]
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self.journal.is_high_water_mark_triggered()
  }
}

/// A lazy wrapper for `KVStore` that defers initialization until first use.
///
/// # Thread Safety
/// `LazyKVStore` is not thread-safe. Mutable operations require exclusive access
/// via `&mut self`. If you need to share a `LazyKVStore` across threads, wrap it
/// in a `Mutex` or similar synchronization primitive.
#[derive(Debug)]
pub struct LazyKVStore {
  store: OnceCell<KVStore>,
  base_path: PathBuf,
  buffer_size: usize,
  high_water_mark_ratio: Option<f32>,
}

impl LazyKVStore {
  /// Create a new `LazyKVStore` with the specified configuration.
  ///
  /// The actual `KVStore` (and its backing files) will not be created until the first method call.
  ///
  /// # Arguments
  /// * `base_path` - Base path for the journal files (extensions will be added automatically)
  /// * `buffer_size` - Size in bytes for each journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  #[must_use]
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> Self {
    Self {
      store: OnceCell::new(),
      base_path: base_path.as_ref().to_path_buf(),
      buffer_size,
      high_water_mark_ratio,
    }
  }

  /// Get a reference to the store, initializing it if necessary.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  fn get_store(&self) -> anyhow::Result<&KVStore> {
    self.store.get_or_try_init(|| {
      KVStore::new(
        &self.base_path,
        self.buffer_size,
        self.high_water_mark_ratio,
      )
    })
  }

  /// Get a mutable reference to the store, initializing it if necessary.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  fn get_mut_store(&mut self) -> anyhow::Result<&mut KVStore> {
    // Ensure that the store is initialized.
    self.get_store()?;

    // Get mutable access. This should always succeed because:
    // 1. We just initialized the store above
    // 2. The temporary reference from get_store() was dropped at the semicolon
    // 3. We have &mut self, so no other references can exist
    // If this returns None, it's an invariant violation (impossible state)
    Ok(self.store.get_mut().ok_or(InvariantError::Invariant)?)
  }

  /// Get a value by key.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn get(&self, key: &str) -> anyhow::Result<Option<&Value>> {
    Ok(self.get_store()?.get(key))
  }

  /// Insert a value for a key, returning the previous value if it existed.
  ///
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized or if the value cannot be written.
  pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<Option<Value>> {
    self.get_mut_store()?.insert(key, value)
  }

  /// Insert multiple key-value pairs in a single operation.
  ///
  /// # Arguments
  /// * `entries` - A slice of key-value pairs to be inserted
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized or if any value cannot be written.
  pub fn insert_multiple(&mut self, entries: &[(String, Value)]) -> anyhow::Result<()> {
    self.get_mut_store()?.insert_multiple(entries)
  }

  /// Remove a key and return its value if it existed.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized or if the deletion cannot be written.
  pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<Value>> {
    self.get_mut_store()?.remove(key)
  }

  /// Check if the store contains a key.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn contains_key(&self, key: &str) -> anyhow::Result<bool> {
    Ok(self.get_store()?.contains_key(key))
  }

  /// Get the number of key-value pairs in the store.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn len(&self) -> anyhow::Result<usize> {
    Ok(self.get_store()?.len())
  }

  /// Check if the store is empty.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn is_empty(&self) -> anyhow::Result<bool> {
    Ok(self.get_store()?.is_empty())
  }

  /// Clear all key-value pairs from the store.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized or if the clearing operation fails.
  pub fn clear(&mut self) -> anyhow::Result<()> {
    self.get_mut_store()?.clear()
  }

  /// Get a reference to the current hash map.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn as_hashmap(&self) -> anyhow::Result<&AHashMap<String, Value>> {
    Ok(self.get_store()?.as_hashmap())
  }

  /// Force compression of the underlying journals.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized or if the compression operation fails.
  pub fn compress(&mut self) -> anyhow::Result<()> {
    self.get_mut_store()?.compress()
  }

  /// Synchronize changes to disk for both journal files.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized or if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.get_store()?.sync()
  }

  /// Get the current buffer usage ratio (0.0 to 1.0).
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn buffer_usage_ratio(&self) -> anyhow::Result<f32> {
    Ok(self.get_store()?.buffer_usage_ratio())
  }

  /// Check if the high water mark has been triggered.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn is_high_water_mark_triggered(&self) -> anyhow::Result<bool> {
    Ok(self.get_store()?.is_high_water_mark_triggered())
  }

  /// Force initialization of the store (which will also create the backing files).
  ///
  /// If the store is already initialized, this is a no-op.
  ///
  /// # Errors
  /// Returns an error if the store cannot be initialized.
  pub fn initialize(&self) -> anyhow::Result<()> {
    self.get_store()?;
    Ok(())
  }
}
