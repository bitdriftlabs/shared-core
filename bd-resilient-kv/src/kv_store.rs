// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::{DoubleBufferedKVJournal, KVJournal, MemMappedKVJournal};
use ahash::AHashMap;
use bd_bonjson::Value;
use std::path::{Path, PathBuf};

/// State of the `KVStore` - either uninitialized or initialized with journals.
enum KVStoreState {
  /// Store has not been initialized yet - stores parameters for lazy creation.
  Uninitialized {
    file_a_path: PathBuf,
    file_b_path: PathBuf,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  },
  /// Store has been initialized with double-buffered journals.
  Initialized(DoubleBufferedKVJournal<MemMappedKVJournal, MemMappedKVJournal>),
}

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
///
/// ## Lazy Initialization
///
/// If the journal files don't exist when creating a new `KVStore`, the store uses lazy
/// initialization - the files are only created on the first write operation. If the files
/// already exist, they are loaded immediately.
///
/// You can force initialization before the first write by calling `ensure_initialized()`.
pub struct KVStore {
  state: KVStoreState,
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
  /// If the files already exist, they will be loaded with their existing contents immediately.
  /// If the files don't exist, journal creation is deferred until the first write operation.
  ///
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
  /// Returns an error if existing journal files cannot be opened or if initialization fails.
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let base = base_path.as_ref();
    let file_a = base.with_extension("jrna");
    let file_b = base.with_extension("jrnb");

    // Check if files exist
    let files_exist = file_a.exists() || file_b.exists();

    let (state, cached_map) = if files_exist {
      // Files exist - initialize immediately
      let journal_a = open_or_create_journal(&file_a, buffer_size, high_water_mark_ratio)?;
      let journal_b = open_or_create_journal(&file_b, buffer_size, high_water_mark_ratio)?;
      let journal = DoubleBufferedKVJournal::new(journal_a, journal_b)?;
      let cached_map = journal.as_hashmap()?;
      (KVStoreState::Initialized(journal), cached_map)
    } else {
      // Files don't exist - defer initialization
      (
        KVStoreState::Uninitialized {
          file_a_path: file_a,
          file_b_path: file_b,
          buffer_size,
          high_water_mark_ratio,
        },
        AHashMap::new(),
      )
    };

    Ok(Self { state, cached_map })
  }

  /// Ensure that the journal files are initialized, even if they don't exist yet.
  ///
  /// If the journals are already initialized, this method does nothing.
  ///
  /// # Errors
  /// Returns an error if the journal files cannot be created.
  pub fn ensure_initialized(&mut self) -> anyhow::Result<()> {
    if matches!(self.state, KVStoreState::Initialized(_)) {
      return Ok(()); // Already initialized
    }

    // Extract parameters and initialize
    let (file_a_path, file_b_path, buffer_size, high_water_mark_ratio) = match &self.state {
      KVStoreState::Uninitialized {
        file_a_path,
        file_b_path,
        buffer_size,
        high_water_mark_ratio,
      } => (
        file_a_path.clone(),
        file_b_path.clone(),
        *buffer_size,
        *high_water_mark_ratio,
      ),
      KVStoreState::Initialized(_) => return Ok(()),
    };

    // Create the journals
    let journal_a = MemMappedKVJournal::new(&file_a_path, buffer_size, high_water_mark_ratio)?;
    let journal_b = MemMappedKVJournal::new(&file_b_path, buffer_size, high_water_mark_ratio)?;
    let journal = DoubleBufferedKVJournal::new(journal_a, journal_b)?;

    // Update state
    self.state = KVStoreState::Initialized(journal);

    Ok(())
  }

  /// Get a reference to the journal, returning None if not initialized.
  fn journal(&self) -> Option<&DoubleBufferedKVJournal<MemMappedKVJournal, MemMappedKVJournal>> {
    match &self.state {
      KVStoreState::Initialized(journal) => Some(journal),
      KVStoreState::Uninitialized { .. } => None,
    }
  }

  /// Get a mutable reference to the journal, returning None if not initialized.
  fn journal_mut(
    &mut self,
  ) -> Option<&mut DoubleBufferedKVJournal<MemMappedKVJournal, MemMappedKVJournal>> {
    match &mut self.state {
      KVStoreState::Initialized(journal) => Some(journal),
      KVStoreState::Uninitialized { .. } => None,
    }
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
    self.ensure_initialized()?;

    let old_value = self.get(&key).cloned();
    if matches!(value, Value::Null) {
      // Inserting null is equivalent to deletion
      if old_value.is_some() {
        self
          .journal_mut()
          .ok_or_else(|| anyhow::anyhow!("Journal not initialized"))?
          .delete(&key)?;
        self.cached_map.remove(&key);
      }
    } else {
      self
        .journal_mut()
        .ok_or_else(|| anyhow::anyhow!("Journal not initialized"))?
        .set(&key, &value)?;
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
    self.ensure_initialized()?;

    self
      .journal_mut()
      .ok_or_else(|| anyhow::anyhow!("Journal not initialized"))?
      .set_multiple(entries)?;

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
    let old_value = self.cached_map.remove(key);
    if old_value.is_some()
      && let Some(journal) = self.journal_mut()
    {
      journal.delete(key)?;
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
    if let Some(journal) = self.journal_mut() {
      journal.clear()?;
      self.cached_map.clear();
    }
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
  /// If the journals are not yet initialized, this method does nothing.
  ///
  /// # Errors
  /// Returns an error if the compression operation fails.
  pub fn compress(&mut self) -> anyhow::Result<()> {
    if let Some(journal) = self.journal_mut() {
      journal.compress()?;
    }
    // No need to refresh cache - compression doesn't change the data,
    // just reorganizes storage for better performance
    Ok(())
  }

  /// Synchronize changes to disk for both journal files.
  ///
  /// If the journals are not yet initialized, this method does nothing.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    if let Some(journal) = self.journal() {
      journal.sync()?;
    }
    Ok(())
  }

  /// Get the current buffer usage ratio (0.0 to 1.0).
  ///
  /// Returns 0.0 if the journals are not yet initialized.
  #[must_use]
  pub fn buffer_usage_ratio(&self) -> f32 {
    self
      .journal()
      .map_or(0.0, super::kv_journal::KVJournal::buffer_usage_ratio)
  }

  /// Check if the high water mark has been triggered.
  ///
  /// Returns false if the journals are not yet initialized.
  #[must_use]
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self
      .journal()
      .is_some_and(super::kv_journal::KVJournal::is_high_water_mark_triggered)
  }
}
