// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DoubleBufferedKVJournal, MemMappedKVJournal, HighWaterMarkCallback, KVJournal};
use bd_bonjson::Value;
use std::collections::HashMap;
use std::path::Path;

/// A persistent key-value store that provides HashMap-like semantics.
/// 
/// KVStore is backed by a DoubleBufferedKVJournal using two MemMappedKVJournal instances
/// for crash-resilient storage with automatic compression and high water mark management.
///
/// The store automatically manages two journal files with extensions ".jrna" and ".jrnb"
/// based on the provided base path.
pub struct KVStore {
  journal: DoubleBufferedKVJournal<MemMappedKVJournal, MemMappedKVJournal>,
}

impl KVStore {
  /// Create a new KVStore with the specified base path and buffer size.
  ///
  /// The actual journal files will be created/opened with extensions ".jrna" and ".jrnb".
  /// If the files already exist, they will be loaded with their existing contents.
  /// If the specified size is larger than existing files, they will be resized while preserving data.
  /// If the specified size is smaller and the existing data doesn't fit, fresh journals will be created.
  ///
  /// # Arguments
  /// * `base_path` - Base path for the journal files (extensions will be added automatically)
  /// * `buffer_size` - Size in bytes for each journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if the journal files cannot be created/opened or if initialization fails.
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let base = base_path.as_ref();
    let file_a = base.with_extension("jrna");
    let file_b = base.with_extension("jrnb");

    // Try to create/open the journal files
    let journal_a = Self::create_or_open_journal(&file_a, buffer_size, high_water_mark_ratio, callback)?;
    let journal_b = Self::create_or_open_journal(&file_b, buffer_size, high_water_mark_ratio, callback)?;

    let journal = DoubleBufferedKVJournal::new(journal_a, journal_b)?;

    Ok(Self { journal })
  }

  /// Create or open a single journal file, handling resizing as needed.
  fn create_or_open_journal<P: AsRef<Path>>(
    file_path: P,
    target_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<MemMappedKVJournal> {
    let path = file_path.as_ref();
    
    if path.exists() {
      // File exists, check its size
      let current_size = std::fs::metadata(&path)?.len() as usize;
      
      if target_size > current_size {
        // Need to expand the file while preserving data
        Self::resize_journal_file(&path, target_size, high_water_mark_ratio, callback)
      } else if target_size < current_size {
        // Target size is smaller than current file
        // Try to open the existing file first
        match MemMappedKVJournal::from_file(&path, high_water_mark_ratio, callback) {
          Ok(journal) => {
            // Check if the data fits in the smaller target size using buffer usage
            let usage_ratio = journal.buffer_usage_ratio();
            let estimated_used_bytes = (current_size as f32 * usage_ratio) as usize;
            
            if estimated_used_bytes <= target_size {
              // Data should fit, keep the existing journal (can't easily shrink mmap files)
              Ok(journal)
            } else {
              // Data might not fit, create fresh journal
              MemMappedKVJournal::new(&path, target_size, high_water_mark_ratio, callback)
            }
          }
          Err(_) => {
            // File exists but is corrupted or invalid, create fresh
            MemMappedKVJournal::new(&path, target_size, high_water_mark_ratio, callback)
          }
        }
      } else {
        // Size matches, try to open existing file
        match MemMappedKVJournal::from_file(&path, high_water_mark_ratio, callback) {
          Ok(journal) => Ok(journal),
          Err(_) => {
            // File exists but is corrupted or invalid, create fresh
            MemMappedKVJournal::new(&path, target_size, high_water_mark_ratio, callback)
          }
        }
      }
    } else {
      // File doesn't exist, create new
      MemMappedKVJournal::new(&path, target_size, high_water_mark_ratio, callback)
    }
  }

  /// Resize an existing journal file while preserving data.
  fn resize_journal_file<P: AsRef<Path>>(
    file_path: P,
    new_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<MemMappedKVJournal> {
    let path = file_path.as_ref();
    
    // Load existing data
    let mut existing_journal = MemMappedKVJournal::from_file(&path, high_water_mark_ratio, callback)?;
    let existing_data = existing_journal.as_hashmap()?;
    
    // Create new journal with larger size
    let mut new_journal = MemMappedKVJournal::new(&path, new_size, high_water_mark_ratio, callback)?;
    
    // Restore the data
    for (key, value) in existing_data {
      new_journal.set(&key, &value)?;
    }
    
    new_journal.sync()?;
    Ok(new_journal)
  }

  /// Get a value by key.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Value>> {
    let map = self.journal.as_hashmap()?;
    Ok(map.get(key).cloned())
  }

  /// Insert a value for a key, returning the previous value if it existed.
  /// 
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// Returns an error if the value cannot be written to the journal.
  pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<Option<Value>> {
    let old_value = self.get(&key)?;
    if matches!(value, Value::Null) {
      // Inserting null is equivalent to deletion
      if old_value.is_some() {
        self.journal.delete(&key)?;
      }
    } else {
      self.journal.set(&key, &value)?;
    }
    Ok(old_value)
  }

  /// Remove a key and return its value if it existed.
  ///
  /// # Errors
  /// Returns an error if the deletion cannot be written to the journal.
  pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<Value>> {
    let old_value = self.get(key)?;
    if old_value.is_some() {
      self.journal.delete(key)?;
    }
    Ok(old_value)
  }

  /// Check if the store contains a key.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn contains_key(&mut self, key: &str) -> anyhow::Result<bool> {
    let map = self.journal.as_hashmap()?;
    Ok(map.contains_key(key))
  }

  /// Get the number of key-value pairs in the store.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn len(&mut self) -> anyhow::Result<usize> {
    let map = self.journal.as_hashmap()?;
    Ok(map.len())
  }

  /// Check if the store is empty.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn is_empty(&mut self) -> anyhow::Result<bool> {
    Ok(self.len()? == 0)
  }

  /// Clear all key-value pairs from the store.
  ///
  /// # Errors
  /// Returns an error if the clearing operation fails.
  pub fn clear(&mut self) -> anyhow::Result<()> {
    let keys: Vec<String> = self.journal.as_hashmap()?.keys().cloned().collect();
    for key in keys {
      self.journal.delete(&key)?;
    }
    Ok(())
  }

  /// Get all keys in the store.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn keys(&mut self) -> anyhow::Result<Vec<String>> {
    let map = self.journal.as_hashmap()?;
    Ok(map.keys().cloned().collect())
  }

  /// Get all values in the store.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn values(&mut self) -> anyhow::Result<Vec<Value>> {
    let map = self.journal.as_hashmap()?;
    Ok(map.values().cloned().collect())
  }

  /// Get all key-value pairs as a HashMap.
  ///
  /// # Errors
  /// Returns an error if the journal cannot be read.
  pub fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    self.journal.as_hashmap()
  }

  /// Force compression of the underlying journals.
  /// 
  /// This operation reinitializes the inactive journal from the active one and switches to it,
  /// which can help reduce fragmentation and optimize storage space.
  ///
  /// # Errors
  /// Returns an error if the compression operation fails.
  pub fn compress(&mut self) -> anyhow::Result<()> {
    self.journal.compress()
  }

  /// Synchronize changes to disk for both journal files.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.journal.sync()
  }

  /// Get the current buffer usage ratio (0.0 to 1.0).
  pub fn buffer_usage_ratio(&self) -> f32 {
    self.journal.buffer_usage_ratio()
  }

  /// Check if the high water mark has been triggered.
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self.journal.is_high_water_mark_triggered()
  }
}
