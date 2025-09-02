// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{KVJournal, HighWaterMarkCallback, in_memory::InMemoryKVJournal};
use bd_bonjson::Value;
use memmap2::{MmapMut, MmapOptions};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;

/// Memory-mapped implementation of a crash-resilient key-value journal.
///
/// This implementation uses memory-mapped files to provide persistence while maintaining
/// the efficiency of in-memory operations. All changes are automatically synced to disk.
#[derive(Debug)]
pub struct MemMappedKVJournal {
  in_memory_kv: InMemoryKVJournal<'static>,
  _mmap: MmapMut, // Keep the mmap alive for the lifetime of the struct
}

impl MemMappedKVJournal {
  /// Create a new memory-mapped KV journal using the provided file path.
  ///
  /// The file will be created if it doesn't exist, or opened if it does.
  /// The file will be resized to the specified size if it's smaller.
  ///
  /// # Arguments
  /// * `file_path` - Path to the file to use for storage
  /// * `size` - Minimum size of the file in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if the file cannot be created/opened or memory-mapped.
  pub fn new<P: AsRef<Path>>(
    file_path: P,
    size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(file_path)?;
    
    // Ensure the file is at least the requested size
    let file_len = file.metadata()?.len();
    if file_len < size as u64 {
      file.set_len(size as u64)?;
    }

    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    
    // Convert the mmap slice to a static lifetime slice
    // This is safe because we keep the mmap alive for the lifetime of the struct
    let buffer: &'static mut [u8] = unsafe {
      std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), mmap.len())
    };

    let in_memory_kv = InMemoryKVJournal::new(buffer, high_water_mark_ratio, callback)?;

    Ok(Self {
      in_memory_kv,
      _mmap: mmap,
    })
  }

  /// Create a new memory-mapped KV journal from an existing file.
  ///
  /// The file must already exist and contain a properly formatted KV journal.
  ///
  /// # Arguments
  /// * `file_path` - Path to the existing file
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if the file cannot be opened, memory-mapped, or contains invalid data.
  pub fn from_file<P: AsRef<Path>>(
    file_path: P,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .open(file_path)?;

    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    
    // Convert the mmap slice to a static lifetime slice
    // This is safe because we keep the mmap alive for the lifetime of the struct
    let buffer: &'static mut [u8] = unsafe {
      std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), mmap.len())
    };

    let in_memory_kv = InMemoryKVJournal::from_buffer(buffer, high_water_mark_ratio, callback)?;

    Ok(Self {
      in_memory_kv,
      _mmap: mmap,
    })
  }

  /// Synchronize changes to disk.
  ///
  /// This forces any changes in the memory-mapped region to be written to the underlying file.
  /// Note that changes are typically synced automatically by the OS, but this provides
  /// explicit control when needed.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    self._mmap.flush()?;
    Ok(())
  }

  /// Get the size of the underlying file in bytes.
  pub fn file_size(&self) -> usize {
    self._mmap.len()
  }

  /// Get a copy of the buffer for testing purposes
  #[cfg(test)]
  pub fn buffer_copy(&self) -> Vec<u8> {
    self.in_memory_kv.buffer_copy()
  }
}

impl KVJournal for MemMappedKVJournal {
  /// Get the current high water mark position.
  fn high_water_mark(&self) -> usize {
    self.in_memory_kv.high_water_mark()
  }

  /// Check if the high water mark has been triggered.
  fn is_high_water_mark_triggered(&self) -> bool {
    self.in_memory_kv.is_high_water_mark_triggered()
  }

  /// Get the current buffer usage as a percentage (0.0 to 1.0).
  fn buffer_usage_ratio(&self) -> f32 {
    self.in_memory_kv.buffer_usage_ratio()
  }

  /// Set key to value in this journal.
  ///
  /// This will create a new journal entry and automatically persist it to the mapped file.
  ///
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.in_memory_kv.set(key, value)
  }

  /// Delete a key from this journal.
  ///
  /// This will create a new journal entry and automatically persist it to the mapped file.
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.in_memory_kv.delete(key)
  }

  /// Get the current state of the journal as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    self.in_memory_kv.as_hashmap()
  }

  /// Get the time when the journal was initialized (nanoseconds since UNIX epoch).
  ///
  /// # Errors
  /// Returns an error if the initialization timestamp cannot be retrieved.
  fn get_init_time(&mut self) -> anyhow::Result<u64> {
    self.in_memory_kv.get_init_time()
  }

  /// Reinitialize this journal using the data from another journal.
  /// The high water mark is not affected.
  ///
  /// # Errors
  /// Returns an error if the other journal cannot be read or if writing to this journal fails.
  fn reinit_from(&mut self, other: &mut dyn KVJournal) -> anyhow::Result<()> {
    // Delegate to the in-memory journal
    self.in_memory_kv.reinit_from(other)?;
    
    // Sync the memory-mapped buffer to disk
    self._mmap.flush()?;
    
    Ok(())
  }
}
