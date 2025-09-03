// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::in_memory::InMemoryKVJournal;
use crate::{HighWaterMarkCallback, KVJournal};
use bd_bonjson::Value;
use memmap2::{MmapMut, MmapOptions};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;

/// Memory-mapped implementation of a crash-resilient key-value journal.
///
/// This implementation uses memory-mapped files to provide persistence while maintaining
/// the efficiency of in-memory operations. All changes are automatically synced to disk.
///
/// # Safety
/// During construction, we unsafely declare mmap's internal buffer as having a static
/// lifetime, but it's actually tied to the lifetime of `in_memory_kv`. This works because
/// nothing external holds a reference to the buffer.
#[derive(Debug)]
pub struct MemMappedKVJournal {
  // Note: mmap MUST de-init AFTER in_memory_kv because mmap uses it.
  mmap: MmapMut,
  in_memory_kv: InMemoryKVJournal<'static>,
}

impl MemMappedKVJournal {
  /// Create a memory-mapped buffer from a file and convert it to a static lifetime slice.
  ///
  /// # Safety
  /// The returned slice has a static lifetime, but it's actually tied to the lifetime of the
  /// `MmapMut`. This is safe as long as the `MmapMut` is kept alive for the entire lifetime of
  /// the slice usage.
  #[allow(clippy::needless_pass_by_value)]
  unsafe fn create_mmap_buffer(
    file: std::fs::File,
  ) -> anyhow::Result<(MmapMut, &'static mut [u8])> {
    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

    // Convert the mmap slice to a static lifetime slice
    // This is safe because we keep the mmap alive for the lifetime of the struct
    let buffer: &'static mut [u8] =
      unsafe { std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), mmap.len()) };

    Ok((mmap, buffer))
  }

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
    callback: Option<HighWaterMarkCallback>,
  ) -> anyhow::Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(file_path)?;

    // Ensure the file is at least the requested size
    let file_len = file.metadata()?.len();
    if file_len < size as u64 {
      file.set_len(size as u64)?;
    }

    let (mmap, buffer) = unsafe { Self::create_mmap_buffer(file)? };

    let in_memory_kv = InMemoryKVJournal::new(buffer, high_water_mark_ratio, callback)?;

    Ok(Self { mmap, in_memory_kv })
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
    callback: Option<HighWaterMarkCallback>,
  ) -> anyhow::Result<Self> {
    let file = OpenOptions::new().read(true).write(true).open(file_path)?;

    let (mmap, buffer) = unsafe { Self::create_mmap_buffer(file)? };

    let in_memory_kv = InMemoryKVJournal::from_buffer(buffer, high_water_mark_ratio, callback)?;

    Ok(Self { mmap, in_memory_kv })
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
    self.mmap.flush()?;
    Ok(())
  }

  /// Get the size of the underlying file in bytes.
  #[must_use]
  pub fn file_size(&self) -> usize {
    self.mmap.len()
  }

  /// Get a copy of the buffer for testing purposes
  #[cfg(test)]
  #[must_use]
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

  /// Clear all key-value pairs from the journal.
  ///
  /// This will reinitialize the journal and automatically persist the changes to the mapped file.
  ///
  /// # Errors
  /// Returns an error if the clearing operation fails.
  fn clear(&mut self) -> anyhow::Result<()> {
    self.in_memory_kv.clear()?;

    // Sync the memory-mapped buffer to disk
    self.mmap.flush()?;

    Ok(())
  }

  /// Get the current state of the journal as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    self.in_memory_kv.as_hashmap()
  }

  /// Get the time when the journal was initialized (nanoseconds since UNIX epoch).
  fn get_init_time(&self) -> u64 {
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
    self.mmap.flush()?;

    Ok(())
  }

  /// Synchronize changes to disk.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  fn sync(&self) -> anyhow::Result<()> {
    self.sync()
  }
}
