// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::versioned::VersionedJournal;
use crate::versioned_kv_journal::TimestampedValue;
use ahash::AHashMap;
use bd_proto::protos::state::payload::StateKeyValuePair;
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::path::Path;

/// Memory-mapped implementation of a timestamped key-value journal.
///
/// This implementation uses memory-mapped files to provide persistence while maintaining
/// the efficiency of in-memory operations. All changes are automatically synced to disk.
/// Each write operation receives a timestamp for point-in-time recovery.
///
/// # Safety
/// During construction, we unsafely declare mmap's internal buffer as having a static
/// lifetime, but it's actually tied to the lifetime of `versioned_kv`. This works because
/// nothing external holds a reference to the buffer.
#[derive(Debug)]
pub struct MemMappedVersionedKVJournal {
  // Note: mmap MUST de-init AFTER versioned_kv because mmap uses it.
  mmap: MmapMut,
  versioned_kv: VersionedJournal<'static, StateKeyValuePair>,
}

impl MemMappedVersionedKVJournal {
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

  /// Create a new memory-mapped versioned KV journal using the provided file path.
  ///
  /// The file will be created if it doesn't exist, or opened if it does.
  /// The file will be resized to the specified size if it's different.
  ///
  /// # Arguments
  /// * `file_path` - Path to the file to use for storage
  /// * `size` - Minimum size of the file in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the file cannot be created/opened or memory-mapped.
  pub fn new<P: AsRef<Path>>(
    file_path: P,
    size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(file_path)?;

    let file_len = file.metadata()?.len();
    if file_len != size as u64 {
      file.set_len(size as u64)?;
    }

    let (mmap, buffer) = unsafe { Self::create_mmap_buffer(file)? };

    let versioned_kv = VersionedJournal::new(buffer, high_water_mark_ratio)?;

    Ok(Self { mmap, versioned_kv })
  }

  /// Create a new memory-mapped versioned KV journal from an existing file.
  ///
  /// The file must already exist and contain a properly formatted versioned KV journal.
  /// The file will be resized to the specified size if it's different.
  ///
  /// # Arguments
  /// * `file_path` - Path to the existing file
  /// * `size` - Size to resize the file to in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the file cannot be opened, memory-mapped, or contains invalid data.
  /// Note: If the new size is smaller than the current file size, data may be truncated.
  pub fn from_file<P: AsRef<Path>>(
    file_path: P,
    size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let file = OpenOptions::new().read(true).write(true).open(file_path)?;

    let file_len = file.metadata()?.len();
    if file_len != size as u64 {
      file.set_len(size as u64)?;
    }

    let (mmap, buffer) = unsafe { Self::create_mmap_buffer(file)? };

    let versioned_kv = VersionedJournal::from_buffer(buffer, high_water_mark_ratio)?;

    Ok(Self { mmap, versioned_kv })
  }

  /// Insert a new entry into the journal with the given payload.
  /// Returns the timestamp of the operation.
  pub fn insert_entry(&mut self, message: impl protobuf::MessageFull) -> anyhow::Result<u64> {
    self.versioned_kv.insert_entry(message)
  }


  /// Check if the high water mark has been triggered.
  #[must_use]
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self.versioned_kv.is_high_water_mark_triggered()
  }

  /// Reconstruct the hashmap with timestamps by replaying all journal entries.
  pub fn to_hashmap_with_timestamps(&self) -> (AHashMap<String, TimestampedValue>, bool) {
    let mut map = AHashMap::new();
    let complete = self.versioned_kv.read(|payload, timestamp| {
      if let Some(value) = payload.value.clone().into_option() {
        map.insert(payload.key.clone(), TimestampedValue { value, timestamp });
      } else {
        map.remove(&payload.key);
      }
    });
    (map, !complete)
  }

  /// Synchronize changes to disk.
  ///
  /// This method explicitly flushes any pending changes to the underlying file.
  /// Note that changes are typically synced automatically by the OS, but this provides
  /// explicit control when needed.
  ///
  /// This is a blocking operation that performs synchronous I/O (`msync()` system call).
  /// In async contexts, consider wrapping this call with `tokio::task::spawn_blocking`.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.mmap.flush().map_err(Into::into)
  }
}
