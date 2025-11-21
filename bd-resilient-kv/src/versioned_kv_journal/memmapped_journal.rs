// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::journal::VersionedJournal;
use crate::Scope;
use crate::versioned_kv_journal::journal::PartialDataLoss;
use bd_time::TimeProvider;
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

/// Memory-mapped implementation of a timestamped journal.
///
/// This implementation uses memory-mapped files to provide persistence while maintaining
/// the efficiency of in-memory operations. All changes are automatically synced to disk.
/// Each write operation receives a timestamp for point-in-time recovery.
///
/// # Safety
/// During construction, we unsafely declare mmap's internal buffer as having a static
/// lifetime, but it's actually tied to the lifetime of `inner`. This works because
/// nothing external holds a reference to the buffer.
pub struct MemMappedVersionedJournal<M> {
  // Note: mmap MUST de-init AFTER versioned_kv because mmap uses it.
  mmap: MmapMut,
  inner: VersionedJournal<'static, M>,
}

impl<M> std::ops::Deref for MemMappedVersionedJournal<M> {
  type Target = VersionedJournal<'static, M>;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl<M> std::ops::DerefMut for MemMappedVersionedJournal<M> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}

impl<M: protobuf::Message> MemMappedVersionedJournal<M> {
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
  /// * `entries` - Iterator of entries to be inserted into the newly created buffer.
  ///
  /// # Errors
  /// Returns an error if the file cannot be created/opened or memory-mapped.
  pub fn new<P: AsRef<Path>>(
    file_path: P,
    size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
    entries: impl IntoIterator<Item = (Scope, String, M, u64)>,
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

    let versioned_kv =
      VersionedJournal::new(buffer, high_water_mark_ratio, time_provider, entries)?;

    Ok(Self {
      mmap,
      inner: versioned_kv,
    })
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
    time_provider: Arc<dyn TimeProvider>,
    f: impl FnMut(Scope, &str, &M, u64),
  ) -> anyhow::Result<(Self, PartialDataLoss)> {
    let file = OpenOptions::new().read(true).write(true).open(file_path)?;

    let file_len = file.metadata()?.len();
    if file_len != size as u64 {
      file.set_len(size as u64)?;
    }

    let (mmap, buffer) = unsafe { Self::create_mmap_buffer(file)? };

    let (versioned_kv, partial_data_loss) =
      VersionedJournal::from_buffer(buffer, high_water_mark_ratio, time_provider, f)?;

    Ok((
      Self {
        mmap,
        inner: versioned_kv,
      },
      partial_data_loss,
    ))
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
  pub fn sync(journal: &Self) -> anyhow::Result<()> {
    journal.mmap.flush().map_err(Into::into)
  }
}
