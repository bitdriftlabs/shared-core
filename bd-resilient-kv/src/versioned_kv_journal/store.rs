// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::versioned_kv_journal::TimestampedValue;
use crate::versioned_kv_journal::file_manager::{self, compress_archived_journal};
use crate::versioned_kv_journal::memmapped_versioned::MemMappedVersionedKVJournal;
use crate::versioned_kv_journal::versioned::VersionedJournal;
use ahash::AHashMap;
use bd_proto::protos::state::payload::{StateKeyValuePair, StateValue};
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub enum DataLoss {
  Total,
  Partial,
  None,
}

/// A persistent key-value store with timestamp tracking.
///
/// `VersionedKVStore` provides HashMap-like semantics backed by a timestamped journal that
///
/// # Rotation Strategy
///
/// When the journal reaches its high water mark, the store automatically rotates to a new journal.
/// The rotation process creates a snapshot of the current state while preserving timestamp
/// semantics for accurate point-in-time recovery.
///
/// For detailed information about timestamp semantics, recovery bucketing, and invariants,
/// see the `VERSIONED_FORMAT.md` documentation.
pub struct VersionedKVStore {
  journal: MemMappedVersionedKVJournal,
  cached_map: AHashMap<String, TimestampedValue>,
  dir_path: PathBuf,
  journal_name: String,
  buffer_size: usize,
  high_water_mark_ratio: Option<f32>,
  current_generation: u64,
}

impl VersionedKVStore {
  /// Create a new `VersionedKVStore` with the specified directory, name, and buffer size.
  ///
  /// The journal file will be named `<name>.jrn.N` where N is the generation number.
  /// If a journal already exists, it will be loaded with its existing contents.
  /// Legacy journals (`<name>.jrn`) are automatically migrated to generation 0.
  /// If the specified size is larger than an existing file, it will be resized while preserving
  /// data. If the specified size is smaller and the existing data doesn't fit, a fresh journal
  /// will be created.
  ///
  /// # Errors
  /// Returns an error if we failed to create or open the journal file.
  pub fn new<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let dir = dir_path.as_ref();

    let (journal_path, generation) = file_manager::find_active_journal(dir, name);

    log::debug!(
      "Opening VersionedKVStore journal at {} (generation {generation})",
      journal_path.display()
    );

    let (journal, mut data_loss) = if journal_path.exists() {
      // Try to open existing journal
      MemMappedVersionedKVJournal::from_file(&journal_path, buffer_size, high_water_mark_ratio)
        .map(|j| (j, DataLoss::None))
        .or_else(|_| {
          // TODO(snowp): Distinguish between partial and total data loss.

          // Data is corrupt or unreadable, create fresh journal
          Ok::<_, anyhow::Error>((
            MemMappedVersionedKVJournal::new(&journal_path, buffer_size, high_water_mark_ratio)?,
            DataLoss::Total,
          ))
        })?
    } else {
      // Create new journal

      (
        MemMappedVersionedKVJournal::new(&journal_path, buffer_size, high_water_mark_ratio)?,
        DataLoss::None,
      )
    };

    let (cached_map, incomplete) = journal.to_hashmap_with_timestamps();

    if incomplete && data_loss == DataLoss::None {
      data_loss = DataLoss::Partial;
    }

    Ok((
      Self {
        journal,
        cached_map,
        dir_path: dir.to_path_buf(),
        journal_name: name.to_string(),
        buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
      },
      data_loss,
    ))
  }

  /// Open an existing `VersionedKVStore` from a pre-existing journal file.
  ///
  /// Unlike `new()`, this method requires the journal file to exist and will fail if it's
  /// missing.
  ///
  /// # Arguments
  /// * `dir_path` - Directory path where the journal is stored
  /// * `name` - Base name of the journal (e.g., "store" for "store.jrn.N")
  /// * `buffer_size` - Size in bytes for the journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if:
  /// - The journal file does not exist
  /// - The journal file cannot be opened
  /// - The journal file contains invalid data
  /// - Initialization fails
  pub fn open_existing<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let dir = dir_path.as_ref();

    let (journal_path, generation) = file_manager::find_active_journal(dir, name);

    let journal =
      MemMappedVersionedKVJournal::from_file(&journal_path, buffer_size, high_water_mark_ratio)?;
    let (cached_map, incomplete) = journal.to_hashmap_with_timestamps();

    Ok((
      Self {
        journal,
        cached_map,
        dir_path: dir.to_path_buf(),
        journal_name: name.to_string(),
        buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
      },
      if incomplete {
        DataLoss::Partial
      } else {
        DataLoss::None
      },
    ))
  }

  /// Get a value by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get(&self, key: &str) -> Option<&StateValue> {
    self.cached_map.get(key).map(|tv| &tv.value)
  }

  /// Get a value with its timestamp by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get_with_timestamp(&self, key: &str) -> Option<&TimestampedValue> {
    self.cached_map.get(key)
  }

  /// Insert a value for a key, returning the timestamp assigned to this write.
  ///
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// Returns an error if the value cannot be written to the journal.
  pub async fn insert(&mut self, key: String, value: StateValue) -> anyhow::Result<u64> {
    let timestamp = if value.value_type.is_none() {
      // Inserting null is equivalent to deletion
      let timestamp = self.journal.insert_entry(StateKeyValuePair {
        key: key.clone(),
        ..Default::default()
      })?;
      self.cached_map.remove(&key);
      timestamp
    } else {
      let timestamp = self.journal.insert_entry(StateKeyValuePair {
        key: key.clone(),
        value: Some(value.clone()).into(),
        ..Default::default()
      })?;
      self
        .cached_map
        .insert(key, TimestampedValue { value, timestamp });
      timestamp
    };

    // Check if rotation is needed
    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal().await?;
    }

    Ok(timestamp)
  }

  /// Remove a key and return the timestamp assigned to this deletion.
  ///
  /// Returns `None` if the key didn't exist, otherwise returns the timestamp.
  ///
  /// # Errors
  /// Returns an error if the deletion cannot be written to the journal.
  pub async fn remove(&mut self, key: &str) -> anyhow::Result<Option<u64>> {
    if !self.cached_map.contains_key(key) {
      return Ok(None);
    }

    let timestamp = self.journal.insert_entry(StateKeyValuePair {
      key: key.to_string(),
      ..Default::default()
    })?;
    self.cached_map.remove(key);

    // Check if rotation is needed
    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal().await?;
    }

    Ok(Some(timestamp))
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

  /// Get a reference to the current hash map with timestamps.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn as_hashmap(&self) -> &AHashMap<String, TimestampedValue> {
    &self.cached_map
  }

  /// Synchronize changes to disk.
  ///
  /// This is a blocking operation that performs synchronous I/O. In async contexts,
  /// consider wrapping this call with `tokio::task::spawn_blocking`.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.journal.sync()
  }

  /// Manually trigger journal rotation.
  ///
  /// This will create a new journal with the current state compacted and archive the old journal.
  /// The archived journal will be compressed using zlib to reduce storage size.
  /// Rotation typically happens automatically when the high water mark is reached, but this
  /// method allows manual control when needed.
  pub async fn rotate_journal(&mut self) -> anyhow::Result<()> {
    // Increment generation counter for new journal
    let next_generation = self.current_generation + 1;
    let new_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{next_generation}", self.journal_name));

    // Create new journal with compacted state
    let new_journal = self.create_rotated_journal(&new_journal_path).await?;

    // Replace in-memory journal with new one (critical section - but no file ops!)
    // The old journal file remains at the previous generation number
    let old_journal = std::mem::replace(&mut self.journal, new_journal);
    let old_generation = self.current_generation;
    self.current_generation = next_generation;

    // Drop the old journal to release the mmap
    drop(old_journal);

    // Best-effort cleanup: compress and archive the old journal
    let old_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{old_generation}", self.journal_name));
    self.cleanup_archived_journal(&old_journal_path).await;

    Ok(())
  }

  /// Clean up after successful rotation (best effort, non-critical).
  ///
  /// This compresses and removes the old journal. Failures are logged but not propagated.
  async fn cleanup_archived_journal(&self, old_journal_path: &Path) {
    // Generate archived path with timestamp
    let rotation_timestamp = self
      .cached_map
      .values()
      .map(|tv| tv.timestamp)
      .max()
      .unwrap_or(0);

    let archived_path = self.dir_path.join(format!(
      "{}.jrn.t{}.zz",
      self.journal_name, rotation_timestamp
    ));

    // Try to compress the old journal
    match compress_archived_journal(old_journal_path, &archived_path).await {
      Ok(()) => {
        // Compression succeeded, remove uncompressed version
        let _ = tokio::fs::remove_file(old_journal_path).await;
      },
      Err(_e) => {
        // Compression failed - keep the uncompressed version as a fallback
      },
    }
  }

  /// Create a new rotated journal with compacted state.
  ///
  /// Note: Rotation cannot fail due to insufficient buffer space. Since rotation creates a new
  /// journal with the same buffer size and compaction only removes redundant updates (old
  /// versions of keys), the compacted state is always ≤ the current journal size. If data fits
  /// during normal operation, it will always fit during rotation.
  async fn create_rotated_journal(
    &self,
    journal_path: &Path,
  ) -> anyhow::Result<MemMappedVersionedKVJournal> {
    // Create in-memory buffer for new journal
    let mut buffer = vec![0u8; self.buffer_size];

    // Use VersionedJournal to create rotated journal in memory
    let _rotated = VersionedJournal::<StateKeyValuePair>::create_rotated_journal(
      &mut buffer,
      &self.cached_map,
      self.high_water_mark_ratio,
    )?;

    // Write buffer to the new journal path
    tokio::fs::write(journal_path, &buffer).await?;

    // Open as memory-mapped journal
    MemMappedVersionedKVJournal::from_file(
      journal_path,
      self.buffer_size,
      self.high_water_mark_ratio,
    )
  }
}
