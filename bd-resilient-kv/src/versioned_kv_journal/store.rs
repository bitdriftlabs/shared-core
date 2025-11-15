// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::versioned_kv_journal::TimestampedValue;
use crate::versioned_kv_journal::file_manager::{self, compress_archived_journal};
use crate::versioned_kv_journal::journal::PartialDataLoss;
use crate::versioned_kv_journal::memmapped_journal::MemMappedVersionedJournal;
use ahash::AHashMap;
use bd_proto::protos::state::payload::{StateKeyValuePair, StateValue};
use bd_time::TimeProvider;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub enum DataLoss {
  Total,
  Partial,
  None,
}

impl From<PartialDataLoss> for DataLoss {
  fn from(value: PartialDataLoss) -> Self {
    match value {
      PartialDataLoss::Yes => Self::Partial,
      PartialDataLoss::None => Self::None,
    }
  }
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
  journal: MemMappedVersionedJournal<StateKeyValuePair>,
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
  /// If the specified size is larger than an existing file, it will be resized while preserving
  /// data. If the specified size is smaller and the existing data doesn't fit, a fresh journal
  /// will be created.
  ///
  /// # Errors
  /// Returns an error if we failed to create or open the journal file.
  pub async fn new<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let dir = dir_path.as_ref();

    let (journal_path, generation) = file_manager::find_active_journal(dir, name).await;

    // TODO(snowp): It would be ideal to be able to start with a small buffer and grow is as needed
    // depending on the particular device need. We can embed size information in the journal header
    // or in the filename itself to facilitate this.

    log::debug!(
      "Opening VersionedKVStore journal at {} (generation {generation})",
      journal_path.display()
    );

    let (journal, initial_state, data_loss) = if journal_path.exists() {
      // Try to open existing journal
      Self::open(
        &journal_path,
        buffer_size,
        high_water_mark_ratio,
        time_provider.clone(),
      )
      .map(|(j, initial_state, data_loss)| (j, initial_state, data_loss.into()))
      .or_else(|_| {
        // Data is corrupt or unreadable, create fresh journal
        Ok::<_, anyhow::Error>((
          MemMappedVersionedJournal::new(
            &journal_path,
            buffer_size,
            high_water_mark_ratio,
            time_provider,
            std::iter::empty(),
          )?,
          AHashMap::default(),
          DataLoss::Total,
        ))
      })?
    } else {
      // Create new journal
      (
        MemMappedVersionedJournal::new(
          &journal_path,
          buffer_size,
          high_water_mark_ratio,
          time_provider,
          std::iter::empty(),
        )?,
        AHashMap::default(),
        DataLoss::None,
      )
    };

    Ok((
      Self {
        journal,
        cached_map: initial_state,
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
  pub async fn open_existing<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let dir = dir_path.as_ref();

    let (journal_path, generation) = file_manager::find_active_journal(dir, name).await;

    let (journal, initial_state, data_loss) = Self::open(
      &journal_path,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
    )?;

    Ok((
      Self {
        journal,
        cached_map: initial_state,
        dir_path: dir.to_path_buf(),
        journal_name: name.to_string(),
        buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
      },
      if matches!(data_loss, PartialDataLoss::Yes) {
        DataLoss::Partial
      } else {
        DataLoss::None
      },
    ))
  }

  fn open(
    journal_path: &Path,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(
    MemMappedVersionedJournal<StateKeyValuePair>,
    AHashMap<String, TimestampedValue>,
    PartialDataLoss,
  )> {
    let mut initial_state = AHashMap::default();
    let (journal, data_loss) = MemMappedVersionedJournal::<StateKeyValuePair>::from_file(
      journal_path,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
      |entry, timestamp| {
        if let Some(value) = entry.value.as_ref() {
          initial_state.insert(
            entry.key.clone(),
            TimestampedValue {
              value: value.clone(),
              timestamp,
            },
          );
        } else {
          initial_state.remove(&entry.key);
        }
      },
    )?;

    Ok((journal, initial_state, data_loss))
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
      // TODO(snowp): Consider doing this out of band to split error handling for the insert and
      // rotation.
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
    MemMappedVersionedJournal::sync(&self.journal)
  }
}


/// Information about a journal rotation. This is used by test code to verify rotation results.
pub struct Rotation {
  pub new_journal_path: PathBuf,
  pub old_journal_path: PathBuf,
  pub snapshot_path: PathBuf,
}

impl VersionedKVStore {
  /// Manually trigger journal rotation, returning the path to the new journal file.
  ///
  /// This will create a new journal with the current state compacted and archive the old journal.
  /// The archived journal will be compressed using zlib to reduce storage size.
  /// Rotation typically happens automatically when the high water mark is reached, but this
  /// method allows manual control when needed.
  pub async fn rotate_journal(&mut self) -> anyhow::Result<Rotation> {
    // Increment generation counter for new journal
    let next_generation = self.current_generation + 1;
    let new_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{next_generation}", self.journal_name));

    // TODO(snowp): This part needs fuzzing and more safeguards.
    // TODO(snowp): Consider doing this out of band to split error handling for the insert and
    // rotation.

    // Create new journal with compacted state
    let new_journal = self.create_rotated_journal(&new_journal_path)?;

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
    let snapshot_path = self.archive_journal(&old_journal_path).await;

    Ok(Rotation {
      new_journal_path,
      old_journal_path,
      snapshot_path,
    })
  }

  /// Archives the old journal by compressing it and removing the original.
  ///
  /// This is a best-effort operation; failures to compress or delete the old journal
  /// are logged but do not cause the rotation to fail.
  async fn archive_journal(&self, old_journal_path: &Path) -> PathBuf {
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

    log::debug!(
      "Archiving journal {} to {}",
      old_journal_path.display(),
      archived_path.display()
    );

    // Try to compress the old journal for longer-term storage.
    if let Err(e) = compress_archived_journal(old_journal_path, &archived_path).await {
      log::warn!(
        "Failed to compress archived journal {}: {}",
        old_journal_path.display(),
        e
      );
    }

    // Remove the uncompressed regardless of compression success. If we succeeded we no longer need
    // it, while if we failed we consider the snapshot lost.
    let _ignored = tokio::fs::remove_file(old_journal_path)
      .await
      .inspect_err(|e| {
        log::warn!(
          "Failed to remove old journal {}: {}",
          old_journal_path.display(),
          e
        );
      });

    archived_path
  }

  /// Create a new rotated journal with compacted state.
  ///
  /// Note: Rotation cannot fail due to insufficient buffer space. Since rotation creates a new
  /// journal with the same buffer size and compaction only removes redundant updates (old
  /// versions of keys), the compacted state is always ≤ the current journal size. If data fits
  /// during normal operation, it will always fit during rotation.
  fn create_rotated_journal(
    &self,
    journal_path: &Path,
  ) -> anyhow::Result<MemMappedVersionedJournal<StateKeyValuePair>> {
    let rotated = MemMappedVersionedJournal::new(
      journal_path,
      self.buffer_size,
      self.high_water_mark_ratio,
      self.journal.time_provider.clone(),
      self.cached_map.iter().map(|kv| {
        (
          StateKeyValuePair {
            key: kv.0.clone(),
            value: Some(kv.1.value.clone()).into(),
            ..Default::default()
          },
          kv.1.timestamp,
        )
      }),
    )?;

    Ok(rotated)
  }
}
