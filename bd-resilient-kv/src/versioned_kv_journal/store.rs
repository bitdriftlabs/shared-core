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
use crate::versioned_kv_journal::retention::RetentionRegistry;
use crate::{Scope, UpdateError};
use ahash::AHashMap;
use bd_error_reporter::reporter::handle_unexpected;
use bd_proto::protos::state::payload::StateValue;
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

/// Information about a journal rotation. This is used by test code to verify rotation results.
pub struct Rotation {
  pub new_journal_path: PathBuf,
  pub old_journal_path: PathBuf,
  pub snapshot_path: PathBuf,
}

/// Result of opening a journal file, containing the journal, initial state, and data loss info.
struct OpenedJournal {
  journal: MemMappedVersionedJournal<StateValue>,
  initial_state: ScopedMaps,
  data_loss: PartialDataLoss,
}

/// Scoped maps that avoid string allocations during lookups.
///
/// Instead of using a single map with `(Scope, String)` as the key (which requires `to_string()`
/// calls on every lookup), we use separate maps per scope and dispatch with a match statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedMaps {
  feature_flags: AHashMap<String, TimestampedValue>,
  global_state: AHashMap<String, TimestampedValue>,
}

impl ScopedMaps {
  fn new() -> Self {
    Self {
      feature_flags: AHashMap::new(),
      global_state: AHashMap::new(),
    }
  }

  fn get(&self, scope: Scope, key: &str) -> Option<&TimestampedValue> {
    match scope {
      Scope::FeatureFlag => self.feature_flags.get(key),
      Scope::GlobalState => self.global_state.get(key),
    }
  }

  fn insert(&mut self, scope: Scope, key: String, value: TimestampedValue) {
    match scope {
      Scope::FeatureFlag => {
        self.feature_flags.insert(key, value);
      },
      Scope::GlobalState => {
        self.global_state.insert(key, value);
      },
    }
  }

  fn remove(&mut self, scope: Scope, key: &str) -> Option<TimestampedValue> {
    match scope {
      Scope::FeatureFlag => self.feature_flags.remove(key),
      Scope::GlobalState => self.global_state.remove(key),
    }
  }

  fn contains_key(&self, scope: Scope, key: &str) -> bool {
    match scope {
      Scope::FeatureFlag => self.feature_flags.contains_key(key),
      Scope::GlobalState => self.global_state.contains_key(key),
    }
  }

  #[must_use]
  pub fn len(&self) -> usize {
    self.feature_flags.len() + self.global_state.len()
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.feature_flags.is_empty() && self.global_state.is_empty()
  }

  pub fn iter(&self) -> impl Iterator<Item = (Scope, &String, &TimestampedValue)> {
    self
      .feature_flags
      .iter()
      .map(|(k, v)| (Scope::FeatureFlag, k, v))
      .chain(
        self
          .global_state
          .iter()
          .map(|(k, v)| (Scope::GlobalState, k, v)),
      )
  }

  fn values(&self) -> impl Iterator<Item = &TimestampedValue> {
    self
      .feature_flags
      .values()
      .chain(self.global_state.values())
  }

  /// Get a mutable entry for the given scope and key, allowing efficient insert/update operations.
  fn entry(
    &mut self,
    scope: Scope,
    key: String,
  ) -> std::collections::hash_map::Entry<'_, String, TimestampedValue> {
    match scope {
      Scope::FeatureFlag => self.feature_flags.entry(key),
      Scope::GlobalState => self.global_state.entry(key),
    }
  }
}


/// Persistent storage implementation using a memory-mapped journal.
struct PersistentStore {
  journal: MemMappedVersionedJournal<StateValue>,
  dir_path: PathBuf,
  journal_name: String,
  buffer_size: usize,
  high_water_mark_ratio: Option<f32>,
  current_generation: u64,
  cached_map: ScopedMaps,
  retention_registry: Arc<RetentionRegistry>,
}

impl PersistentStore {
  async fn new<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
    retention_registry: Arc<RetentionRegistry>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let dir = dir_path.as_ref();
    let (journal_path, generation) = file_manager::find_active_journal(dir, name).await;

    log::debug!(
      "Opening VersionedKVStore journal at {} (generation {generation})",
      journal_path.display()
    );

    let (journal, initial_state, data_loss) = if journal_path.exists() {
      Self::open(
        &journal_path,
        buffer_size,
        high_water_mark_ratio,
        time_provider.clone(),
      )
      .map(|opened| {
        (
          opened.journal,
          opened.initial_state,
          opened.data_loss.into(),
        )
      })
      .or_else(|_| {
        Ok::<_, anyhow::Error>((
          MemMappedVersionedJournal::new(
            &journal_path,
            buffer_size,
            high_water_mark_ratio,
            time_provider,
            std::iter::empty(),
          )?,
          ScopedMaps::new(),
          DataLoss::Total,
        ))
      })?
    } else {
      (
        MemMappedVersionedJournal::new(
          &journal_path,
          buffer_size,
          high_water_mark_ratio,
          time_provider,
          std::iter::empty(),
        )?,
        ScopedMaps::new(),
        DataLoss::None,
      )
    };

    Ok((
      Self {
        journal,
        dir_path: dir.to_path_buf(),
        journal_name: name.to_string(),
        buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
        cached_map: initial_state,
        retention_registry,
      },
      data_loss,
    ))
  }

  async fn open_existing<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
    retention_registry: Arc<RetentionRegistry>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let dir = dir_path.as_ref();
    let (journal_path, generation) = file_manager::find_active_journal(dir, name).await;

    let opened = Self::open(
      &journal_path,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
    )?;

    Ok((
      Self {
        journal: opened.journal,
        dir_path: dir.to_path_buf(),
        journal_name: name.to_string(),
        buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
        cached_map: opened.initial_state,
        retention_registry,
      },
      if matches!(opened.data_loss, PartialDataLoss::Yes) {
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
  ) -> anyhow::Result<OpenedJournal> {
    let mut initial_state = ScopedMaps::new();
    let (journal, data_loss) = MemMappedVersionedJournal::<StateValue>::from_file(
      journal_path,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
      |scope, key, value, timestamp| {
        if value.value_type.is_some() {
          initial_state.insert(
            scope,
            key.to_string(),
            TimestampedValue {
              value: value.clone(),
              timestamp,
            },
          );
        } else {
          initial_state.remove(scope, key);
        }
      },
    )?;

    Ok(OpenedJournal {
      journal,
      initial_state,
      data_loss,
    })
  }

  async fn insert(
    &mut self,
    scope: Scope,
    key: &str,
    value: StateValue,
  ) -> Result<u64, UpdateError> {
    let timestamp = if value.value_type.is_none() {
      let timestamp = self
        .journal
        .insert_entry(scope, key, StateValue::default())?;
      self.cached_map.remove(scope, key);
      timestamp
    } else {
      let timestamp = self.journal.insert_entry(scope, key, value.clone())?;
      self.cached_map.insert(
        scope,
        key.to_string(),
        TimestampedValue { value, timestamp },
      );
      timestamp
    };

    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal().await?;
    }

    Ok(timestamp)
  }

  async fn remove(&mut self, scope: Scope, key: &str) -> Result<Option<u64>, UpdateError> {
    if !self.cached_map.contains_key(scope, key) {
      return Ok(None);
    }

    let timestamp = self
      .journal
      .insert_entry(scope, key, StateValue::default())?;
    self.cached_map.remove(scope, key);

    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal().await?;
    }

    Ok(Some(timestamp))
  }

  fn sync(&self) -> anyhow::Result<()> {
    MemMappedVersionedJournal::sync(&self.journal)
  }

  fn journal_path(&self) -> PathBuf {
    self.dir_path.join(format!(
      "{}.jrn.{}",
      self.journal_name, self.current_generation
    ))
  }

  async fn rotate_journal(&mut self) -> anyhow::Result<Rotation> {
    let next_generation = self.current_generation + 1;
    let old_generation = self.current_generation;
    self.current_generation = next_generation;

    log::debug!("Rotating journal to generation {next_generation}");

    // TODO(snowp): This part needs fuzzing and more safeguards around I/O errors.
    // TODO(snowp): Consider doing this out of band to split error handling for the insert and
    // rotation.

    // Create new journal with compacted state. This doens't touch the file containing the old
    // journal.
    let new_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{next_generation}", self.journal_name));

    // Note: Rotation cannot fail due to insufficient buffer space. Since rotation creates a new
    // journal with the same buffer size and compaction only removes redundant updates (old
    // versions of keys), the compacted state is always ≤ the current journal size. If data fits
    // during normal operation, it will always fit during rotation.

    MemMappedVersionedJournal::sync(&self.journal)?;
    let time_provider = self.journal.time_provider.clone();
    let new_journal = MemMappedVersionedJournal::new(
      &new_journal_path,
      self.buffer_size,
      self.high_water_mark_ratio,
      time_provider,
      self
        .cached_map
        .iter()
        .map(|(frame_type, key, tv)| (frame_type, key.clone(), tv.value.clone(), tv.timestamp)),
    )?;
    self.journal = new_journal;

    // Best-effort cleanup: compress and archive the old journal
    let old_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{old_generation}", self.journal_name));

    let rotation_timestamp = self
      .cached_map
      .values()
      .map(|tv| tv.timestamp)
      .max()
      .unwrap_or(0);

    // Check if we need to create a snapshot based on retention requirements
    let min_retention = self.retention_registry.min_retention_timestamp().await;
    // If min_retention is None (no handles), don't create snapshot
    // If min_retention is Some(0), retain everything (at least one handle wants all data)
    // If min_retention > rotation_timestamp, no one needs this snapshot
    let should_create_snapshot = match min_retention {
      None => false,
      Some(0) => true,
      Some(ts) => ts <= rotation_timestamp,
    };

    // Store snapshots in a separate subdirectory
    let snapshots_dir = self.dir_path.join("snapshots");
    let archived_path = snapshots_dir.join(format!(
      "{}.jrn.g{}.t{}.zz",
      self.journal_name, old_generation, rotation_timestamp
    ));

    if should_create_snapshot {
      log::debug!(
        "Archiving journal {} to {}",
        old_journal_path.display(),
        archived_path.display()
      );

      // Create snapshots directory if it doesn't exist
      if let Err(e) = tokio::fs::create_dir_all(&snapshots_dir).await {
        log::debug!(
          "Failed to create snapshots directory {}: {}",
          snapshots_dir.display(),
          e
        );
      } else {
        // Try to compress the old journal for longer-term storage.
        if let Err(e) = compress_archived_journal(&old_journal_path, &archived_path).await {
          log::debug!(
            "Failed to compress archived journal {}: {}",
            old_journal_path.display(),
            e
          );
        }

        // After creating a snapshot, trigger cleanup of old snapshots
        handle_unexpected(
          super::cleanup::cleanup_old_snapshots(&snapshots_dir, &self.retention_registry).await,
          "old snapshot cleanup",
        );
      }
    } else {
      log::debug!(
        "Skipping snapshot creation for {} (no retention required)",
        old_journal_path.display()
      );
    }

    // Remove the uncompressed journal regardless of compression success or skip. If we succeeded we
    // no longer need it, while if we failed we consider the snapshot lost.
    let _ignored = tokio::fs::remove_file(&old_journal_path)
      .await
      .inspect_err(|e| {
        log::debug!(
          "Failed to remove old journal {}: {}",
          old_journal_path.display(),
          e
        );
      });

    Ok(Rotation {
      new_journal_path,
      old_journal_path,
      snapshot_path: archived_path,
    })
  }
}

/// In-memory storage implementation with no persistence.
struct InMemoryStore {
  time_provider: Arc<dyn TimeProvider>,
  cached_map: ScopedMaps,
  max_bytes: Option<usize>,
  current_size_bytes: usize,
}

impl InMemoryStore {
  fn new(time_provider: Arc<dyn TimeProvider>, max_bytes: Option<usize>) -> Self {
    Self {
      time_provider,
      cached_map: ScopedMaps::new(),
      max_bytes,
      current_size_bytes: 0,
    }
  }

  /// Estimate the size in bytes of a key-value entry.
  ///
  /// This provides a conservative estimate of memory usage including:
  /// - Key string storage
  /// - Value protobuf size
  /// - Timestamp storage
  /// - `HashMap` overhead
  fn estimate_entry_size(key: &str, value: &StateValue) -> usize {
    const TIMESTAMP_SIZE: usize = 8; // u64
    const HASHMAP_OVERHEAD: usize = 24; // Approximate per-entry overhead in AHashMap

    let key_size = key.len();
    let value_size: usize = protobuf::MessageDyn::compute_size_dyn(value)
      .try_into()
      .unwrap_or(0);

    key_size + value_size + TIMESTAMP_SIZE + HASHMAP_OVERHEAD
  }

  fn insert(&mut self, scope: Scope, key: &str, value: &StateValue) -> Result<u64, UpdateError> {
    use std::collections::hash_map::Entry;

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let timestamp = self.time_provider.now().unix_timestamp_nanos() as u64 / 1_000;

    if value.value_type.is_none() {
      // Deletion - reclaim space
      if let Some(old_value) = self.cached_map.remove(scope, key) {
        let old_size = Self::estimate_entry_size(key, &old_value.value);
        self.current_size_bytes = self.current_size_bytes.saturating_sub(old_size);
      }
    } else {
      let new_entry_size = Self::estimate_entry_size(key, value);

      // Use entry API to avoid multiple lookups
      match self.cached_map.entry(scope, key.to_string()) {
        Entry::Occupied(mut entry) => {
          // Replacing existing entry - calculate size delta
          let old_size = Self::estimate_entry_size(key, &entry.get().value);
          let size_delta = new_entry_size.saturating_sub(old_size);

          // Check capacity before replacing
          if let Some(max_bytes) = self.max_bytes {
            let new_size = self.current_size_bytes.saturating_add(size_delta);
            if new_size > max_bytes {
              return Err(UpdateError::CapacityExceeded);
            }
          }

          // Replace the value
          entry.insert(TimestampedValue {
            value: value.clone(),
            timestamp,
          });
          self.current_size_bytes = self.current_size_bytes.saturating_add(size_delta);
        },
        Entry::Vacant(entry) => {
          // New entry - check if we have capacity
          if let Some(max_bytes) = self.max_bytes {
            let new_size = self.current_size_bytes.saturating_add(new_entry_size);
            if new_size > max_bytes {
              return Err(UpdateError::CapacityExceeded);
            }
          }

          // Insert the new value
          entry.insert(TimestampedValue {
            value: value.clone(),
            timestamp,
          });
          self.current_size_bytes = self.current_size_bytes.saturating_add(new_entry_size);
        },
      }
    }

    Ok(timestamp)
  }

  fn remove(&mut self, scope: Scope, key: &str) -> Option<u64> {
    if !self.cached_map.contains_key(scope, key) {
      return None;
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let timestamp = self.time_provider.now().unix_timestamp_nanos() as u64 / 1_000;
    self.cached_map.remove(scope, key);

    Some(timestamp)
  }
}

/// Storage backend enum that dispatches to either persistent or in-memory implementation.
enum StoreBackend {
  Persistent(PersistentStore),
  InMemory(InMemoryStore),
}

/// A key-value store with timestamp tracking and optional persistence.
///
/// `VersionedKVStore` provides HashMap-like semantics with optional persistence via a timestamped
/// journal.
///
/// # Storage Modes
///
/// The store can operate in two modes:
/// - **Persistent**: Data is written to a memory-mapped journal file and survives process restarts.
/// - **In-Memory**: Data is kept only in memory and lost when the store is dropped.
///
/// # Rotation Strategy (Persistent mode only)
///
/// When the journal reaches its high water mark, the store automatically rotates to a new journal.
/// The rotation process creates a snapshot of the current state while preserving timestamp
/// semantics for accurate point-in-time recovery.
///
/// For detailed information about timestamp semantics, recovery bucketing, and invariants,
/// see the `VERSIONED_FORMAT.md` documentation.
pub struct VersionedKVStore {
  backend: StoreBackend,
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
    retention_registry: Arc<RetentionRegistry>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let (store, data_loss) = PersistentStore::new(
      dir_path,
      name,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
      retention_registry,
    )
    .await?;

    Ok((
      Self {
        backend: StoreBackend::Persistent(store),
      },
      data_loss,
    ))
  }

  /// Create a new in-memory `VersionedKVStore` with no persistence.
  ///
  /// The in-memory store keeps all data in RAM and does not persist to disk.
  /// Data is lost when the store is dropped.
  ///
  /// # Arguments
  ///
  /// * `time_provider` - Provides timestamps for operations
  /// * `max_bytes` - Optional maximum memory usage in bytes. If None, no limit is enforced.
  ///
  /// # Size Limit
  ///
  /// If `max_bytes` is specified, insert operations will fail with an error when the limit
  /// is exceeded. The size calculation includes:
  /// - Key strings
  /// - Value protobuf data
  /// - Timestamps
  /// - `HashMap` overhead
  ///
  /// The size limit is approximate and uses conservative estimates.
  #[must_use]
  pub fn new_in_memory(time_provider: Arc<dyn TimeProvider>, max_bytes: Option<usize>) -> Self {
    Self {
      backend: StoreBackend::InMemory(InMemoryStore::new(time_provider, max_bytes)),
    }
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
    retention_registry: Arc<RetentionRegistry>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let (store, data_loss) = PersistentStore::open_existing(
      dir_path,
      name,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
      retention_registry,
    )
    .await?;

    Ok((
      Self {
        backend: StoreBackend::Persistent(store),
      },
      data_loss,
    ))
  }

  /// Get a value by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get(&self, scope: Scope, key: &str) -> Option<&StateValue> {
    let cached_map = match &self.backend {
      StoreBackend::Persistent(store) => &store.cached_map,
      StoreBackend::InMemory(store) => &store.cached_map,
    };
    cached_map.get(scope, key).map(|tv| &tv.value)
  }

  /// Get a value with its timestamp by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get_with_timestamp(&self, scope: Scope, key: &str) -> Option<&TimestampedValue> {
    let cached_map = match &self.backend {
      StoreBackend::Persistent(store) => &store.cached_map,
      StoreBackend::InMemory(store) => &store.cached_map,
    };
    cached_map.get(scope, key)
  }

  /// Insert a value for a key, returning the timestamp assigned to this write.
  ///
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// - Returns `UpdateError::CapacityExceeded` if the in-memory store would exceed its size limit
  /// - Returns `UpdateError::System` if the value cannot be written to the journal (persistent
  ///   mode)
  pub async fn insert(
    &mut self,
    scope: Scope,
    key: String,
    value: StateValue,
  ) -> Result<u64, UpdateError> {
    match &mut self.backend {
      StoreBackend::Persistent(store) => store.insert(scope, &key, value).await,
      StoreBackend::InMemory(store) => store.insert(scope, &key, &value),
    }
  }

  /// Remove a key and return the timestamp assigned to this deletion.
  ///
  /// Returns `None` if the key didn't exist, otherwise returns the timestamp.
  ///
  /// # Errors
  /// Returns an error if the deletion cannot be written to the journal (persistent mode only).
  pub async fn remove(&mut self, scope: Scope, key: &str) -> Result<Option<u64>, UpdateError> {
    match &mut self.backend {
      StoreBackend::Persistent(store) => store.remove(scope, key).await,
      StoreBackend::InMemory(store) => Ok(store.remove(scope, key)),
    }
  }

  /// Check if the store contains a key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn contains_key(&self, scope: Scope, key: &str) -> bool {
    let cached_map = match &self.backend {
      StoreBackend::Persistent(store) => &store.cached_map,
      StoreBackend::InMemory(store) => &store.cached_map,
    };
    cached_map.contains_key(scope, key)
  }

  /// Get the number of key-value pairs in the store.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn len(&self) -> usize {
    let cached_map = match &self.backend {
      StoreBackend::Persistent(store) => &store.cached_map,
      StoreBackend::InMemory(store) => &store.cached_map,
    };
    cached_map.len()
  }

  /// Check if the store is empty.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Get a reference to the current state.
  ///
  /// This operation is O(1) as it returns a reference to the in-memory cache.
  #[must_use]
  pub fn as_hashmap(&self) -> &ScopedMaps {
    match &self.backend {
      StoreBackend::Persistent(store) => &store.cached_map,
      StoreBackend::InMemory(store) => &store.cached_map,
    }
  }

  /// Get a reference to the current state.
  ///
  /// This operation is O(1) as it returns a reference to the in-memory cache.
  #[must_use]
  pub fn state(&self) -> &ScopedMaps {
    match &self.backend {
      StoreBackend::Persistent(store) => &store.cached_map,
      StoreBackend::InMemory(store) => &store.cached_map,
    }
  }

  /// Synchronize changes to disk.
  ///
  /// This is a blocking operation that performs synchronous I/O. In async contexts,
  /// consider wrapping this call with `tokio::task::spawn_blocking`.
  ///
  /// For in-memory stores, this is a no-op.
  pub fn sync(&self) -> anyhow::Result<()> {
    match &self.backend {
      StoreBackend::Persistent(store) => store.sync(),
      StoreBackend::InMemory(_) => Ok(()),
    }
  }

  /// Returns the path to the current primary journal file.
  ///
  /// Returns `None` for in-memory stores.
  #[must_use]
  pub fn journal_path(&self) -> Option<PathBuf> {
    match &self.backend {
      StoreBackend::Persistent(store) => Some(store.journal_path()),
      StoreBackend::InMemory(_) => None,
    }
  }

  /// Manually trigger journal rotation, returning the path to the new journal file.
  ///
  /// This will create a new journal with the current state compacted and archive the old journal.
  /// The archived journal will be compressed using zlib to reduce storage size.
  /// Rotation typically happens automatically when the high water mark is reached, but this
  /// method allows manual control when needed.
  ///
  /// # Errors
  /// Returns an error if called on an in-memory store or if rotation fails.
  pub async fn rotate_journal(&mut self) -> anyhow::Result<Rotation> {
    match &mut self.backend {
      StoreBackend::Persistent(store) => store.rotate_journal().await,
      StoreBackend::InMemory(_) => anyhow::bail!("Cannot rotate journal on in-memory store"),
    }
  }
}
