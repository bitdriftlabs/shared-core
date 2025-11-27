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

/// Common statistics for versioned KV store operations.
#[derive(Clone)]
struct CommonStats {
  capacity_exceeded_unrecoverable: bd_client_stats_store::Counter,
}

impl CommonStats {
  fn new(stats: &bd_client_stats_store::Scope) -> Self {
    Self {
      capacity_exceeded_unrecoverable: stats.scope("kv").counter("capacity_exceeded_unrecoverable"),
    }
  }
}

/// Configuration for persistent store with dynamic growth capabilities.
#[derive(Debug, Clone)]
pub struct PersistentStoreConfig {
  /// Starting buffer size - will be rounded to next power of 2 if needed.
  pub initial_buffer_size: usize,

  /// Maximum total capacity in bytes (e.g., 10MB). Always enforced to prevent unbounded growth.
  pub max_capacity_bytes: usize,

  /// High water mark ratio for triggering rotation (0.1-1.0).
  pub high_water_mark_ratio: f32,
}

impl Default for PersistentStoreConfig {
  fn default() -> Self {
    Self {
      initial_buffer_size: 8 * 1024,   // 8KB
      max_capacity_bytes: 1024 * 1024, // 1MB
      high_water_mark_ratio: 0.8,
    }
  }
}

impl PersistentStoreConfig {
  /// Normalize the configuration by applying defaults to invalid values.
  /// This is designed for dynamic config scenarios where validation errors can't be communicated.
  pub fn normalize(&mut self) {
    const DEFAULT_INITIAL_SIZE: usize = 8 * 1024; // 8KB
    const DEFAULT_MAX_CAPACITY: usize = 1024 * 1024; // 1MB
    const DEFAULT_RATIO: f32 = 0.7;

    const ABSOLUTE_MIN: usize = 4096; // 4KB
    const ABSOLUTE_MAX: usize = 1024 * 1024 * 1024; // 1GB

    // Clamp initial_buffer_size to valid range
    if self.initial_buffer_size < ABSOLUTE_MIN || self.initial_buffer_size > ABSOLUTE_MAX {
      self.initial_buffer_size = DEFAULT_INITIAL_SIZE;
    }

    // Round up to next power of 2 if needed
    if !self.initial_buffer_size.is_power_of_two() {
      self.initial_buffer_size = self.initial_buffer_size.next_power_of_two();
    }

    // Validate and fix max_capacity_bytes - always enforce a reasonable maximum
    if self.max_capacity_bytes < self.initial_buffer_size || self.max_capacity_bytes > ABSOLUTE_MAX
    {
      // If max is invalid, use a safe default (10MB)
      self.max_capacity_bytes = DEFAULT_MAX_CAPACITY;
    }

    // Clamp high_water_mark_ratio to valid range [0.1, 1.0]
    if !self.high_water_mark_ratio.is_finite()
      || !(0.1 ..= 1.0).contains(&self.high_water_mark_ratio)
    {
      self.high_water_mark_ratio = DEFAULT_RATIO;
    }
  }
}


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

/// Scoped maps that store key-value pairs separately per scope.
///
/// Instead of using a single map with `(Scope, String)` as the key (which requires `to_string()`
/// calls on every lookup), we use separate maps per scope and dispatch with a match statement.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ScopedMaps {
  pub feature_flags: AHashMap<String, TimestampedValue>,
  pub global_state: AHashMap<String, TimestampedValue>,
}

impl ScopedMaps {
  #[must_use]
  pub fn get(&self, scope: Scope, key: &str) -> Option<&TimestampedValue> {
    match scope {
      Scope::FeatureFlag => self.feature_flags.get(key),
      Scope::GlobalState => self.global_state.get(key),
    }
  }

  pub fn insert(&mut self, scope: Scope, key: String, value: TimestampedValue) {
    match scope {
      Scope::FeatureFlag => {
        self.feature_flags.insert(key, value);
      },
      Scope::GlobalState => {
        self.global_state.insert(key, value);
      },
    }
  }

  pub fn remove(&mut self, scope: Scope, key: &str) -> Option<TimestampedValue> {
    match scope {
      Scope::FeatureFlag => self.feature_flags.remove(key),
      Scope::GlobalState => self.global_state.remove(key),
    }
  }

  #[must_use]
  pub fn contains_key(&self, scope: Scope, key: &str) -> bool {
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
  high_water_mark_ratio: f32,
  current_generation: u64,
  cached_map: ScopedMaps,
  retention_registry: Arc<RetentionRegistry>,
  // Configuration for dynamic sizing
  initial_buffer_size: usize,
  max_capacity_bytes: usize,
  // Stats
  stats: CommonStats,
}

impl PersistentStore {
  async fn new<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    retention_registry: Arc<RetentionRegistry>,
    stats: CommonStats,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let buffer_size = config.initial_buffer_size;
    let high_water_mark_ratio = config.high_water_mark_ratio;
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
          ScopedMaps::default(),
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
        ScopedMaps::default(),
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
        initial_buffer_size: config.initial_buffer_size,
        max_capacity_bytes: config.max_capacity_bytes,
        stats,
      },
      data_loss,
    ))
  }

  fn open(
    journal_path: &Path,
    buffer_size: usize,
    high_water_mark_ratio: f32,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<OpenedJournal> {
    let mut initial_state = ScopedMaps::default();
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

  /// Get current timestamp in microseconds.
  #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
  fn current_timestamp(&self) -> u64 {
    self.journal.time_provider.now().unix_timestamp_nanos() as u64 / 1_000
  }

  async fn insert(
    &mut self,
    scope: Scope,
    key: &str,
    value: StateValue,
  ) -> Result<u64, UpdateError> {
    let timestamp = if value.value_type.is_none() {
      // Deletion
      let timestamp = self
        .try_insert_with_rotation(scope, key, &StateValue::default())
        .await?;
      self.cached_map.remove(scope, key);
      timestamp
    } else {
      // Insert/update
      let timestamp = self.try_insert_with_rotation(scope, key, &value).await?;
      self.cached_map.insert(
        scope,
        key.to_string(),
        TimestampedValue {
          value: value.clone(),
          timestamp,
        },
      );
      timestamp
    };

    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal().await?;
    }

    Ok(timestamp)
  }

  async fn extend_entries(
    &mut self,
    entries: Vec<(Scope, String, StateValue)>,
  ) -> Result<u64, UpdateError> {
    if entries.is_empty() {
      // Return current timestamp for empty batch (no-op)
      return Ok(self.current_timestamp());
    }

    // Try to insert all entries with rotation handling, preserving order
    // This consumes the entries vector
    let timestamp = self.try_extend_with_rotation(&entries).await?;

    // Update cached_map for all entries in order
    // We iterate again, but this avoids cloning during the write phase
    for (scope, key, value) in entries {
      if value.value_type.is_none() {
        // Deletion
        self.cached_map.remove(scope, &key);
      } else {
        // Insert/update
        self
          .cached_map
          .insert(scope, key, TimestampedValue { value, timestamp });
      }
    }

    // Check if rotation is needed after extend
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
      .try_insert_with_rotation(scope, key, &StateValue::default())
      .await?;
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
    self.rotate_journal_with_hint(0).await
  }

  /// Calculate the compacted size estimate by summing encoded sizes of all live entries.
  fn calculate_compacted_size(&self) -> usize {
    self
      .cached_map
      .iter()
      .map(|(_, key, tv)| {
        super::framing::Frame::compute_encoded_size(key.as_str(), tv.timestamp, &tv.value)
      })
      .sum()
  }

  /// Calculate what the new buffer size would be after rotation with the given hint.
  ///
  /// Returns the new buffer size based on:
  /// - Current compacted size
  /// - Additional space hint
  /// - High water mark ratio
  /// - Current buffer size (never shrink)
  /// - Max capacity bytes (cap at max)
  fn calculate_new_buffer_size(&self, min_additional_space: usize) -> usize {
    let compacted_size_estimate = self.calculate_compacted_size();
    let total_size_needed = compacted_size_estimate + min_additional_space;

    #[allow(
      clippy::cast_precision_loss,
      clippy::cast_possible_truncation,
      clippy::cast_sign_loss
    )]
    let target_size = (total_size_needed as f32 / self.high_water_mark_ratio).ceil() as usize;
    let suggested_size = target_size.next_power_of_two();

    suggested_size
      .max(self.buffer_size)
      .min(self.max_capacity_bytes)
  }

  /// Check if rotation would allow an entry of the given size to fit.
  ///
  /// Returns true if:
  /// - The entry fits within `max_capacity_bytes` AND
  /// - After compaction and potential buffer growth, there would be enough space
  fn would_rotation_help(&self, entry_size: usize) -> bool {
    // Entry must fit within absolute max capacity
    if entry_size > self.max_capacity_bytes {
      return false;
    }

    let compacted_size_estimate = self.calculate_compacted_size();
    let total_size_needed = compacted_size_estimate + entry_size;
    let new_buffer_size = self.calculate_new_buffer_size(entry_size);

    // Check if the compacted state plus new entry would fit in the new buffer. This effectively
    // checks if we're able to grow the buffer enough to accommodate the new entry.
    total_size_needed <= new_buffer_size
  }

  /// Try to insert an entry into the journal, rotating if needed on capacity exceeded.
  ///
  /// This helper encapsulates the pattern of:
  /// 1. Try to insert
  /// 2. On `CapacityExceeded`, check if rotation would help
  /// 3. If yes, rotate and retry
  /// 4. If no, propagate the error
  async fn try_insert_with_rotation(
    &mut self,
    scope: Scope,
    key: &str,
    value: &StateValue,
  ) -> Result<u64, UpdateError> {
    match self.journal.insert_entry_ref(scope, key, value) {
      Ok(timestamp) => Ok(timestamp),
      Err(UpdateError::CapacityExceeded) => {
        // Calculate size needed for this entry
        let entry_size = super::framing::Frame::compute_encoded_size(key, u64::MAX, value);

        // Check if rotation would help. If not, fail immediately.
        if !self.would_rotation_help(entry_size) {
          self.stats.capacity_exceeded_unrecoverable.inc();
          return Err(UpdateError::CapacityExceeded);
        }

        // Rotate with knowledge of incoming entry size
        self.rotate_journal_with_hint(entry_size).await?;

        // Retry insert after rotation
        self.journal.insert_entry_ref(scope, key, value)
      },
      Err(e) => Err(e),
    }
  }

  /// Try to extend the journal with entries, rotating if needed on capacity exceeded.
  ///
  /// This is similar to `try_insert_with_rotation` but handles multiple entries atomically
  /// while preserving their order.
  async fn try_extend_with_rotation(
    &mut self,
    entries: &[(Scope, String, StateValue)],
  ) -> Result<u64, UpdateError> {
    // Try the extend operation with references (no clones!)
    let entries_iter = entries
      .iter()
      .map(|(scope, key, value)| (*scope, key.as_str(), value));

    match self.journal.extend_entries_ref(entries_iter) {
      Ok(timestamp) => Ok(timestamp),
      Err(UpdateError::CapacityExceeded) => {
        // Calculate total size needed for all entries
        let total_size: usize = entries
          .iter()
          .map(|(_, key, value)| {
            super::framing::Frame::compute_encoded_size(key.as_str(), u64::MAX, value)
          })
          .sum();

        // Check if rotation would help. If not, fail immediately.
        if !self.would_rotation_help(total_size) {
          self.stats.capacity_exceeded_unrecoverable.inc();
          return Err(UpdateError::CapacityExceeded);
        }

        // Rotate with knowledge of incoming batch size
        self.rotate_journal_with_hint(total_size).await?;

        // Retry extend operation after rotation (still using references)
        let entries_retry_iter = entries
          .iter()
          .map(|(scope, key, value)| (*scope, key.as_str(), value));
        self.journal.extend_entries_ref(entries_retry_iter)
      },
      Err(e) => Err(e),
    }
  }

  /// Rotate the journal with a hint about the minimum additional space needed.
  ///
  /// This is used when an insert fails due to capacity exceeded and we need to ensure
  /// the new buffer is large enough for the incoming entry.
  async fn rotate_journal_with_hint(
    &mut self,
    min_additional_space: usize,
  ) -> anyhow::Result<Rotation> {
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

    // Calculate new buffer size with dynamic growth
    let compacted_size_estimate = self.calculate_compacted_size();
    let new_buffer_size = self.calculate_new_buffer_size(min_additional_space);

    log::debug!(
      "Rotation sizing: compacted={} bytes, hint={} bytes, new_buffer={} bytes (max={})",
      compacted_size_estimate,
      min_additional_space,
      new_buffer_size,
      self.max_capacity_bytes
    );

    MemMappedVersionedJournal::sync(&self.journal)?;
    let time_provider = self.journal.time_provider.clone();
    let new_journal = MemMappedVersionedJournal::new(
      &new_journal_path,
      new_buffer_size,
      self.high_water_mark_ratio,
      time_provider,
      self
        .cached_map
        .iter()
        .map(|(frame_type, key, tv)| (frame_type, key.clone(), tv.value.clone(), tv.timestamp)),
    )?;

    // Update buffer_size to reflect the new journal's size
    self.buffer_size = new_buffer_size;
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
  stats: CommonStats,
}

impl InMemoryStore {
  fn new(
    time_provider: Arc<dyn TimeProvider>,
    max_bytes: Option<usize>,
    stats: CommonStats,
  ) -> Self {
    Self {
      time_provider,
      cached_map: ScopedMaps::default(),
      max_bytes,
      current_size_bytes: 0,
      stats,
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

  /// Get current timestamp in microseconds.
  #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
  fn current_timestamp(&self) -> u64 {
    self.time_provider.now().unix_timestamp_nanos() as u64 / 1_000
  }

  fn insert(&mut self, scope: Scope, key: &str, value: &StateValue) -> Result<u64, UpdateError> {
    use std::collections::hash_map::Entry;

    let timestamp = self.current_timestamp();

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
              self.stats.capacity_exceeded_unrecoverable.inc();
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
              self.stats.capacity_exceeded_unrecoverable.inc();
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

  fn extend_entries(
    &mut self,
    entries: Vec<(Scope, String, StateValue)>,
  ) -> Result<u64, UpdateError> {
    if entries.is_empty() {
      // Return current timestamp for empty batch (no-op)
      return Ok(self.current_timestamp());
    }

    let timestamp = self.current_timestamp();

    // Calculate total size change for all entries
    let mut total_size_delta: isize = 0;

    for (scope, key, value) in &entries {
      if value.value_type.is_none() {
        // Deletion - will reclaim space
        if let Some(old_value) = self.cached_map.get(*scope, key) {
          let old_size = Self::estimate_entry_size(key, &old_value.value);
          #[allow(clippy::cast_possible_wrap)]
          {
            total_size_delta -= old_size as isize;
          }
        }
      } else {
        let new_entry_size = Self::estimate_entry_size(key, value);

        if let Some(old_value) = self.cached_map.get(*scope, key) {
          // Replacing existing entry
          let old_size = Self::estimate_entry_size(key, &old_value.value);
          #[allow(clippy::cast_possible_wrap)]
          {
            total_size_delta += new_entry_size as isize - old_size as isize;
          }
        } else {
          // New entry
          #[allow(clippy::cast_possible_wrap)]
          {
            total_size_delta += new_entry_size as isize;
          }
        }
      }
    }

    // Check capacity before applying any changes
    if let Some(max_bytes) = self.max_bytes {
      #[allow(clippy::cast_sign_loss)]
      let new_size = if total_size_delta >= 0 {
        self
          .current_size_bytes
          .saturating_add(total_size_delta as usize)
      } else {
        self
          .current_size_bytes
          .saturating_sub((-total_size_delta) as usize)
      };

      if new_size > max_bytes {
        self.stats.capacity_exceeded_unrecoverable.inc();
        return Err(UpdateError::CapacityExceeded);
      }
    }

    // Apply all changes atomically
    for (scope, key, value) in entries {
      if value.value_type.is_none() {
        // Deletion
        if let Some(old_value) = self.cached_map.remove(scope, &key) {
          let old_size = Self::estimate_entry_size(&key, &old_value.value);
          self.current_size_bytes = self.current_size_bytes.saturating_sub(old_size);
        }
      } else {
        let new_entry_size = Self::estimate_entry_size(&key, &value);

        if let Some(old_value) = self.cached_map.get(scope, &key) {
          // Update existing entry
          let old_size = Self::estimate_entry_size(&key, &old_value.value);
          self.current_size_bytes = self.current_size_bytes.saturating_sub(old_size);
        }

        self
          .cached_map
          .insert(scope, key, TimestampedValue { value, timestamp });
        self.current_size_bytes = self.current_size_bytes.saturating_add(new_entry_size);
      }
    }

    Ok(timestamp)
  }

  fn remove(&mut self, scope: Scope, key: &str) -> Option<u64> {
    if !self.cached_map.contains_key(scope, key) {
      return None;
    }

    let timestamp = self.current_timestamp();
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
  /// Create a new `VersionedKVStore` with the specified directory, name, and configuration.
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
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    retention_registry: Arc<RetentionRegistry>,
    stats: &bd_client_stats_store::Scope,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let common_stats = CommonStats::new(stats);

    let (store, data_loss) = PersistentStore::new(
      dir_path,
      name,
      config,
      time_provider,
      retention_registry,
      common_stats,
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
  /// * `stats` - Stats scope for emitting metrics
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
  pub fn new_in_memory(
    time_provider: Arc<dyn TimeProvider>,
    max_bytes: Option<usize>,
    stats: &bd_client_stats_store::Scope,
  ) -> Self {
    let common_stats = CommonStats::new(stats);

    Self {
      backend: StoreBackend::InMemory(InMemoryStore::new(time_provider, max_bytes, common_stats)),
    }
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

  /// Insert multiple key-value pairs with a shared timestamp.
  ///
  /// All entries are written with the same timestamp. If any entry fails to write to the journal,
  /// the operation is rolled back and an error is returned.
  ///
  /// For persistent stores, this operation handles rotation and retries automatically if needed.
  /// If empty, this is a no-op that returns the current timestamp.
  ///
  /// Note: Entries with `Value::Null` are treated as deletions.
  ///
  /// # Errors
  /// - Returns `UpdateError::CapacityExceeded` if the batch would exceed capacity limits
  /// - Returns `UpdateError::System` if the batch cannot be written (persistent mode)
  pub async fn extend(
    &mut self,
    entries: Vec<(Scope, String, StateValue)>,
  ) -> Result<u64, UpdateError> {
    match &mut self.backend {
      StoreBackend::Persistent(store) => store.extend_entries(entries).await,
      StoreBackend::InMemory(store) => store.extend_entries(entries),
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

  /// Clear all keys in a given scope with a shared timestamp.
  ///
  /// All entries in the scope are deleted with the same timestamp. Returns `None` if the scope
  /// was empty (no-op).
  ///
  /// This is implemented using `extend()` for efficiency.
  ///
  /// # Errors
  /// Returns an error if the deletions cannot be written to the journal (persistent mode only).
  pub async fn clear(&mut self, scope: Scope) -> Result<Option<u64>, UpdateError> {
    // Collect all keys in this scope
    let keys: Vec<String> = self
      .as_hashmap()
      .iter()
      .filter_map(|(s, key, _)| if s == scope { Some(key.clone()) } else { None })
      .collect();

    if keys.is_empty() {
      return Ok(None);
    }

    // Create deletion entries (null values)
    let entries: Vec<(Scope, String, StateValue)> = keys
      .into_iter()
      .map(|key| (scope, key, StateValue::default()))
      .collect();

    // Use extend for batch deletion with shared timestamp
    let timestamp = self.extend(entries).await?;
    Ok(Some(timestamp))
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

  /// Get the initial buffer size configured for this store.
  ///
  /// For persistent stores, returns the initial buffer size from the configuration.
  /// For in-memory stores, returns 0.
  #[must_use]
  pub fn initial_buffer_size(&self) -> usize {
    match &self.backend {
      StoreBackend::Persistent(store) => store.initial_buffer_size,
      StoreBackend::InMemory(_) => 0,
    }
  }

  /// Get the current buffer size of the journal.
  ///
  /// This may differ from `initial_buffer_size` if the journal has grown dynamically.
  /// For in-memory stores, returns 0.
  #[must_use]
  pub fn current_buffer_size(&self) -> usize {
    match &self.backend {
      StoreBackend::Persistent(store) => store.buffer_size,
      StoreBackend::InMemory(_) => 0,
    }
  }

  /// Get the maximum capacity in bytes configured for this store.
  ///
  /// For in-memory stores, returns 0.
  #[must_use]
  pub fn max_capacity_bytes(&self) -> usize {
    match &self.backend {
      StoreBackend::Persistent(store) => store.max_capacity_bytes,
      StoreBackend::InMemory(_) => 0,
    }
  }
}
