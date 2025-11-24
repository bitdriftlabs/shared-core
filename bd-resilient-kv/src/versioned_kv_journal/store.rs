// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Scope;
use crate::versioned_kv_journal::TimestampedValue;
use crate::versioned_kv_journal::file_manager::{self, compress_archived_journal};
use crate::versioned_kv_journal::journal::PartialDataLoss;
use crate::versioned_kv_journal::memmapped_journal::MemMappedVersionedJournal;
use crate::versioned_kv_journal::retention::RetentionRegistry;
use ahash::AHashMap;
use bd_error_reporter::reporter::handle_unexpected;
use bd_proto::protos::state::payload::StateValue;
use bd_time::TimeProvider;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configuration for persistent store with dynamic growth capabilities.
#[derive(Debug, Clone)]
pub struct PersistentStoreConfig {
  /// Starting buffer size - will be rounded to next power of 2 if needed.
  pub initial_buffer_size: usize,

  /// Maximum total capacity in bytes (e.g., 10MB).
  pub max_capacity_bytes: Option<usize>,

  /// High water mark ratio for triggering rotation (0.0-1.0).
  pub high_water_mark_ratio: Option<f32>,
}

impl Default for PersistentStoreConfig {
  fn default() -> Self {
    Self {
      initial_buffer_size: 8 * 1024,             // 8KB
      max_capacity_bytes: Some(1 * 1024 * 1024), // 1MB
      high_water_mark_ratio: Some(0.8),
    }
  }
}

impl PersistentStoreConfig {
  /// Normalize the configuration by applying defaults to invalid values.
  /// This is designed for dynamic config scenarios where validation errors can't be communicated.
  pub fn normalize(&mut self) {
    const DEFAULT_INITIAL_SIZE: usize = 8 * 1024; // 8KB
    const DEFAULT_MAX_CAPACITY: usize = 1 * 1024 * 1024; // 1MB
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
    if let Some(max) = self.max_capacity_bytes
      && (max < self.initial_buffer_size || max > ABSOLUTE_MAX)
    {
      // If max is invalid, use a safe default (10MB)
      self.max_capacity_bytes = Some(DEFAULT_MAX_CAPACITY);
    } else if self.max_capacity_bytes.is_none() {
      // If not specified, use a safe default (10MB)
      self.max_capacity_bytes = Some(DEFAULT_MAX_CAPACITY);
    }

    // Clamp high_water_mark_ratio to valid range [0.1, 1.0]
    if let Some(ratio) = self.high_water_mark_ratio
      && (!ratio.is_finite() || !(0.1 ..= 1.0).contains(&ratio))
    {
      self.high_water_mark_ratio = Some(DEFAULT_RATIO);
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
  journal: MemMappedVersionedJournal<StateValue>,
  cached_map: AHashMap<(Scope, String), TimestampedValue>,
  dir_path: PathBuf,
  journal_name: String,

  // Dynamic growth configuration
  initial_buffer_size: usize,
  max_capacity_bytes: Option<usize>,
  current_buffer_size: usize,
  high_water_mark_ratio: Option<f32>,

  current_generation: u64,
  retention_registry: Arc<RetentionRegistry>,
}

impl VersionedKVStore {
  /// Determine the appropriate buffer size for an existing journal file.
  ///
  /// Uses the file size as the current buffer size, with validation and reconciliation
  /// against the provided configuration.
  async fn determine_buffer_size(
    journal_path: &Path,
    config: &PersistentStoreConfig,
  ) -> anyhow::Result<usize> {
    const MIN_BUFFER_SIZE: usize = 21; // HEADER_SIZE (17) + min frame (4)
    const MAX_REASONABLE_SIZE: usize = 1024 * 1024 * 1024; // 1GB

    if let Ok(metadata) = tokio::fs::metadata(journal_path).await {
      #[allow(clippy::cast_possible_truncation)]
      let file_size = metadata.len() as usize;

      // Sanity check: minimum size
      if file_size < MIN_BUFFER_SIZE {
        log::debug!(
          "Journal file {} too small ({} bytes), using initial_buffer_size",
          journal_path.display(),
          file_size
        );
        return Ok(config.initial_buffer_size);
      }

      // Sanity check: maximum reasonable size
      if file_size > MAX_REASONABLE_SIZE {
        log::debug!(
          "Journal file {} suspiciously large ({} bytes), using initial_buffer_size",
          journal_path.display(),
          file_size
        );
        return Ok(config.initial_buffer_size);
      }

      // Apply new config's max capacity as a cap
      let size = config
        .max_capacity_bytes
        .map_or(file_size, |max| file_size.min(max));

      // Consider growing to new baseline if config increased
      if size < config.initial_buffer_size
        && config
          .max_capacity_bytes
          .is_none_or(|max| config.initial_buffer_size <= max)
      {
        log::debug!(
          "Growing journal from {} bytes to initial_buffer_size {} bytes",
          file_size,
          config.initial_buffer_size
        );
        return Ok(config.initial_buffer_size);
      }

      Ok(size)
    } else {
      // New file - use config
      Ok(config.initial_buffer_size)
    }
  }

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
        initial_buffer_size: buffer_size,
        max_capacity_bytes: None,
        current_buffer_size: buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
        retention_registry,
      },
      data_loss,
    ))
  }

  /// Create a new `VersionedKVStore` with dynamic growth configuration.
  ///
  /// The journal file will be named `<name>.jrn.N` where N is the generation number.
  /// If a journal already exists, it will be loaded with its existing contents.
  /// The store will start with `initial_buffer_size` and grow dynamically as needed.
  ///
  /// # Errors
  /// Returns an error if we failed to create or open the journal file, or if the
  /// configuration is invalid.
  pub async fn new_with_config<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    mut config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    retention_registry: Arc<RetentionRegistry>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    // Normalize configuration (apply defaults to invalid values)
    config.normalize();

    let dir = dir_path.as_ref();

    let (journal_path, generation) = file_manager::find_active_journal(dir, name).await;

    // Determine appropriate buffer size based on file existence and config
    let buffer_size = Self::determine_buffer_size(&journal_path, &config).await?;

    log::debug!(
      "Initializing VersionedKVStore at {}: initial_buffer_size={}, max_capacity={:?}, \
       current_buffer_size={}, generation={generation}",
      journal_path.display(),
      config.initial_buffer_size,
      config.max_capacity_bytes,
      buffer_size
    );

    let (journal, initial_state, data_loss) = if journal_path.exists() {
      // Try to open existing journal
      Self::open(
        &journal_path,
        buffer_size,
        config.high_water_mark_ratio,
        time_provider.clone(),
      )
      .map(|(j, initial_state, data_loss)| (j, initial_state, data_loss.into()))
      .or_else(|_| {
        // Data is corrupt or unreadable, create fresh journal
        Ok::<_, anyhow::Error>((
          MemMappedVersionedJournal::new(
            &journal_path,
            buffer_size,
            config.high_water_mark_ratio,
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
          config.high_water_mark_ratio,
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
        initial_buffer_size: config.initial_buffer_size,
        max_capacity_bytes: config.max_capacity_bytes,
        current_buffer_size: buffer_size,
        high_water_mark_ratio: config.high_water_mark_ratio,
        current_generation: generation,
        retention_registry,
      },
      data_loss,
    ))
  }

  /// Open an existing `VersionedKVStore` with dynamic growth configuration.
  ///
  /// Unlike `new_with_config()`, this method requires the journal file to exist and will fail if
  /// it's missing.
  ///
  /// # Arguments
  /// * `dir_path` - Directory path where the journal is stored
  /// * `name` - Base name of the journal (e.g., "store" for "store.jrn.N")
  /// * `config` - Configuration for the persistent store
  /// * `time_provider` - Time provider for generating timestamps
  /// * `retention_registry` - Registry for retention management
  ///
  /// # Errors
  /// Returns an error if:
  /// - The configuration is invalid
  /// - The journal file does not exist
  /// - The journal file cannot be opened
  /// - The journal file contains invalid data
  /// - Initialization fails
  pub async fn open_existing_with_config<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    mut config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    retention_registry: Arc<RetentionRegistry>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    // Normalize configuration (apply defaults to invalid values)
    config.normalize();

    let dir = dir_path.as_ref();

    let (journal_path, generation) = file_manager::find_active_journal(dir, name).await;

    // Determine appropriate buffer size
    let buffer_size = Self::determine_buffer_size(&journal_path, &config).await?;

    let (journal, initial_state, data_loss) = Self::open(
      &journal_path,
      buffer_size,
      config.high_water_mark_ratio,
      time_provider,
    )?;

    Ok((
      Self {
        journal,
        cached_map: initial_state,
        dir_path: dir.to_path_buf(),
        journal_name: name.to_string(),
        initial_buffer_size: config.initial_buffer_size,
        max_capacity_bytes: config.max_capacity_bytes,
        current_buffer_size: buffer_size,
        high_water_mark_ratio: config.high_water_mark_ratio,
        current_generation: generation,
        retention_registry,
      },
      if matches!(data_loss, PartialDataLoss::Yes) {
        DataLoss::Partial
      } else {
        DataLoss::None
      },
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
    retention_registry: Arc<RetentionRegistry>,
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
        initial_buffer_size: buffer_size,
        max_capacity_bytes: None,
        current_buffer_size: buffer_size,
        high_water_mark_ratio,
        current_generation: generation,
        retention_registry,
      },
      if matches!(data_loss, PartialDataLoss::Yes) {
        DataLoss::Partial
      } else {
        DataLoss::None
      },
    ))
  }

  #[allow(clippy::type_complexity)]
  fn open(
    journal_path: &Path,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(
    MemMappedVersionedJournal<StateValue>,
    AHashMap<(Scope, String), TimestampedValue>,
    PartialDataLoss,
  )> {
    let mut initial_state = AHashMap::default();
    let (journal, data_loss) = MemMappedVersionedJournal::<StateValue>::from_file(
      journal_path,
      buffer_size,
      high_water_mark_ratio,
      time_provider,
      |scope, key, value, timestamp| {
        if value.value_type.is_some() {
          initial_state.insert(
            (scope, key.to_string()),
            TimestampedValue {
              value: value.clone(),
              timestamp,
            },
          );
        } else {
          initial_state.remove(&(scope, key.to_string()));
        }
      },
    )?;

    Ok((journal, initial_state, data_loss))
  }

  /// Get a value by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get(&self, scope: Scope, key: &str) -> Option<&StateValue> {
    self
      .cached_map
      .get(&(scope, key.to_string()))
      .map(|tv| &tv.value)
  }

  /// Get a value with its timestamp by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get_with_timestamp(&self, scope: Scope, key: &str) -> Option<&TimestampedValue> {
    self.cached_map.get(&(scope, key.to_string()))
  }

  /// Get a value with its scope and timestamp by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get_with_metadata(&self, scope: Scope, key: &str) -> Option<(Scope, &TimestampedValue)> {
    self
      .cached_map
      .get(&(scope, key.to_string()))
      .map(|tv| (scope, tv))
  }

  /// Insert a value for a key, returning the timestamp assigned to this write.
  ///
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// Returns an error if the value cannot be written to the journal.
  pub async fn insert(
    &mut self,
    scope: Scope,
    key: String,
    value: StateValue,
  ) -> anyhow::Result<u64> {
    let timestamp = if value.value_type.is_none() {
      // Inserting null is equivalent to deletion
      let timestamp = self
        .journal
        .insert_entry(scope, &key, StateValue::default())?;
      self.cached_map.remove(&(scope, key));
      timestamp
    } else {
      let timestamp = self.journal.insert_entry(scope, &key, value.clone())?;
      self
        .cached_map
        .insert((scope, key), TimestampedValue { value, timestamp });
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
  pub async fn remove(&mut self, scope: Scope, key: &str) -> anyhow::Result<Option<u64>> {
    if !self.cached_map.contains_key(&(scope, key.to_string())) {
      return Ok(None);
    }

    let timestamp = self
      .journal
      .insert_entry(scope, key, StateValue::default())?;
    self.cached_map.remove(&(scope, key.to_string()));

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
  pub fn contains_key(&self, scope: Scope, key: &str) -> bool {
    self.cached_map.contains_key(&(scope, key.to_string()))
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
  pub fn as_hashmap(&self) -> &AHashMap<(Scope, String), TimestampedValue> {
    &self.cached_map
  }

  /// Synchronize changes to disk.
  ///
  /// This is a blocking operation that performs synchronous I/O. In async contexts,
  /// consider wrapping this call with `tokio::task::spawn_blocking`.
  pub fn sync(&self) -> anyhow::Result<()> {
    MemMappedVersionedJournal::sync(&self.journal)
  }

  /// Returns the path to the current primary journal file.
  #[must_use]
  pub fn journal_path(&self) -> PathBuf {
    self.dir_path.join(format!(
      "{}.jrn.{}",
      self.journal_name, self.current_generation
    ))
  }

  /// Returns the initial buffer size configured for this store.
  #[must_use]
  pub fn initial_buffer_size(&self) -> usize {
    self.initial_buffer_size
  }

  /// Returns the current buffer size of the journal.
  #[must_use]
  pub fn current_buffer_size(&self) -> usize {
    self.current_buffer_size
  }

  /// Returns the maximum capacity in bytes, if configured.
  #[must_use]
  pub fn max_capacity_bytes(&self) -> Option<usize> {
    self.max_capacity_bytes
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
  ///
  /// If a retention registry is configured and no subsystem requires historical snapshots,
  /// the old journal will be deleted without creating a compressed snapshot.
  /// Calculate the size needed for the compacted state.
  ///
  /// This estimates how much space the current entries would require when written
  /// to a new journal during rotation.
  fn estimate_compacted_size(&self) -> usize {
    self
      .cached_map
      .iter()
      .map(|((_scope, key), tv)| {
        // Estimate per entry:
        // - scope: 1 byte
        // - key_len varint: typically 1 byte for short keys
        // - key: key.len() bytes
        // - timestamp varint: typically 8-9 bytes
        // - payload: protobuf size
        // - crc: 4 bytes
        // - frame length varint: typically 1-2 bytes

        let key_len = key.len();
        let payload_size: usize = protobuf::MessageDyn::compute_size_dyn(&tv.value)
          .try_into()
          .unwrap_or(0);

        // Conservative estimate with typical varint sizes
        let frame_overhead = 20; // Covers all varints, scope, and CRC
        key_len + payload_size + frame_overhead
      })
      .sum::<usize>()
      + 17 // Journal header size
  }

  /// Calculate the next buffer size based on compacted data size.
  ///
  /// Uses power-of-2 growth strategy with 50% headroom for future writes.
  fn calculate_next_buffer_size(&self, compacted_size: usize) -> usize {
    // Add 50% headroom for future writes
    #[allow(
      clippy::cast_precision_loss,
      clippy::cast_possible_truncation,
      clippy::cast_sign_loss
    )]
    let target_size = (compacted_size as f32 * 1.5) as usize;

    // No growth needed if current buffer is sufficient
    if target_size <= self.current_buffer_size {
      return self.current_buffer_size;
    }

    // Round up to next power of 2
    let new_size = target_size.next_power_of_two();

    // Apply capacity cap
    self
      .max_capacity_bytes
      .map_or(new_size, |max| new_size.min(max))
  }

  pub async fn rotate_journal(&mut self) -> anyhow::Result<Rotation> {
    let next_generation = self.current_generation + 1;
    let old_generation = self.current_generation;
    self.current_generation = next_generation;

    // Calculate space needed for compacted state
    let compacted_size = self.estimate_compacted_size();

    log::debug!(
      "Rotating journal to generation {next_generation}: compacted_size={} bytes, \
       current_buffer_size={} bytes",
      compacted_size,
      self.current_buffer_size
    );

    // Check if compacted state fits in max capacity
    if let Some(max) = self.max_capacity_bytes
      && compacted_size > max
    {
      // This should only happen if we decrease the max capacity to below current usage
      // during process restart, as otherwise we couldn't have grown to this point.
      log::debug!("Compaction size {compacted_size} exceeds max capacity {max} bytes");

      // TODO(snowp): We need a strategy for this since we are allowing for dynamic configuration
      // of max capacity. Options include:
      // - Further growth beyond max capacity (not ideal as it violates config)
      // - Failing rotation and keeping current journal (risky as we are at high water mark)
      // - Truncating data (not ideal as it violates durability guarantees)
      // - Alerting and requiring manual intervention
      anyhow::bail!(
        "Cannot rotate: compacted state ({compacted_size} bytes) exceeds max capacity ({max} \
         bytes)"
      );
    }

    // Determine new buffer size (may grow)
    let old_buffer_size = self.current_buffer_size;
    let new_buffer_size = self.calculate_next_buffer_size(compacted_size);
    self.current_buffer_size = new_buffer_size;

    if new_buffer_size == old_buffer_size {
      log::debug!(
        "Journal rotation: generation {} → {}, maintaining size {} bytes, entries: {}",
        old_generation,
        next_generation,
        new_buffer_size,
        self.cached_map.len()
      );
    } else {
      #[allow(clippy::cast_precision_loss)]
      let growth_ratio = new_buffer_size as f32 / old_buffer_size as f32;
      log::debug!(
        "Journal growth: generation {} → {}, size {} → {} bytes ({:.2}x growth), entries: {}",
        old_generation,
        next_generation,
        old_buffer_size,
        new_buffer_size,
        growth_ratio,
        self.cached_map.len()
      );
    }

    // Create new journal with compacted state
    let new_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{next_generation}", self.journal_name));

    MemMappedVersionedJournal::sync(&self.journal)?;
    let new_journal = self.create_rotated_journal(&new_journal_path)?;
    self.journal = new_journal;

    // Best-effort cleanup: compress and archive the old journal
    let old_journal_path = self
      .dir_path
      .join(format!("{}.jrn.{old_generation}", self.journal_name));
    let snapshot_path = self
      .archive_journal(&old_journal_path, old_generation)
      .await;

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
  ///
  /// If a retention registry is configured, this method checks if any subsystem requires
  /// snapshots. If no retention is needed, the journal is deleted without compression.
  /// After creating a snapshot, this method triggers cleanup of old snapshots.
  ///
  /// Snapshots are stored in a `snapshots/` subdirectory to keep them separate from
  /// active journal files.
  async fn archive_journal(&self, old_journal_path: &Path, generation: u64) -> PathBuf {
    // Get the maximum timestamp from the current state to use for the snapshot filename
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
      self.journal_name, generation, rotation_timestamp
    ));

    if should_create_snapshot {
      log::debug!(
        "Archiving journal {} to {}",
        old_journal_path.display(),
        archived_path.display()
      );

      // TODO(snowp): In cases where we are logging but were unable to create snapshots this
      // would lose data needed for recovery. Consider surfacing this in a better way.

      // Create snapshots directory if it doesn't exist
      if let Err(e) = tokio::fs::create_dir_all(&snapshots_dir).await {
        // This is an expected failure, e.g. permission denied or disk full so gracefully log and
        // skip snapshot creation.
        log::debug!(
          "Failed to create snapshots directory {}: {}",
          snapshots_dir.display(),
          e
        );
      } else {
        // Try to compress the old journal for longer-term storage.
        if let Err(e) = compress_archived_journal(old_journal_path, &archived_path).await {
          // This is an expected failure, e.g. permission denied or disk full so gracefully log and
          // skip snapshot creation.
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

    // Remove the uncompressed regardless of compression success or skip. If we succeeded we no
    // longer need it, while if we failed we consider the snapshot lost.
    let _ignored = tokio::fs::remove_file(old_journal_path)
      .await
      .inspect_err(|e| {
        log::debug!(
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
  ) -> anyhow::Result<MemMappedVersionedJournal<StateValue>> {
    MemMappedVersionedJournal::new(
      journal_path,
      self.current_buffer_size,
      self.high_water_mark_ratio,
      self.journal.time_provider.clone(),
      self
        .cached_map
        .iter()
        .map(|((frame_type, key), tv)| (*frame_type, key.clone(), tv.value.clone(), tv.timestamp)),
    )
  }
}
