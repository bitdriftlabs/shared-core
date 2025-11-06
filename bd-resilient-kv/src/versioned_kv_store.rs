// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::{MemMappedVersionedKVJournal, TimestampedValue, VersionedKVJournal};
use ahash::AHashMap;
use bd_bonjson::Value;
use std::path::{Path, PathBuf};

/// Callback invoked when journal rotation occurs.
///
/// The callback receives:
/// - `old_journal_path`: The path to the archived journal file that was just rotated out
/// - `new_journal_path`: The path to the new active journal file
/// - `rotation_timestamp`: The timestamp at which rotation occurred (snapshot timestamp)
///
/// This callback can be used to trigger asynchronous upload of archived journals to remote
/// storage, perform cleanup, or other post-rotation operations.
pub type RotationCallback = Box<dyn FnMut(&Path, &Path, u64) + Send>;

/// Compress an archived journal using zlib.
///
/// This function compresses the source file to the destination using zlib compression.
/// The compression is performed in a blocking task to avoid holding up the async runtime.
async fn compress_archived_journal(source: &Path, dest: &Path) -> anyhow::Result<()> {
  let source = source.to_owned();
  let dest = dest.to_owned();

  tokio::task::spawn_blocking(move || {
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use std::io::{BufReader, copy};

    let source_file = std::fs::File::open(&source)?;
    let dest_file = std::fs::File::create(&dest)?;
    let mut encoder = ZlibEncoder::new(dest_file, Compression::new(5));
    copy(&mut BufReader::new(source_file), &mut encoder)?;
    encoder.finish()?;
    Ok::<_, anyhow::Error>(())
  })
  .await?
}

/// A persistent key-value store with timestamp tracking.
///
/// `VersionedKVStore` provides HashMap-like semantics backed by a timestamped journal that
/// assigns a monotonically increasing timestamp to each write operation. This enables:
/// - Audit logs with timestamp tracking for every write (timestamps serve as logical clocks)
/// - Point-in-time recovery at any historical timestamp
/// - Correlation with external timestamped event streams
/// - Automatic journal rotation when high water mark is reached
/// - Optional callbacks for post-rotation operations (e.g., remote backup)
///
/// # Timestamp Semantics
///
/// Timestamps are monotonically increasing logical clocks (nanoseconds since UNIX epoch):
/// - Each write gets a timestamp >= all previous writes
/// - If system clock goes backward, timestamps are clamped to maintain ordering
/// - Multiple operations may share the same timestamp if system clock hasn't advanced
/// - Enables natural correlation with timestamped event buffers for upload
///
/// For performance optimization, `VersionedKVStore` maintains an in-memory cache of the
/// current key-value data to provide O(1) read operations and avoid expensive journal
/// decoding on every access.
///
/// # Rotation Strategy
///
/// When the journal reaches its high water mark, the store automatically rotates to a new journal.
/// The rotation process creates a snapshot of the current state while preserving timestamp
/// semantics for accurate point-in-time recovery.
///
/// ## Rotation Process
/// 1. Computes `rotation_timestamp` = max timestamp of all current entries
/// 2. Archives old journal as `<name>.jrn.t<rotation_timestamp>.zz` (compressed)
/// 3. Creates new journal with `base_timestamp = rotation_timestamp`
/// 4. Writes compacted state with **original timestamps preserved**
/// 5. Continues normal operations in the new journal
///
/// ## Timestamp Semantics Across Snapshots
///
/// Compacted entries in the new journal preserve their original timestamps, which means entry
/// timestamps may overlap across adjacent snapshots. The filename timestamp (`t300`, `t500`)
/// represents the rotation point (snapshot boundary), not the minimum timestamp of entries.
///
/// For detailed information about timestamp semantics, recovery bucketing, and invariants,
/// see the `VersionedRecovery` documentation.
///
/// # Example
/// ```ignore
/// use bd_resilient_kv::VersionedKVStore;
/// use bd_bonjson::Value;
///
/// let mut store = VersionedKVStore::new("/path/to/dir", "mystore", 1024 * 1024, None)?;
///
/// // Insert with timestamp tracking
/// let t1 = store.insert("key1".to_string(), Value::from(42))?;
/// let t2 = store.insert("key2".to_string(), Value::from("hello"))?;
/// ```
pub struct VersionedKVStore {
  journal: MemMappedVersionedKVJournal,
  cached_map: AHashMap<String, TimestampedValue>,
  dir_path: PathBuf,
  journal_name: String,
  buffer_size: usize,
  high_water_mark_ratio: Option<f32>,
  rotation_callback: Option<RotationCallback>,
}

impl VersionedKVStore {
  /// Create a new `VersionedKVStore` with the specified directory, name, and buffer size.
  ///
  /// The journal file will be named `<name>.jrn` within the specified directory.
  /// If the file already exists, it will be loaded with its existing contents.
  /// If the specified size is larger than an existing file, it will be resized while preserving
  /// data. If the specified size is smaller and the existing data doesn't fit, a fresh journal
  /// will be created.
  ///
  /// # Arguments
  /// * `dir_path` - Directory path where the journal will be stored
  /// * `name` - Base name for the journal (e.g., "store" will create "store.jrn")
  /// * `buffer_size` - Size in bytes for the journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the journal file cannot be created/opened or if initialization fails.
  pub fn new<P: AsRef<Path>>(
    dir_path: P,
    name: &str,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let dir = dir_path.as_ref();
    let journal_path = dir.join(format!("{name}.jrn"));

    let journal = if journal_path.exists() {
      // Try to open existing journal
      MemMappedVersionedKVJournal::from_file(&journal_path, buffer_size, high_water_mark_ratio)
        .or_else(|_| {
          // Data is corrupt or unreadable, create fresh with base version 1
          MemMappedVersionedKVJournal::new(&journal_path, buffer_size, 1, high_water_mark_ratio)
        })?
    } else {
      // Create new journal with base version 1
      MemMappedVersionedKVJournal::new(&journal_path, buffer_size, 1, high_water_mark_ratio)?
    };

    let cached_map = journal.as_hashmap_with_timestamps()?;

    Ok(Self {
      journal,
      cached_map,
      dir_path: dir.to_path_buf(),
      journal_name: name.to_string(),
      buffer_size,
      high_water_mark_ratio,
      rotation_callback: None,
    })
  }

  /// Open an existing `VersionedKVStore` from a pre-existing journal file.
  ///
  /// Unlike `new()`, this method requires the journal file to exist and will fail if it's
  /// missing.
  ///
  /// # Arguments
  /// * `dir_path` - Directory path where the journal is stored
  /// * `name` - Base name of the journal (e.g., "store" for "store.jrn")
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
  ) -> anyhow::Result<Self> {
    let dir = dir_path.as_ref();
    let journal_path = dir.join(format!("{name}.jrn"));

    let journal =
      MemMappedVersionedKVJournal::from_file(&journal_path, buffer_size, high_water_mark_ratio)?;
    let cached_map = journal.as_hashmap_with_timestamps()?;

    Ok(Self {
      journal,
      cached_map,
      dir_path: dir.to_path_buf(),
      journal_name: name.to_string(),
      buffer_size,
      high_water_mark_ratio,
      rotation_callback: None,
    })
  }

  /// Get the path to the active journal file.
  fn journal_path(&self) -> PathBuf {
    self.dir_path.join(format!("{}.jrn", self.journal_name))
  }

  /// Set a callback to be invoked when journal rotation occurs.
  ///
  /// The callback receives the path to the archived journal file, the new active journal file,
  /// and the rotation version. This can be used to trigger asynchronous upload of archived
  /// journals to remote storage.
  pub fn set_rotation_callback(&mut self, callback: RotationCallback) {
    self.rotation_callback = Some(callback);
  }

  /// Get a value by key.
  ///
  /// This operation is O(1) as it reads from the in-memory cache.
  #[must_use]
  pub fn get(&self, key: &str) -> Option<&Value> {
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
  pub async fn insert(&mut self, key: String, value: Value) -> anyhow::Result<u64> {
    let timestamp = if matches!(value, Value::Null) {
      // Inserting null is equivalent to deletion
      let timestamp = self.journal.delete_versioned(&key)?;
      self.cached_map.remove(&key);
      timestamp
    } else {
      let timestamp = self.journal.set_versioned(&key, &value)?;
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

    let timestamp = self.journal.delete_versioned(key)?;
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
  pub fn as_hashmap_with_timestamps(&self) -> &AHashMap<String, TimestampedValue> {
    &self.cached_map
  }

  /// Get a reference to the current hash map (values only, without timestamps).
  ///
  /// Note: This method creates a temporary hashmap. For better performance,
  /// consider using `get()` for individual lookups or `as_hashmap_with_timestamps()`
  /// if you need the full map with timestamps.
  ///
  /// This operation is O(n) where n is the number of keys.
  #[must_use]
  pub fn as_hashmap(&self) -> AHashMap<String, Value> {
    self
      .cached_map
      .iter()
      .map(|(k, tv)| (k.clone(), tv.value.clone()))
      .collect()
  }

  /// Synchronize changes to disk.
  ///
  /// This is a blocking operation that performs synchronous I/O. In async contexts,
  /// consider wrapping this call with `tokio::task::spawn_blocking`.
  ///
  /// # Errors
  /// Returns an error if the sync operation fails.
  pub fn sync(&self) -> anyhow::Result<()> {
    self.journal.sync()
  }

  /// Get the current buffer usage ratio (0.0 to 1.0).
  #[must_use]
  pub fn buffer_usage_ratio(&self) -> f32 {
    self.journal.buffer_usage_ratio()
  }

  /// Check if the high water mark has been triggered.
  #[must_use]
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self.journal.is_high_water_mark_triggered()
  }

  /// Manually trigger journal rotation.
  ///
  /// This will create a new journal with the current state compacted and archive the old journal.
  /// The archived journal will be compressed using zlib to reduce storage size.
  /// Rotation typically happens automatically when the high water mark is reached, but this
  /// method allows manual control when needed.
  ///
  /// # Errors
  /// Returns an error if rotation fails.
  pub async fn rotate_journal(&mut self) -> anyhow::Result<()> {
    // Get the maximum timestamp from current state for rotation tracking
    let rotation_timestamp = self
      .cached_map
      .values()
      .map(|tv| tv.timestamp)
      .max()
      .unwrap_or(0);

    // Generate archived journal path with rotation timestamp (compressed)
    let archived_path = self.generate_archived_path(rotation_timestamp);

    // Create new journal with rotated state
    let new_journal = self.create_rotated_journal(rotation_timestamp).await?;

    // Replace old journal with new one
    let old_journal = std::mem::replace(&mut self.journal, new_journal);

    // Move old journal to temporary location
    drop(old_journal); // Release mmap before moving file
    let journal_path = self.journal_path();
    let temp_uncompressed = self.dir_path.join(format!("{}.jrn.old", self.journal_name));
    tokio::fs::rename(&journal_path, &temp_uncompressed).await?;

    // Rename new journal to base path
    let temp_path = self.dir_path.join(format!("{}.jrn.tmp", self.journal_name));
    tokio::fs::rename(&temp_path, &journal_path).await?;

    // Compress the archived journal
    compress_archived_journal(&temp_uncompressed, &archived_path).await?;

    // Remove uncompressed version
    tokio::fs::remove_file(&temp_uncompressed).await?;

    // Invoke rotation callback if set
    if let Some(ref mut callback) = self.rotation_callback {
      callback(&archived_path, &journal_path, rotation_timestamp);
    }

    Ok(())
  }

  /// Generate the archived journal path for a given rotation timestamp.
  /// Archived journals use the .zz extension to indicate zlib compression.
  fn generate_archived_path(&self, rotation_timestamp: u64) -> PathBuf {
    self.dir_path.join(format!(
      "{}.jrn.t{}.zz",
      self.journal_name, rotation_timestamp
    ))
  }

  /// Create a new rotated journal with compacted state.
  async fn create_rotated_journal(
    &self,
    base_timestamp: u64,
  ) -> anyhow::Result<MemMappedVersionedKVJournal> {
    // Create temporary journal file
    let temp_path = self.dir_path.join(format!("{}.jrn.tmp", self.journal_name));

    // Create in-memory buffer for new journal
    let mut buffer = vec![0u8; self.buffer_size];

    // Use VersionedKVJournal to create rotated journal in memory
    let _rotated = VersionedKVJournal::create_rotated_journal(
      &mut buffer,
      base_timestamp,
      &self.cached_map,
      self.high_water_mark_ratio,
    )?;

    // Write buffer to temporary file
    tokio::fs::write(&temp_path, &buffer).await?;

    // Open as memory-mapped journal
    MemMappedVersionedKVJournal::from_file(&temp_path, self.buffer_size, self.high_water_mark_ratio)
  }
}
