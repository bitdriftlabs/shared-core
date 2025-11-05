// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::{MemMappedVersionedKVJournal, TimestampedValue, VersionedKVJournal};
use ahash::AHashMap;
use bd_bonjson::Value;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Callback invoked when journal rotation occurs.
///
/// The callback receives:
/// - `old_journal_path`: The path to the archived journal file that was just rotated out
/// - `new_journal_path`: The path to the new active journal file
/// - `rotation_version`: The version at which rotation occurred (snapshot version)
///
/// This callback can be used to trigger asynchronous upload of archived journals to remote
/// storage, perform cleanup, or other post-rotation operations.
pub type RotationCallback = Box<dyn FnMut(&Path, &Path, u64) + Send>;

/// Compress an archived journal using zlib.
fn compress_archived_journal(source: &Path, dest: &Path) -> anyhow::Result<()> {
  let journal_bytes = std::fs::read(source)?;

  let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(3));
  encoder.write_all(&journal_bytes)?;
  let compressed = encoder.finish()?;

  std::fs::write(dest, compressed)?;

  Ok(())
}

/// A persistent key-value store with version tracking.
///
/// `VersionedKVStore` provides HashMap-like semantics backed by a versioned journal that
/// assigns a monotonically increasing version number to each write operation. This enables:
/// - Audit logs with version tracking for every write
/// - Automatic journal rotation when high water mark is reached
/// - Optional callbacks for post-rotation operations (e.g., remote backup)
///
/// For performance optimization, `VersionedKVStore` maintains an in-memory cache of the
/// current key-value data to provide O(1) read operations and avoid expensive journal
/// decoding on every access.
///
/// # Rotation Strategy
/// When the journal reaches its high water mark, the store automatically:
/// 1. Creates a new journal file with a rotated name (e.g., `store.jrn.v12345`)
/// 2. Writes the current state as versioned entries at the rotation version
/// 3. Archives the old journal for potential upload/cleanup
/// 4. Continues normal operations in the new journal
///
/// # Example
/// ```ignore
/// use bd_resilient_kv::VersionedKVStore;
/// use bd_bonjson::Value;
///
/// let mut store = VersionedKVStore::new("mystore.jrn", 1024 * 1024, None)?;
///
/// // Insert with version tracking
/// let v1 = store.insert("key1".to_string(), Value::from(42))?;
/// let v2 = store.insert("key2".to_string(), Value::from("hello"))?;
/// ```
pub struct VersionedKVStore {
  journal: MemMappedVersionedKVJournal,
  cached_map: AHashMap<String, TimestampedValue>,
  base_path: PathBuf,
  buffer_size: usize,
  high_water_mark_ratio: Option<f32>,
  rotation_callback: Option<RotationCallback>,
}

impl VersionedKVStore {
  /// Create a new `VersionedKVStore` with the specified path and buffer size.
  ///
  /// If the file already exists, it will be loaded with its existing contents.
  /// If the specified size is larger than an existing file, it will be resized while preserving
  /// data. If the specified size is smaller and the existing data doesn't fit, a fresh journal
  /// will be created.
  ///
  /// # Arguments
  /// * `file_path` - Path for the journal file
  /// * `buffer_size` - Size in bytes for the journal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the journal file cannot be created/opened or if initialization fails.
  pub fn new<P: AsRef<Path>>(
    file_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let path = file_path.as_ref();
    let base_path = path.to_path_buf();

    let journal = if path.exists() {
      // Try to open existing journal
      MemMappedVersionedKVJournal::from_file(path, buffer_size, high_water_mark_ratio).or_else(
        |_| {
          // Data is corrupt or unreadable, create fresh with base version 1
          MemMappedVersionedKVJournal::new(path, buffer_size, 1, high_water_mark_ratio)
        },
      )?
    } else {
      // Create new journal with base version 1
      MemMappedVersionedKVJournal::new(path, buffer_size, 1, high_water_mark_ratio)?
    };

    let cached_map = journal.as_hashmap_with_timestamps()?;

    Ok(Self {
      journal,
      cached_map,
      base_path,
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
  /// * `file_path` - Path to the existing journal file
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
    file_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let path = file_path.as_ref();
    let base_path = path.to_path_buf();

    let journal = MemMappedVersionedKVJournal::from_file(path, buffer_size, high_water_mark_ratio)?;
    let cached_map = journal.as_hashmap_with_timestamps()?;

    Ok(Self {
      journal,
      cached_map,
      base_path,
      buffer_size,
      high_water_mark_ratio,
      rotation_callback: None,
    })
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

  /// Insert a value for a key, returning the version number assigned to this write.
  ///
  /// Note: Inserting `Value::Null` is equivalent to removing the key.
  ///
  /// # Errors
  /// Returns an error if the value cannot be written to the journal.
  pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<u64> {
    let version = if matches!(value, Value::Null) {
      // Inserting null is equivalent to deletion
      let (version, _timestamp) = self.journal.delete_versioned(&key)?;
      self.cached_map.remove(&key);
      version
    } else {
      let (version, timestamp) = self.journal.set_versioned(&key, &value)?;
      self
        .cached_map
        .insert(key, TimestampedValue { value, timestamp });
      version
    };

    // Check if rotation is needed
    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal()?;
    }

    Ok(version)
  }

  /// Remove a key and return the version number assigned to this deletion.
  ///
  /// Returns `None` if the key didn't exist, otherwise returns the version number.
  ///
  /// # Errors
  /// Returns an error if the deletion cannot be written to the journal.
  pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<u64>> {
    if !self.cached_map.contains_key(key) {
      return Ok(None);
    }

    let (version, _timestamp) = self.journal.delete_versioned(key)?;
    self.cached_map.remove(key);

    // Check if rotation is needed
    if self.journal.is_high_water_mark_triggered() {
      self.rotate_journal()?;
    }

    Ok(Some(version))
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

  /// Get the current version number.
  #[must_use]
  pub fn current_version(&self) -> u64 {
    self.journal.current_version()
  }

  /// Get the base version (first version in this journal).
  #[must_use]
  pub fn base_version(&self) -> u64 {
    self.journal.base_version()
  }

  /// Synchronize changes to disk.
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
  pub fn rotate_journal(&mut self) -> anyhow::Result<()> {
    let rotation_version = self.journal.current_version();

    // Generate archived journal path with rotation version (compressed)
    let archived_path = self.generate_archived_path(rotation_version);

    // Create new journal with rotated state
    let new_journal = self.create_rotated_journal(rotation_version)?;

    // Replace old journal with new one
    let old_journal = std::mem::replace(&mut self.journal, new_journal);

    // Move old journal to temporary location
    drop(old_journal); // Release mmap before moving file
    let temp_uncompressed = self.base_path.with_extension("jrn.old");
    std::fs::rename(&self.base_path, &temp_uncompressed)?;

    // Rename new journal to base path
    let temp_path = self.base_path.with_extension("jrn.tmp");
    std::fs::rename(&temp_path, &self.base_path)?;

    // Compress the archived journal
    compress_archived_journal(&temp_uncompressed, &archived_path)?;

    // Remove uncompressed version
    std::fs::remove_file(&temp_uncompressed)?;

    // Invoke rotation callback if set
    if let Some(ref mut callback) = self.rotation_callback {
      callback(&archived_path, &self.base_path, rotation_version);
    }

    Ok(())
  }

  /// Generate the archived journal path for a given rotation version.
  /// Archived journals use the .zz extension to indicate zlib compression.
  fn generate_archived_path(&self, rotation_version: u64) -> PathBuf {
    let mut path = self.base_path.clone();
    if let Some(file_name) = path.file_name() {
      let new_name = format!("{}.v{}.zz", file_name.to_string_lossy(), rotation_version);
      path.set_file_name(new_name);
    }
    path
  }

  /// Create a new rotated journal with compacted state.
  fn create_rotated_journal(
    &self,
    rotation_version: u64,
  ) -> anyhow::Result<MemMappedVersionedKVJournal> {
    // Create temporary journal file
    let temp_path = self.base_path.with_extension("jrn.tmp");

    // Create in-memory buffer for new journal
    let mut buffer = vec![0u8; self.buffer_size];

    // Use VersionedKVJournal to create rotated journal in memory
    let _rotated = VersionedKVJournal::create_rotated_journal(
      &mut buffer,
      rotation_version,
      &self.cached_map,
      self.high_water_mark_ratio,
    )?;

    // Write buffer to temporary file
    std::fs::write(&temp_path, &buffer)?;

    // Open as memory-mapped journal
    MemMappedVersionedKVJournal::from_file(&temp_path, self.buffer_size, self.high_water_mark_ratio)
  }
}
