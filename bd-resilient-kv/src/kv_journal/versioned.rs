// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use ahash::AHashMap;
use bd_bonjson::Value;
use bd_bonjson::decoder::from_slice;
use bd_bonjson::encoder::encode_into_buf;
use bd_bonjson::serialize_primitives::serialize_array_begin;
use bd_client_common::error::InvariantError;
use bytes::BufMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Versioned implementation of a key-value journaling system that tracks write versions
/// for point-in-time recovery.
///
/// Each write operation is assigned a monotonically increasing version number, enabling
/// exact state reconstruction at any historical version.
#[derive(Debug)]
pub struct VersionedKVJournal<'a> {
  #[allow(dead_code)]
  format_version: u64,
  position: usize,
  buffer: &'a mut [u8],
  high_water_mark: usize,
  high_water_mark_triggered: bool,
  initialized_at_unix_time_ns: u64,
  current_version: AtomicU64,
  base_version: u64, // First version in this journal
}

// Versioned KV files have the following structure:
// | Position | Data                     | Type           |
// |----------|--------------------------|----------------|
// | 0        | Format Version           | u64            |
// | 8        | Position                 | u64            |
// | 16       | Type Code: Array Start   | u8             |
// | 17       | Metadata Object          | BONJSON Object |
// | ...      | Versioned Journal Entry  | BONJSON Object |
// | ...      | Versioned Journal Entry  | BONJSON Object |
//
// Metadata object: {"initialized": <timestamp>, "format_version": 2, "base_version": <version>}
// Journal entries: {"v": <version>, "t": <timestamp>, "k": "<key>", "o": <value or null>}

const VERSION: u64 = 2; // The versioned format version
const INVALID_VERSION: u64 = 0; // 0 will never be a valid version

const HEADER_SIZE: usize = 16;
const ARRAY_BEGIN: usize = 16;
const METADATA_OFFSET: usize = 17;

// Minimum buffer size for a valid journal
const MIN_BUFFER_SIZE: usize = HEADER_SIZE + 4;

/// Get current timestamp in nanoseconds since UNIX epoch.
fn current_timestamp() -> anyhow::Result<u64> {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map_err(|_| InvariantError::Invariant.into())
    .and_then(|d| u64::try_from(d.as_nanos()).map_err(|_| InvariantError::Invariant.into()))
}

fn read_version(buffer: &[u8]) -> anyhow::Result<u64> {
  let version_bytes: [u8; 8] = buffer[.. 8].try_into()?;
  let version = u64::from_le_bytes(version_bytes);
  if version != VERSION {
    anyhow::bail!("Unsupported version: {version}, expected {VERSION}");
  }
  Ok(version)
}

/// Write to the version field of a journal buffer.
fn write_version_field(buffer: &mut [u8], version: u64) {
  let version_bytes = version.to_le_bytes();
  buffer[0 .. 8].copy_from_slice(&version_bytes);
}

/// Write the version to a journal buffer.
fn write_version(buffer: &mut [u8]) {
  write_version_field(buffer, VERSION);
}

/// Invalidate the version field of a journal buffer.
fn invalidate_version(buffer: &mut [u8]) {
  write_version_field(buffer, INVALID_VERSION);
}

fn read_position(buffer: &[u8]) -> anyhow::Result<usize> {
  let position_bytes: [u8; 8] = buffer[8 .. 16].try_into()?;
  let position_u64 = u64::from_le_bytes(position_bytes);
  let position = usize::try_from(position_u64)
    .map_err(|_| anyhow::anyhow!("Position {position_u64} too large for usize"))?;
  let buffer_len = buffer.len();
  if position >= buffer_len {
    anyhow::bail!("Invalid position: {position}, buffer size: {buffer_len}",);
  }
  Ok(position)
}

/// Write the position to a journal buffer.
fn write_position(buffer: &mut [u8], position: usize) {
  let position_bytes = (position as u64).to_le_bytes();
  buffer[8 .. 16].copy_from_slice(&position_bytes);
}

/// Read the bonjson payload in this buffer.
fn read_bonjson_payload(buffer: &[u8]) -> anyhow::Result<Value> {
  let position = read_position(buffer)?;
  let slice_to_decode = &buffer[ARRAY_BEGIN .. position];

  match from_slice(slice_to_decode) {
    Ok((_, decoded)) => Ok(decoded),
    Err(bd_bonjson::decoder::DecodeError::Partial { partial_value, .. }) => Ok(partial_value),
    Err(e) => anyhow::bail!("Failed to decode buffer: {e:?}"),
  }
}

/// Create and write the metadata section of a versioned journal.
fn write_metadata(buffer: &mut [u8], timestamp: u64, base_version: u64) -> anyhow::Result<usize> {
  let buffer_len = buffer.len();
  let mut cursor = &mut buffer[METADATA_OFFSET ..];

  // Create metadata object
  let mut metadata = AHashMap::new();
  metadata.insert("initialized".to_string(), Value::Unsigned(timestamp));
  metadata.insert("format_version".to_string(), Value::Unsigned(VERSION));
  metadata.insert("base_version".to_string(), Value::Unsigned(base_version));

  // Write metadata object
  encode_into_buf(&mut cursor, &Value::Object(metadata))
    .map_err(|e| anyhow::anyhow!("Failed to encode metadata object: {e:?}"))?;

  Ok(buffer_len - cursor.remaining_mut())
}

/// Extract metadata from the buffer.
fn extract_metadata_from_buffer(buffer: &[u8]) -> anyhow::Result<(u64, u64)> {
  let array = read_bonjson_payload(buffer)?;
  if let Value::Array(entries) = array
    && let Some(Value::Object(obj)) = entries.first()
  {
    let timestamp = if let Some(Value::Unsigned(ts)) = obj.get("initialized") {
      *ts
    } else if let Some(Value::Signed(ts)) = obj.get("initialized") {
      #[allow(clippy::cast_sign_loss)]
      (*ts as u64)
    } else {
      anyhow::bail!("No initialized timestamp found in metadata");
    };

    let base_version = if let Some(Value::Unsigned(bv)) = obj.get("base_version") {
      *bv
    } else if let Some(Value::Signed(bv)) = obj.get("base_version") {
      #[allow(clippy::cast_sign_loss)]
      (*bv as u64)
    } else {
      0 // Default to 0 if not found (for compatibility)
    };

    return Ok((timestamp, base_version));
  }
  anyhow::bail!("No valid metadata found");
}

fn validate_buffer_len(buffer: &[u8]) -> anyhow::Result<usize> {
  let buffer_len = buffer.len();
  if buffer_len < MIN_BUFFER_SIZE {
    anyhow::bail!(
      "Buffer too small: {buffer_len} bytes, but need at least {MIN_BUFFER_SIZE} bytes"
    );
  }
  Ok(buffer_len)
}

/// Validate high water mark ratio and calculate the position from buffer length.
fn calculate_high_water_mark(
  buffer_len: usize,
  high_water_mark_ratio: Option<f32>,
) -> anyhow::Result<usize> {
  let ratio = high_water_mark_ratio.unwrap_or(0.8);
  if !(0.0 ..= 1.0).contains(&ratio) {
    anyhow::bail!("High water mark ratio must be between 0.0 and 1.0, got: {ratio}");
  }

  #[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
  )]
  let high_water_mark = (buffer_len as f32 * ratio) as usize;
  Ok(high_water_mark)
}

impl<'a> VersionedKVJournal<'a> {
  /// Create a new versioned journal using the provided buffer as storage space.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer
  /// * `base_version` - The starting version for this journal
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if serialization fails or if `high_water_mark_ratio` is invalid.
  pub fn new(
    buffer: &'a mut [u8],
    base_version: u64,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    // If this operation gets interrupted, the buffer must be considered invalid.
    invalidate_version(buffer);

    let buffer_len = validate_buffer_len(buffer)?;
    let high_water_mark = calculate_high_water_mark(buffer_len, high_water_mark_ratio)?;

    // Write array begin marker right after header
    let mut cursor = &mut buffer[HEADER_SIZE ..];
    serialize_array_begin(&mut cursor).map_err(|_| InvariantError::Invariant)?;

    // Write metadata with current timestamp and base version
    let timestamp = current_timestamp()?;
    let position = write_metadata(buffer, timestamp, base_version)?;

    write_position(buffer, position);
    write_version(buffer);

    Ok(Self {
      format_version: VERSION,
      position,
      buffer,
      high_water_mark,
      high_water_mark_triggered: false,
      initialized_at_unix_time_ns: timestamp,
      current_version: AtomicU64::new(base_version),
      base_version,
    })
  }

  /// Create a new versioned journal with state loaded from the provided buffer.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer containing existing versioned KV data
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the buffer is invalid, corrupted, or if `high_water_mark_ratio` is
  /// invalid.
  pub fn from_buffer(
    buffer: &'a mut [u8],
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    let buffer_len = validate_buffer_len(buffer)?;
    let format_version = read_version(buffer)?;
    let position = read_position(buffer)?;
    let (init_timestamp, base_version) = extract_metadata_from_buffer(buffer)?;
    let high_water_mark = calculate_high_water_mark(buffer_len, high_water_mark_ratio)?;

    // Find the highest version in the journal
    let highest_version = Self::find_highest_version(buffer)?;
    let current_version = highest_version.unwrap_or(base_version);

    Ok(Self {
      format_version,
      position,
      buffer,
      high_water_mark,
      high_water_mark_triggered: position >= high_water_mark,
      initialized_at_unix_time_ns: init_timestamp,
      current_version: AtomicU64::new(current_version),
      base_version,
    })
  }

  /// Find the highest version number in the journal by scanning all entries.
  fn find_highest_version(buffer: &[u8]) -> anyhow::Result<Option<u64>> {
    let array = read_bonjson_payload(buffer)?;
    let mut max_version: Option<u64> = None;

    if let Value::Array(entries) = array {
      for (index, entry) in entries.iter().enumerate() {
        // Skip metadata (first entry)
        if index == 0 {
          continue;
        }

        if let Value::Object(obj) = entry {
          if let Some(Value::Unsigned(v)) = obj.get("v") {
            max_version = Some(max_version.map_or(*v, |current| current.max(*v)));
          } else if let Some(Value::Signed(v)) = obj.get("v") {
            #[allow(clippy::cast_sign_loss)]
            let version = *v as u64;
            max_version = Some(max_version.map_or(version, |current| current.max(version)));
          }
        }
      }
    }

    Ok(max_version)
  }

  /// Get the current version number.
  #[must_use]
  pub fn current_version(&self) -> u64 {
    self.current_version.load(Ordering::SeqCst)
  }

  /// Get the base version (first version in this journal).
  #[must_use]
  pub fn base_version(&self) -> u64 {
    self.base_version
  }

  fn set_position(&mut self, position: usize) {
    self.position = position;
    write_position(self.buffer, position);
    self.check_high_water_mark();
  }

  fn check_high_water_mark(&mut self) {
    if self.position >= self.high_water_mark {
      self.trigger_high_water();
    }
  }

  fn trigger_high_water(&mut self) {
    self.high_water_mark_triggered = true;
  }

  /// Write a versioned journal entry.
  fn write_versioned_entry(
    &mut self,
    version: u64,
    key: &str,
    value: &Value,
  ) -> anyhow::Result<()> {
    let buffer_len = self.buffer.len();
    let mut cursor = &mut self.buffer[self.position ..];

    // Create entry object: {"v": version, "t": timestamp, "k": key, "o": value}
    let timestamp = current_timestamp()?;
    let mut entry = AHashMap::new();
    entry.insert("v".to_string(), Value::Unsigned(version));
    entry.insert("t".to_string(), Value::Unsigned(timestamp));
    entry.insert("k".to_string(), Value::String(key.to_string()));
    entry.insert("o".to_string(), value.clone());

    encode_into_buf(&mut cursor, &Value::Object(entry))
      .map_err(|e| anyhow::anyhow!("Failed to encode versioned entry: {e:?}"))?;

    let remaining = cursor.remaining_mut();
    self.set_position(buffer_len - remaining);
    Ok(())
  }

  /// Set a key-value pair with automatic version increment.
  pub fn set_versioned(&mut self, key: &str, value: &Value) -> anyhow::Result<u64> {
    let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
    self.write_versioned_entry(version, key, value)?;
    Ok(version)
  }

  /// Delete a key with automatic version increment.
  pub fn delete_versioned(&mut self, key: &str) -> anyhow::Result<u64> {
    let version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
    self.write_versioned_entry(version, key, &Value::Null)?;
    Ok(version)
  }

  /// Get the high water mark position.
  #[must_use]
  pub fn high_water_mark(&self) -> usize {
    self.high_water_mark
  }

  /// Check if the high water mark has been triggered.
  #[must_use]
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self.high_water_mark_triggered
  }

  /// Get the current buffer usage as a percentage (0.0 to 1.0).
  #[must_use]
  pub fn buffer_usage_ratio(&self) -> f32 {
    #[allow(clippy::cast_precision_loss)]
    let position_f32 = self.position as f32;
    #[allow(clippy::cast_precision_loss)]
    let buffer_len_f32 = self.buffer.len() as f32;
    position_f32 / buffer_len_f32
  }

  /// Get the initialization timestamp.
  #[must_use]
  pub fn get_init_time(&self) -> u64 {
    self.initialized_at_unix_time_ns
  }

  /// Reconstruct the hashmap by replaying all journal entries.
  pub fn as_hashmap(&self) -> anyhow::Result<AHashMap<String, Value>> {
    let array = read_bonjson_payload(self.buffer)?;
    let mut map = AHashMap::new();

    if let Value::Array(entries) = array {
      for (index, entry) in entries.iter().enumerate() {
        // Skip metadata (first entry)
        if index == 0 {
          continue;
        }

        if let Value::Object(obj) = entry {
          // Extract key and operation from versioned entry
          if let Some(Value::String(key)) = obj.get("k")
            && let Some(operation) = obj.get("o")
          {
            if operation.is_null() {
              map.remove(key);
            } else {
              map.insert(key.clone(), operation.clone());
            }
          }
        }
      }
    }

    Ok(map)
  }

  /// Reconstruct the hashmap at a specific version by replaying entries up to that version.
  pub fn as_hashmap_at_version(
    &self,
    target_version: u64,
  ) -> anyhow::Result<AHashMap<String, Value>> {
    let array = read_bonjson_payload(self.buffer)?;
    let mut map = AHashMap::new();

    if let Value::Array(entries) = array {
      for (index, entry) in entries.iter().enumerate() {
        // Skip metadata (first entry)
        if index == 0 {
          continue;
        }

        if let Value::Object(obj) = entry {
          // Check version
          let entry_version = if let Some(Value::Unsigned(v)) = obj.get("v") {
            *v
          } else if let Some(Value::Signed(v)) = obj.get("v") {
            #[allow(clippy::cast_sign_loss)]
            (*v as u64)
          } else {
            continue; // Skip entries without version
          };

          // Only apply entries up to target version
          if entry_version > target_version {
            break;
          }

          // Extract key and operation
          if let Some(Value::String(key)) = obj.get("k")
            && let Some(operation) = obj.get("o")
          {
            if operation.is_null() {
              map.remove(key);
            } else {
              map.insert(key.clone(), operation.clone());
            }
          }
        }
      }
    }

    Ok(map)
  }

  /// Get a copy of the buffer for testing purposes
  #[cfg(test)]
  #[must_use]
  pub fn buffer_copy(&self) -> Vec<u8> {
    self.buffer.to_vec()
  }
}

/// Rotation utilities for creating new journals with compacted state
impl<'a> VersionedKVJournal<'a> {
  /// Create a new journal initialized with the compacted state from a snapshot version.
  ///
  /// The new journal will have all current key-value pairs written as versioned entries
  /// at the `snapshot_version`, followed by the ability to continue with incremental writes.
  ///
  /// # Arguments
  /// * `buffer` - The buffer to write the new journal to
  /// * `snapshot_version` - The version to assign to all compacted state entries
  /// * `state` - The current key-value state to write
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark
  ///
  /// # Errors
  /// Returns an error if serialization fails or buffer is too small.
  pub fn create_rotated_journal(
    buffer: &'a mut [u8],
    snapshot_version: u64,
    state: &AHashMap<String, Value>,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    // Create a new journal with the snapshot version as the base
    let mut journal = Self::new(buffer, snapshot_version, high_water_mark_ratio)?;

    // Write all current state as versioned entries at the snapshot version
    let timestamp = current_timestamp()?;
    for (key, value) in state {
      let buffer_len = journal.buffer.len();
      let mut cursor = &mut journal.buffer[journal.position ..];

      // Create entry object: {"v": version, "t": timestamp, "k": key, "o": value}
      let mut entry = AHashMap::new();
      entry.insert("v".to_string(), Value::Unsigned(snapshot_version));
      entry.insert("t".to_string(), Value::Unsigned(timestamp));
      entry.insert("k".to_string(), Value::String(key.clone()));
      entry.insert("o".to_string(), value.clone());

      encode_into_buf(&mut cursor, &Value::Object(entry))
        .map_err(|e| anyhow::anyhow!("Failed to encode state entry: {e:?}"))?;

      let remaining = cursor.remaining_mut();
      journal.set_position(buffer_len - remaining);
    }

    Ok(journal)
  }
}
