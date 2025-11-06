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
use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a value with its associated timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampedValue {
  /// The value stored in the key-value store.
  pub value: Value,
  /// The timestamp (in nanoseconds since UNIX epoch) when this value was last written.
  pub timestamp: u64,
}

/// Versioned implementation of a key-value journaling system that tracks write timestamps
/// for point-in-time recovery.
///
/// Each write operation is assigned a monotonically increasing timestamp (in nanoseconds
/// since UNIX epoch), enabling exact state reconstruction at any historical timestamp.
/// The monotonicity is enforced by clamping: if the system clock goes backwards, we reuse
/// the same timestamp value to maintain ordering guarantees without artificial clock skew.
#[derive(Debug)]
pub struct VersionedKVJournal<'a> {
  #[allow(dead_code)]
  format_version: u64,
  position: usize,
  buffer: &'a mut [u8],
  high_water_mark: usize,
  high_water_mark_triggered: bool,
  initialized_at_unix_time_ns: u64,
  current_version: u64,
  base_version: u64, // First version in this journal
  last_timestamp: u64, // Most recent timestamp written (for monotonic enforcement)
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
//
// # Timestamp Semantics
//
// Timestamps serve as logical clocks with monotonic guarantees rather than pure wall time:
// - Each write gets a timestamp that is guaranteed to be >= previous writes
// - If system clock goes backward, timestamps are clamped to last_timestamp (reuse same value)
// - This ensures total ordering while allowing correlation with external timestamped systems
// - Version numbers (v) are maintained for backward compatibility and as secondary ordering

const VERSION: u64 = 2; // The versioned format version
const INVALID_VERSION: u64 = 0; // 0 will never be a valid version

const HEADER_SIZE: usize = 16;
const ARRAY_BEGIN: usize = 16;
const METADATA_OFFSET: usize = 17;

// Minimum buffer size for a valid journal
const MIN_BUFFER_SIZE: usize = HEADER_SIZE + 4;

/// Helper function to read a u64 field from a BONJSON object.
///
/// BONJSON's decoder automatically converts unsigned values that fit in i64 to signed values
/// during decoding (see bd-bonjson/src/decoder.rs:227-234). This means that even though we
/// write `Value::Unsigned(version)`, the decoder returns `Value::Signed(version as i64)`.
///
/// TODO(snowp): Consider changing BONJSON's decoder to preserve the original unsigned type
/// to avoid this normalization behavior and eliminate the need for this helper.
fn read_u64_field(obj: &AHashMap<String, Value>, key: &str) -> Option<u64> {
  match obj.get(key) {
    Some(Value::Unsigned(v)) => Some(*v),
    Some(Value::Signed(v)) if *v >= 0 =>
    {
      #[allow(clippy::cast_sign_loss)]
      Some(*v as u64)
    },
    _ => None,
  }
}

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
    let timestamp = read_u64_field(obj, "initialized")
      .ok_or_else(|| anyhow::anyhow!("No initialized timestamp found in metadata"))?;

    let base_version = read_u64_field(obj, "base_version").unwrap_or(0);

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
      current_version: base_version,
      base_version,
      last_timestamp: timestamp,
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

    // Find the highest timestamp in the journal
    let highest_timestamp = Self::find_highest_timestamp(buffer)?;
    let last_timestamp = highest_timestamp.unwrap_or(init_timestamp);

    Ok(Self {
      format_version,
      position,
      buffer,
      high_water_mark,
      high_water_mark_triggered: position >= high_water_mark,
      initialized_at_unix_time_ns: init_timestamp,
      current_version,
      base_version,
      last_timestamp,
    })
  }

  /// Find the highest version number in the journal.
  ///
  /// Since versions are monotonically increasing, this simply returns the version
  /// from the last entry in the journal.
  fn find_highest_version(buffer: &[u8]) -> anyhow::Result<Option<u64>> {
    let array = read_bonjson_payload(buffer)?;

    if let Value::Array(entries) = array {
      // Skip metadata (index 0) and get the last actual entry
      // Since versions are monotonically increasing, the last entry has the highest version
      if entries.len() > 1
        && let Some(Value::Object(obj)) = entries.last()
      {
        return Ok(read_u64_field(obj, "v"));
      }
    }

    Ok(None)
  }

  /// Find the highest timestamp in the journal.
  ///
  /// Since timestamps are monotonically increasing, this simply returns the timestamp
  /// from the last entry in the journal.
  fn find_highest_timestamp(buffer: &[u8]) -> anyhow::Result<Option<u64>> {
    let array = read_bonjson_payload(buffer)?;

    if let Value::Array(entries) = array {
      // Skip metadata (index 0) and get the last actual entry
      // Since timestamps are monotonically increasing, the last entry has the highest timestamp
      if entries.len() > 1
        && let Some(Value::Object(obj)) = entries.last()
      {
        return Ok(read_u64_field(obj, "t"));
      }
    }

    Ok(None)
  }

  /// Get the current version number.
  #[must_use]
  pub fn current_version(&self) -> u64 {
    self.current_version
  }

  /// Get the base version (first version in this journal).
  #[must_use]
  pub fn base_version(&self) -> u64 {
    self.base_version
  }

  /// Get the next monotonically increasing timestamp.
  ///
  /// This ensures that even if the system clock goes backwards, timestamps remain
  /// monotonically increasing by clamping to `last_timestamp` (reusing the same value).
  /// This prevents artificial clock skew while maintaining ordering guarantees.
  fn next_monotonic_timestamp(&mut self) -> anyhow::Result<u64> {
    let current = current_timestamp()?;
    let monotonic = std::cmp::max(current, self.last_timestamp);
    self.last_timestamp = monotonic;
    Ok(monotonic)
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

  /// Write a versioned journal entry and return the timestamp.
  fn write_versioned_entry(
    &mut self,
    version: u64,
    key: &str,
    value: &Value,
  ) -> anyhow::Result<u64> {
    // Get monotonically increasing timestamp before borrowing buffer
    let timestamp = self.next_monotonic_timestamp()?;

    let buffer_len = self.buffer.len();
    let mut cursor = &mut self.buffer[self.position ..];

    // Create entry object: {"v": version, "t": timestamp, "k": key, "o": value}
    // TODO(snowp): It would be nice to be able to pass impl AsRef<str> for key or Cow to avoid
    // allocating small strings repeatedly.
    let entry = AHashMap::from([
      ("v".to_string(), Value::Unsigned(version)),
      ("t".to_string(), Value::Unsigned(timestamp)),
      ("k".to_string(), Value::String(key.to_string())),
      ("o".to_string(), value.clone()),
    ]);

    encode_into_buf(&mut cursor, &Value::Object(entry))
      .map_err(|e| anyhow::anyhow!("Failed to encode versioned entry: {e:?}"))?;

    let remaining = cursor.remaining_mut();
    self.set_position(buffer_len - remaining);
    Ok(timestamp)
  }

  /// Set a key-value pair with automatic version increment.
  /// Returns a tuple of (version, timestamp).
  ///
  /// The timestamp is monotonically increasing and serves as the primary ordering mechanism.
  /// If the system clock goes backwards, timestamps are clamped to maintain monotonicity.
  pub fn set_versioned(&mut self, key: &str, value: &Value) -> anyhow::Result<(u64, u64)> {
    self.current_version += 1;
    let version = self.current_version;
    let timestamp = self.write_versioned_entry(version, key, value)?;
    Ok((version, timestamp))
  }

  /// Delete a key with automatic version increment.
  /// Returns a tuple of (version, timestamp).
  ///
  /// The timestamp is monotonically increasing and serves as the primary ordering mechanism.
  /// If the system clock goes backwards, timestamps are clamped to maintain monotonicity.
  pub fn delete_versioned(&mut self, key: &str) -> anyhow::Result<(u64, u64)> {
    self.current_version += 1;
    let version = self.current_version;
    let timestamp = self.write_versioned_entry(version, key, &Value::Null)?;
    Ok((version, timestamp))
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

  /// Reconstruct the hashmap with timestamps by replaying all journal entries.
  pub fn as_hashmap_with_timestamps(&self) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
    let array = read_bonjson_payload(self.buffer)?;
    let mut map = AHashMap::new();

    if let Value::Array(entries) = array {
      for (index, entry) in entries.iter().enumerate() {
        // Skip metadata (first entry)
        if index == 0 {
          continue;
        }

        if let Value::Object(obj) = entry {
          // Extract key, operation, and timestamp from versioned entry
          if let Some(Value::String(key)) = obj.get("k")
            && let Some(operation) = obj.get("o")
          {
            // Extract timestamp (default to 0 if not found)
            let timestamp = read_u64_field(obj, "t").unwrap_or(0);

            if operation.is_null() {
              map.remove(key);
            } else {
              map.insert(
                key.clone(),
                TimestampedValue {
                  value: operation.clone(),
                  timestamp,
                },
              );
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
  /// at the `snapshot_version`, using their original timestamps to preserve historical accuracy.
  /// The journal's monotonic timestamp enforcement will respect the highest timestamp in the
  /// provided state.
  ///
  /// # Arguments
  /// * `buffer` - The buffer to write the new journal to
  /// * `snapshot_version` - The version to assign to all compacted state entries
  /// * `state` - The current key-value state with timestamps to write
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark
  ///
  /// # Errors
  /// Returns an error if serialization fails or buffer is too small.
  pub fn create_rotated_journal(
    buffer: &'a mut [u8],
    snapshot_version: u64,
    state: &AHashMap<String, TimestampedValue>,
    high_water_mark_ratio: Option<f32>,
  ) -> anyhow::Result<Self> {
    // Create a new journal with the snapshot version as the base
    let mut journal = Self::new(buffer, snapshot_version, high_water_mark_ratio)?;

    // Find the maximum timestamp in the state to maintain monotonicity
    let max_state_timestamp = state.values().map(|tv| tv.timestamp).max().unwrap_or(0);

    // Write all current state as versioned entries at the snapshot version
    // Use the original timestamp from each entry to preserve historical accuracy
    for (key, timestamped_value) in state {
      let buffer_len = journal.buffer.len();
      let mut cursor = &mut journal.buffer[journal.position ..];

      // Update last_timestamp to ensure monotonicity is maintained
      // We use the actual timestamp from the entry, but track the maximum for future writes
      journal.last_timestamp = std::cmp::max(journal.last_timestamp, timestamped_value.timestamp);

      // Create entry object: {"v": version, "t": timestamp, "k": key, "o": value}
      // TODO(snowp): It would be nice to be able to pass impl AsRef<str> for key or Cow to avoid
      // allocating small strings repeatedly.
      let entry = AHashMap::from([
        ("v".to_string(), Value::Unsigned(snapshot_version)),
        (
          "t".to_string(),
          Value::Unsigned(timestamped_value.timestamp),
        ),
        ("k".to_string(), Value::String(key.clone())),
        ("o".to_string(), timestamped_value.value.clone()),
      ]);

      encode_into_buf(&mut cursor, &Value::Object(entry))
        .map_err(|e| anyhow::anyhow!("Failed to encode state entry: {e:?}"))?;

      let remaining = cursor.remaining_mut();
      journal.set_position(buffer_len - remaining);
    }

    // Ensure last_timestamp reflects the maximum timestamp we've written
    journal.last_timestamp = std::cmp::max(journal.last_timestamp, max_state_timestamp);

    Ok(journal)
  }
}
