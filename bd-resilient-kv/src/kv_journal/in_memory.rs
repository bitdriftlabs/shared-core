// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::KVJournal;
use bd_bonjson::{Value, ValueRef};
use bd_bonjson::decoder::from_slice;
use bd_bonjson::encoder::{encode_into_buf, encode_ref_into_buf};
use bd_bonjson::serialize_primitives::{
  SerializationError,
  serialize_array_begin,
  serialize_container_end,
  serialize_map_begin,
  serialize_string,
};
use bd_client_common::error::InvariantError;
use bytes::BufMut;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// In-memory implementation of a key-value journaling system whose data can be recovered up to the
/// last successful write checkpoint.
#[derive(Debug)]
pub struct InMemoryKVJournal<'a> {
  #[allow(dead_code)] // Used for validation and debugging but not in runtime operations
  version: u64,
  position: usize,
  buffer: &'a mut [u8],
  high_water_mark: usize,
  high_water_mark_triggered: bool,
  initialized_at_unix_time_ns: u64,
}

// KV files have the following structure:
// | Position | Data                     | Type           |
// |----------|--------------------------|----------------|
// | 0        | Version                  | u64            |
// | 8        | Position                 | u64            |
// | 16       | Type Code: Array Start   | u8             |
// | 17       | Metadata Object          | BONJSON Object |
// | ...      | Journal entry            | BONJSON Object |
// | ...      | Journal entry            | BONJSON Object |
// | ...      | ...                      | ...            |
// The last Container End byte (to terminate the array) is not stored in the file.
// The metadata object contains an "initialized" key with a u64 timestamp in nanoseconds.

const VERSION: u64 = 1; // The current version of this data format
const INVALID_VERSION: u64 = 0; // 0 will never be a valid version

const HEADER_SIZE: usize = 16;
const ARRAY_BEGIN: usize = 16;
const METADATA_OFFSET: usize = 17;

// 8 bytes: version
// 8 bytes: position
// 1 byte: array open
// 1 byte: metadata hashmap open
// 1 byte: metadata hashmap close
// 1 byte: space for array close byte
const MIN_BUFFER_SIZE: usize = HEADER_SIZE + 4;

fn read_version(buffer: &[u8]) -> anyhow::Result<u64> {
  let version_bytes: [u8; 8] = buffer[.. 8].try_into()?;
  let version = u64::from_le_bytes(version_bytes);
  if version != VERSION {
    anyhow::bail!("Unsupported version: {version}, expected {VERSION}");
  }
  Ok(version)
}

/// Write to the version field of a journal buffer.
/// Assumption: We've already validated that the length is at least `MIN_BUFFER_SIZE`.
fn write_version_field(buffer: &mut [u8], version: u64) {
  let version_bytes = version.to_le_bytes();
  buffer[0 .. 8].copy_from_slice(&version_bytes);
}

/// Write the version to a journal buffer.
/// Assumption: We've already validated that the length is at least `MIN_BUFFER_SIZE`.
fn write_version(buffer: &mut [u8]) {
  write_version_field(buffer, VERSION);
}

/// Invalidate the version field of a journal buffer.
/// This ensures that a buffer undergoing major changes will be invalid if interrupted partway.
/// Assumption: We've already validated that the length is at least `MIN_BUFFER_SIZE`.
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
/// Assumption: We've already validated that the length is at least `MIN_BUFFER_SIZE`.
fn write_position(buffer: &mut [u8], position: usize) {
  let position_bytes = (position as u64).to_le_bytes();
  buffer[8 .. 16].copy_from_slice(&position_bytes);
}

/// Read the bonjson payload in this buffer.
/// This should always be a `Value::Array`
fn read_bonjson_payload(buffer: &[u8]) -> anyhow::Result<Value> {
  let position = read_position(buffer)?;

  // Extract the slice from the array start to the current position.
  // This will always result in an incomplete array, which is fine since
  // we only want to decode what we have so far.
  let slice_to_decode = &buffer[ARRAY_BEGIN .. position];

  match from_slice(slice_to_decode) {
    Ok((_, decoded)) => Ok(decoded), // Shouldn't happen, but here for completeness
    Err(bd_bonjson::decoder::DecodeError::Partial { partial_value, .. }) => Ok(partial_value),
    Err(e) => anyhow::bail!("Failed to decode buffer: {e:?}"),
  }
}

/// Create and write the metadata section of a journal with given timestamp.
/// The metadata section starts at offset `METADATA_OFFSET` in the buffer.
/// Returns the position after the metadata (`METADATA_OFFSET` + metadata length).
/// Assumption: We've already validated that the length is at least `MIN_BUFFER_SIZE`.
fn write_metadata(buffer: &mut [u8], timestamp: u64) -> anyhow::Result<usize> {
  let buffer_len = buffer.len();
  let mut cursor = &mut buffer[METADATA_OFFSET ..];

  // Create metadata object
  let mut metadata = HashMap::new();
  metadata.insert("initialized".to_string(), Value::Unsigned(timestamp));

  // Write metadata object
  encode_into_buf(&mut cursor, &Value::Object(metadata))
    .map_err(|e| anyhow::anyhow!("Failed to encode metadata object: {e:?}"))?;

  Ok(buffer_len - cursor.remaining_mut())
}

/// Extract the initialization timestamp from the metadata section of a journal buffer.
fn extract_timestamp_from_buffer(buffer: &[u8]) -> anyhow::Result<u64> {
  let array = read_bonjson_payload(buffer)?;
  if let Value::Array(entries) = array
    && let Some(Value::Object(obj)) = entries.first()
  {
    if let Some(Value::Unsigned(timestamp)) = obj.get("initialized") {
      return Ok(*timestamp);
    } else if let Some(Value::Signed(timestamp)) = obj.get("initialized") {
      // BONJSON may store it as signed if it can fit without bloat.
      #[allow(clippy::cast_sign_loss)]
      return Ok(*timestamp as u64);
    }
  }
  anyhow::bail!("No valid metadata with initialized timestamp found");
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

/// Get current timestamp in nanoseconds since UNIX epoch.
fn current_timestamp() -> anyhow::Result<u64> {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map_err(|_| InvariantError::Invariant.into())
    .and_then(|d| u64::try_from(d.as_nanos()).map_err(|_| InvariantError::Invariant.into()))
}

impl<'a> InMemoryKVJournal<'a> {
  /// Create a new journal using the provided buffer as storage space.
  ///
  /// The buffer will be overwritten.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// # Errors
  /// Returns an error if serialization fails or if `high_water_mark_ratio` is invalid.
  pub fn new(buffer: &'a mut [u8], high_water_mark_ratio: Option<f32>) -> anyhow::Result<Self> {
    // If this operation gets interrupted, the buffer must be considered invalid.
    invalidate_version(buffer);

    let buffer_len = validate_buffer_len(buffer)?;
    let high_water_mark = calculate_high_water_mark(buffer_len, high_water_mark_ratio)?;

    // Write array begin marker right after header
    let mut cursor = &mut buffer[HEADER_SIZE ..];
    serialize_array_begin(&mut cursor).map_err(|_| InvariantError::Invariant)?;

    // Write metadata with current timestamp
    let timestamp = current_timestamp()?;
    let position = write_metadata(buffer, timestamp)?;

    write_position(buffer, position);

    write_version(buffer);

    Ok(Self {
      version: VERSION,
      position,
      buffer,
      high_water_mark,
      high_water_mark_triggered: false,
      initialized_at_unix_time_ns: timestamp,
    })
  }

  /// Create a new journal with state loaded from the provided buffer.
  ///
  /// The buffer is expected to already contain a properly formatted journal file.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer containing existing KV data
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
    let version = read_version(buffer)?;
    let position = read_position(buffer)?;
    let init_timestamp = extract_timestamp_from_buffer(buffer)?;
    let high_water_mark = calculate_high_water_mark(buffer_len, high_water_mark_ratio)?;

    Ok(Self {
      version,
      position,
      buffer,
      high_water_mark,
      high_water_mark_triggered: position >= high_water_mark,
      initialized_at_unix_time_ns: init_timestamp,
    })
  }

  /// Get a copy of the buffer for testing purposes
  #[cfg(test)]
  #[must_use]
  pub fn buffer_copy(&self) -> Vec<u8> {
    self.buffer.to_vec()
  }

  fn set_position(&mut self, position: usize) {
    self.position = position;
    write_position(self.buffer, position);
    self.check_high_water_mark();
  }

  /// Check if the current position has exceeded the high water mark and trigger callback if needed.
  fn check_high_water_mark(&mut self) {
    if self.position >= self.high_water_mark {
      self.trigger_high_water();
    }
  }

  fn trigger_high_water(&mut self) {
    self.high_water_mark_triggered = true;
  }

  fn write_journal_entry(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    let buffer_len = self.buffer.len();
    let mut cursor = &mut self.buffer[self.position ..];

    // Write the journal entry using BufMut - position is tracked automatically
    serialize_map_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize map begin: {e:?}"))?;

    serialize_string(&mut cursor, key)
      .map_err(|e| anyhow::anyhow!("Failed to serialize string key: {e:?}"))?;

    encode_into_buf(&mut cursor, value)
      .map_err(|e| anyhow::anyhow!("Failed to encode value: {e:?}"))?;

    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;

    let remaining = cursor.remaining_mut();
    self.set_position(buffer_len - remaining);
    Ok(())
  }

  fn write_journal_entries(&mut self, entries: &HashMap<String, Value>) -> anyhow::Result<()> {
    let buffer_len = self.buffer.len();
    let mut cursor = &mut self.buffer[self.position ..];

    // Try to write all entries as a single BONJSON object
    let encode_result = encode_ref_into_buf(&mut cursor, &ValueRef::Object(entries));

    match encode_result {
      Ok(_bytes_written) => {
        let remaining = cursor.remaining_mut();
        self.set_position(buffer_len - remaining);
        Ok(())
      },
      Err(SerializationError::BufferFull) => {
        // We didn't have enough space to write all entries at once.
        self.trigger_high_water();
        Err(anyhow::anyhow!(
          "Failed to encode entries object: {:?}",
          SerializationError::BufferFull
        ))
      },
      Err(e) => Err(anyhow::anyhow!("Failed to encode entries object: {e:?}")),
    }
  }
}

impl KVJournal for InMemoryKVJournal<'_> {
  /// Get the current high water mark position.
  fn high_water_mark(&self) -> usize {
    self.high_water_mark
  }

  /// Check if the high water mark has been triggered.
  fn is_high_water_mark_triggered(&self) -> bool {
    self.high_water_mark_triggered
  }

  /// Get the current buffer usage as a percentage (0.0 to 1.0).
  fn buffer_usage_ratio(&self) -> f32 {
    #[allow(clippy::cast_precision_loss)]
    let position_f32 = self.position as f32;
    #[allow(clippy::cast_precision_loss)]
    let buffer_len_f32 = self.buffer.len() as f32;
    position_f32 / buffer_len_f32
  }

  /// Generate a new journal entry recording the setting of a key to a value.
  ///
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.write_journal_entry(key, value)
  }

  /// Generate a new journal entry recording the setting of multiple key-value pairs.
  ///
  /// This is more efficient than calling `set()` multiple times as it writes
  /// all entries as a single BONJSON object rather than individual objects.
  ///
  /// Note: Setting any value to `Value::Null` will mark that entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entries cannot be written. If an error occurs,
  /// no data will have been written.
  fn set_multiple(&mut self, entries: &HashMap<String, Value>) -> anyhow::Result<()> {
    if entries.is_empty() {
      return Ok(());
    }

    // Write all entries as a single BONJSON object for efficiency
    self.write_journal_entries(entries)
  }

  /// Generate a new journal entry recording the deletion of a key.
  ///
  /// This will create a new journal entry.
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.set(key, &Value::Null)
  }

  /// Clear all key-value pairs from the journal.
  ///
  /// This is more efficient than calling `delete()` on each key individually
  /// as it reinitializes the journal with empty state rather than creating
  /// multiple deletion journal entries.
  ///
  /// # Errors
  /// Returns an error if the clearing operation fails.
  fn clear(&mut self) -> anyhow::Result<()> {
    // If this operation gets interrupted, the buffer must be considered invalid.
    invalidate_version(self.buffer);

    // Reinitialize with empty state by writing just the metadata
    let timestamp = current_timestamp()?;
    let position = write_metadata(self.buffer, timestamp)?;

    // Update cached timestamp and position
    self.initialized_at_unix_time_ns = timestamp;
    self.set_position(position);

    write_version(self.buffer);
    Ok(())
  }

  /// Get the current state of the journal as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&self) -> anyhow::Result<HashMap<String, Value>> {
    let array = read_bonjson_payload(self.buffer)?;
    let mut map = HashMap::new();
    if let Value::Array(entries) = array {
      for (index, entry) in entries.iter().enumerate() {
        if let Value::Object(obj) = entry {
          // Skip the first entry (metadata) when building the data hashmap
          if index == 0 && obj.contains_key("initialized") {
            continue;
          }

          for (k, v) in obj {
            if v.is_null() {
              map.remove(k);
            } else {
              map.insert(k.clone(), v.clone());
            }
          }
        }
      }
    }

    Ok(map)
  }

  /// Get the time when the journal was initialized (nanoseconds since UNIX epoch).
  fn get_init_time(&self) -> u64 {
    self.initialized_at_unix_time_ns
  }

  /// Reinitialize this journal using the data from another journal.
  ///
  /// # Errors
  /// Returns an error if the other journal cannot be read or if writing to this journal fails.
  fn reinit_from(&mut self, other: &dyn KVJournal) -> anyhow::Result<()> {
    // Get all data from the other journal
    let data = other.as_hashmap()?;

    // If this operation gets interrupted, the buffer must be considered invalid.
    invalidate_version(self.buffer);

    // Reset high water mark triggered flag since we're reinitializing
    self.high_water_mark_triggered = false;

    // Write metadata with current timestamp
    let timestamp = current_timestamp()?;
    let mut position = write_metadata(self.buffer, timestamp)?;

    // Update cached timestamp
    self.initialized_at_unix_time_ns = timestamp;

    // Write the data from the other journal if it's not empty
    if !data.is_empty() {
      let buffer_len = self.buffer.len();
      let mut cursor = &mut self.buffer[position ..];

      encode_into_buf(&mut cursor, &Value::Object(data))
        .map_err(|e| anyhow::anyhow!("Failed to encode data: {e:?}"))?;

      // Calculate new position
      position = position + (buffer_len - position - cursor.remaining_mut());
    }

    self.set_position(position);

    write_version(self.buffer);
    Ok(())
  }
}
