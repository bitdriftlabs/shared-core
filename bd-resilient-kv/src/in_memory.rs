// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{KVJournal, HighWaterMarkCallback};
use bd_bonjson::Value;
use bd_bonjson::type_codes::TypeCode::ContainerEnd;
use bd_bonjson::decoder::from_slice;
use bd_bonjson::encoder::encode_into_buf;
use bd_bonjson::serialize_primitives::{
  serialize_array_begin,
  serialize_container_end,
  serialize_map_begin,
  serialize_string,
};
use bytes::BufMut;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const VERSION: u64 = 1;

/// In-memory implementation of a key-value journaling system whose data can be recovered up to the last successful write checkpoint.
#[derive(Debug)]
pub struct InMemoryKVJournal<'a> {
  #[allow(dead_code)] // Used for validation and debugging but not in runtime operations
  version: u64,
  position: usize,
  buffer: &'a mut [u8],
  high_water_mark: usize,
  high_water_mark_callback: Option<HighWaterMarkCallback>,
  high_water_mark_triggered: bool,
  initialized_at_unix_time_ns: u64,
}

impl<'a> InMemoryKVJournal<'a> {
  /// Create a new journal using the provided buffer as storage space.
  ///
  /// The buffer will be overwritten.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if serialization fails or if high_water_mark_ratio is invalid.
  pub fn new(
    buffer: &'a mut [u8], 
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    if buffer.len() < 16 {
      anyhow::bail!(
        "Buffer too small: {} bytes, need at least 16 bytes for header",
        buffer.len()
      );
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
    
    // Validate high water mark ratio
    let ratio = Self::validate_high_water_mark_ratio(high_water_mark_ratio)?;

    // Write version
    let version_bytes = VERSION.to_le_bytes();
    buffer[0..8].copy_from_slice(&version_bytes);
    
    // Write array begin marker at position 16
    let buffer_len = buffer.len();
    let mut cursor = &mut buffer[16..];
    serialize_array_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize array begin: {e:?}"))?;
    
    // Write metadata with current timestamp
    let timestamp = Self::current_timestamp()?;
    let position = Self::write_metadata(buffer, timestamp)?;
    
    // Write initial position
    let position_bytes = (position as u64).to_le_bytes();
    buffer[8..16].copy_from_slice(&position_bytes);
    
    // Calculate high water mark position
    let high_water_mark = (buffer_len as f32 * ratio) as usize;

    Ok(Self {
      version: VERSION,
      position,
      buffer,
      high_water_mark,
      high_water_mark_callback: callback,
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
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if the buffer is invalid, corrupted, or if high_water_mark_ratio is invalid.
  pub fn from_buffer(
    buffer: &'a mut [u8],
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    if buffer.len() < 16 {
      anyhow::bail!(
        "Buffer too small: {} bytes, need at least 16 bytes for header",
        buffer.len()
      );
    }
    
    // Validate high water mark ratio
    let ratio = Self::validate_high_water_mark_ratio(high_water_mark_ratio)?;

    let version_bytes: [u8; 8] = buffer[.. 8]
      .try_into()
      .map_err(|_| anyhow::anyhow!("Failed to read version bytes"))?;

    let position_bytes: [u8; 8] = buffer[8 .. 16]
      .try_into()
      .map_err(|_| anyhow::anyhow!("Failed to read position bytes"))?;

    let position_value = u64::from_le_bytes(position_bytes);
    let position_usize = usize::try_from(position_value)
      .map_err(|_| anyhow::anyhow!("Position value too large: {position_value}"))?;

    // Calculate high water mark position
    let high_water_mark = (buffer.len() as f32 * ratio) as usize;
    
    let version = u64::from_le_bytes(version_bytes);
    if version != VERSION {
      anyhow::bail!("Unsupported version: {}, expected {}", version, VERSION);
    }
    if position_usize >= buffer.len() {
      anyhow::bail!(
        "Invalid position: {}, buffer size: {}",
        position_usize,
        buffer.len()
      );
    }

    // Extract timestamp from existing metadata
    let init_timestamp = Self::extract_timestamp_from_buffer(&buffer[16..position_usize])?;
    
    // Create final struct after all validation is complete
    Ok(Self {
      version,
      position: position_usize,
      buffer, 
      high_water_mark,
      high_water_mark_callback: callback,
      high_water_mark_triggered: position_usize >= high_water_mark,
      initialized_at_unix_time_ns: init_timestamp,
    })
  }

  /// Get a copy of the buffer for testing purposes
  #[cfg(test)]
  pub fn buffer_copy(&self) -> Vec<u8> {
    self.buffer.to_vec()
  }

  /// Validate high water mark ratio and return the validated value.
  fn validate_high_water_mark_ratio(ratio: Option<f32>) -> anyhow::Result<f32> {
    let ratio = ratio.unwrap_or(0.8);
    if !(0.0..=1.0).contains(&ratio) {
      anyhow::bail!("High water mark ratio must be between 0.0 and 1.0, got: {}", ratio);
    }
    Ok(ratio)
  }

  /// Create and write metadata object with given timestamp.
  /// This data is always written starting at offset 17 in the buffer.
  /// Returns the new current position (17 + metadata length).
  fn write_metadata(buffer: &mut [u8], timestamp: u64) -> anyhow::Result<usize> {
    let buffer_len = buffer.len();
    let mut cursor = &mut buffer[17..];
    
    // Create metadata object
    let mut metadata = HashMap::new();
    metadata.insert("initialized".to_string(), Value::Unsigned(timestamp));
    
    // Write metadata object
    encode_into_buf(&mut cursor, &Value::Object(metadata))
      .map_err(|e| anyhow::anyhow!("Failed to encode metadata object: {e:?}"))?;
    
    // Return new current position (17 + bytes written)
    Ok(17 + (buffer_len - 17 - cursor.remaining_mut()))
  }

  /// Get current timestamp in nanoseconds since UNIX epoch.
  fn current_timestamp() -> anyhow::Result<u64> {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map_err(|e| anyhow::anyhow!("System time error: {e}"))
      .map(|d| d.as_nanos() as u64)
  }

  fn set_position(&mut self, position: usize) {
    self.position = position;
    let position_bytes = (position as u64).to_le_bytes();
    self.buffer[8..16].copy_from_slice(&position_bytes);
    
    // Check high water mark
    self.check_high_water_mark();
  }

  /// Check if the current position has exceeded the high water mark and trigger callback if needed.
  fn check_high_water_mark(&mut self) {
    if !self.high_water_mark_triggered && self.position >= self.high_water_mark {
      self.high_water_mark_triggered = true;
      if let Some(callback) = self.high_water_mark_callback {
        callback(self.position, self.buffer.len(), self.high_water_mark);
      }
    }
  }

  /// Extract the initialization timestamp from a buffer containing journal data.
  fn extract_timestamp_from_buffer(buffer: &[u8]) -> anyhow::Result<u64> {
    // We need to create a temporary closed array to parse
    let mut temp_buffer = buffer.to_vec();
    temp_buffer.push(ContainerEnd as u8); // Close off the open array
    
    let (_, decoded) = from_slice(&temp_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to decode buffer for timestamp extraction: {e:?}"))?;
    
    if let Value::Array(entries) = decoded {
      for entry in &entries {
        if let Value::Object(obj) = entry {
          if obj.contains_key("initialized") {
            if let Some(Value::Unsigned(timestamp)) = obj.get("initialized") {
              return Ok(*timestamp);
            } else if let Some(Value::Signed(timestamp)) = obj.get("initialized") {
              // Handle the case where timestamp was stored as signed
              return Ok(*timestamp as u64);
            }
          }
        }
      }
    }
    
    anyhow::bail!("No valid metadata with initialized timestamp found");
  }

  fn write_journal_entry(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    let initial_position = self.position;
    let buffer_len = self.buffer.len();
    let mut cursor = &mut self.buffer[self.position..];
    
    // Write the journal entry using BufMut - position is tracked automatically
    serialize_map_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize map begin: {e:?}"))?;
    
    serialize_string(&mut cursor, key)
      .map_err(|e| anyhow::anyhow!("Failed to serialize string key: {e:?}"))?;
    
    encode_into_buf(&mut cursor, value)
      .map_err(|e| anyhow::anyhow!("Failed to encode value: {e:?}"))?;
    
    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;
    
    // Calculate new position from remaining capacity
    let bytes_written = buffer_len - initial_position - cursor.remaining_mut();
    self.set_position(initial_position + bytes_written);
    Ok(())
  }
}

impl<'a> KVJournal for InMemoryKVJournal<'a> {
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
    self.position as f32 / self.buffer.len() as f32
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
    // Reinitialize with empty state by writing just the metadata
    let timestamp = Self::current_timestamp()?;
    let position = Self::write_metadata(self.buffer, timestamp)?;
    
    // Update cached timestamp and position
    self.initialized_at_unix_time_ns = timestamp;
    self.set_position(position);
    
    Ok(())
  }

  /// Get the current state of the journal as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    // Recall that the beginning of a journal has an "array open" byte (at position 16). We close
    // it here by inserting a "container end" byte at `self.position` (which points to one past
    // the end of the last committed change). Then the entire journal can be read as a single
    // BONJSON document consisting of an array of journal entries.
    // Inserting this byte won't affect the journal's operation, because anything in
    // `self.buffer` from `self.position` onward is considered "garbage".
    let mut cursor = &mut self.buffer[self.position..];
    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;
    
    let buffer = &self.buffer[16..];
    let (_, decoded) = from_slice(buffer)
      .map_err(|e| anyhow::anyhow!("Failed to decode buffer: {e:?}"))?;
    let mut map = HashMap::new();
    if let Value::Array(entries) = decoded {
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
  ///
  /// # Errors
  /// Returns an error if the initialization timestamp cannot be retrieved.
  fn get_init_time(&mut self) -> anyhow::Result<u64> {
    Ok(self.initialized_at_unix_time_ns)
  }

  /// Reinitialize this journal using the data from another journal.
  ///
  /// # Errors
  /// Returns an error if the other journal cannot be read or if writing to this journal fails.
  fn reinit_from(&mut self, other: &mut dyn KVJournal) -> anyhow::Result<()> {
    // Get all data from the other journal
    let data = other.as_hashmap()?;
    
    // Write metadata with current timestamp
    let timestamp = Self::current_timestamp()?;
    let mut position = Self::write_metadata(self.buffer, timestamp)?;
    
    // Update cached timestamp
    self.initialized_at_unix_time_ns = timestamp;
    
    // Write the data from the other journal if it's not empty
    if !data.is_empty() {
      let buffer_len = self.buffer.len();
      let mut cursor = &mut self.buffer[position..];
      
      encode_into_buf(&mut cursor, &Value::Object(data))
        .map_err(|e| anyhow::anyhow!("Failed to encode data: {e:?}"))?;
      
      // Calculate new position
      position = position + (buffer_len - position - cursor.remaining_mut());
    }
    
    self.set_position(position);
    Ok(())
  }
}