// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{ResilientKv, HighWaterMarkCallback};
use bd_bonjson::Value;
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

/// In-memory implementation of a crash-resilient key-value store that can be recovered even if writing is interrupted.
#[derive(Debug)]
pub struct InMemoryResilientKv<'a> {
  version: u64,
  position: usize,
  buffer: &'a mut [u8],
  high_water_mark: usize,
  high_water_mark_callback: Option<HighWaterMarkCallback>,
  high_water_mark_triggered: bool,
}

impl<'a> InMemoryResilientKv<'a> {
  /// Create a new KV store using the provided buffer as storage space.
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
    // KV files have the following structure:
    // | Position | Data                     | Type           |
    // |----------|--------------------------|----------------|
    // | 0        | Version                  | u64            |
    // | 8        | Position                 | u64            |
    // | 16       | Type Code: Array Start   | u8             |
    // | 17       | Metadata Object          | BONJSON Object |
    // | ...      | Initial data             | BONJSON Object |
    // | ...      | Journal entry            | BONJSON Object |
    // | ...      | ...                      | ...            |
    // The last Container End byte (to terminate the array) is not stored in the file.
    // The metadata object contains an "initialized" key with a u64 timestamp in nanoseconds.
    
    // Validate high water mark ratio
    let ratio = high_water_mark_ratio.unwrap_or(0.8);
    if !(0.0..=1.0).contains(&ratio) {
      anyhow::bail!("High water mark ratio must be between 0.0 and 1.0, got: {}", ratio);
    }

    // Write version
    let version_bytes = VERSION.to_le_bytes();
    buffer[0..8].copy_from_slice(&version_bytes);
    
    // Write array begin marker at position 16
    let buffer_len = buffer.len();
    let mut cursor = &mut buffer[16..];
    serialize_array_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize array begin: {e:?}"))?;
    
    // Write metadata object first
    serialize_map_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize metadata map begin: {e:?}"))?;
    
    serialize_string(&mut cursor, "initialized")
      .map_err(|e| anyhow::anyhow!("Failed to serialize initialized key: {e:?}"))?;
    
    // Get current time in nanoseconds since UNIX epoch
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map_err(|e| anyhow::anyhow!("System time error: {e}"))?
      .as_nanos() as u64;
    
    encode_into_buf(&mut cursor, &Value::Unsigned(now))
      .map_err(|e| anyhow::anyhow!("Failed to encode initialized timestamp: {e:?}"))?;
    
    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize metadata container end: {e:?}"))?;
    
    // Calculate position from remaining capacity
    let position = buffer_len - cursor.remaining_mut();
    
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
    })
  }

  /// Create a new KV store with state loaded from the provided buffer.
  ///
  /// The buffer is expected to already contain a properly formatted KV store file.
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
    let ratio = high_water_mark_ratio.unwrap_or(0.8);
    if !(0.0..=1.0).contains(&ratio) {
      anyhow::bail!("High water mark ratio must be between 0.0 and 1.0, got: {}", ratio);
    }

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
    
    let kv = Self {
      version: u64::from_le_bytes(version_bytes),
      position: position_usize,
      buffer,
      high_water_mark,
      high_water_mark_callback: callback,
      high_water_mark_triggered: position_usize >= high_water_mark,
    };

    if kv.version != VERSION {
      anyhow::bail!("Unsupported version: {}, expected {}", kv.version, VERSION);
    }
    if kv.position >= kv.buffer.len() {
      anyhow::bail!(
        "Invalid position: {}, buffer size: {}",
        kv.position,
        kv.buffer.len()
      );
    }

    // Validate that metadata exists and has the "initialized" key
    if let Err(e) = Self::validate_metadata(&kv.buffer[16..kv.position]) {
      anyhow::bail!("Invalid metadata in buffer: {}", e);
    }

    Ok(kv)
  }

  /// Create a new KV store from an existing one by copying its current state into the provided
  /// buffer.
  ///
  /// All journal entries from the old store will be replayed, resulting in a single journal entry
  /// in the new KV store. The metadata from the source store will be copied.
  ///
  /// The buffer will be overwritten.
  ///
  /// # Errors
  /// Returns an error if serialization or encoding fails.
  pub fn from_kv_store(buffer: &'a mut [u8], kv_store: &mut Self) -> anyhow::Result<Self> {
    // Extract metadata timestamp from source store
    let metadata_timestamp = kv_store.extract_metadata()?;
    
    // Create new store with basic structure
    let mut kv = Self::new_with_timestamp(buffer, None, None, metadata_timestamp)?;
    let initial_position = kv.position;
    let buffer_len = kv.buffer.len();
    let mut cursor = &mut kv.buffer[kv.position..];
    
    // Write all entries as a single map using BufMut
    serialize_map_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize map begin: {e:?}"))?;
    
    let hashmap = kv_store.as_hashmap()?;
    for (k, v) in &hashmap {
      serialize_string(&mut cursor, k)
        .map_err(|e| anyhow::anyhow!("Failed to serialize string key: {e:?}"))?;
      
      encode_into_buf(&mut cursor, v)
        .map_err(|e| anyhow::anyhow!("Failed to encode value: {e:?}"))?;
    }
    
    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;
    
    // Calculate new position from remaining capacity
    let bytes_written = buffer_len - initial_position - cursor.remaining_mut();
    kv.set_position(initial_position + bytes_written);

    Ok(kv)
  }

  /// Create a new KV store using the provided buffer with a specific timestamp for metadata.
  /// This is used internally when copying from existing stores.
  fn new_with_timestamp(
    buffer: &'a mut [u8], 
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>,
    timestamp: u64
  ) -> anyhow::Result<Self> {
    // Validate high water mark ratio
    let ratio = high_water_mark_ratio.unwrap_or(0.8);
    if !(0.0..=1.0).contains(&ratio) {
      anyhow::bail!("High water mark ratio must be between 0.0 and 1.0, got: {}", ratio);
    }

    // Write version
    let version_bytes = VERSION.to_le_bytes();
    buffer[0..8].copy_from_slice(&version_bytes);
    
    // Write array begin marker at position 16
    let buffer_len = buffer.len();
    let mut cursor = &mut buffer[16..];
    serialize_array_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize array begin: {e:?}"))?;
    
    // Write metadata object with provided timestamp
    serialize_map_begin(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize metadata map begin: {e:?}"))?;
    
    serialize_string(&mut cursor, "initialized")
      .map_err(|e| anyhow::anyhow!("Failed to serialize initialized key: {e:?}"))?;
    
    encode_into_buf(&mut cursor, &Value::Unsigned(timestamp))
      .map_err(|e| anyhow::anyhow!("Failed to encode initialized timestamp: {e:?}"))?;
    
    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize metadata container end: {e:?}"))?;
    
    // Calculate position from remaining capacity
    let position = buffer_len - cursor.remaining_mut();
    
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
    })
  }

  /// Get a copy of the buffer for testing purposes
  #[cfg(test)]
  pub fn buffer_copy(&self) -> Vec<u8> {
    self.buffer.to_vec()
  }

  #[allow(dead_code)]
  fn set_version(&mut self, version: u64) {
    self.version = version;
    let version_bytes = version.to_le_bytes();
    self.buffer[0..8].copy_from_slice(&version_bytes);
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

  /// Extract metadata from the current buffer and return the timestamp.
  fn extract_metadata(&mut self) -> anyhow::Result<u64> {
    // Create a temporary copy of the buffer to avoid modifying the original
    let mut temp_buffer = self.buffer.to_vec();
    
    // Close the array in the temporary copy
    let mut cursor = &mut temp_buffer[self.position..];
    serialize_container_end(&mut cursor)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end for metadata extraction: {e:?}"))?;
    
    let buffer = &temp_buffer[16..];
    let (_, decoded) = from_slice(buffer)
      .map_err(|e| anyhow::anyhow!("Failed to decode buffer for metadata: {e:?}"))?;
    
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

  /// Validate that the buffer contains proper metadata with an "initialized" key.
  fn validate_metadata(buffer: &[u8]) -> anyhow::Result<()> {
    // We need to create a temporary closed array to parse
    let mut temp_buffer = buffer.to_vec();
    temp_buffer.push(0x9b); // Add container end byte
    
    let (_, decoded) = from_slice(&temp_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to decode buffer for validation: {e:?}"))?;
    
    if let Value::Array(entries) = decoded {
      if let Some(Value::Object(metadata)) = entries.first() {
        if metadata.contains_key("initialized") {
          return Ok(());
        }
      }
    }
    
    anyhow::bail!("No valid metadata with 'initialized' key found");
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

impl<'a> ResilientKv for InMemoryResilientKv<'a> {
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

  /// Set key to value in this kv store.
  ///
  /// This will create a new journal entry.
  ///
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.write_journal_entry(key, value)
  }

  /// Delete a key from this kv store.
  ///
  /// This will create a new journal entry.
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.set(key, &Value::Null)
  }

  /// Get the current state of the kv store as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    // Recall that the beginning of a kv store has an "array open" byte (at position 16). We close
    // it here by inserting a "container end" byte at `self.position` (which points to one past
    // the end of the last committed change). Then the entire journal can be read as a single
    // BONJSON document consisting of an array of journal entries.
    // Inserting this byte won't affect the key-value store's operation, because anything in
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
}