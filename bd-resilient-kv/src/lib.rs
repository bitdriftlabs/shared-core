// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./kv_test.rs"]
mod kv_test;

use bd_bonjson::Value;
use bd_bonjson::decoder::from_slice;
use bd_bonjson::encoder::encode_into_buf;
use bd_bonjson::serialize_primitives::{
  serialize_array_begin,
  serialize_container_end,
  serialize_map_begin,
  serialize_string,
};
use std::collections::HashMap;

const VERSION: u64 = 1;

/// A crash-resilient key-value store that can be recovered even if writing is interrupted.
pub struct ResilientKv<'a> {
  version: u64,
  position: usize,
  buffer: &'a mut [u8],
}

impl<'a> ResilientKv<'a> {
  /// Create a new KV store using the provided buffer as storage space.
  ///
  /// The buffer will be overwritten.
  ///
  /// # Errors
  /// Returns an error if serialization fails.
  pub fn new(buffer: &'a mut [u8]) -> anyhow::Result<Self> {
    // KV files have the following structure:
    // | Position | Data                     | Type           |
    // |----------|--------------------------|----------------|
    // | 0        | Version                  | u64            |
    // | 8        | Position                 | u64            |
    // | 16       | Type Code: Array Start   | u8             |
    // | 17       | Initial data             | BONJSON Object |
    // | ...      | Journal entry            | BONJSON Object |
    // | ...      | ...                      | ...            |
    // The last Container End byte (to terminate the array) is not stored in the file.

    let mut position = 16;
    
    // Write version
    let version_bytes = VERSION.to_le_bytes();
    buffer[0..8].copy_from_slice(&version_bytes);
    
    // Write array begin marker
    let mut writable_buffer = &mut buffer[position..];
    let bytes_written = serialize_array_begin(&mut writable_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to serialize array begin: {e:?}"))?;
    position += bytes_written;
    
    // Write initial position
    let position_bytes = (position as u64).to_le_bytes();
    buffer[8..16].copy_from_slice(&position_bytes);

    Ok(Self {
      version: VERSION,
      position,
      buffer,
    })
  }

  /// Create a new KV store with state loaded from the provided buffer.
  ///
  /// The buffer is expected to already contain a properly formatted KV store file.
  ///
  /// # Errors
  /// Returns an error if the buffer is invalid or corrupted.
  pub fn from_buffer(buffer: &'a mut [u8]) -> anyhow::Result<Self> {
    if buffer.len() < 16 {
      anyhow::bail!(
        "Buffer too small: {} bytes, need at least 16 bytes for header",
        buffer.len()
      );
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

    let kv = Self {
      version: u64::from_le_bytes(version_bytes),
      position: position_usize,
      buffer,
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

    Ok(kv)
  }

  /// Create a new KV store from an existing one by copying its current state into the provided
  /// buffer.
  ///
  /// All journal entries from the old store will be replayed, resulting in a single journal entry
  /// in the new KV store.
  ///
  /// The buffer will be overwritten.
  ///
  /// # Errors
  /// Returns an error if serialization or encoding fails.
  pub fn from_kv_store(buffer: &'a mut [u8], kv_store: &mut Self) -> anyhow::Result<Self> {
    let mut kv = Self::new(buffer)?;
    let mut position = kv.position;
    
    let mut writable_buffer = &mut kv.buffer[position..];
    let bytes_written = serialize_map_begin(&mut writable_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to serialize map begin: {e:?}"))?;
    position += bytes_written;
    
    let hashmap = kv_store.as_hashmap()?;
    for (k, v) in &hashmap {
      let mut writable_buffer = &mut kv.buffer[position..];
      let bytes_written = serialize_string(&mut writable_buffer, k)
        .map_err(|e| anyhow::anyhow!("Failed to serialize string key: {e:?}"))?;
      position += bytes_written;
      
      let mut writable_buffer = &mut kv.buffer[position..];
      let bytes_written = encode_into_buf(&mut writable_buffer, v)
        .map_err(|e| anyhow::anyhow!("Failed to encode value: {e:?}"))?;
      position += bytes_written;
    }
    
    let mut writable_buffer = &mut kv.buffer[position..];
    let bytes_written = serialize_container_end(&mut writable_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;
    position += bytes_written;
    
    kv.set_position(position);

    Ok(kv)
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
  }

  fn write_journal_entry(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    let mut position = self.position;
    
    // Fill in the map containing the next journal entry
    let mut writable_buffer = &mut self.buffer[position..];
    let bytes_written = serialize_map_begin(&mut writable_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to serialize map begin: {e:?}"))?;
    position += bytes_written;
    
    let mut writable_buffer = &mut self.buffer[position..];
    let bytes_written = serialize_string(&mut writable_buffer, key)
      .map_err(|e| anyhow::anyhow!("Failed to serialize string key: {e:?}"))?;
    position += bytes_written;
    
    let mut writable_buffer = &mut self.buffer[position..];
    let bytes_written = encode_into_buf(&mut writable_buffer, value)
      .map_err(|e| anyhow::anyhow!("Failed to encode value: {e:?}"))?;
    position += bytes_written;
    
    let mut writable_buffer = &mut self.buffer[position..];
    let bytes_written = serialize_container_end(&mut writable_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;
    position += bytes_written;
    
    // Then update position to commit the change
    self.set_position(position);
    Ok(())
  }

  /// Set key to value in this kv store.
  ///
  /// This will create a new journal entry.
  ///
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  pub fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.write_journal_entry(key, value)
  }

  /// Delete a key from this kv store.
  ///
  /// This will create a new journal entry.
  ///
  /// # Errors
  /// Returns an error if the journal entry cannot be written.
  pub fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.set(key, &Value::Null)
  }

  /// Get the current state of the kv store as a `HashMap`.
  ///
  /// # Errors
  /// Returns an error if the buffer cannot be decoded.
  pub fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    // Recall that the beginning of a kv store has an "array open" byte (at position 16). We close
    // it here by inserting a "container end" byte at `self.position` (which points to one past
    // the end of the last committed change). Then the entire journal can be read as a single
    // BONJSON document consisting of an array of journal entries.
    // Inserting this byte won't affect the key-value store's operation, because anything in
    // `self.buffer` from `self.position` onward is considered "garbage".
    let mut writable_buffer = &mut self.buffer[self.position..];
    serialize_container_end(&mut writable_buffer)
      .map_err(|e| anyhow::anyhow!("Failed to serialize container end: {e:?}"))?;
    
    let buffer = &self.buffer[16..];
    let (_, decoded) = from_slice(buffer)
      .map_err(|e| anyhow::anyhow!("Failed to decode buffer: {e:?}"))?;
    let mut map = HashMap::new();
    if let Value::Array(entries) = decoded {
      for entry in entries {
        if let Value::Object(obj) = entry {
          for (k, v) in obj {
            if v.is_null() {
              map.remove(&k);
            } else {
              map.insert(k, v);
            }
          }
        }
      }
    }

    Ok(map)
  }
}
