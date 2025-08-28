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
use bd_bonjson::decoder::decode;
use bd_bonjson::encoder::encode_into;
use bd_bonjson::serialize_primitives::{
  serialize_array_begin,
  serialize_container_end,
  serialize_map_begin,
  serialize_string,
};
use std::collections::HashMap;
use std::fmt;

/// A `ByteBuffer` provides a way to access a contiguous block of memory as a byte slice.
pub trait ByteBuffer {
  fn as_slice(&self) -> &[u8];
  fn as_mutable_slice(&mut self) -> &mut [u8];
}

pub struct BasicByteBuffer {
  data: Vec<u8>,
}

impl BasicByteBuffer {
  #[must_use]
  pub fn new(data: Vec<u8>) -> Self {
    Self { data }
  }
}

impl ByteBuffer for BasicByteBuffer {
  fn as_slice(&self) -> &[u8] {
    &self.data
  }

  fn as_mutable_slice(&mut self) -> &mut [u8] {
    &mut self.data
  }
}

#[derive(Debug, Clone)]
pub enum ResilientKvError {
  SerializationError(String),
  EncodingError(String),
  DecodingError(String),
  BufferSizeError(String),
}

impl fmt::Display for ResilientKvError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
      Self::EncodingError(msg) => write!(f, "Encoding error: {msg}"),
      Self::DecodingError(msg) => write!(f, "Decoding error: {msg}"),
      Self::BufferSizeError(msg) => write!(f, "Buffer size error: {msg}"),
    }
  }
}

impl std::error::Error for ResilientKvError {}

const VERSION: u64 = 1;

/// A crash-resilient key-value store that can be recovered even if writing is interrupted.
pub struct ResilientKv {
  version: u64,
  position: usize,
  buffer: Box<dyn ByteBuffer>,
}

impl ResilientKv {
  /// Create a new KV store using the provided buffer as storage space. The buffer will be
  /// overwritten.
  pub fn new(buffer: Box<dyn ByteBuffer>) -> Result<Self, ResilientKvError> {
    let mut kv = Self {
      version: 0,
      position: 0,
      buffer,
    };
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

    kv.set_version(VERSION);
    let mut position = 16;
    position += serialize_array_begin(kv.buffer_at_position(position)).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize array begin: {e:?}"))
    })?;
    kv.set_position(position);
    // println!("Initial buffer: {:x?}", &kv.buffer.as_slice());

    Ok(kv)
  }

  /// Create a new KV store with state loaded from the provided buffer. The buffer is expected to
  /// already contain a properly formatted KV store file.
  pub fn from_buffer(buffer: Box<dyn ByteBuffer>) -> Result<Self, ResilientKvError> {
    let buffer_slice = buffer.as_slice();

    if buffer_slice.len() < 16 {
      return Err(ResilientKvError::BufferSizeError(format!(
        "Buffer too small: {} bytes, need at least 16 bytes for header",
        buffer_slice.len()
      )));
    }

    let version_bytes: [u8; 8] = buffer_slice[.. 8]
      .try_into()
      .map_err(|_| ResilientKvError::BufferSizeError("Failed to read version bytes".to_string()))?;

    let position_bytes: [u8; 8] = buffer_slice[8 .. 16].try_into().map_err(|_| {
      ResilientKvError::BufferSizeError("Failed to read position bytes".to_string())
    })?;

    let position_value = u64::from_le_bytes(position_bytes);
    let position_usize = usize::try_from(position_value).map_err(|_| {
      ResilientKvError::BufferSizeError(format!("Position value too large: {position_value}"))
    })?;

    let kv = Self {
      version: u64::from_le_bytes(version_bytes),
      position: position_usize,
      buffer,
    };

    if kv.version != VERSION {
      return Err(ResilientKvError::DecodingError(format!(
        "Unsupported version: {}, expected {}",
        kv.version, VERSION
      )));
    }
    if kv.position >= kv.buffer.as_slice().len() {
      return Err(ResilientKvError::BufferSizeError(format!(
        "Invalid position: {}, buffer size: {}",
        kv.position,
        kv.buffer.as_slice().len()
      )));
    }

    Ok(kv)
  }

  /// Create a new KV store from an existing one by copying its current state into the provided
  /// buffer.
  /// All journal entries from the old store will be replayed, resulting in a single journal entry
  /// in the new KV store. The buffer will be overwritten.
  pub fn from_kv_store(
    buffer: Box<dyn ByteBuffer>,
    kv_store: &mut Self,
  ) -> Result<Self, ResilientKvError> {
    let mut kv = Self::new(buffer)?;
    let mut position = kv.position;
    position += serialize_map_begin(kv.buffer_at_position(position)).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize map begin: {e:?}"))
    })?;
    let hashmap = kv_store.as_hashmap()?;
    for (k, v) in hashmap {
      position += serialize_string(kv.buffer_at_position(position), &k).map_err(|e| {
        ResilientKvError::SerializationError(format!("Failed to serialize string key: {e:?}"))
      })?;
      position += encode_into(kv.buffer_at_position(position), &v)
        .map_err(|e| ResilientKvError::EncodingError(format!("Failed to encode value: {e:?}")))?;
    }
    position += serialize_container_end(kv.buffer_at_position(position)).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize container end: {e:?}"))
    })?;
    kv.set_position(position);

    Ok(kv)
  }

  fn set_version(&mut self, version: u64) {
    self.version = version;
    self.set_bytes(0, &version.to_le_bytes());
  }

  fn set_position(&mut self, position: usize) {
    self.position = position;
    self.set_bytes(8, &(position as u64).to_le_bytes());
  }

  fn buffer_at_position(&mut self, position: usize) -> &mut [u8] {
    &mut self.buffer.as_mutable_slice()[position ..]
  }

  fn set_bytes(&mut self, position: usize, bytes: &[u8]) {
    let buffer = self.buffer_at_position(position);
    buffer[.. bytes.len()].copy_from_slice(bytes);
  }

  fn write_journal_entry(&mut self, key: &str, value: &Value) -> Result<(), ResilientKvError> {
    let mut position = self.position;
    // Fill in the map containing the next journal entry
    position += serialize_map_begin(self.buffer_at_position(position)).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize map begin: {e:?}"))
    })?;
    position += serialize_string(self.buffer_at_position(position), key).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize string key: {e:?}"))
    })?;
    position += encode_into(self.buffer_at_position(position), value)
      .map_err(|e| ResilientKvError::EncodingError(format!("Failed to encode value: {e:?}")))?;
    position += serialize_container_end(self.buffer_at_position(position)).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize container end: {e:?}"))
    })?;
    // Then update position to commit the change
    self.set_position(position);
    Ok(())
  }

  /// Set key to value in this kv store.
  /// This will create a new journal entry.
  /// Note: Setting to `Value::Null` will mark the entry for DELETION!
  pub fn set(&mut self, key: &str, value: &Value) -> Result<(), ResilientKvError> {
    self.write_journal_entry(key, value)
  }

  /// Delete a key from this kv store.
  /// This will create a new journal entry.
  pub fn delete(&mut self, key: &str) -> Result<(), ResilientKvError> {
    self.set(key, &Value::Null)
  }

  /// Get the current state of the kv store as a `HashMap`.
  pub fn as_hashmap(&mut self) -> Result<HashMap<String, Value>, ResilientKvError> {
    // Recall that the beginning of a kv store has an "array open" byte (at position 16). We close
    // it here by inserting a "container end" byte at `self.position` (which points to one past
    // the end of the last committed change). Then the entire journal can be read as a single
    // BONJSON document consisting of an array of journal entries.
    // Inserting this byte won't affect the key-value store's operation, because anything in
    // `self.buffer` from `self.position` onward is considered "garbage".
    serialize_container_end(self.buffer_at_position(self.position)).map_err(|e| {
      ResilientKvError::SerializationError(format!("Failed to serialize container end: {e:?}"))
    })?;
    // println!("Buffer, full: {:x?}", self.buffer.as_slice());
    let buffer = self.buffer_at_position(16);
    // println!("Buffer, pos 16: {:x?}", buffer);
    let decoded: Value = decode(buffer)
      .map_err(|e| ResilientKvError::DecodingError(format!("Failed to decode buffer: {e:?}")))?;
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
