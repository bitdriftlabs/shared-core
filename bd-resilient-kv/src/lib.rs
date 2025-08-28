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
use bd_bonjson::serialize_primitives::{serialize_array_begin, serialize_container_end, serialize_map_begin, serialize_string};
use std::collections::HashMap;

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

/// A crash-resilient key-value store that can be recovered even if writing is interrupted.
pub struct ResilientKv {
  version: u64,
  position: usize,
  buffer: Box<dyn ByteBuffer>,
}

impl ResilientKv {
  #[must_use]
  pub fn new(buffer: Box<dyn ByteBuffer>) -> Self {
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

    kv.set_version(1);
    let mut position = 16;
    position += serialize_array_begin(kv.buffer_at_position(position)).unwrap();
    kv.set_position(position);
    // println!("Initial buffer: {:x?}", &kv.buffer.as_slice());

    kv
  }

  pub fn from_kv_store(buffer: Box<dyn ByteBuffer>, kv_store: &mut Self) -> Self {
    let mut kv = Self::new(buffer);
    let mut position = kv.position;
    position += serialize_map_begin(kv.buffer_at_position(position)).unwrap();
    for (k, v) in kv_store.as_hashmap() {
      position += serialize_string(kv.buffer_at_position(position), &k).unwrap();
      position += encode_into(kv.buffer_at_position(position), &v).unwrap();
    }
    position += serialize_container_end(kv.buffer_at_position(position)).unwrap();
    kv.set_position(position);

    kv
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

  fn write_journal_entry(&mut self, key: &str, value: &Value) {
    let mut position = self.position;
    // Fill in the map containing the next journal entry
    position += serialize_map_begin(self.buffer_at_position(position)).unwrap();
    position += serialize_string(self.buffer_at_position(position), key).unwrap();
    position += encode_into(self.buffer_at_position(position), value).unwrap();
    position += serialize_container_end(self.buffer_at_position(position)).unwrap();
    // Then update position to commit the change
    self.set_position(position);
  }

  /// Set key to value in this kv store.
  /// This will create a new journal entry.
  pub fn set(&mut self, key: &str, value: &Value) {
    self.write_journal_entry(key, value);
  }

  /// Delete a key from this kv store.
  /// This will create a new journal entry.
  pub fn delete(&mut self, key: &str) {
    self.set(key, &Value::Null);
  }

  /// Get the current state of the kv store as a `HashMap`.
  pub fn as_hashmap(&mut self) -> HashMap<String, Value> {
    // Position points to one past the end of the last committed change.
    // Recall that the beginning of a kv store has an "array open" (at position 16). We close it here.
    // Then the entire journal can be read as a single BONJSON document consisting of an array of journal entries.
    serialize_container_end(self.buffer_at_position(self.position)).unwrap();
    // println!("Buffer, full: {:x?}", self.buffer.as_slice());
    let buffer = self.buffer_at_position(16);
    // println!("Buffer, pos 16: {:x?}", buffer);
    let decoded: Value = decode(buffer).unwrap();
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

    map
  }
}
