// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./encoder_test.rs"]
mod encoder_test;

use crate::Value;
use crate::serialize_primitives::{
  SerializationError,
  serialize_array_begin,
  serialize_boolean,
  serialize_container_end,
  serialize_f64,
  serialize_i64,
  serialize_map_begin,
  serialize_null,
  serialize_string_header,
  serialize_u64,
};
use std::collections::HashMap;

/// An encoder for converting `Value` instances into BONJSON byte format.
pub struct Encoder {
  buffer: Vec<u8>,
}

impl Encoder {
  #[must_use]
  pub fn new() -> Self {
    Self { buffer: Vec::new() }
  }

  #[must_use]
  pub fn with_capacity(capacity: usize) -> Self {
    Self {
      buffer: Vec::with_capacity(capacity),
    }
  }

  /// Encodes a `Value` into BONJSON format and returns the resulting bytes.
  ///
  /// # Arguments
  /// * `value` - The value to encode
  ///
  /// # Returns
  /// * `Ok(Vec<u8>)` - The encoded bytes on success
  /// * `Err(SerializationError)` - If encoding fails
  pub fn encode(&mut self, value: &Value) -> Result<Vec<u8>, SerializationError> {
    self.buffer.clear();
    self.encode_value(value)?;
    Ok(self.buffer.clone())
  }

  /// Encodes a value into the internal buffer.
  fn encode_value(&mut self, value: &Value) -> Result<(), SerializationError> {
    match value {
      Value::Null => {
        let mut temp_buffer: [u8; 1] = [0; 1];
        let size = serialize_null(&mut temp_buffer)?;
        self.buffer.extend_from_slice(&temp_buffer[.. size]);
      },
      Value::Bool(b) => {
        let mut temp_buffer: [u8; 1] = [0; 1];
        let size = serialize_boolean(&mut temp_buffer, *b)?;
        self.buffer.extend_from_slice(&temp_buffer[.. size]);
      },
      Value::Float(f) => {
        let mut temp_buffer: [u8; 16] = [0; 16];
        let size = serialize_f64(&mut temp_buffer, *f)?;
        self.buffer.extend_from_slice(&temp_buffer[.. size]);
      },
      Value::Signed(i) => {
        let mut temp_buffer: [u8; 16] = [0; 16];
        let size = serialize_i64(&mut temp_buffer, *i)?;
        self.buffer.extend_from_slice(&temp_buffer[.. size]);
      },
      Value::Unsigned(u) => {
        let mut temp_buffer: [u8; 16] = [0; 16];
        let size = serialize_u64(&mut temp_buffer, *u)?;
        self.buffer.extend_from_slice(&temp_buffer[.. size]);
      },
      Value::String(s) => {
        self.encode_string(s)?;
      },
      Value::Array(arr) => {
        self.encode_array(arr)?;
      },
      Value::Object(obj) => {
        self.encode_object(obj)?;
      },
    }
    Ok(())
  }

  /// Encodes a string value into the buffer.
  fn encode_string(&mut self, s: &str) -> Result<(), SerializationError> {
    // String header
    let mut temp_buffer: [u8; 16] = [0; 16]; // Should be enough for any string header
    let header_size = serialize_string_header(&mut temp_buffer, s)?;
    self.buffer.extend_from_slice(&temp_buffer[.. header_size]);

    // String content
    self.buffer.extend_from_slice(s.as_bytes());

    Ok(())
  }

  /// Encodes an array value.
  fn encode_array(&mut self, arr: &[Value]) -> Result<(), SerializationError> {
    // Array start marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_array_begin(&mut temp_buffer)?;
    self.buffer.extend_from_slice(&temp_buffer[.. size]);

    // Encode each element
    for item in arr {
      self.encode_value(item)?;
    }

    // Array end marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_container_end(&mut temp_buffer)?;
    self.buffer.extend_from_slice(&temp_buffer[.. size]);

    Ok(())
  }

  /// Encodes an object value.
  fn encode_object(&mut self, obj: &HashMap<String, Value>) -> Result<(), SerializationError> {
    // Object start marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_map_begin(&mut temp_buffer)?;
    self.buffer.extend_from_slice(&temp_buffer[.. size]);

    // Encode each key-value pair
    for (key, value) in obj {
      // Encode key as string
      self.encode_string(key)?;
      // Encode value
      self.encode_value(value)?;
    }

    // Object end marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_container_end(&mut temp_buffer)?;
    self.buffer.extend_from_slice(&temp_buffer[.. size]);

    Ok(())
  }

  /// Returns the current buffer contents without consuming the encoder.
  #[must_use]
  pub fn buffer(&self) -> &[u8] {
    &self.buffer
  }

  /// Clears the internal buffer for reuse.
  pub fn clear(&mut self) {
    self.buffer.clear();
  }
}

impl Default for Encoder {
  fn default() -> Self {
    Self::new()
  }
}

/// An in-place encoder for converting `Value` instances into BONJSON byte format
/// using a pre-allocated slice.
pub struct InPlaceEncoder {
  position: usize,
}

impl InPlaceEncoder {
  /// Creates a new in-place encoder.
  ///
  /// # Returns
  /// A new `InPlaceEncoder` instance
  #[must_use]
  pub fn new() -> Self {
    Self { position: 0 }
  }

  /// Encodes a `Value` into BONJSON format in the provided buffer.
  ///
  /// # Arguments
  /// * `buffer` - The mutable slice to encode into
  /// * `value` - The value to encode
  ///
  /// # Returns
  /// * `Ok(usize)` - The number of bytes written on success
  /// * `Err(SerializationError)` - If encoding fails (including buffer full)
  pub fn encode(&mut self, buffer: &mut [u8], value: &Value) -> Result<usize, SerializationError> {
    self.position = 0;
    self.encode_value(buffer, value)?;
    Ok(self.position)
  }

  /// Writes bytes to the buffer at the current position.
  fn write_bytes(&mut self, buffer: &mut [u8], data: &[u8]) -> Result<(), SerializationError> {
    let bytes_needed = data.len();
    if self.position + bytes_needed > buffer.len() {
      return Err(SerializationError::BufferFull);
    }
    
    buffer[self.position..self.position + bytes_needed].copy_from_slice(data);
    self.position += bytes_needed;
    Ok(())
  }

  /// Encodes a value into the buffer at the current position.
  fn encode_value(&mut self, buffer: &mut [u8], value: &Value) -> Result<(), SerializationError> {
    match value {
      Value::Null => {
        let mut temp_buffer: [u8; 1] = [0; 1];
        let size = serialize_null(&mut temp_buffer)?;
        self.write_bytes(buffer, &temp_buffer[..size])?;
      },
      Value::Bool(b) => {
        let mut temp_buffer: [u8; 1] = [0; 1];
        let size = serialize_boolean(&mut temp_buffer, *b)?;
        self.write_bytes(buffer, &temp_buffer[..size])?;
      },
      Value::Float(f) => {
        let mut temp_buffer: [u8; 16] = [0; 16];
        let size = serialize_f64(&mut temp_buffer, *f)?;
        self.write_bytes(buffer, &temp_buffer[..size])?;
      },
      Value::Signed(i) => {
        let mut temp_buffer: [u8; 16] = [0; 16];
        let size = serialize_i64(&mut temp_buffer, *i)?;
        self.write_bytes(buffer, &temp_buffer[..size])?;
      },
      Value::Unsigned(u) => {
        let mut temp_buffer: [u8; 16] = [0; 16];
        let size = serialize_u64(&mut temp_buffer, *u)?;
        self.write_bytes(buffer, &temp_buffer[..size])?;
      },
      Value::String(s) => {
        self.encode_string(buffer, s)?;
      },
      Value::Array(arr) => {
        self.encode_array(buffer, arr)?;
      },
      Value::Object(obj) => {
        self.encode_object(buffer, obj)?;
      },
    }
    Ok(())
  }

  /// Encodes a string value into the buffer.
  fn encode_string(&mut self, buffer: &mut [u8], s: &str) -> Result<(), SerializationError> {
    // String header
    let mut temp_buffer: [u8; 16] = [0; 16]; // Should be enough for any string header
    let header_size = serialize_string_header(&mut temp_buffer, s)?;
    self.write_bytes(buffer, &temp_buffer[..header_size])?;

    // String content
    self.write_bytes(buffer, s.as_bytes())?;

    Ok(())
  }

  /// Encodes an array value.
  fn encode_array(&mut self, buffer: &mut [u8], arr: &[Value]) -> Result<(), SerializationError> {
    // Array start marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_array_begin(&mut temp_buffer)?;
    self.write_bytes(buffer, &temp_buffer[..size])?;

    // Encode each element
    for item in arr {
      self.encode_value(buffer, item)?;
    }

    // Array end marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_container_end(&mut temp_buffer)?;
    self.write_bytes(buffer, &temp_buffer[..size])?;

    Ok(())
  }

  /// Encodes an object value.
  fn encode_object(&mut self, buffer: &mut [u8], obj: &HashMap<String, Value>) -> Result<(), SerializationError> {
    // Object start marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_map_begin(&mut temp_buffer)?;
    self.write_bytes(buffer, &temp_buffer[..size])?;

    // Encode each key-value pair
    for (key, value) in obj {
      // Encode key as string
      self.encode_string(buffer, key)?;
      // Encode value
      self.encode_value(buffer, value)?;
    }

    // Object end marker
    let mut temp_buffer: [u8; 1] = [0; 1];
    let size = serialize_container_end(&mut temp_buffer)?;
    self.write_bytes(buffer, &temp_buffer[..size])?;

    Ok(())
  }
}

impl Default for InPlaceEncoder {
  fn default() -> Self {
    Self::new()
  }
}
