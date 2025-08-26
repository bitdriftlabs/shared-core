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

/// Encodes a `Value` into BONJSON format in the provided buffer.
///
/// This function writes BONJSON data directly to a mutable slice without
/// allocating additional memory. It's useful for embedded systems or other
/// scenarios where memory allocation should be avoided.
///
/// # Arguments
/// * `buffer` - The mutable slice to encode into
/// * `value` - The value to encode
///
/// # Returns
/// * `Ok(usize)` - The number of bytes written on success
/// * `Err(SerializationError)` - If encoding fails (including buffer full)
pub fn encode_in_place(buffer: &mut [u8], value: &Value) -> Result<usize, SerializationError> {
  let mut position = 0;
  encode_value_in_place(buffer, value, &mut position)?;
  Ok(position)
}

/// Writes bytes to the buffer at the current position.
fn write_bytes_in_place(buffer: &mut [u8], data: &[u8], position: &mut usize) -> Result<(), SerializationError> {
  let bytes_needed = data.len();
  if *position + bytes_needed > buffer.len() {
    return Err(SerializationError::BufferFull);
  }
  
  buffer[*position..*position + bytes_needed].copy_from_slice(data);
  *position += bytes_needed;
  Ok(())
}

/// Encodes a value into the buffer at the current position.
fn encode_value_in_place(buffer: &mut [u8], value: &Value, position: &mut usize) -> Result<(), SerializationError> {
  match value {
    Value::Null => {
      let mut temp_buffer: [u8; 1] = [0; 1];
      let size = serialize_null(&mut temp_buffer)?;
      write_bytes_in_place(buffer, &temp_buffer[..size], position)?;
    },
    Value::Bool(b) => {
      let mut temp_buffer: [u8; 1] = [0; 1];
      let size = serialize_boolean(&mut temp_buffer, *b)?;
      write_bytes_in_place(buffer, &temp_buffer[..size], position)?;
    },
    Value::Float(f) => {
      let mut temp_buffer: [u8; 16] = [0; 16];
      let size = serialize_f64(&mut temp_buffer, *f)?;
      write_bytes_in_place(buffer, &temp_buffer[..size], position)?;
    },
    Value::Signed(i) => {
      let mut temp_buffer: [u8; 16] = [0; 16];
      let size = serialize_i64(&mut temp_buffer, *i)?;
      write_bytes_in_place(buffer, &temp_buffer[..size], position)?;
    },
    Value::Unsigned(u) => {
      let mut temp_buffer: [u8; 16] = [0; 16];
      let size = serialize_u64(&mut temp_buffer, *u)?;
      write_bytes_in_place(buffer, &temp_buffer[..size], position)?;
    },
    Value::String(s) => {
      encode_string_in_place(buffer, s, position)?;
    },
    Value::Array(arr) => {
      encode_array_in_place(buffer, arr, position)?;
    },
    Value::Object(obj) => {
      encode_object_in_place(buffer, obj, position)?;
    },
  }
  Ok(())
}
/// Encodes a string value into the buffer.
fn encode_string_in_place(buffer: &mut [u8], s: &str, position: &mut usize) -> Result<(), SerializationError> {
  // String header
  let mut temp_buffer: [u8; 16] = [0; 16]; // Should be enough for any string header
  let header_size = serialize_string_header(&mut temp_buffer, s)?;
  write_bytes_in_place(buffer, &temp_buffer[..header_size], position)?;

  // String content
  write_bytes_in_place(buffer, s.as_bytes(), position)?;

  Ok(())
}

/// Encodes an array value.
fn encode_array_in_place(buffer: &mut [u8], arr: &[Value], position: &mut usize) -> Result<(), SerializationError> {
  // Array start marker
  let mut temp_buffer: [u8; 1] = [0; 1];
  let size = serialize_array_begin(&mut temp_buffer)?;
  write_bytes_in_place(buffer, &temp_buffer[..size], position)?;

  // Encode each element
  for item in arr {
    encode_value_in_place(buffer, item, position)?;
  }

  // Array end marker
  let mut temp_buffer: [u8; 1] = [0; 1];
  let size = serialize_container_end(&mut temp_buffer)?;
  write_bytes_in_place(buffer, &temp_buffer[..size], position)?;

  Ok(())
}

/// Encodes an object value.
fn encode_object_in_place(buffer: &mut [u8], obj: &HashMap<String, Value>, position: &mut usize) -> Result<(), SerializationError> {
  // Object start marker
  let mut temp_buffer: [u8; 1] = [0; 1];
  let size = serialize_map_begin(&mut temp_buffer)?;
  write_bytes_in_place(buffer, &temp_buffer[..size], position)?;

  // Encode each key-value pair
  for (key, value) in obj {
    // Encode key as string
    encode_string_in_place(buffer, key, position)?;
    // Encode value
    encode_value_in_place(buffer, value, position)?;
  }

  // Object end marker
  let mut temp_buffer: [u8; 1] = [0; 1];
  let size = serialize_container_end(&mut temp_buffer)?;
  write_bytes_in_place(buffer, &temp_buffer[..size], position)?;

  Ok(())
}
