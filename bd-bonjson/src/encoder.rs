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

/// Encodes a `Value` into BONJSON format and returns the resulting bytes.
///
/// This function manages its own buffer allocation and automatically resizes
/// as needed to accommodate the encoded data.
///
/// # Arguments
/// * `buffer` - The buffer to use for encoding (will be cleared and reused)
/// * `value` - The value to encode
///
/// # Returns
/// * `Ok(Vec<u8>)` - The encoded bytes on success
/// * `Err(SerializationError)` - If encoding fails
pub fn encode(buffer: &mut Vec<u8>, value: &Value) -> Result<Vec<u8>, SerializationError> {
  // Start with a reasonable initial capacity
  const INITIAL_CAPACITY: usize = 1024;

  buffer.clear();
  if buffer.capacity() < INITIAL_CAPACITY {
    buffer.reserve(INITIAL_CAPACITY);
  }

  loop {
    // Resize buffer to current capacity
    buffer.resize(buffer.capacity(), 0);

    // Try to encode in place
    match encode_into(buffer, value) {
      Ok(bytes_written) => {
        // Success! Truncate to actual size and return
        buffer.truncate(bytes_written);
        return Ok(buffer.clone());
      },
      Err(SerializationError::BufferFull) => {
        // Buffer too small, double the capacity and try again
        let new_capacity = buffer.capacity() * 2;
        buffer.reserve(new_capacity - buffer.capacity());
      },
      Err(e) => {
        // Other error, propagate it
        return Err(e);
      },
    }
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
pub fn encode_into(buffer: &mut [u8], value: &Value) -> Result<usize, SerializationError> {
  encode_value_into(buffer, value)
}

/// Writes bytes to the buffer and returns the number of bytes written.
fn write_bytes_into(buffer: &mut [u8], data: &[u8]) -> Result<usize, SerializationError> {
  let bytes_needed = data.len();
  if bytes_needed > buffer.len() {
    return Err(SerializationError::BufferFull);
  }

  buffer[.. bytes_needed].copy_from_slice(data);
  Ok(bytes_needed)
}

/// Encodes a value into a buffer.
fn encode_value_into(buffer: &mut [u8], value: &Value) -> Result<usize, SerializationError> {
  match value {
    Value::Null => {
      let mut temp_buffer: [u8; 1] = [0; 1];
      let size = serialize_null(&mut temp_buffer)?;
      write_bytes_into(buffer, &temp_buffer[.. size])
    },
    Value::Bool(b) => {
      let mut temp_buffer: [u8; 1] = [0; 1];
      let size = serialize_boolean(&mut temp_buffer, *b)?;
      write_bytes_into(buffer, &temp_buffer[.. size])
    },
    Value::Float(f) => {
      let mut temp_buffer: [u8; 16] = [0; 16];
      let size = serialize_f64(&mut temp_buffer, *f)?;
      write_bytes_into(buffer, &temp_buffer[.. size])
    },
    Value::Signed(i) => {
      let mut temp_buffer: [u8; 16] = [0; 16];
      let size = serialize_i64(&mut temp_buffer, *i)?;
      write_bytes_into(buffer, &temp_buffer[.. size])
    },
    Value::Unsigned(u) => {
      let mut temp_buffer: [u8; 16] = [0; 16];
      let size = serialize_u64(&mut temp_buffer, *u)?;
      write_bytes_into(buffer, &temp_buffer[.. size])
    },
    Value::String(s) => encode_string_into(buffer, s),
    Value::Array(arr) => encode_array_into(buffer, arr),
    Value::Object(obj) => encode_object_into(buffer, obj),
  }
}

/// Encodes an array into a buffer.
fn encode_array_into(buffer: &mut [u8], arr: &[Value]) -> Result<usize, SerializationError> {
  let mut position = 0;

  // Array start marker
  let mut temp_buffer: [u8; 1] = [0; 1];
  let size = serialize_array_begin(&mut temp_buffer)?;
  if position + size > buffer.len() {
    return Err(SerializationError::BufferFull);
  }
  buffer[position .. position + size].copy_from_slice(&temp_buffer[.. size]);
  position += size;

  // Encode each element
  for item in arr {
    let bytes_written = encode_value_into(&mut buffer[position ..], item)?;
    position += bytes_written;
  }

  // Array end marker
  let size = serialize_container_end(&mut temp_buffer)?;
  if position + size > buffer.len() {
    return Err(SerializationError::BufferFull);
  }
  buffer[position .. position + size].copy_from_slice(&temp_buffer[.. size]);
  position += size;

  Ok(position)
}

/// Encodes an object into a buffer.
fn encode_object_into(
  buffer: &mut [u8],
  obj: &HashMap<String, Value>,
) -> Result<usize, SerializationError> {
  let mut position = 0;

  // Object start marker
  let mut temp_buffer: [u8; 1] = [0; 1];
  let size = serialize_map_begin(&mut temp_buffer)?;
  if position + size > buffer.len() {
    return Err(SerializationError::BufferFull);
  }
  buffer[position .. position + size].copy_from_slice(&temp_buffer[.. size]);
  position += size;

  // Encode each key-value pair
  for (key, value) in obj {
    // Encode key as string
    let bytes_written = encode_string_into(&mut buffer[position ..], key)?;
    position += bytes_written;

    // Encode value
    let bytes_written = encode_value_into(&mut buffer[position ..], value)?;
    position += bytes_written;
  }

  // Object end marker
  let size = serialize_container_end(&mut temp_buffer)?;
  if position + size > buffer.len() {
    return Err(SerializationError::BufferFull);
  }
  buffer[position .. position + size].copy_from_slice(&temp_buffer[.. size]);
  position += size;

  Ok(position)
}

/// Encodes a string into a buffer.
fn encode_string_into(buffer: &mut [u8], s: &str) -> Result<usize, SerializationError> {
  let mut temp_buffer: [u8; 16] = [0; 16];
  let header_size = serialize_string_header(&mut temp_buffer, s)?;

  let total_needed = header_size + s.len();
  if total_needed > buffer.len() {
    return Err(SerializationError::BufferFull);
  }

  // Write header
  buffer[.. header_size].copy_from_slice(&temp_buffer[.. header_size]);
  // Write string data
  buffer[header_size .. header_size + s.len()].copy_from_slice(s.as_bytes());

  Ok(total_needed)
}
