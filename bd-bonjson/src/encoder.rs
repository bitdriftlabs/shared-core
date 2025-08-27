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

/// Encodes a value into a buffer.
fn encode_value_into(buffer: &mut [u8], value: &Value) -> Result<usize, SerializationError> {
  match value {
    Value::Null => serialize_null(buffer),
    Value::Bool(b) => serialize_boolean(buffer, *b),
    Value::Float(f) => serialize_f64(buffer, *f),
    Value::Signed(i) => serialize_i64(buffer, *i),
    Value::Unsigned(u) => serialize_u64(buffer, *u),
    Value::String(s) => encode_string_into(buffer, s),
    Value::Array(arr) => encode_array_into(buffer, arr),
    Value::Object(obj) => encode_object_into(buffer, obj),
  }
}

/// Encodes an array into a buffer.
fn encode_array_into(buffer: &mut [u8], arr: &[Value]) -> Result<usize, SerializationError> {
  let mut position = 0;

  // Array start marker
  let size = serialize_array_begin(&mut buffer[position ..])?;
  position += size;

  // Encode each element
  for item in arr {
    let bytes_written = encode_value_into(&mut buffer[position ..], item)?;
    position += bytes_written;
  }

  // Array end marker
  let size = serialize_container_end(&mut buffer[position ..])?;
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
  let size = serialize_map_begin(&mut buffer[position ..])?;
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
  let size = serialize_container_end(&mut buffer[position ..])?;
  position += size;

  Ok(position)
}

/// Encodes a string into a buffer.
fn encode_string_into(buffer: &mut [u8], s: &str) -> Result<usize, SerializationError> {
  // Serialize header directly into the output buffer
  let header_size = serialize_string_header(buffer, s)?;

  // Check if there's enough space for the string data
  if header_size + s.len() > buffer.len() {
    return Err(SerializationError::BufferFull);
  }

  // Write string data after the header
  buffer[header_size .. header_size + s.len()].copy_from_slice(s.as_bytes());

  Ok(header_size + s.len())
}
