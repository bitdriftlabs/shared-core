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
use bytes::BufMut;
use std::collections::HashMap;

/// Encodes a `Value` into BONJSON format and returns the modified buffer.
///
/// This function will automatically resize the passed-in buffer
/// as needed to accommodate the encoded data.
///
/// Note: The buffer will automatically reserve 1024 bytes if it's smaller.
///
/// # Arguments
/// * `buffer` - The buffer to use for encoding (will be cleared and reused)
/// * `value` - The value to encode
///
/// # Returns
/// * `Ok(&mut Vec<u8>)` - The encoded buffer on success
/// * `Err(SerializationError)` - If encoding fails
pub fn encode<'a>(
  buffer: &'a mut Vec<u8>,
  value: &Value,
) -> Result<&'a mut Vec<u8>, SerializationError> {
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
    match encode_into(buffer.as_mut(), value) {
      Ok(bytes_written) => {
        // Success! Truncate to actual size and return
        buffer.truncate(bytes_written);
        return Ok(buffer);
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
  encode_value_into_buf(&mut (&mut *buffer), value)
}

/// Encodes a `Value` into BONJSON format using a `BufMut`.
///
/// This function writes BONJSON data directly to a `BufMut` implementation,
/// which can automatically track position and provide better ergonomics.
///
/// # Arguments
/// * `buf` - The mutable buffer that implements `BufMut`
/// * `value` - The value to encode
///
/// # Returns
/// * `Ok(usize)` - The number of bytes written on success
/// * `Err(SerializationError)` - If encoding fails (including buffer full)
pub fn encode_into_buf<B: BufMut>(buf: &mut B, value: &Value) -> Result<usize, SerializationError> {
  encode_value_into_buf(buf, value)
}

/// Encodes a value into a buffer using `BufMut`.
fn encode_value_into_buf<B: BufMut>(buf: &mut B, value: &Value) -> Result<usize, SerializationError> {
  let start_remaining = buf.remaining_mut();
  match value {
    Value::Null => serialize_null(buf)?,
    Value::Bool(b) => serialize_boolean(buf, *b)?,
    Value::Float(f) => serialize_f64(buf, *f)?,
    Value::Signed(i) => serialize_i64(buf, *i)?,
    Value::Unsigned(u) => serialize_u64(buf, *u)?,
    Value::String(s) => encode_string_into_buf(buf, s)?,
    Value::Array(arr) => encode_array_into_buf(buf, arr)?,
    Value::Object(obj) => encode_object_into_buf(buf, obj)?,
  };
  Ok(start_remaining - buf.remaining_mut())
}

/// Encodes an array into a buffer using `BufMut`.
fn encode_array_into_buf<B: BufMut>(buf: &mut B, arr: &[Value]) -> Result<usize, SerializationError> {
  let start_remaining = buf.remaining_mut();

  // Array start marker
  serialize_array_begin(buf)?;

  // Encode each element
  for item in arr {
    encode_value_into_buf(buf, item)?;
  }

  // Array end marker
  serialize_container_end(buf)?;

  Ok(start_remaining - buf.remaining_mut())
}

/// Encodes an object into a buffer using `BufMut`.
fn encode_object_into_buf<B: BufMut>(
  buf: &mut B,
  obj: &HashMap<String, Value>,
) -> Result<usize, SerializationError> {
  let start_remaining = buf.remaining_mut();

  // Object start marker
  serialize_map_begin(buf)?;

  // Encode each key-value pair
  for (key, value) in obj {
    // Encode key as string
    encode_string_into_buf(buf, key)?;

    // Encode value
    encode_value_into_buf(buf, value)?;
  }

  // Object end marker
  serialize_container_end(buf)?;

  Ok(start_remaining - buf.remaining_mut())
}

/// Encodes a string into a buffer using `BufMut`.
fn encode_string_into_buf<B: BufMut>(buf: &mut B, s: &str) -> Result<usize, SerializationError> {
  let start_remaining = buf.remaining_mut();

  // Serialize header
  serialize_string_header(buf, s)?;

  // Check if there's enough space for the string data
  if buf.remaining_mut() < s.len() {
    return Err(SerializationError::BufferFull);
  }

  // Write string data
  buf.put_slice(s.as_bytes());

  Ok(start_remaining - buf.remaining_mut())
}
