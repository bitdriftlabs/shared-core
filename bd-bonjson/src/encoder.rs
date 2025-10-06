// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./encoder_test.rs"]
mod encoder_test;

use crate::{Value, ValueRef};
use crate::serialize_primitives::{
  SerializationError,
  serialize_array_begin,
  serialize_boolean,
  serialize_container_end,
  serialize_f64,
  serialize_i64,
  serialize_map_begin,
  serialize_null,
  serialize_string,
  serialize_u64,
};
use bytes::BufMut;
use std::collections::HashMap;

/// Encodes a `Value` into a `&mut Vec<u8>`.
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
pub fn encode_into_vec<'a>(
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
    match encode_into_slice(buffer.as_mut(), value) {
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

/// Encodes a `Value` into a `&mut [u8]`.
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
pub fn encode_into_slice(buffer: &mut [u8], value: &Value) -> Result<usize, SerializationError> {
  encode_into_buf(&mut (&mut *buffer), value)
}

/// Encodes a `Value` into a `BufMut`.
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
  let start_remaining = buf.remaining_mut();
  encode_into_buf_impl(buf, value)?;
  Ok(start_remaining - buf.remaining_mut())
}

pub fn encode_into_buf_impl<B: BufMut>(
  buf: &mut B,
  value: &Value,
) -> Result<(), SerializationError> {
  match value {
    Value::Null => serialize_null(buf)?,
    Value::Bool(b) => serialize_boolean(buf, *b)?,
    Value::Float(f) => serialize_f64(buf, *f)?,
    Value::Signed(i) => serialize_i64(buf, *i)?,
    Value::Unsigned(u) => serialize_u64(buf, *u)?,
    Value::String(s) => serialize_string(buf, s)?,
    Value::Array(arr) => {
      encode_array_into_buf(buf, arr)?;
      0
    },
    Value::Object(obj) => {
      encode_object_into_buf(buf, obj)?;
      0
    },
  };
  Ok(())
}

/// Encodes an array into a buffer using `BufMut`.
fn encode_array_into_buf<B: BufMut>(buf: &mut B, arr: &[Value]) -> Result<(), SerializationError> {
  serialize_array_begin(buf)?;
  for item in arr {
    encode_into_buf_impl(buf, item)?;
  }
  serialize_container_end(buf)?;
  Ok(())
}

/// Encodes an object into a buffer using `BufMut`.
fn encode_object_into_buf<B: BufMut>(
  buf: &mut B,
  obj: &HashMap<String, Value>,
) -> Result<(), SerializationError> {
  serialize_map_begin(buf)?;
  for (key, value) in obj {
    serialize_string(buf, key)?;
    encode_into_buf_impl(buf, value)?;
  }
  serialize_container_end(buf)?;
  Ok(())
}

/// Encode a ValueRef into a BufMut without cloning.
/// This provides true zero-copy encoding for referenced data.
///
/// # Arguments
/// * `buf` - The mutable buffer that implements `BufMut`
/// * `value` - The ValueRef to encode
///
/// # Returns
/// * `Ok(usize)` - The number of bytes written on success
/// * `Err(SerializationError)` - If encoding fails (including buffer full)
pub fn encode_ref_into_buf<B: BufMut>(buf: &mut B, value: &ValueRef<'_>) -> Result<usize, SerializationError> {
  let start_remaining = buf.remaining_mut();
  encode_ref_into_buf_impl(buf, value)?;
  Ok(start_remaining - buf.remaining_mut())
}

/// Internal implementation for encoding ValueRef without cloning.
fn encode_ref_into_buf_impl<B: BufMut>(
  buf: &mut B,
  value: &ValueRef<'_>,
) -> Result<(), SerializationError> {
  match value {
    ValueRef::Null => serialize_null(buf)?,
    ValueRef::Bool(b) => serialize_boolean(buf, *b)?,
    ValueRef::Float(f) => serialize_f64(buf, *f)?,
    ValueRef::Signed(i) => serialize_i64(buf, *i)?,
    ValueRef::Unsigned(u) => serialize_u64(buf, *u)?,
    ValueRef::String(s) => serialize_string(buf, s)?,
    ValueRef::Array(arr) => {
      encode_array_ref_into_buf(buf, arr)?;
      0
    },
    ValueRef::Object(obj) => {
      encode_object_ref_into_buf(buf, obj)?;
      0
    },
  };
  Ok(())
}

/// Encodes an array reference into a buffer using `BufMut` without cloning.
fn encode_array_ref_into_buf<B: BufMut>(buf: &mut B, arr: &[Value]) -> Result<(), SerializationError> {
  serialize_array_begin(buf)?;
  for item in arr {
    encode_into_buf_impl(buf, item)?;
  }
  serialize_container_end(buf)?;
  Ok(())
}

/// Encodes an object reference into a buffer using `BufMut` without cloning.
fn encode_object_ref_into_buf<B: BufMut>(
  buf: &mut B,
  obj: &HashMap<String, Value>,
) -> Result<(), SerializationError> {
  serialize_map_begin(buf)?;
  for (key, value) in obj {
    serialize_string(buf, key)?;
    encode_into_buf_impl(buf, value)?;
  }
  serialize_container_end(buf)?;
  Ok(())
}
