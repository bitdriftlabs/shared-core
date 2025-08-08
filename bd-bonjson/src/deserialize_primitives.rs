// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::type_codes::TypeCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeserializationError {
  ContinuationBitNotSupported,
  ExpectedArray,
  ExpectedBoolean,
  ExpectedFloat,
  ExpectedFloat32,
  ExpectedMap,
  ExpectedNull,
  ExpectedSignedInteger,
  ExpectedString,
  ExpectedUnsignedInteger,
  InvalidUTF8,
  LongNumberNotSupported,
  NonStringKeyInMap,
  PrematureEnd,
  UnexpectedTypeCode,
}

pub type Result<T> = std::result::Result<T, DeserializationError>;

fn require_bytes(src: &[u8], byte_count: usize) -> Result<()> {
  if byte_count > src.len() {
    Err(DeserializationError::PrematureEnd)
  } else {
    Ok(())
  }
}

fn copy_bytes_to(src: &[u8], dst: &mut [u8], byte_count: usize) -> Result<()> {
  require_bytes(src, byte_count)?;
  dst[.. byte_count].copy_from_slice(&src[.. byte_count]);
  Ok(())
}

fn deserialize_byte(src: &[u8]) -> Result<(usize, u8)> {
  require_bytes(src, 1)?;
  Ok((1, src[0]))
}

fn deserialize_string_contents(src: &[u8], size: usize) -> Result<(usize, &str)> {
  require_bytes(src, size)?;
  let string = std::str::from_utf8(&src[.. size]).map_err(|_| DeserializationError::InvalidUTF8)?;
  Ok((size, string))
}

pub fn peek_type_code(src: &[u8]) -> Result<u8> {
  let (_, type_code) = deserialize_byte(src)?;
  Ok(type_code)
}

pub fn deserialize_type_code(src: &[u8]) -> Result<(usize, u8)> {
  deserialize_byte(src)
}

pub fn deserialize_unsigned_after_type_code(src: &[u8], type_code: u8) -> Result<(usize, u64)> {
  let byte_count = ((type_code & 7) + 1) as usize;
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(src, &mut bytes, byte_count)?;
  Ok((byte_count, u64::from_le_bytes(bytes)))
}

pub fn deserialize_signed_after_type_code(src: &[u8], type_code: u8) -> Result<(usize, i64)> {
  let byte_count = ((type_code & 7) + 1) as usize;
  require_bytes(src, byte_count)?;
  let is_negative = src[byte_count - 1] >> 7;
  let mut bytes: [u8; 8] = [is_negative * 0xff; 8];
  bytes[.. byte_count].copy_from_slice(&src[.. byte_count]);
  Ok((byte_count, i64::from_le_bytes(bytes)))
}

pub fn deserialize_f16_after_type_code(src: &[u8]) -> Result<(usize, f32)> {
  let mut bytes: [u8; 4] = [0; 4];
  // Note: Copying only 2 bytes into a 4 byte buffer because this is a bfloat.
  copy_bytes_to(src, &mut bytes[2 ..], 2)?;
  Ok((2, f32::from_le_bytes(bytes)))
}

pub fn deserialize_f32_after_type_code(src: &[u8]) -> Result<(usize, f32)> {
  let mut bytes: [u8; 4] = [0; 4];
  copy_bytes_to(src, &mut bytes, 4)?;
  Ok((4, f32::from_le_bytes(bytes)))
}

pub fn deserialize_f64_after_type_code(src: &[u8]) -> Result<(usize, f64)> {
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(src, &mut bytes, 8)?;
  Ok((8, f64::from_le_bytes(bytes)))
}

pub fn deserialize_short_string_after_type_code(
  src: &[u8],
  type_code: u8,
) -> Result<(usize, &str)> {
  deserialize_string_contents(src, type_code as usize - TypeCode::String as usize)
}

// Returns:
// - Number of bytes before the length payload begins
// - Number of bytes containing the length payload
// - Number of bits to shift right to yield the final length payload
fn decode_chunk_length_header(length_header: u8) -> (usize, usize, usize) {
  if (length_header & 0x01) == 0x01 {
    (0, 1, 1)
  } else if (length_header & 0x03) == 0x02 {
    (0, 2, 2)
  } else if (length_header & 0x07) == 0x04 {
    (0, 3, 3)
  } else if (length_header & 0x0f) == 0x08 {
    (0, 4, 4)
  } else if (length_header & 0x1f) == 0x10 {
    (0, 5, 5)
  } else if (length_header & 0x3f) == 0x20 {
    (0, 6, 6)
  } else if (length_header & 0x7f) == 0x40 {
    (0, 7, 7)
  } else if length_header == 0x80 {
    (0, 8, 8)
  } else {
    (1, 8, 0)
  }
}

// Returns:
// - Size of the header in bytes
// - Length of the chunk in fixed-size elements (usually bytes)
// - Continuation bit
fn deserialize_chunk_header(src: &[u8]) -> Result<(usize, usize, bool)> {
  let (_, length_header) = deserialize_byte(src)?;
  let (length_skip_size, length_payload_size, length_shift_by) =
    decode_chunk_length_header(length_header);
  let length_total_size = length_skip_size + length_payload_size;
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(&src[length_skip_size ..], &mut bytes, length_payload_size)?;
  let payload = u64::from_le_bytes(bytes) >> length_shift_by;
  let continuation_bit = (payload & 1) == 1;
  let length = payload >> 1;
  Ok((length_total_size, length as usize, continuation_bit))
}

pub fn deserialize_long_string_after_type_code(src: &[u8]) -> Result<(usize, &str)> {
  let (header_size, chunk_size, continuation_bit) = deserialize_chunk_header(src)?;
  if continuation_bit {
    // Note: Deliberately not supporting chunked strings since we don't use them.
    return Err(DeserializationError::ContinuationBitNotSupported);
  }
  let current = &src[header_size ..];
  require_bytes(current, chunk_size)?;
  let (v_size, v) = deserialize_string_contents(current, chunk_size)?;
  Ok((header_size + v_size, v))
}
