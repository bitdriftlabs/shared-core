// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::type_codes::TypeCode;
use bytes::Buf;

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

fn require_bytes<B: Buf>(src: &B, byte_count: usize) -> Result<()> {
  if byte_count > src.remaining() {
    Err(DeserializationError::PrematureEnd)
  } else {
    Ok(())
  }
}

fn copy_bytes_to<B: Buf>(src: &mut B, dst: &mut [u8], byte_count: usize) -> Result<()> {
  require_bytes(src, byte_count)?;
  src.copy_to_slice(&mut dst[.. byte_count]);
  Ok(())
}

fn deserialize_byte<B: Buf>(src: &mut B) -> Result<u8> {
  require_bytes(src, 1)?;
  Ok(src.get_u8())
}

fn deserialize_string_contents<B: Buf>(src: &mut B, size: usize) -> Result<String> {
  require_bytes(src, size)?;
  let mut data = vec![0u8; size];
  src.copy_to_slice(&mut data);
  let string = std::str::from_utf8(&data).map_err(|_| DeserializationError::InvalidUTF8)?;
  Ok(string.to_string())
}

fn peek_byte<B: Buf>(src: &B) -> Result<u8> {
  require_bytes(src, 1)?;
  Ok(src.chunk()[0])
}

pub fn peek_type_code<B: Buf>(src: &B) -> Result<u8> {
  peek_byte(src)
}

pub fn deserialize_type_code<B: Buf>(src: &mut B) -> Result<u8> {
  deserialize_byte(src)
}

fn deserialize_uint_of_length<B: Buf>(src: &mut B, byte_count: usize) -> Result<u64> {
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(src, &mut bytes, byte_count)?;
  Ok(u64::from_le_bytes(bytes))
}

pub fn deserialize_unsigned_after_type_code<B: Buf>(src: &mut B, type_code: u8) -> Result<u64> {
  let byte_count = ((type_code & 7) + 1) as usize;
  deserialize_uint_of_length(src, byte_count)
}

pub fn deserialize_signed_after_type_code<B: Buf>(src: &mut B, type_code: u8) -> Result<i64> {
  let byte_count = ((type_code & 7) + 1) as usize;
  require_bytes(src, byte_count)?;
  let mut bytes: [u8; 8] = [0; 8];
  src.copy_to_slice(&mut bytes[.. byte_count]);
  if bytes[byte_count - 1] >> 7 != 0 {
    // If the sign bit is set, sign extend the rest of the bytes using 0xff.
    bytes[byte_count ..].fill(0xff);
  }
  Ok(i64::from_le_bytes(bytes))
}

pub fn deserialize_f16_after_type_code<B: Buf>(src: &mut B) -> Result<f32> {
  let mut bytes: [u8; 4] = [0; 4];
  // Note: Copying only 2 bytes into a 4 byte buffer because this is a bfloat.
  copy_bytes_to(src, &mut bytes[2 ..], 2)?;
  Ok(f32::from_le_bytes(bytes))
}

pub fn deserialize_f32_after_type_code<B: Buf>(src: &mut B) -> Result<f32> {
  require_bytes(src, 4)?;
  Ok(src.get_f32_le())
}

pub fn deserialize_f64_after_type_code<B: Buf>(src: &mut B) -> Result<f64> {
  require_bytes(src, 8)?;
  Ok(src.get_f64_le())
}

pub fn deserialize_short_string_after_type_code<B: Buf>(
  src: &mut B,
  type_code: u8,
) -> Result<String> {
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
// - Length of the chunk in fixed-size elements (usually bytes)
// - Continuation bit
fn deserialize_chunk_header<B: Buf>(src: &mut B) -> Result<(usize, bool)> {
  let length_header = peek_byte(src)?;
  let (length_skip_size, length_payload_size, length_shift_by) =
    decode_chunk_length_header(length_header);

  // Skip to the payload if necessary. `skip_size` is either 0 or 1 (see
  // `decode_chunk_length_header`). The `peek_byte()` call already checked for 1 byte, which is
  // the maximum we'd need here.
  src.advance(length_skip_size);

  let payload = deserialize_uint_of_length(src, length_payload_size)? >> length_shift_by;
  let continuation_bit = (payload & 1) == 1;
  let length = payload >> 1;
  Ok((length as usize, continuation_bit))
}

pub fn deserialize_long_string_after_type_code<B: Buf>(src: &mut B) -> Result<String> {
  let (chunk_size, continuation_bit) = deserialize_chunk_header(src)?;
  if continuation_bit {
    // Note: Deliberately not supporting chunked strings since we don't use them.
    return Err(DeserializationError::ContinuationBitNotSupported);
  }
  deserialize_string_contents(src, chunk_size)
}
