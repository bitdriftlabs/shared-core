// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::type_codes::TypeCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeserializationError {
  PrematureEnd,
  InvalidUTF8,
  UnexpectedTypeCode,
  ExpectedNull,
  ExpectedBoolean,
  ExpectedUnsignedInteger,
  ExpectedSignedInteger,
  ExpectedFloat32,
  ExpectedFloat,
  ExpectedString,
  ExpectedArray,
  ExpectedMap,
  UnexpectedContinuationBit,
  UnterminatedContainer,
  NonStringKeyInMap,
}

impl DeserializationError {
  pub fn kind(&self) -> DeserializationError {
    *self // Return a copy of the error
  }
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
  dst[..byte_count].copy_from_slice(&src[..byte_count]);
  Ok(())
}

fn deserialize_byte(src: &[u8]) -> Result<(usize, u8)> {
  require_bytes(src, 1)?;
  Ok((1, src[0]))
}

fn deserialize_string_contents(src: &[u8], size: usize) -> Result<(usize, &str)> {
  require_bytes(src, size)?;
  let string = std::str::from_utf8(&src[..size]).map_err(|_| DeserializationError::InvalidUTF8)?;
  Ok((size, string))
}

fn deserialize_specific_type_code(src: &[u8], expected_type_code: TypeCode) -> Result<usize> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code == expected_type_code as u8 {
    Ok(size)
  } else {
    Err(DeserializationError::UnexpectedTypeCode)
  }
}

pub fn peek_type_code(src: &[u8]) -> Result<u8> {
  let (_, type_code) = deserialize_byte(src)?;
  Ok(type_code)
}

pub fn deserialize_type_code(src: &[u8]) -> Result<(usize, u8)> {
  deserialize_byte(src)
}

pub fn deserialize_null(src: &[u8]) -> Result<usize> {
  deserialize_specific_type_code(src, TypeCode::Null)
}

pub fn deserialize_array_start(src: &[u8]) -> Result<usize> {
  deserialize_specific_type_code(src, TypeCode::ArrayStart)
}

pub fn deserialize_map_start(src: &[u8]) -> Result<usize> {
  deserialize_specific_type_code(src, TypeCode::MapStart)
}

pub fn deserialize_container_end(src: &[u8]) -> Result<usize> {
  deserialize_specific_type_code(src, TypeCode::ContainerEnd)
}

pub fn deserialize_bool(src: &[u8]) -> Result<(usize, bool)> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code == TypeCode::True as u8 {
    Ok((size, true))
  } else if type_code == TypeCode::False as u8 {
    Ok((size, false))
  } else {
    Err(DeserializationError::ExpectedBoolean)
  }
}

pub fn deserialize_unsigned_after_type_code(src: &[u8], type_code: u8) -> Result<(usize, u64)> {
  let byte_count = ((type_code & 7) + 1) as usize;
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(src, &mut bytes, byte_count)?;
  Ok((byte_count, u64::from_le_bytes(bytes)))
}

pub fn deserialize_unsigned_integer(src: &[u8]) -> Result<(usize, u64)> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code <= TypeCode::P100 as u8 {
    return Ok((1, type_code as u64));
  }
  if type_code >= TypeCode::Unsigned as u8 && type_code <= TypeCode::UnsignedEnd as u8 {
    let (v_size, v) = deserialize_unsigned_after_type_code(&src[1..], type_code)?;
    Ok((size + v_size, v))
  } else if type_code >= TypeCode::Signed as u8 && type_code <= TypeCode::SignedEnd as u8 {
    let (v_size, v) = deserialize_signed_after_type_code(&src[1..], type_code)?;
    if v < 0 {
      Err(DeserializationError::ExpectedUnsignedInteger)
    } else {
      Ok((size + v_size, v as u64))
    }
  } else {
    Err(DeserializationError::ExpectedUnsignedInteger)
  }
}

pub fn deserialize_signed_after_type_code(src: &[u8], type_code: u8) -> Result<(usize, i64)> {
  let byte_count = ((type_code & 7) + 1) as usize;
  require_bytes(src, byte_count)?;
  let is_negative = src[byte_count - 1] >> 7;
  let mut bytes: [u8; 8] = [is_negative * 0xff; 8];
  bytes[..byte_count].copy_from_slice(&src[..byte_count]);
  Ok((byte_count, i64::from_le_bytes(bytes)))
}

pub fn deserialize_signed_integer(src: &[u8]) -> Result<(usize, i64)> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code <= TypeCode::P100 as u8 || type_code >= TypeCode::N100 as u8 {
    return Ok((1, type_code as i8 as i64));
  }
  if type_code >= TypeCode::Signed as u8 && type_code <= TypeCode::SignedEnd as u8 {
    let (v_size, v) = deserialize_signed_after_type_code(&src[1..], type_code)?;
    Ok((size + v_size, v))
  } else if type_code >= TypeCode::Unsigned as u8 && type_code <= TypeCode::UnsignedEnd as u8 {
    let (v_size, v) = deserialize_unsigned_after_type_code(&src[1..], type_code)?;
    if v > i64::MAX as u64 {
      Err(DeserializationError::ExpectedSignedInteger)
    } else {
      Ok((size + v_size, v as i64))
    }
  } else {
    Err(DeserializationError::ExpectedSignedInteger)
  }
}

pub fn deserialize_f16_after_type_code(src: &[u8]) -> Result<(usize, f32)> {
  let mut bytes: [u8; 4] = [0; 4];
  // Note: Copying only 2 bytes into a 4 byte buffer because this is a bfloat.
  copy_bytes_to(src, &mut bytes[2..], 2)?;
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

pub fn deserialize_f32(src: &[u8]) -> Result<(usize, f32)> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code == TypeCode::Float16 as u8 {
    let (v_size, v) = deserialize_f16_after_type_code(&src[1..])?;
    Ok((size + v_size, v))
  } else if type_code == TypeCode::Float32 as u8 {
    let (v_size, v) = deserialize_f32_after_type_code(&src[1..])?;
    Ok((size + v_size, v))
  } else {
    Err(DeserializationError::ExpectedFloat32)
  }
}

pub fn deserialize_f64(src: &[u8]) -> Result<(usize, f64)> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code == TypeCode::Float16 as u8 {
    let (v_size, v) = deserialize_f16_after_type_code(&src[1..])?;
    Ok((size + v_size, v as f64))
  } else if type_code == TypeCode::Float32 as u8 {
    let (v_size, v) = deserialize_f32_after_type_code(&src[1..])?;
    Ok((size + v_size, v as f64))
  } else if type_code == TypeCode::Float64 as u8 {
    let (v_size, v) = deserialize_f64_after_type_code(&src[1..])?;
    Ok((size + v_size, v))
  } else {
    Err(DeserializationError::ExpectedFloat)
  }
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
  } else if (length_header & 0xff) == 0x80 {
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
  copy_bytes_to(src, &mut bytes, length_total_size)?;
  let payload = u64::from_le_bytes(bytes) >> length_shift_by;
  let continuation_bit = (payload & 1) == 1;
  let length = payload >> 1;
  Ok((length_total_size, length as usize, continuation_bit))
}

pub fn deserialize_long_string_after_type_code(src: &[u8]) -> Result<(usize, &str)> {
  let (header_size, chunk_size, continuation_bit) = deserialize_chunk_header(src)?;
  if continuation_bit {
    // Note: Deliberately not supporting chunked strings since we don't use them.
    return Err(DeserializationError::UnexpectedContinuationBit);
  }
  let current = &src[header_size..];
  require_bytes(current, chunk_size)?;
  let (v_size, v) = deserialize_string_contents(current, chunk_size)?;
  Ok((header_size + v_size, v))
}

pub fn deserialize_string(src: &[u8]) -> Result<(usize, &str)> {
  let (size, type_code) = deserialize_type_code(src)?;
  if type_code >= TypeCode::String as u8 && type_code <= TypeCode::StringEnd as u8 {
    let (v_size, v) = deserialize_short_string_after_type_code(&src[1..], type_code)?;
    Ok((size + v_size, v))
  } else if type_code == TypeCode::LongString as u8 {
    let (v_size, v) = deserialize_long_string_after_type_code(&src[1..])?;
    Ok((size + v_size, v))
  } else {
    Err(DeserializationError::ExpectedString)
  }
}
