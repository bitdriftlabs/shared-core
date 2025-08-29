// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::type_codes::TypeCode;
use bytes::BufMut;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationError {
  BufferFull,
  Io,
  Invariant,
}
pub type Result<T> = std::result::Result<T, SerializationError>;

impl From<std::num::TryFromIntError> for SerializationError {
  fn from(_err: std::num::TryFromIntError) -> Self {
    Self::Invariant
  }
}

/// Ensures that the buffer has at least `required_bytes` remaining capacity.
fn require_bytes<B: BufMut>(dst: &B, required_bytes: usize) -> Result<()> {
  if dst.remaining_mut() < required_bytes {
    Err(SerializationError::BufferFull)
  } else {
    Ok(())
  }
}

// Returns: skip bytes, copy bytes, shift amount, unary code
fn derive_chunk_length_header_data(payload: u64) -> (u8, u8, u8, u8) {
  match payload {
    0x00 ..= 0x7F => (0, 1, 1, 0x01),
    0x80 ..= 0x3FFF => (0, 2, 2, 0x02),
    0x4000 ..= 0x1F_FFFF => (0, 3, 3, 0x04),
    0x20_0000 ..= 0xFFF_FFFF => (0, 4, 4, 0x08),
    0x1000_0000 ..= 0x7_FFFF_FFFF => (0, 5, 5, 0x10),
    0x8_0000_0000 ..= 0x3FF_FFFF_FFFF => (0, 6, 6, 0x20),
    0x4000_0000_0000 ..= 0x1_FFFF_FFFF_FFFF => (0, 7, 7, 0x40),
    0x20_0000_0000_0000 ..= 0xFF_FFFF_FFFF_FFFF => (0, 8, 8, 0x80),
    _ => (1, 8, 0, 0x00),
  }
}

// Assumption: dst has at least 9 bytes of space
fn serialize_chunk_header_unchecked<B: BufMut>(dst: &mut B, length: u64, continuation_bit: bool) -> usize {
  let payload = (length << 1) | u64::from(continuation_bit);
  let (skip_bytes, copy_bytes, shift_amount, unary_code) = derive_chunk_length_header_data(payload);
  let total_size = (skip_bytes + copy_bytes) as usize;
  let encoded = (payload << shift_amount) | u64::from(unary_code);
  let bytes = encoded.to_le_bytes();
  
  if skip_bytes > 0 {
    dst.put_u8(unary_code);
  }
  dst.put_slice(&bytes[.. copy_bytes as usize]);
  total_size
}

fn serialize_type_code<B: BufMut>(dst: &mut B, type_code: TypeCode) -> Result<usize> {
  require_bytes(dst, 1)?;
  dst.put_u8(type_code as u8);
  Ok(1)
}

pub fn serialize_array_begin<B: BufMut>(dst: &mut B) -> Result<usize> {
  serialize_type_code(dst, TypeCode::ArrayStart)
}

pub fn serialize_map_begin<B: BufMut>(dst: &mut B) -> Result<usize> {
  serialize_type_code(dst, TypeCode::MapStart)
}

pub fn serialize_container_end<B: BufMut>(dst: &mut B) -> Result<usize> {
  serialize_type_code(dst, TypeCode::ContainerEnd)
}

fn serialize_small_int<B: BufMut>(dst: &mut B, v: u8) -> Result<usize> {
  require_bytes(dst, 1)?;
  dst.put_u8(v);
  Ok(1)
}

pub fn serialize_u64<B: BufMut>(dst: &mut B, v: u64) -> Result<usize> {
  if v <= TypeCode::P100 as u64 {
    return serialize_small_int(dst, (v & 0xff) as u8);
  }

  let bytes = v.to_le_bytes();

  let mut index = bytes.len() - 1;
  while index > 0 {
    if bytes[index] != 0 {
      break;
    }
    index -= 1;
  }
  let payload_size = index + 1;
  let total_size = payload_size + 1;
  let is_byte_high_bit_set = bytes[index] >> 7;
  let use_signed_form = (!is_byte_high_bit_set) & 1;
  let type_code = TypeCode::Unsigned as u8 | (use_signed_form << 3) | u8::try_from(index)?;
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(type_code);
  dst.put_slice(&bytes[.. payload_size]);
  Ok(total_size)
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
pub fn serialize_i64<B: BufMut>(dst: &mut B, v: i64) -> Result<usize> {
  if v >= i64::from(TypeCode::N100 as i8) && v <= TypeCode::P100 as i64 {
    return serialize_small_int(dst, v as u8);
  }

  let bytes = v.to_le_bytes();

  let mut index = bytes.len() - 1;
  while index > 0 {
    if bytes[index] != 0 && bytes[index] != 0xff {
      break;
    }
    index -= 1;
  }
  let payload_size = index + 1;
  let total_size = payload_size + 1;
  let is_negative = (v >> 63) as u8;
  let is_byte_high_bit_set = bytes[index] >> 7;
  let use_signed_form = (is_negative | !is_byte_high_bit_set) & 1;
  let type_code = TypeCode::Unsigned as u8 | (use_signed_form << 3) | u8::try_from(index)?;
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(type_code);
  dst.put_slice(&bytes[.. payload_size]);
  Ok(total_size)
}

fn serialize_f16<B: BufMut>(dst: &mut B, v: f32) -> Result<usize> {
  let bytes = v.to_le_bytes();
  let total_size = 2 + 1;
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(TypeCode::Float16 as u8);
  dst.put_u8(bytes[2]);
  dst.put_u8(bytes[3]);
  Ok(total_size)
}

pub fn serialize_f32<B: BufMut>(dst: &mut B, v: f32) -> Result<usize> {
  let bytes = v.to_le_bytes();
  let as_bits = v.to_bits();
  if (as_bits & 0xffff_0000) == as_bits {
    return serialize_f16(dst, v);
  }

  let total_size = 4 + 1;
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(TypeCode::Float32 as u8);
  dst.put_slice(&bytes);
  Ok(total_size)
}

#[allow(clippy::cast_possible_truncation)]
pub fn serialize_f64<B: BufMut>(dst: &mut B, v: f64) -> Result<usize> {
  let as_f32 = v as f32;
  // This needs to be an exact match to ensure that we can round-trip without any loss.
  if f64::from(as_f32).to_bits() == v.to_bits() {
    return serialize_f32(dst, as_f32);
  }

  let total_size = 8 + 1;
  let bytes = v.to_le_bytes();
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(TypeCode::Float64 as u8);
  dst.put_slice(&bytes);
  Ok(total_size)
}

fn serialize_short_string_header<B: BufMut>(dst: &mut B, str: &str) -> Result<usize> {
  let total_size = 1;
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(TypeCode::String as u8 + u8::try_from(str.len())?);
  Ok(total_size)
}

fn serialize_long_string_header<B: BufMut>(dst: &mut B, str: &str) -> Result<usize> {
  let mut header: [u8; 9] = [0; 9];
  let header_size = serialize_chunk_header_unchecked(&mut header.as_mut_slice(), str.len() as u64, false);
  let total_size = header_size + 1;
  
  require_bytes(dst, total_size)?;
  
  dst.put_u8(TypeCode::LongString as u8);
  dst.put_slice(&header[.. header_size]);
  Ok(total_size)
}

pub fn serialize_string_header<B: BufMut>(dst: &mut B, str: &str) -> Result<usize> {
  if str.len() <= 15 {
    serialize_short_string_header(dst, str)
  } else {
    serialize_long_string_header(dst, str)
  }
}

pub fn serialize_boolean<B: BufMut>(dst: &mut B, v: bool) -> Result<usize> {
  serialize_type_code(dst, if v { TypeCode::True } else { TypeCode::False })
}

pub fn serialize_null<B: BufMut>(dst: &mut B) -> Result<usize> {
  serialize_type_code(dst, TypeCode::Null)
}
