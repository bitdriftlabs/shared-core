#[cfg(test)]
#[path = "./primitives_test.rs"]
mod primitives_test;

use crate::type_codes::TypeCode;
use crate::{Error, Result};

fn require_bytes(src: &[u8], byte_count: usize) -> Result<()> {
  if byte_count > src.len() {
    Err(Error::InvalidSerialization)
  } else {
    Ok(())
  }
}

// ==========
// Serializer
// ==========

// Returns: skip bytes, copy bytes, shift amount, unary code
fn derive_chunk_length_header_data(payload: u64) -> (u8, u8, u8, u8) {
  match payload {
    0x00 ..= 0x7F => (0, 1, 1, 0x01),
    0x80 ..= 0x3FFF => (0, 2, 2, 0x02),
    0x4000 ..= 0x1FFFFF => (0, 3, 3, 0x04),
    0x200000 ..= 0xFFFFFFF => (0, 4, 4, 0x08),
    0x10000000 ..= 0x7FFFFFFFF => (0, 5, 5, 0x10),
    0x800000000 ..= 0x3FFFFFFFFFF => (0, 6, 6, 0x20),
    0x40000000000 ..= 0x1FFFFFFFFFFFF => (0, 7, 7, 0x40),
    0x2000000000000 ..= 0xFFFFFFFFFFFFFF => (0, 8, 8, 0x80),
    _ => (1, 8, 0, 0x00),
  }
}

// Assumption: dst has at least 9 bytes of space
fn serialize_chunk_header_unchecked(dst: &mut [u8], length: u64, continuation_bit: bool) -> usize {
  let payload = (length << 1) | continuation_bit as u64;
  let (skip_bytes, copy_bytes, shift_amount, unary_code) = derive_chunk_length_header_data(payload);
  let total_size = (skip_bytes + copy_bytes) as usize;
  let encoded = (payload << shift_amount) | unary_code as u64;
  let bytes = encoded.to_le_bytes();
  dst[0] = unary_code;
  dst[skip_bytes as usize .. total_size].copy_from_slice(&bytes[.. copy_bytes as usize]);
  total_size
}

fn serialize_type_code(dst: &mut [u8], type_code: TypeCode) -> Result<usize> {
  require_bytes(dst, 1).and_then(|_| {
    dst[0] = type_code as u8;
    Ok(1)
  })
}

pub fn serialize_array_begin(dst: &mut [u8]) -> Result<usize> {
  serialize_type_code(dst, TypeCode::ArrayStart)
}

pub fn serialize_map_begin(dst: &mut [u8]) -> Result<usize> {
  serialize_type_code(dst, TypeCode::MapStart)
}

pub fn serialize_container_end(dst: &mut [u8]) -> Result<usize> {
  serialize_type_code(dst, TypeCode::ContainerEnd)
}

fn serialize_small_int(dst: &mut [u8], v: u8) -> Result<usize> {
  require_bytes(dst, 1).and_then(|_| {
    dst[0] = v;
    Ok(1)
  })
}

pub fn serialize_u64(dst: &mut [u8], v: u64) -> Result<usize> {
  if v <= TypeCode::P100 as u64 {
    return serialize_small_int(dst, v as u8);
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
  let type_code = TypeCode::Unsigned as u8 | (use_signed_form << 3) | index as u8;
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = type_code;
    dst[1 .. total_size].copy_from_slice(&bytes[.. payload_size]);
    Ok(total_size)
  })
}

pub fn serialize_i64(dst: &mut [u8], v: i64) -> Result<usize> {
  if v >= TypeCode::N100 as i8 as i64 && v <= TypeCode::P100 as i64 {
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
  let type_code = TypeCode::Unsigned as u8 | (use_signed_form << 3) | index as u8;
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = type_code;
    dst[1 .. total_size].copy_from_slice(&bytes[.. payload_size]);
    Ok(total_size)
  })
}

fn serialize_f16(dst: &mut [u8], v: f32) -> Result<usize> {
  let bytes = v.to_le_bytes();
  let total_size = 2 + 1;
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = TypeCode::Float16 as u8;
    dst[1] = bytes[2];
    dst[2] = bytes[3];
    return Ok(total_size);
  })
}

pub fn serialize_f32(dst: &mut [u8], v: f32) -> Result<usize> {
  let bytes = v.to_le_bytes();
  let as_bits = v.to_bits();
  if (as_bits & 0xffff0000) == as_bits {
    return serialize_f16(dst, v);
  }

  let total_size = 4 + 1;
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = TypeCode::Float32 as u8;
    dst[1 .. total_size].copy_from_slice(&bytes);
    Ok(total_size)
  })
}

pub fn serialize_f64(dst: &mut [u8], v: f64) -> Result<usize> {
  let as_f32 = v as f32;
  if as_f32 as f64 == v {
    return serialize_f32(dst, as_f32);
  }

  let total_size = 8 + 1;
  let bytes = v.to_le_bytes();
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = TypeCode::Float64 as u8;
    dst[1 .. bytes.len()+1].copy_from_slice(&bytes);
    Ok(total_size)
  })
}

fn serialize_short_string_header(dst: &mut [u8], str: &str) -> Result<usize> {
  let total_size = 1;
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = TypeCode::String as u8 + str.len() as u8;
    Ok(total_size)
  })
}

fn serialize_long_string_header(dst: &mut [u8], str: &str) -> Result<usize> {
  let mut header: [u8; 9] = [0; 9];
  let header_size = serialize_chunk_header_unchecked(&mut header, str.len() as u64, false);
  let total_size = header_size + 1;
  require_bytes(dst, total_size).and_then(|_| {
    dst[0] = TypeCode::LongString as u8;
    dst[1 .. header_size + 1].copy_from_slice(&header[.. header_size]);
    Ok(total_size)
  })
}

pub fn serialize_string_header(dst: &mut [u8], str: &str) -> Result<usize> {
  if str.len() <= 15 {
    serialize_short_string_header(dst, str)
  } else {
    serialize_long_string_header(dst, str)
  }
}

fn serialize_string_contents(dst: &mut [u8], v: &str) -> Result<usize> {
  let string_size = v.len();
  require_bytes(dst, string_size).and_then(|_| {
    dst[.. string_size].copy_from_slice(v.as_bytes());
    Ok(string_size)
  })
}

fn serialize_short_string(dst: &mut [u8], v: &str) -> Result<usize> {
  serialize_short_string_header(dst, v).and_then(|header_size| {
    serialize_string_contents(&mut dst[header_size ..], v)
      .and_then(|string_size| Ok(header_size + string_size))
  })
}

fn serialize_long_string(dst: &mut [u8], v: &str) -> Result<usize> {
  serialize_long_string_header(dst, v).and_then(|header_size| {
    serialize_string_contents(&mut dst[header_size ..], v)
      .and_then(|string_size| Ok(header_size + string_size))
  })
}

pub fn serialize_string(dst: &mut [u8], v: &str) -> Result<usize> {
  if v.len() <= 15 {
    serialize_short_string(dst, v)
  } else {
    serialize_long_string(dst, v)
  }
}

pub fn serialize_boolean(dst: &mut [u8], v: bool) -> Result<usize> {
  serialize_type_code(dst, if v { TypeCode::True } else { TypeCode::False })
}

pub fn serialize_null(dst: &mut [u8]) -> Result<usize> {
  serialize_type_code(dst, TypeCode::Null)
}

// ============
// Deserializer
// ============

fn copy_bytes_to(src: &[u8], dst: &mut [u8], byte_count: usize) -> Result<()> {
  require_bytes(src, byte_count).and_then(|_| {
    dst[.. byte_count].copy_from_slice(&src[.. byte_count]);
    Ok(())
  })
}

fn deserialize_byte(src: &[u8]) -> Result<(usize, u8)> {
  require_bytes(src, 1).and_then(|_| Ok((1, src[0])))
}

fn deserialize_string_contents(src: &[u8], size: usize) -> Result<(usize, &str)> {
  require_bytes(src, size).and_then(|_| {
    let string = match std::str::from_utf8(&src[.. size]) {
      Ok(v) => v,
      Err(_e) => return Err(Error::InvalidDeserialization),
    };
    Ok((size, string))
  })
}

fn deserialize_specific_type_code(src: &[u8], expected_type_code: TypeCode) -> Result<usize> {
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code == expected_type_code as u8 {
      Ok(size)
    } else {
      Err(Error::InvalidSerialization)
    }
  })
}

pub fn peek_type_code(src: &[u8]) -> Result<u8> {
  deserialize_byte(src).and_then(|(_size, v)| Ok(v))
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
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code == TypeCode::True as u8 {
      Ok((size, true))
    } else if type_code == TypeCode::False as u8 {
      Ok((size, false))
    } else {
      Err(Error::InvalidDeserialization)
    }
  })
}

pub fn deserialize_unsigned_after_type_code(src: &[u8], type_code: u8) -> Result<(usize, u64)> {
  let byte_count = ((type_code & 7) + 1) as usize;
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(src, &mut bytes, byte_count)
    .and_then(|_| Ok((byte_count, u64::from_le_bytes(bytes))))
}

pub fn deserialize_unsigned(src: &[u8]) -> Result<(usize, u64)> {
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code <= TypeCode::P100 as u8 {
        return Ok((1, type_code as u64));
    }
    if type_code >= TypeCode::Unsigned as u8 && type_code <= TypeCode::UnsignedEnd as u8 {
      deserialize_unsigned_after_type_code(&src[1 ..], type_code)
        .and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else if type_code >= TypeCode::Signed as u8 && type_code <= TypeCode::SignedEnd as u8 {
      deserialize_signed_after_type_code(&src[1 ..], type_code)
        .and_then(|(v_size, v)| {
          if v < 0 {
            Err(Error::InvalidDeserialization)
          } else {
            Ok((size + v_size, v as u64))
          }
        })
    } else {
      Err(Error::InvalidDeserialization)
    }
  })
}

pub fn deserialize_signed_after_type_code(src: &[u8], type_code: u8) -> Result<(usize, i64)> {
  let byte_count = ((type_code & 7) + 1) as usize;
  require_bytes(src, byte_count).and_then(|_| {
    let is_negative = src[byte_count - 1] >> 7;
    let mut bytes: [u8; 8] = [is_negative * 0xff; 8];
    bytes[.. byte_count].copy_from_slice(&src[.. byte_count]);
    Ok((byte_count, i64::from_le_bytes(bytes)))
  })
}

pub fn deserialize_signed(src: &[u8]) -> Result<(usize, i64)> {
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code <= TypeCode::P100 as u8 || type_code >= TypeCode::N100 as u8 {
        return Ok((1, type_code as i8 as i64));
    }
    if type_code >= TypeCode::Signed as u8 && type_code <= TypeCode::SignedEnd as u8 {
      deserialize_signed_after_type_code(&src[1 ..], type_code)
        .and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else if type_code >= TypeCode::Unsigned as u8 && type_code <= TypeCode::UnsignedEnd as u8 {
      deserialize_unsigned_after_type_code(&src[1 ..], type_code)
        .and_then(|(v_size, v)| {
          if v > i64::MAX as u64 {
            Err(Error::InvalidDeserialization)
          } else {
            Ok((size + v_size, v as i64))
          }
        })
    } else {
      Err(Error::InvalidDeserialization)
    }
  })
}

pub fn deserialize_f16_after_type_code(src: &[u8]) -> Result<(usize, f32)> {
  let mut bytes: [u8; 4] = [0; 4];
  // Note: Copying only 2 bytes into a 4 byte buffer because this is a bfloat.
  copy_bytes_to(src, &mut bytes[2..], 2).and_then(|_| {
    Ok((2, f32::from_le_bytes(bytes)))
  })
}

pub fn deserialize_f32_after_type_code(src: &[u8]) -> Result<(usize, f32)> {
  let mut bytes: [u8; 4] = [0; 4];
  copy_bytes_to(src, &mut bytes, 4).and_then(|_| Ok((4, f32::from_le_bytes(bytes))))
}

pub fn deserialize_f64_after_type_code(src: &[u8]) -> Result<(usize, f64)> {
  let mut bytes: [u8; 8] = [0; 8];
  copy_bytes_to(src, &mut bytes, 8).and_then(|_| Ok((8, f64::from_le_bytes(bytes))))
}

pub fn deserialize_f32(src: &[u8]) -> Result<(usize, f32)> {
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code == TypeCode::Float16 as u8 {
      deserialize_f16_after_type_code(&src[1 ..]).and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else if type_code == TypeCode::Float32 as u8 {
      deserialize_f32_after_type_code(&src[1 ..]).and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else {
      Err(Error::InvalidDeserialization)
    }
  })
}

pub fn deserialize_f64(src: &[u8]) -> Result<(usize, f64)> {
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code == TypeCode::Float16 as u8 {
      deserialize_f16_after_type_code(&src[1 ..])
        .and_then(|(v_size, v)| Ok((size + v_size, v as f64)))
    } else if type_code == TypeCode::Float32 as u8 {
      deserialize_f32_after_type_code(&src[1 ..])
        .and_then(|(v_size, v)| Ok((size + v_size, v as f64)))
    } else if type_code == TypeCode::Float64 as u8 {
      deserialize_f64_after_type_code(&src[1 ..]).and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else {
      Err(Error::InvalidDeserialization)
    }
  })
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
  deserialize_byte(src).and_then(|(_size, length_header)| {
    let (length_skip_size, length_payload_size, length_shift_by) =
      decode_chunk_length_header(length_header);
    let length_total_size = length_skip_size + length_payload_size;
    let mut bytes: [u8; 8] = [0; 8];
    copy_bytes_to(src, &mut bytes, length_total_size).and_then(|_| {
      let payload = u64::from_le_bytes(bytes) >> length_shift_by;
      let continuation_bit = (payload & 1) == 1;
      let length = payload >> 1;
      Ok((length_total_size, length as usize, continuation_bit))
    })
  })
}

pub fn deserialize_long_string_after_type_code(src: &[u8]) -> Result<(usize, &str)> {
  deserialize_chunk_header(src).and_then(|(header_size, chunk_size, continuation_bit)| {
    if continuation_bit {
      // TODO: Don't support this
      return Err(Error::InvalidDeserialization);
    }
    let current = &src[header_size..];
    require_bytes(current, chunk_size)
      .and_then(|_| deserialize_string_contents(current, chunk_size)).and_then(|(v_size, v)| {
        Ok((header_size + v_size, v))
      })
  })
}

pub fn deserialize_string(src: &[u8]) -> Result<(usize, &str)> {
  deserialize_type_code(src).and_then(|(size, type_code)| {
    if type_code >= TypeCode::String as u8 && type_code <= TypeCode::StringEnd as u8 {
      deserialize_short_string_after_type_code(&src[1 ..], type_code)
        .and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else if type_code == TypeCode::LongString as u8 {
      deserialize_long_string_after_type_code(&src[1 ..])
        .and_then(|(v_size, v)| Ok((size + v_size, v)))
    } else {
      Err(Error::InvalidDeserialization)
    }
  })
}
