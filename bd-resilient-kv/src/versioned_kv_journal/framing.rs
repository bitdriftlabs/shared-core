// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Wire format framing for journal entries.
//!
//! Per-entry format:
//! ```text
//! [length: u32][timestamp_micros: varint][payload: bytes][crc32: u32]
//! ```
//!
//! - `length`: Total length of the frame (timestamp + payload + crc)
//! - `timestamp_micros`: Microseconds since UNIX epoch (varint encoded)
//! - `payload`: Opaque binary data (format determined by caller)
//! - `crc32`: CRC32 checksum of (`timestamp_bytes` + payload)

use bytes::BufMut;
use crc32fast::Hasher;

/// Maximum varint size for u64 (10 bytes)
const MAX_VARINT_SIZE: usize = 10;

/// Encode a u64 as a varint into the buffer.
/// Returns the number of bytes written.
pub fn encode_varint(value: u64, buf: &mut [u8]) -> usize {
  let mut val = value;
  let mut idx = 0;

  #[allow(clippy::cast_possible_truncation)]
  while val >= 0x80 {
    buf[idx] = (val as u8) | 0x80;
    val >>= 7;
    idx += 1;
  }
  #[allow(clippy::cast_possible_truncation)]
  {
    buf[idx] = val as u8;
  }
  idx + 1
}

/// Decode a varint from the buffer.
/// Returns (value, `bytes_read`) or None if buffer is incomplete/invalid.
#[must_use]
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
  let mut value: u64 = 0;
  let mut shift = 0;

  for (idx, &byte) in buf.iter().enumerate() {
    if idx >= MAX_VARINT_SIZE {
      return None; // Varint too long
    }

    value |= u64::from(byte & 0x7F) << shift;
    shift += 7;

    if byte & 0x80 == 0 {
      return Some((value, idx + 1));
    }
  }

  None // Incomplete varint
}

/// Frame structure for a journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame<M> {
  /// Timestamp in microseconds since UNIX epoch.
  pub timestamp_micros: u64,
  /// Opaque payload data.
  pub payload: M,
}

impl<M> Frame<M> {
  pub fn decode_timestamp(buf: &[u8]) -> anyhow::Result<(u64, usize)> {
    let (timestamp_micros, timestamp_len) =
      decode_varint(buf).ok_or_else(|| anyhow::anyhow!("Invalid varint"))?;
    Ok((timestamp_micros, timestamp_len))
  }
}

impl<M: protobuf::Message> Frame<M> {
  /// Create a new frame.
  #[must_use]
  pub fn new(timestamp_micros: u64, payload: M) -> Self {
    Self {
      timestamp_micros,
      payload,
    }
  }

  /// Calculate the encoded size of this frame.
  #[must_use]
  pub fn encoded_size(&self) -> usize {
    // Calculate varint size
    let mut temp_buf = [0u8; MAX_VARINT_SIZE];
    let varint_size = encode_varint(self.timestamp_micros, &mut temp_buf);
    let payload_size: usize = self.payload.compute_size().try_into().unwrap_or(0);

    // length(4) + timestamp_varint + payload + crc(4)
    4 + varint_size + payload_size + 4
  }

  /// Encode this frame into a buffer.
  ///
  /// # Errors
  /// Returns an error if the buffer is too small.
  pub fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
    let required_size = self.encoded_size();
    if buf.len() < required_size {
      anyhow::bail!(
        "Buffer too small: need {} bytes, have {} bytes",
        required_size,
        buf.len()
      );
    }

    let mut cursor = buf;

    // Encode timestamp to calculate frame length
    let mut timestamp_buf = [0u8; MAX_VARINT_SIZE];
    let timestamp_len = encode_varint(self.timestamp_micros, &mut timestamp_buf);

    let payload_bytes = self
      .payload
      .write_to_bytes()
      .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;

    // Frame length = timestamp + payload + crc
    let frame_len = timestamp_len + payload_bytes.len() + 4;
    #[allow(clippy::cast_possible_truncation)]
    {
      cursor.put_u32_le(frame_len as u32);
    }

    // Write timestamp varint
    cursor.put_slice(&timestamp_buf[.. timestamp_len]);

    // Write payload
    cursor.put_slice(&payload_bytes);

    // Calculate CRC over timestamp + payload
    let mut hasher = Hasher::new();
    hasher.update(&timestamp_buf[.. timestamp_len]);
    hasher.update(payload_bytes.as_slice());
    let crc = hasher.finalize();

    // Write CRC
    cursor.put_u32_le(crc);

    Ok(required_size)
  }

  /// Decode a frame from a buffer.
  ///
  /// Returns (Frame, `bytes_consumed`) or error if invalid/incomplete.
  pub fn decode(buf: &[u8]) -> anyhow::Result<(Self, usize)> {
    if buf.len() < 4 {
      anyhow::bail!("Buffer too small for length field");
    }

    // Read frame length
    let frame_len = u32::from_le_bytes(buf[0 .. 4].try_into()?) as usize;

    // Check if we have the complete frame
    let total_len = 4 + frame_len; // length field + frame
    if buf.len() < total_len {
      anyhow::bail!(
        "Incomplete frame: need {} bytes, have {} bytes",
        total_len,
        buf.len()
      );
    }

    let frame_data = &buf[4 .. total_len];

    // Decode timestamp varint
    let (timestamp_micros, timestamp_len) =
      decode_varint(frame_data).ok_or_else(|| anyhow::anyhow!("Invalid varint"))?;

    // Extract payload and CRC
    if frame_data.len() < timestamp_len + 4 {
      anyhow::bail!("Frame too small for CRC");
    }

    let payload_end = frame_data.len() - 4;
    let payload = frame_data[timestamp_len .. payload_end].to_vec();
    let stored_crc = u32::from_le_bytes(frame_data[payload_end ..].try_into()?);

    // Verify CRC
    let mut hasher = Hasher::new();
    hasher.update(&frame_data[.. timestamp_len]); // timestamp bytes
    hasher.update(&payload); // payload
    let computed_crc = hasher.finalize();

    if stored_crc != computed_crc {
      anyhow::bail!("CRC mismatch: expected 0x{stored_crc:08x}, got 0x{computed_crc:08x}");
    }

    let payload = M::parse_from_bytes(&payload)
      .map_err(|e| anyhow::anyhow!("Failed to parse payload: {}", e))?;

    Ok((Self::new(timestamp_micros, payload), total_len))
  }
}

#[cfg(test)]
#[path = "./framing_test.rs"]
mod tests;
