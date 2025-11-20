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
//! [length: varint][scope: u8][key_len: varint][key: bytes][timestamp_micros: varint][payload: bytes][crc32: u32]
//! ```
//!
//! - `length`: Total length of the frame (`scope` + `key_len` + key + timestamp + payload + crc),
//!   varint encoded
//! - `scope`: Scope identifier for namespacing (u8)
//! - `key_len`: Length of the key in bytes (varint encoded)
//! - `key`: Key string bytes (UTF-8)
//! - `timestamp_micros`: Microseconds since UNIX epoch (varint encoded)
//! - `payload`: Opaque binary data (format determined by caller)
//! - `crc32`: CRC32 checksum of (`scope` + `key_len` + key + `timestamp_bytes` + payload)

#[cfg(test)]
#[path = "./framing_test.rs"]
mod tests;

use crate::Scope;
use bytes::BufMut;
use crc32fast::Hasher;

mod varint;

const CRC_LEN: usize = 4;

/// Frame structure for a journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame<'a, M> {
  /// Scope identifier for namespacing.
  pub scope: Scope,
  /// Key for this entry.
  pub key: &'a str,
  /// Timestamp in microseconds since UNIX epoch.
  pub timestamp_micros: u64,
  /// Opaque payload data.
  pub payload: M,
}

impl<'a, M: protobuf::Message> Frame<'a, M> {
  /// Create a new frame.
  #[must_use]
  pub fn new(scope: Scope, key: &'a str, timestamp_micros: u64, payload: M) -> Self {
    Self {
      scope,
      key,
      timestamp_micros,
      payload,
    }
  }

  /// Calculate the encoded size of this frame.
  #[must_use]
  pub fn encoded_size(&self) -> usize {
    let scope_size = 1; // u8
    let key_bytes = self.key.as_bytes();
    let key_len_varint_size = varint::compute_size(key_bytes.len() as u64);
    let key_size = key_bytes.len();
    let timestamp_varint_size = varint::compute_size(self.timestamp_micros);
    let payload_size: usize = self.payload.compute_size().try_into().unwrap_or(0);

    let frame_content_len =
      scope_size + key_len_varint_size + key_size + timestamp_varint_size + payload_size + CRC_LEN;
    let length_varint_size = varint::compute_size(frame_content_len as u64);

    length_varint_size + frame_content_len
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

    // Encode scope
    let scope_byte = [self.scope.to_u8()];

    // Encode key length and key
    let key_bytes = self.key.as_bytes();
    let mut key_len_buf = [0u8; varint::MAX_SIZE];
    let key_len_varint_len = varint::encode(key_bytes.len() as u64, &mut key_len_buf);

    // Encode timestamp
    let mut timestamp_buf = [0u8; varint::MAX_SIZE];
    let timestamp_len = varint::encode(self.timestamp_micros, &mut timestamp_buf);

    let payload_bytes = self
      .payload
      .write_to_bytes()
      .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {e}"))?;

    // Frame length = scope + key_len + key + timestamp + payload + crc
    let frame_len =
      1 + key_len_varint_len + key_bytes.len() + timestamp_len + payload_bytes.len() + CRC_LEN;

    // Encode frame length as varint
    let mut length_buf = [0u8; varint::MAX_SIZE];
    let length_len = varint::encode(frame_len as u64, &mut length_buf);
    cursor.put_slice(&length_buf[.. length_len]);

    cursor.put_slice(&scope_byte);
    cursor.put_slice(&key_len_buf[.. key_len_varint_len]);
    cursor.put_slice(key_bytes);
    cursor.put_slice(&timestamp_buf[.. timestamp_len]);
    cursor.put_slice(&payload_bytes);

    let mut hasher = Hasher::new();
    hasher.update(&scope_byte);
    hasher.update(&key_len_buf[.. key_len_varint_len]);
    hasher.update(key_bytes);
    hasher.update(&timestamp_buf[.. timestamp_len]);
    hasher.update(payload_bytes.as_slice());
    let crc = hasher.finalize();

    cursor.put_u32_le(crc);

    Ok(required_size)
  }

  /// Decode a frame from a buffer.
  ///
  /// Returns (Frame, `bytes_consumed`) or error if invalid/incomplete.
  pub fn decode(buf: &'a [u8]) -> anyhow::Result<(Self, usize)> {
    // Decode frame length varint
    let (frame_len_u64, length_len) =
      varint::decode(buf).ok_or_else(|| anyhow::anyhow!("Invalid length varint"))?;

    let frame_len = usize::try_from(frame_len_u64)
      .map_err(|_| anyhow::anyhow!("Frame length too large: {frame_len_u64}"))?;

    // Check if we have the complete frame
    let total_len = length_len + frame_len; // length varint + frame content
    if buf.len() < total_len {
      anyhow::bail!(
        "Incomplete frame: need {} bytes, have {} bytes",
        total_len,
        buf.len()
      );
    }

    let frame_data = &buf[length_len .. total_len];

    // Decode scope (u8)
    if frame_data.is_empty() {
      anyhow::bail!("Frame too small for scope");
    }
    let scope = Scope::from_repr(frame_data[0])
      .ok_or_else(|| anyhow::anyhow!("Invalid scope value: {}", frame_data[0]))?;
    let mut offset = 1;

    // Decode key length varint
    let (key_len_u64, key_len_varint_len) = varint::decode(&frame_data[offset ..])
      .ok_or_else(|| anyhow::anyhow!("Invalid key length varint"))?;
    let key_len = usize::try_from(key_len_u64)
      .map_err(|_| anyhow::anyhow!("Key length too large: {key_len_u64}"))?;
    offset += key_len_varint_len;

    // Decode key
    if frame_data.len() < offset + key_len {
      anyhow::bail!("Frame too small for key");
    }
    let key = str::from_utf8(&frame_data[offset .. offset + key_len])
      .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in key: {e}"))?;
    offset += key_len;

    // Decode timestamp varint
    let (timestamp_micros, timestamp_len) = varint::decode(&frame_data[offset ..])
      .ok_or_else(|| anyhow::anyhow!("Invalid timestamp varint"))?;
    offset += timestamp_len;

    // Extract payload and CRC
    if frame_data.len() < offset + CRC_LEN {
      anyhow::bail!("Frame too small for CRC");
    }

    let payload_end = frame_data.len() - CRC_LEN;
    let payload = frame_data[offset .. payload_end].to_vec();
    let stored_crc = u32::from_le_bytes(frame_data[payload_end ..].try_into()?);

    // Verify CRC
    let mut hasher = Hasher::new();
    hasher.update(&frame_data[.. payload_end]); // Everything except CRC
    let computed_crc = hasher.finalize();

    if stored_crc != computed_crc {
      anyhow::bail!("CRC mismatch: expected 0x{stored_crc:08x}, got 0x{computed_crc:08x}");
    }

    let payload =
      M::parse_from_bytes(&payload).map_err(|e| anyhow::anyhow!("Failed to parse payload: {e}"))?;

    Ok((Self::new(scope, key, timestamp_micros, payload), total_len))
  }
}
