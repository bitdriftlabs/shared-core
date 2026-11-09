// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

/// Maximum varint size for u64 (10 bytes)
pub const MAX_SIZE: usize = 10;

/// Calculate the size of a u64 when encoded as a varint.
#[allow(clippy::cast_possible_truncation)]
pub fn compute_size(value: u64) -> usize {
  // Safe cast: varint encoding of u64 is at most 10 bytes, which fits in usize on all platforms
  ::protobuf::rt::compute_raw_varint64_size(value) as usize
}

/// Encode a u64 as a varint into the buffer.
/// Returns the number of bytes written.
pub fn encode(value: u64, mut buf: &mut [u8]) -> usize {
  let size = compute_size(value);
  debug_assert!(buf.len() >= size, "Buffer too small for varint encoding");

  if protobuf::CodedOutputStream::new(&mut buf)
    .write_raw_varint64(value)
    .is_err()
  {
    // Should never happen as we ensure that there is enough space elsewhere.
    return 0;
  }

  size
}

/// Decode a varint from the buffer.
/// Returns (value, `bytes_read`) or None if buffer is incomplete/invalid.
#[must_use]
pub fn decode(buf: &[u8]) -> Option<(u64, usize)> {
  let mut value = 0_u64;
  for (index, byte) in buf.iter().copied().take(MAX_SIZE).enumerate() {
    let bits = u64::from(byte & 0x7f);
    // A u64's tenth varint byte can contain only its most significant bit.
    if index == MAX_SIZE - 1 && (byte & 0x80 != 0 || bits > 1) {
      return None;
    }
    value |= bits << (index * 7);

    if byte & 0x80 == 0 {
      let bytes_read = index + 1;
      // Journal frames always use the canonical encoding emitted by encode(). Rejecting
      // overlong values prevents a following field from being interpreted at the wrong offset.
      return (compute_size(value) == bytes_read).then_some((value, bytes_read));
    }
  }
  None
}
