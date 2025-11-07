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
  let value = protobuf::CodedInputStream::from_bytes(buf)
    .read_raw_varint64()
    .ok()?;

  let bytes_read = compute_size(value);
  Some((value, bytes_read))
}
