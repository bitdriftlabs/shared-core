// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::versioned_kv_journal::TimestampedValue;
use crate::versioned_kv_journal::framing::{Frame, decode_raw_frame};
use crate::versioned_kv_journal::journal::{HEADER_SIZE, VERSION};
use crate::{MAX_COMPRESSED_STATE_SNAPSHOT_BYTES, Scope};
use ahash::{AHashMap, AHashSet};
use bd_proto::protos::state::payload::StateValue;
use flate2::{Decompress, FlushDecompress, Status};
use protobuf::Message;
use std::io::{ErrorKind, Read};

/// A utility for recovering state at arbitrary timestamps from journal snapshots.
///
/// This utility operates on raw uncompressed byte slices from archived journal snapshots
/// (created during rotation) and can reconstruct the key-value state at any historical
/// timestamp by replaying journal entries.
///
/// # Recovery Model
///
/// Recovery works exclusively with journal snapshots - complete archived journals created
/// during rotation. Each snapshot contains the full compacted state at the time of rotation,
/// with all entries preserving their original timestamps.
#[derive(Debug)]
pub struct VersionedRecovery {
  snapshots: Vec<SnapshotInfo>,
}

#[derive(Debug)]
struct SnapshotInfo {
  data: Vec<u8>,
  snapshot_timestamp: u64,
}

/// A fully validated state mutation decoded from a versioned journal.
///
/// A missing [`StateValue::value_type`] is the journal's tombstone representation.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedStateChange {
  pub scope: Scope,
  pub key: String,
  pub value: StateValue,
  pub timestamp_micros: u64,
}

fn journal_position(data: &[u8]) -> anyhow::Result<usize> {
  if data.len() < HEADER_SIZE {
    anyhow::bail!("Buffer too small: {}", data.len());
  }

  if data[0] != VERSION {
    anyhow::bail!("Unsupported version: {}, expected {VERSION}", data[0]);
  }

  let position_bytes: [u8; 8] = data[1 .. 9]
    .try_into()
    .map_err(|_| anyhow::anyhow!("Failed to read position"))?;
  let position_u64 = u64::from_le_bytes(position_bytes);
  let position = usize::try_from(position_u64)
    .map_err(|_| anyhow::anyhow!("Position {position_u64} too large for usize"))?;

  if position < HEADER_SIZE {
    anyhow::bail!("Invalid position: {position}, must be at least {HEADER_SIZE}");
  }
  Ok(position)
}

fn journal_end(data: &[u8]) -> anyhow::Result<usize> {
  let position = journal_position(data)?;
  if position > data.len() {
    anyhow::bail!("Invalid position: {position}, buffer size: {}", data.len());
  }

  Ok(position)
}

fn validate_compressed_journal_size(compressed: &[u8]) -> anyhow::Result<()> {
  if compressed.is_empty() {
    anyhow::bail!("State snapshot is empty");
  }
  if compressed.len() > MAX_COMPRESSED_STATE_SNAPSHOT_BYTES {
    anyhow::bail!(
      "State snapshot exceeds compressed-size limit: {} > {MAX_COMPRESSED_STATE_SNAPSHOT_BYTES}",
      compressed.len()
    );
  }

  Ok(())
}

fn decompress_journal(compressed: &[u8], max_decompressed_bytes: usize) -> anyhow::Result<Vec<u8>> {
  validate_compressed_journal_size(compressed)?;

  let max_bytes_with_sentinel = max_decompressed_bytes
    .checked_add(1)
    .ok_or_else(|| anyhow::anyhow!("Invalid decompressed-size limit"))?;
  let mut decoder = StrictZlibReader::new(compressed);
  let mut journal = Vec::new();
  decoder
    .by_ref()
    .take(max_bytes_with_sentinel as u64)
    .read_to_end(&mut journal)?;
  if journal.len() > max_decompressed_bytes {
    anyhow::bail!(
      "State snapshot exceeds decompressed-size limit: {} > {max_decompressed_bytes}",
      journal.len()
    );
  }

  Ok(journal)
}

//
// StrictZlibReader
//

/// A zlib reader that requires the stream to end cleanly and consumes all input bytes.
///
/// `flate2::read::ZlibDecoder` may yield decoded bytes before it has observed a complete zlib
/// trailer. Artifact ingestion must not accept that truncated prefix as a valid snapshot.
struct StrictZlibReader<'a> {
  decompressor: Decompress,
  input: &'a [u8],
  input_offset: usize,
  stream_finished: bool,
}

impl<'a> StrictZlibReader<'a> {
  fn new(input: &'a [u8]) -> Self {
    Self {
      decompressor: Decompress::new(true),
      input,
      input_offset: 0,
      stream_finished: false,
    }
  }
}

impl Read for StrictZlibReader<'_> {
  fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
    if output.is_empty() || self.stream_finished {
      return Ok(0);
    }

    loop {
      let input_before = self.decompressor.total_in();
      let output_before = self.decompressor.total_out();
      let status = self
        .decompressor
        .decompress(
          &self.input[self.input_offset ..],
          output,
          FlushDecompress::None,
        )
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
      let consumed = usize::try_from(self.decompressor.total_in() - input_before)
        .map_err(|_| std::io::Error::other("zlib input offset overflow"))?;
      let produced = usize::try_from(self.decompressor.total_out() - output_before)
        .map_err(|_| std::io::Error::other("zlib output offset overflow"))?;
      self.input_offset += consumed;

      if status == Status::StreamEnd {
        if self.input_offset != self.input.len() {
          return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "State snapshot has trailing data after the zlib stream",
          ));
        }
        self.stream_finished = true;
        return Ok(produced);
      }
      if produced != 0 {
        return Ok(produced);
      }
      if self.input_offset == self.input.len() {
        return Err(std::io::Error::new(
          ErrorKind::UnexpectedEof,
          "State snapshot ended before the zlib stream completed",
        ));
      }
      if consumed == 0 {
        return Err(std::io::Error::new(
          ErrorKind::InvalidData,
          "State snapshot zlib decoder made no progress",
        ));
      }
    }
  }
}

//
// LimitedReader
//

/// A decompression reader that accepts at most the configured number of output bytes.
struct LimitedReader<R> {
  reader: R,
  bytes_read: usize,
  max_bytes: usize,
}

impl<R> LimitedReader<R> {
  fn new(reader: R, max_bytes: usize) -> Self {
    Self {
      reader,
      bytes_read: 0,
      max_bytes,
    }
  }
}

impl<R: Read> Read for LimitedReader<R> {
  fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
    if buffer.is_empty() {
      return Ok(0);
    }

    if self.bytes_read == self.max_bytes {
      let mut sentinel = [0; 1];
      if self.reader.read(&mut sentinel)? == 0 {
        return Ok(0);
      }
      return Err(std::io::Error::new(
        ErrorKind::InvalidData,
        "State snapshot exceeds decompressed-size limit",
      ));
    }

    let readable_bytes = buffer.len().min(self.max_bytes - self.bytes_read);
    let read = self.reader.read(&mut buffer[.. readable_bytes])?;
    self.bytes_read += read;
    Ok(read)
  }
}

fn read_frame<R: Read>(reader: &mut R, remaining_bytes: usize) -> anyhow::Result<(Vec<u8>, usize)> {
  let mut length_bytes = Vec::with_capacity(10);
  let mut frame_len = 0_u64;
  for index in 0 .. 10 {
    let mut byte = [0; 1];
    reader.read_exact(&mut byte)?;
    length_bytes.push(byte[0]);

    let value = u64::from(byte[0] & 0x7f);
    if index == 9 && value > 1 {
      anyhow::bail!("Invalid frame length varint");
    }
    let shift = index * 7;
    frame_len |= value
      .checked_shl(shift)
      .ok_or_else(|| anyhow::anyhow!("Invalid frame length varint"))?;
    if byte[0] & 0x80 == 0 {
      let frame_len = usize::try_from(frame_len)
        .map_err(|_| anyhow::anyhow!("Frame length too large: {frame_len}"))?;
      let encoded_len = length_bytes
        .len()
        .checked_add(frame_len)
        .ok_or_else(|| anyhow::anyhow!("Frame length overflow"))?;
      if encoded_len > remaining_bytes {
        anyhow::bail!("Frame extends past the journal end");
      }

      let mut frame = length_bytes;
      frame.resize(encoded_len, 0);
      let frame_data_start = encoded_len - frame_len;
      reader.read_exact(&mut frame[frame_data_start ..])?;
      return Ok((frame, encoded_len));
    }
  }

  anyhow::bail!("Invalid frame length varint")
}

fn drain_to_end<R: Read>(reader: &mut R) -> anyhow::Result<()> {
  let mut buffer = [0; 8 * 1024];
  while reader.read(&mut buffer)? != 0 {}
  Ok(())
}

/// Decode every frame in an uncompressed versioned state journal.
///
/// Unlike local journal recovery, which may tolerate partial data loss after a crash, callers
/// ingesting a remotely uploaded snapshot must reject any malformed or truncated frame. This
/// function therefore validates the complete range declared by the journal header before
/// returning a single state change.
pub fn decode_journal(data: &[u8]) -> anyhow::Result<Vec<DecodedStateChange>> {
  let position = journal_end(data)?;
  let mut changes = Vec::new();
  let mut offset = HEADER_SIZE;
  while offset < position {
    let (frame, bytes_read) = Frame::<StateValue>::decode(&data[offset .. position])
      .map_err(|error| anyhow::anyhow!("Invalid journal frame at offset {offset}: {error}"))?;
    changes.push(DecodedStateChange {
      scope: frame.scope,
      key: frame.key.to_string(),
      value: frame.payload,
      timestamp_micros: frame.timestamp_micros,
    });
    offset += bytes_read;
  }

  Ok(changes)
}

/// Boundedly decompress a state snapshot and extract all distinct non-empty string values for a
/// single state key.
///
/// The scanner validates the journal header, every frame's bounds, and every frame's checksum. It
/// only protobuf-decodes payloads for the requested `(scope, key)`, avoiding unnecessary state
/// deserialization in routing-only consumers. It retains only one encoded frame at a time rather
/// than materializing the entire decompressed journal.
pub fn extract_non_empty_string_values_from_compressed_journal(
  compressed: &[u8],
  max_decompressed_bytes: usize,
  scope: Scope,
  key: &str,
  max_values: usize,
) -> anyhow::Result<Vec<String>> {
  if max_values == 0 {
    anyhow::bail!("State snapshot value limit must be positive");
  }
  validate_compressed_journal_size(compressed)?;
  let mut reader = LimitedReader::new(StrictZlibReader::new(compressed), max_decompressed_bytes);
  let mut header = [0; HEADER_SIZE];
  reader.read_exact(&mut header)?;
  let position = journal_position(&header)?;
  if position > max_decompressed_bytes {
    anyhow::bail!(
      "State snapshot exceeds decompressed-size limit: {position} > {max_decompressed_bytes}"
    );
  }
  let mut values = Vec::new();
  let mut seen_values = AHashSet::new();
  let mut offset = HEADER_SIZE;
  while offset < position {
    let (encoded_frame, bytes_read) = read_frame(&mut reader, position - offset)?;
    let (frame, decoded_bytes_read) = decode_raw_frame(&encoded_frame)
      .map_err(|error| anyhow::anyhow!("Invalid journal frame at offset {offset}: {error}"))?;
    if decoded_bytes_read != bytes_read {
      anyhow::bail!("Invalid journal frame length at offset {offset}");
    }
    if frame.scope == scope && frame.key == key {
      let value = StateValue::parse_from_bytes(frame.payload)
        .map_err(|error| anyhow::anyhow!("Invalid state value at offset {offset}: {error}"))?;
      if let Some(bd_proto::protos::state::payload::state_value::Value_type::StringValue(value)) =
        value.value_type
        && !value.is_empty()
        && seen_values.insert(value.clone())
      {
        values.push(value);
        if values.len() > max_values {
          anyhow::bail!(
            "State snapshot exceeds distinct value limit: {} > {max_values}",
            values.len()
          );
        }
      }
    }
    offset += bytes_read;
  }

  // The journal is a memory-mapped file and may include trailing capacity after `position`.
  // Drain it to validate the zlib stream and apply the output limit without retaining that padding.
  drain_to_end(&mut reader)?;

  Ok(values)
}

/// Boundedly decompress and decode a state-snapshot artifact.
///
/// The caller supplies the server's decompressed-size limit. A corrupted zlib stream, a stream
/// that expands beyond that limit, or any malformed journal frame is rejected.
pub fn decode_compressed_journal(
  compressed: &[u8],
  max_decompressed_bytes: usize,
) -> anyhow::Result<Vec<DecodedStateChange>> {
  let journal = decompress_journal(compressed, max_decompressed_bytes)?;
  decode_journal(&journal)
}

impl VersionedRecovery {
  /// Create a new recovery utility from a list of uncompressed snapshot byte slices.
  ///
  /// The snapshots should be provided in chronological order (oldest to newest).
  /// Each snapshot must be a valid uncompressed versioned journal (VERSION 1 format).
  ///
  /// # Arguments
  ///
  /// * `snapshots` - A vector of tuples containing (`snapshot_data`, `snapshot_timestamp`). The
  ///   `snapshot_timestamp` represents when this snapshot was created (archived during rotation).
  ///
  /// # Errors
  ///
  /// Returns an error if any snapshot is invalid or cannot be parsed.
  ///
  /// # Note
  ///
  /// Callers must decompress snapshot data before passing it to this method if the data
  /// is compressed (e.g., with zlib).
  pub fn new(snapshots: Vec<(&[u8], u64)>) -> anyhow::Result<Self> {
    let snapshot_infos = snapshots
      .into_iter()
      .map(|(data, snapshot_timestamp)| SnapshotInfo {
        data: data.to_vec(),
        snapshot_timestamp,
      })
      .collect();

    Ok(Self {
      snapshots: snapshot_infos,
    })
  }

  /// Recover the key-value state at a specific timestamp.
  ///
  /// This method replays all snapshot entries from all provided snapshots up to and including
  /// the target timestamp, reconstructing the exact state at that point in time.
  ///
  /// ## Important: "Up to and including" semantics
  ///
  /// When recovering at timestamp T, **ALL entries with timestamp ≤ T are included**.
  /// This is critical because timestamps are monotonically non-decreasing (not strictly
  /// increasing): if the system clock doesn't advance between writes, multiple entries
  /// will share the same timestamp value. These entries must all be included to ensure
  /// a consistent view of the state.
  ///
  /// Entries with the same timestamp are applied in version order (which reflects write
  /// order), so later writes correctly overwrite earlier ones ("last write wins").
  ///
  /// # Arguments
  ///
  /// * `target_timestamp` - The timestamp (in microseconds since UNIX epoch) to recover state at
  ///
  /// # Returns
  ///
  /// A hashmap containing all key-value pairs with their timestamps as they existed at the
  /// target timestamp.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// - The target timestamp is not found in any snapshot
  /// - Snapshot data is corrupted or invalid
  pub fn recover_at_timestamp(
    &self,
    target_timestamp: u64,
  ) -> anyhow::Result<AHashMap<(Scope, String), TimestampedValue>> {
    let mut map = AHashMap::new();

    // Replay snapshots up to and including the snapshot that was created at or after
    // target_timestamp. A snapshot with snapshot_timestamp T contains all state up to time T.
    for snapshot in &self.snapshots {
      // Replay entries from this snapshot up to target_timestamp
      replay_journal_to_timestamp(&snapshot.data, target_timestamp, &mut map)?;

      // If this snapshot was created at or after our target timestamp, we're done.
      // This snapshot contains all state up to target_timestamp.
      if snapshot.snapshot_timestamp >= target_timestamp {
        break;
      }
    }

    Ok(map)
  }

  /// Get the current state from the latest snapshot.
  ///
  /// Since each snapshot contains the complete compacted state at rotation time,
  /// only the last snapshot needs to be read to get the current state.
  ///
  /// # Errors
  ///
  /// Returns an error if snapshot data is corrupted or invalid.
  pub fn recover_current(&self) -> anyhow::Result<AHashMap<(Scope, String), TimestampedValue>> {
    let mut map = AHashMap::new();

    // Optimization: Only read the last snapshot since rotation writes the complete
    // compacted state, so the last snapshot contains all current state.
    if let Some(last_snapshot) = self.snapshots.last() {
      replay_journal_to_timestamp(&last_snapshot.data, u64::MAX, &mut map)?;
    }

    Ok(map)
  }
}

/// Replay snapshot entries up to and including the target timestamp.
///
/// This function processes all entries with timestamp ≤ `target_timestamp`.
/// The "up to and including" behavior is essential because timestamps are monotonically
/// non-decreasing (not strictly increasing): if the system clock doesn't advance between
/// writes, multiple entries may share the same timestamp. All such entries must be
/// applied to ensure state consistency.
///
/// Entries are processed in version order, ensuring "last write wins" semantics when
/// multiple operations affect the same key at the same timestamp.
fn replay_journal_to_timestamp(
  buffer: &[u8],
  target_timestamp: u64,
  map: &mut AHashMap<(Scope, String), TimestampedValue>,
) -> anyhow::Result<()> {
  if buffer.len() < HEADER_SIZE {
    anyhow::bail!("Buffer too small: {}", buffer.len());
  }

  // Read position from header (bytes 1-8)
  let position_bytes: [u8; 8] = buffer[1 .. 9]
    .try_into()
    .map_err(|_| anyhow::anyhow!("Failed to read position"))?;
  #[allow(clippy::cast_possible_truncation)]
  let position = u64::from_le_bytes(position_bytes) as usize;

  if position < HEADER_SIZE {
    anyhow::bail!("Invalid position: {position}, must be at least {HEADER_SIZE}");
  }

  if position > buffer.len() {
    anyhow::bail!(
      "Invalid position: {position}, buffer size: {}",
      buffer.len()
    );
  }

  // Decode frames from the journal data.
  let mut offset = 0;
  let data = &buffer[HEADER_SIZE .. position];

  while offset < data.len() {
    match Frame::<StateValue>::decode(&data[offset ..]) {
      Ok((frame, bytes_read)) => {
        // Only apply entries up to target timestamp.
        if frame.timestamp_micros > target_timestamp {
          break;
        }

        if frame.payload.value_type.is_none() {
          // Deletion (StateValue with no value_type set).
          map.remove(&(frame.scope, frame.key.to_string()));
        } else {
          // Insertion - store the protobuf StateValue with (scope, key) tuple.
          map.insert(
            (frame.scope, frame.key.to_string()),
            TimestampedValue {
              value: frame.payload,
              timestamp: frame.timestamp_micros,
            },
          );
        }

        offset += bytes_read;
      },
      Err(_) => {
        // End of valid data or corrupted frame.
        break;
      },
    }
  }

  Ok(())
}
