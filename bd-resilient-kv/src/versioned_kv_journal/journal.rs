// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::framing::Frame;
use bd_client_common::error::InvariantError;
use bd_time::TimeProvider;
use std::sync::Arc;

/// Indicates whether partial data loss has occurred. Partial data loss is detected when the
/// journal would be parsed from disk, but we were not able to find valid records up to `position`
/// as stored in the header.
pub enum PartialDataLoss {
  Yes,
  None,
}

/// Timestamped implementation of a journaling system that uses timestamps
/// as the version identifier for point-in-time recovery.
///
/// Each write operation is assigned a monotonically non-decreasing timestamp (in microseconds
/// since UNIX epoch), enabling exact state reconstruction at any historical timestamp.
/// The monotonicity is enforced by clamping: if the system clock goes backwards, we reuse
/// the same timestamp value to maintain ordering guarantees. When timestamps collide,
/// journal ordering determines precedence.
pub struct VersionedJournal<'a, M> {
  position: usize,
  buffer: &'a mut [u8],
  high_water_mark: usize,
  high_water_mark_triggered: bool,
  last_timestamp: u64, // Most recent timestamp written (for monotonic enforcement)
  pub(crate) time_provider: Arc<dyn TimeProvider>,
  _payload_marker: std::marker::PhantomData<M>,
}

// Versioned KV files have the following structure:
// | Position | Data                     | Type           |
// |----------|--------------------------|----------------|
// | 0        | Format Version           | u64            |
// | 8        | Position                 | u64            |
// | 16       | Reserved                 | u8             |
// | 17       | Frame 1                  | Framed Entry   |
// | ...      | Frame 2                  | Framed Entry   |
// | ...      | Frame N                  | Framed Entry   |
//
// Frame format: [length: u32][timestamp_micros: varint][protobuf_payload: bytes][crc32: u32]
//
// # Timestamp Semantics
//
// Timestamps serve as both version identifiers and logical clocks with monotonic guarantees:
// - Each write gets a timestamp that is guaranteed to be >= previous writes (non-decreasing)
// - If system clock goes backward, timestamps are clamped to last_timestamp (reuse same value)
// - When timestamps collide, journal ordering determines precedence
// - This ensures total ordering while allowing correlation with external timestamped systems

// The journal format version, incremented on incompatible changes.
const VERSION: u64 = 1;

const HEADER_SIZE: usize = 17;

// Minimum buffer size for a valid journal
const MIN_BUFFER_SIZE: usize = HEADER_SIZE + 4;

/// Returns by
struct BufferState {
  highest_timestamp: u64,
  partial_data_loss: PartialDataLoss,
}

/// Write to the version field of a journal buffer.
fn write_version_field(buffer: &mut [u8], version: u64) {
  let version_bytes = version.to_le_bytes();
  buffer[0 .. 8].copy_from_slice(&version_bytes);
}

/// Write the version to a journal buffer.
fn write_version(buffer: &mut [u8]) {
  write_version_field(buffer, VERSION);
}

fn read_position(buffer: &[u8]) -> anyhow::Result<usize> {
  let position_bytes: [u8; 8] = buffer[8 .. 16].try_into()?;
  let position_u64 = u64::from_le_bytes(position_bytes);
  let position = usize::try_from(position_u64)
    .map_err(|_| anyhow::anyhow!("Position {position_u64} too large for usize"))?;
  let buffer_len = buffer.len();
  if position >= buffer_len {
    anyhow::bail!("Invalid position: {position}, buffer size: {buffer_len}",);
  }
  Ok(position)
}

/// Write the position to a journal buffer.
fn write_position(buffer: &mut [u8], position: usize) {
  let position_bytes = (position as u64).to_le_bytes();
  buffer[8 .. 16].copy_from_slice(&position_bytes);
}

fn validate_buffer_len(buffer: &[u8]) -> anyhow::Result<usize> {
  let buffer_len = buffer.len();
  if buffer_len < MIN_BUFFER_SIZE {
    anyhow::bail!(
      "Buffer too small: {buffer_len} bytes, but need at least {MIN_BUFFER_SIZE} bytes"
    );
  }
  Ok(buffer_len)
}

/// Validate high water mark ratio and calculate the position from buffer length.
fn calculate_high_water_mark(
  buffer_len: usize,
  high_water_mark_ratio: Option<f32>,
) -> anyhow::Result<usize> {
  let ratio = high_water_mark_ratio.unwrap_or(0.8);
  if !(0.0 ..= 1.0).contains(&ratio) {
    anyhow::bail!("High water mark ratio must be between 0.0 and 1.0, got: {ratio}");
  }

  #[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
  )]
  let high_water_mark = (buffer_len as f32 * ratio) as usize;
  Ok(high_water_mark)
}

impl<'a, M: protobuf::Message> VersionedJournal<'a, M> {
  /// Create a new versioned journal using the provided buffer as storage space.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `entries` - Iterator of entries to be inserted into the newly created buffer.
  ///
  /// # Errors
  /// Returns an error if the buffer is too small or if `high_water_mark_ratio` is invalid.
  pub fn new(
    buffer: &'a mut [u8],
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
    entries: impl IntoIterator<Item = (M, u64)>,
  ) -> anyhow::Result<Self> {
    let buffer_len = validate_buffer_len(buffer)?;
    let high_water_mark = calculate_high_water_mark(buffer_len, high_water_mark_ratio)?;

    // Write header
    let mut position = HEADER_SIZE;

    let mut max_state_timestamp = None;

    // Write all current state with their original timestamps
    for (entry, timestamp) in entries {
      max_state_timestamp = Some(timestamp);

      let frame = Frame::new(timestamp, entry);

      // Encode frame
      let available_space = &mut buffer[position ..];
      let encoded_len = frame.encode(available_space)?;

      position += encoded_len;
    }

    write_position(buffer, position);
    write_version(buffer);
    buffer[16] = 0; // Reserved byte

    let now = Self::unix_timestamp_micros(time_provider.as_ref())?;

    Ok(Self {
      position,
      buffer,
      high_water_mark,
      high_water_mark_triggered: false,
      last_timestamp: max_state_timestamp.unwrap_or(now),
      time_provider,
      _payload_marker: std::marker::PhantomData,
    })
  }

  /// Create a new versioned journal with state loaded from the provided buffer.
  ///
  /// # Arguments
  /// * `buffer` - The storage buffer containing existing versioned KV data
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  ///
  /// # Errors
  /// Returns an error if the buffer is invalid, corrupted, or if `high_water_mark_ratio` is
  /// invalid.
  pub fn from_buffer(
    buffer: &'a mut [u8],
    high_water_mark_ratio: Option<f32>,
    time_provider: Arc<dyn TimeProvider>,
    f: impl FnMut(&M, u64),
  ) -> anyhow::Result<(Self, PartialDataLoss)> {
    let buffer_len = validate_buffer_len(buffer)?;
    let position = read_position(buffer)?;
    let high_water_mark = calculate_high_water_mark(buffer_len, high_water_mark_ratio)?;

    // Read version
    let version_bytes: [u8; 8] = buffer[0 .. 8].try_into()?;
    let version = u64::from_le_bytes(version_bytes);

    if version != VERSION {
      anyhow::bail!("Unsupported version: {version}, expected {VERSION}");
    }

    // Find initialization timestamp and highest timestamp in the journal
    let buffer_state = Self::iterate_buffer(buffer, position, f);

    Ok((
      Self {
        position,
        buffer,
        high_water_mark,
        high_water_mark_triggered: position >= high_water_mark,
        last_timestamp: buffer_state.highest_timestamp,
        time_provider,
        _payload_marker: std::marker::PhantomData,
      },
      buffer_state.partial_data_loss,
    ))
  }

  /// Scan the journal to find the highest timestamp and apply the provided function to each entry.
  /// This is used during initialization to reconstruct state and also detects partial data loss.
  fn iterate_buffer(buffer: &[u8], position: usize, mut f: impl FnMut(&M, u64)) -> BufferState {
    let mut cursor = HEADER_SIZE;
    let mut state = BufferState {
      highest_timestamp: 0,
      partial_data_loss: PartialDataLoss::None,
    };

    while cursor < position {
      let remaining = &buffer[cursor .. position];

      if let Ok((frame, consumed)) = Frame::<M>::decode(remaining) {
        f(&frame.payload, frame.timestamp_micros);
        state.highest_timestamp = frame.timestamp_micros;
        cursor += consumed;
      } else {
        // Stop on first decode error (partial frame or corruption)
        log::debug!("Journal decode error at position {cursor}, marking partial data loss");
        state.partial_data_loss = PartialDataLoss::Yes;
        break;
      }
    }

    state
  }

  /// Get the next monotonically increasing timestamp.
  ///
  /// This ensures that even if the system clock goes backwards, timestamps remain
  /// monotonically increasing by clamping to `last_timestamp` (reusing the same value).
  /// This prevents artificial clock skew while maintaining ordering guarantees.
  fn next_monotonic_timestamp(&mut self) -> anyhow::Result<u64> {
    let current = self.current_timestamp()?;
    let monotonic = std::cmp::max(current, self.last_timestamp);
    self.last_timestamp = monotonic;
    Ok(monotonic)
  }

  fn set_position(&mut self, position: usize) {
    self.position = position;
    write_position(self.buffer, position);
    self.check_high_water_mark();
  }

  fn check_high_water_mark(&mut self) {
    if self.position >= self.high_water_mark {
      self.trigger_high_water();
    }
  }

  fn trigger_high_water(&mut self) {
    self.high_water_mark_triggered = true;
  }

  /// Insert a new entry into the journal with the given payload.
  /// Returns the timestamp of the operation.
  ///
  /// The timestamp is monotonically non-decreasing and serves as the version identifier.
  /// If the system clock goes backwards, timestamps are clamped to maintain monotonicity.
  pub fn insert_entry(&mut self, message: M) -> anyhow::Result<u64> {
    let timestamp = self.next_monotonic_timestamp()?;

    // Create payload
    let frame = Frame::new(timestamp, message);

    // Encode frame
    let available_space = &mut self.buffer[self.position ..];
    let encoded_len = frame.encode(available_space)?;

    self.set_position(self.position + encoded_len);
    Ok(timestamp)
  }

  /// Check if the high water mark has been triggered.
  #[must_use]
  pub fn is_high_water_mark_triggered(&self) -> bool {
    self.high_water_mark_triggered
  }

  /// Get current timestamp in microseconds since UNIX epoch.
  fn current_timestamp(&self) -> std::result::Result<u64, InvariantError> {
    Self::unix_timestamp_micros(self.time_provider.as_ref())
  }

  fn unix_timestamp_micros(
    time_provider: &dyn TimeProvider,
  ) -> std::result::Result<u64, InvariantError> {
    time_provider
      .now()
      .unix_timestamp_nanos()
      .checked_div(1_000)
      .and_then(|micros| micros.try_into().ok())
      .ok_or(InvariantError::Invariant)
  }
}
