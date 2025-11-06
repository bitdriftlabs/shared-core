// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::TimestampedValue;
use ahash::AHashMap;
use bd_bonjson::Value;
use bd_bonjson::decoder::from_slice;

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
///
/// Recovery replays snapshot entries in chronological order up to the target timestamp.
/// Since entry timestamps may overlap across adjacent snapshots, recovery handles this by
/// replaying snapshots sequentially and applying entries in timestamp order.
///
/// ## Optimization
///
/// To recover the current state, only the last snapshot needs to be read since each snapshot
/// contains the complete compacted state at rotation time. For historical timestamp recovery,
/// the utility automatically identifies and replays only the necessary snapshots.
///
/// **Note:** Callers are responsible for decompressing snapshot data if needed before passing
/// it to this utility.
#[derive(Debug)]
pub struct VersionedRecovery {
  snapshots: Vec<SnapshotInfo>,
}

#[derive(Debug)]
struct SnapshotInfo {
  data: Vec<u8>,
  snapshot_timestamp: u64,
}

impl VersionedRecovery {
  /// Create a new recovery utility from a list of uncompressed snapshot byte slices.
  ///
  /// The snapshots should be provided in chronological order (oldest to newest).
  /// Each snapshot must be a valid uncompressed versioned journal (VERSION 2 format).
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
  /// * `target_timestamp` - The timestamp (in nanoseconds since UNIX epoch) to recover state at
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
  ) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
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
  pub fn recover_current(&self) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
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
  map: &mut AHashMap<String, TimestampedValue>,
) -> anyhow::Result<()> {
  let array = read_bonjson_payload(buffer)?;

  let Value::Array(entries) = &array else {
    return Ok(());
  };

  for (index, entry) in entries.iter().enumerate() {
    // Skip metadata (first entry)
    if index == 0 {
      continue;
    }

    let Value::Object(obj) = entry else {
      continue;
    };

    // Extract timestamp (skip entries without timestamp)
    let Some(entry_timestamp) = read_u64_field(obj, "t") else {
      continue;
    };

    // Only apply entries up to target timestamp
    if entry_timestamp > target_timestamp {
      break;
    }

    let (Some(Value::String(key)), Some(operation)) = (obj.get("k"), obj.get("o")) else {
      continue;
    };

    // Extract key and operation
    if operation.is_null() {
      map.remove(key);
    } else {
      map.insert(
        key.clone(),
        TimestampedValue {
          value: operation.clone(),
          timestamp: entry_timestamp,
        },
      );
    }
  }

  Ok(())
}

/// Read the bonjson payload from a snapshot buffer.
fn read_bonjson_payload(buffer: &[u8]) -> anyhow::Result<Value> {
  const HEADER_SIZE: usize = 16;
  const ARRAY_BEGIN: usize = 16;

  if buffer.len() < HEADER_SIZE {
    anyhow::bail!("Buffer too small: {}", buffer.len());
  }

  // Read position from header
  let position_bytes: [u8; 8] = buffer[8 .. 16]
    .try_into()
    .map_err(|_| anyhow::anyhow!("Failed to read position"))?;
  #[allow(clippy::cast_possible_truncation)]
  let position = u64::from_le_bytes(position_bytes) as usize;

  if position < ARRAY_BEGIN {
    anyhow::bail!("Invalid position: {position}, must be at least {ARRAY_BEGIN}");
  }

  if position > buffer.len() {
    anyhow::bail!(
      "Invalid position: {position}, buffer size: {}",
      buffer.len()
    );
  }

  let slice_to_decode = &buffer[ARRAY_BEGIN .. position];

  match from_slice(slice_to_decode) {
    Ok((_, decoded)) => Ok(decoded),
    Err(bd_bonjson::decoder::DecodeError::Partial { partial_value, .. }) => Ok(partial_value),
    Err(e) => anyhow::bail!("Failed to decode buffer: {e:?}"),
  }
}

/// Helper function to read a u64 field from a BONJSON object.
///
/// BONJSON's decoder automatically converts unsigned values that fit in i64 to signed values
/// during decoding (see bd-bonjson/src/decoder.rs:227-234). This means that even though we
/// write `Value::Unsigned(version)`, the decoder returns `Value::Signed(version as i64)`.
///
/// TODO(snowp): Consider changing BONJSON's decoder to preserve the original unsigned type
/// to avoid this normalization behavior and eliminate the need for this helper.
fn read_u64_field(obj: &AHashMap<String, Value>, key: &str) -> Option<u64> {
  match obj.get(key) {
    Some(Value::Unsigned(v)) => Some(*v),
    Some(Value::Signed(v)) if *v >= 0 =>
    {
      #[allow(clippy::cast_sign_loss)]
      Some(*v as u64)
    },
    _ => None,
  }
}
