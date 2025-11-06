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

/// A utility for recovering state at arbitrary timestamps from raw journal data.
///
/// This utility operates on raw uncompressed byte slices from timestamped journals and can
/// reconstruct the key-value state at any historical timestamp by replaying journal entries.
///
/// # Recovery Model
///
/// Recovery works by replaying journal entries in chronological order up to the target timestamp.
/// When journals are rotated, compacted entries preserve their original timestamps, which means
/// entry timestamps may overlap across adjacent journal snapshots. Recovery handles this correctly
/// by replaying journals sequentially and applying entries in timestamp order.
///
/// ## Optimization
///
/// To recover the current state, only the last journal needs to be read since rotation writes
/// the complete compacted state with original timestamps preserved. For historical timestamp
/// recovery, the utility automatically identifies and replays only the necessary journals.
///
/// **Note:** Callers are responsible for decompressing journal data if needed before passing
/// it to this utility.
#[derive(Debug)]
pub struct VersionedRecovery {
  journals: Vec<JournalInfo>,
}

#[derive(Debug)]
struct JournalInfo {
  data: Vec<u8>,
  rotation_timestamp: u64,
}

impl VersionedRecovery {
  /// Create a new recovery utility from a list of uncompressed journal byte slices with rotation
  /// timestamps.
  ///
  /// The journals should be provided in chronological order (oldest to newest).
  /// Each journal must be a valid uncompressed versioned journal (VERSION 2 format).
  ///
  /// # Arguments
  ///
  /// * `journals` - A vector of tuples containing (`journal_data`, `rotation_timestamp`). The
  ///   `rotation_timestamp` represents when this journal was archived (the snapshot boundary). For
  ///   the active journal (not yet rotated), use `u64::MAX`.
  ///
  /// # Errors
  ///
  /// Returns an error if any journal is invalid or cannot be parsed.
  ///
  /// # Note
  ///
  /// Callers must decompress journal data before passing it to this method if the data
  /// is compressed (e.g., with zlib).
  pub fn new(journals: Vec<(&[u8], u64)>) -> anyhow::Result<Self> {
    let journal_infos = journals
      .into_iter()
      .map(|(data, rotation_timestamp)| JournalInfo {
        data: data.to_vec(),
        rotation_timestamp,
      })
      .collect();

    Ok(Self {
      journals: journal_infos,
    })
  }

  /// Recover the key-value state at a specific timestamp.
  ///
  /// This method replays all journal entries from all provided journals up to and including
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
  /// - The target timestamp is not found in any journal
  /// - Journal data is corrupted or invalid
  pub fn recover_at_timestamp(
    &self,
    target_timestamp: u64,
  ) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
    let mut map = AHashMap::new();

    // Replay journals up to and including the journal that was active at target_timestamp.
    // A journal with rotation_timestamp T was the active journal for all timestamps <= T.
    for journal in &self.journals {
      // Replay entries from this journal up to target_timestamp
      replay_journal_to_timestamp(&journal.data, target_timestamp, &mut map)?;

      // If this journal was rotated at or after our target timestamp, we're done.
      // This journal contains all state up to target_timestamp.
      if journal.rotation_timestamp >= target_timestamp {
        break;
      }
    }

    Ok(map)
  }

  /// Get the current state (at the latest timestamp).
  ///
  /// # Errors
  ///
  /// Returns an error if journal data is corrupted or invalid.
  pub fn recover_current(&self) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
    let mut map = AHashMap::new();

    // Optimization: Only read the last journal since journal rotation writes
    // the complete state at the snapshot timestamp, so the last journal contains
    // all current state.
    if let Some(last_journal) = self.journals.last() {
      replay_journal_to_timestamp(&last_journal.data, u64::MAX, &mut map)?;
    }

    Ok(map)
  }
}

/// Replay journal entries up to and including the target timestamp.
///
/// This function processes all journal entries with timestamp ≤ `target_timestamp`.
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

  if let Value::Array(entries) = array {
    for (index, entry) in entries.iter().enumerate() {
      // Skip metadata (first entry)
      if index == 0 {
        continue;
      }

      if let Value::Object(obj) = entry {
        // Extract timestamp (skip entries without timestamp)
        let Some(entry_timestamp) = read_u64_field(obj, "t") else {
          continue;
        };

        // Only apply entries up to target timestamp
        if entry_timestamp > target_timestamp {
          break;
        }

        // Extract key and operation
        if let Some(Value::String(key)) = obj.get("k")
          && let Some(operation) = obj.get("o")
        {
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
      }
    }
  }

  Ok(())
}

/// Read the bonjson payload from a journal buffer.
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
