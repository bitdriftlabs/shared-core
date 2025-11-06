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
use flate2::read::ZlibDecoder;
use std::io::Read;
use std::path::Path;

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
/// This utility operates on raw byte slices from timestamped journals and can reconstruct
/// the key-value state at any historical timestamp by replaying journal entries.
///
/// # Timestamp-Based Recovery
///
/// Timestamps are monotonically non-decreasing logical clocks (not pure wall time),
/// enabling snapshots that match specific event buffer timestamps.
///
/// ## Snapshot Bucketing and Entry Timestamp Overlaps
///
/// Entry timestamps may overlap across adjacent snapshots because compacted entries preserve
/// their original timestamps during rotation. This design provides implementation simplicity
/// and audit trail preservation without affecting recovery correctness.
///
/// **Design rationale:** Preserving original timestamps is not strictly required for
/// point-in-time state reconstruction, but provides benefits at zero cost:
/// - **Implementation simplicity**: No timestamp rewriting logic needed during rotation
/// - **Semantic accuracy**: Maintains "when was this value last modified" for audit trails
/// - **Future-proof**: Preserves historical information that may become useful
///
/// Each snapshot has:
/// - `min_timestamp`: Minimum entry timestamp in the snapshot (from actual entries)
/// - `max_timestamp`: Maximum entry timestamp in the snapshot (from actual entries)
/// - Filename timestamp: The rotation point (equals `max_timestamp` of archived journal)
///
/// Example timeline:
/// ```text
/// Snapshot 1: store.jrn.t300.zz
///   - Entries: foo@100, bar@200, foo@300
///   - min_timestamp: 100, max_timestamp: 300
///   - Range: [100, 300]
///
/// Snapshot 2: store.jrn.t500.zz
///   - Compacted entries: foo@300, bar@200 (original timestamps!)
///   - New entries: baz@400, qux@500
///   - min_timestamp: 200, max_timestamp: 500
///   - Range: [200, 500] — overlaps with [100, 300]!
/// ```
///
/// ## Recovery Bucketing Model
///
/// To recover state for multiple logs at different timestamps efficiently:
///
/// 1. **Bucket logs by snapshot:** Compare log timestamp against each snapshot's `[min_timestamp,
///    max_timestamp]` range
/// 2. **Sequential replay:** For each bucket, replay journals sequentially up to target timestamp
/// 3. **State reconstruction:** Overlapping timestamps are handled correctly because compacted
///    entries represent the state at rotation time
///
/// Example: Recovering logs at timestamps [100, 250, 400, 500]
/// - Log@100: Use Snapshot 1 (100 is in range [100, 300])
/// - Log@250: Use Snapshot 1 (250 is in range [100, 300])
/// - Log@400: Use Snapshot 2 (400 is in range [200, 500], replay compacted state + new entries)
/// - Log@500: Use Snapshot 2 (500 is in range [200, 500])
///
/// ## Invariants
///
/// - Filename timestamps strictly increase (t300 < t500)
/// - Entry timestamp ranges may overlap between adjacent snapshots
/// - Sequential replay produces correct state at any timestamp
///
/// Supports both compressed (zlib) and uncompressed journals. Compressed journals are
/// automatically detected and decompressed transparently.
#[derive(Debug)]
pub struct VersionedRecovery {
  journals: Vec<JournalInfo>,
}

#[derive(Debug)]
struct JournalInfo {
  data: Vec<u8>,
  min_timestamp: u64,
  max_timestamp: u64,
}

impl VersionedRecovery {
  /// Create a new recovery utility from a list of journal byte slices.
  ///
  /// The journals should be provided in chronological order (oldest to newest).
  /// Each journal must be a valid versioned journal (VERSION 2 format).
  /// Journals may be compressed with zlib or uncompressed - decompression is automatic.
  ///
  /// # Errors
  ///
  /// Returns an error if any journal is invalid or cannot be parsed.
  pub fn new(journals: Vec<&[u8]>) -> anyhow::Result<Self> {
    let mut journal_infos = Vec::new();

    for data in journals {
      // Detect and decompress if needed
      let decompressed = decompress_if_needed(data)?;
      let (min_timestamp, max_timestamp) = extract_timestamp_range(&decompressed)?;
      journal_infos.push(JournalInfo {
        data: decompressed,
        min_timestamp,
        max_timestamp,
      });
    }

    Ok(Self {
      journals: journal_infos,
    })
  }

  /// Create a new recovery utility from journal file paths.
  ///
  /// This is an async convenience method that reads journal files from disk.
  /// The journals should be provided in chronological order (oldest to newest).
  ///
  /// # Errors
  ///
  /// Returns an error if any file cannot be read or if any journal is invalid.
  pub async fn from_files(journal_paths: Vec<&Path>) -> anyhow::Result<Self> {
    let mut journal_data = Vec::new();

    for path in journal_paths {
      let data = tokio::fs::read(path).await?;
      journal_data.push(data);
    }

    // Convert Vec<Vec<u8>> to Vec<&[u8]>
    let journal_slices: Vec<&[u8]> = journal_data.iter().map(Vec::as_slice).collect();

    Self::new(journal_slices)
  }

  /// Get the range of timestamps available in the recovery utility.
  ///
  /// Returns (`min_timestamp`, `max_timestamp`) tuple representing the earliest and latest
  /// timestamps that can be recovered.
  #[must_use]
  pub fn timestamp_range(&self) -> Option<(u64, u64)> {
    if self.journals.is_empty() {
      return None;
    }

    let min = self.journals.first().map(|j| j.min_timestamp)?;
    let max = self.journals.last().map(|j| j.max_timestamp)?;
    Some((min, max))
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

    // Find all journals that might contain entries up to target timestamp
    for journal in &self.journals {
      // Skip journals that start after our target
      if journal.min_timestamp > target_timestamp {
        break;
      }

      // Replay entries from this journal
      replay_journal_to_timestamp(&journal.data, target_timestamp, &mut map)?;

      // If this journal contains the target timestamp, we're done
      if journal.max_timestamp >= target_timestamp {
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

/// Decompress journal data if it's zlib-compressed, otherwise return as-is.
///
/// Detection: Checks for zlib magic bytes first (RFC 1950). If not present, validates
/// as uncompressed journal by checking format version.
fn decompress_if_needed(data: &[u8]) -> anyhow::Result<Vec<u8>> {
  const HEADER_SIZE: usize = 16;

  // Check for zlib magic bytes first (RFC 1950)
  // Zlib compressed data starts with 0x78 followed by a second byte where:
  // - 0x01 (no/low compression)
  // - 0x5E (also valid)
  // - 0x9C (default compression)
  // - 0xDA (best compression)
  // The second byte's lower 5 bits are the window size, and bit 5 is the FDICT flag.
  // We check that bit 5 (0x20) is not set for typical zlib streams without preset dictionary.
  if data.len() >= 2 && data[0] == 0x78 && (data[1] & 0x20) == 0 {
    // Looks like zlib compressed data
    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    return Ok(decompressed);
  }

  // Otherwise, treat as uncompressed and validate it's a proper journal
  if data.len() >= HEADER_SIZE {
    // Read format version (first 8 bytes as u64 little-endian)
    let version_bytes: [u8; 8] = data[0 .. 8]
      .try_into()
      .map_err(|_| anyhow::anyhow!("Failed to read version bytes"))?;
    let format_version = u64::from_le_bytes(version_bytes);

    // Check for known format versions
    if format_version == 1 || format_version == 2 {
      return Ok(data.to_vec());
    }

    anyhow::bail!("Invalid journal format version: {format_version}");
  }

  anyhow::bail!("Data too small to be valid journal (size: {})", data.len())
}

/// Extract the minimum/maximum timestamps from a journal.
///
/// Returns (`min_timestamp`, `max_timestamp`).
/// These are computed from actual entry timestamps in the journal.
fn extract_timestamp_range(buffer: &[u8]) -> anyhow::Result<(u64, u64)> {
  let array = read_bonjson_payload(buffer)?;

  let mut min_timestamp = u64::MAX;
  let mut max_timestamp = 0;

  if let Value::Array(entries) = array {
    // Process entries to find min/max timestamps (skip metadata at index 0)
    for (index, entry) in entries.iter().enumerate() {
      if index == 0 {
        continue; // Skip metadata
      }

      if let Value::Object(obj) = entry
        && let Some(t) = read_u64_field(obj, "t")
      {
        min_timestamp = min_timestamp.min(t);
        max_timestamp = max_timestamp.max(t);
      }
    }
  }

  // If no entries found, default to (0, 0)
  if min_timestamp == u64::MAX {
    min_timestamp = 0;
    max_timestamp = 0;
  }

  Ok((min_timestamp, max_timestamp))
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
