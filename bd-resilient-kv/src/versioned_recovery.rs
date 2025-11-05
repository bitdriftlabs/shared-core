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

/// A utility for recovering state at arbitrary versions from raw journal data.
///
/// This utility operates on raw byte slices from versioned journals and can reconstruct
/// the key-value state at any historical version by replaying journal entries.
///
/// Supports both compressed (zlib) and uncompressed journals. Compressed journals are
/// automatically detected and decompressed transparently.
///
/// # Usage
///
/// ```ignore
/// use bd_resilient_kv::VersionedRecovery;
///
/// // Load journal data as byte slices (may be compressed or uncompressed)
/// let archived_journal = std::fs::read("store.jrn.v30000.zz")?; // Compressed
/// let active_journal = std::fs::read("store.jrn")?; // Uncompressed
///
/// // Create recovery utility with both journals
/// let recovery = VersionedRecovery::new(vec![&archived_journal, &active_journal])?;
///
/// // Recover state at specific version
/// let state_at_25000 = recovery.recover_at_version(25000)?;
/// ```
#[derive(Debug)]
pub struct VersionedRecovery {
  journals: Vec<JournalInfo>,
}

#[derive(Debug)]
struct JournalInfo {
  data: Vec<u8>,
  base_version: u64,
  max_version: u64,
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
      let (base_version, max_version) = extract_version_range(&decompressed)?;
      journal_infos.push(JournalInfo {
        data: decompressed,
        base_version,
        max_version,
      });
    }

    Ok(Self {
      journals: journal_infos,
    })
  }

  /// Recover the key-value state at a specific version.
  ///
  /// This method replays all journal entries from all provided journals up to and including
  /// the target version, reconstructing the exact state at that point in time.
  ///
  /// # Arguments
  ///
  /// * `target_version` - The version to recover state at
  ///
  /// # Returns
  ///
  /// A hashmap containing all key-value pairs with their timestamps as they existed at the
  /// target version.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// - The target version is not found in any journal
  /// - Journal data is corrupted or invalid
  pub fn recover_at_version(
    &self,
    target_version: u64,
  ) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
    let mut map = AHashMap::new();

    // Find all journals that might contain entries up to target version
    for journal in &self.journals {
      // Skip journals that start after our target
      if journal.base_version > target_version {
        break;
      }

      // Replay entries from this journal
      replay_journal_to_version(&journal.data, target_version, &mut map)?;

      // If this journal contains the target version, we're done
      if journal.max_version >= target_version {
        break;
      }
    }

    Ok(map)
  }

  /// Get the range of versions available in the recovery utility.
  ///
  /// Returns (`min_version`, `max_version`) tuple representing the earliest and latest
  /// versions that can be recovered.
  #[must_use]
  pub fn version_range(&self) -> Option<(u64, u64)> {
    if self.journals.is_empty() {
      return None;
    }

    let min = self.journals.first().map(|j| j.base_version)?;
    let max = self.journals.last().map(|j| j.max_version)?;
    Some((min, max))
  }

  /// Get the current state (at the latest version).
  ///
  /// # Errors
  ///
  /// Returns an error if journal data is corrupted or invalid.
  pub fn recover_current(&self) -> anyhow::Result<AHashMap<String, TimestampedValue>> {
    let mut map = AHashMap::new();

    for journal in &self.journals {
      replay_journal_to_version(&journal.data, u64::MAX, &mut map)?;
    }

    Ok(map)
  }
}

/// Decompress journal data if it's zlib-compressed, otherwise return as-is.
///
/// Detection: Try to read the header. If it's a valid journal header (format version at offset 0),
/// it's uncompressed. Otherwise, attempt zlib decompression.
fn decompress_if_needed(data: &[u8]) -> anyhow::Result<Vec<u8>> {
  const HEADER_SIZE: usize = 16;

  // Check if data looks like a valid uncompressed journal
  if data.len() >= HEADER_SIZE {
    // Read format version (first 8 bytes as u64 little-endian)
    let version_bytes: [u8; 8] = data[0 .. 8]
      .try_into()
      .map_err(|_| anyhow::anyhow!("Failed to read version bytes"))?;
    let format_version = u64::from_le_bytes(version_bytes);

    // If format version is 1 or 2, it's likely uncompressed
    if format_version == 1 || format_version == 2 {
      return Ok(data.to_vec());
    }
  }

  // Try to decompress as zlib
  let mut decoder = ZlibDecoder::new(data);
  let mut decompressed = Vec::new();
  decoder.read_to_end(&mut decompressed)?;

  Ok(decompressed)
}

/// Extract the base version and maximum version from a journal.
fn extract_version_range(buffer: &[u8]) -> anyhow::Result<(u64, u64)> {
  let array = read_bonjson_payload(buffer)?;

  // Extract base_version from metadata (default to 1 if not found)
  let base_version = if let Value::Array(entries) = &array
    && let Some(Value::Object(obj)) = entries.first()
  {
    if let Some(Value::Unsigned(base)) = obj.get("base_version") {
      *base
    } else if let Some(Value::Signed(base)) = obj.get("base_version") {
      #[allow(clippy::cast_sign_loss)]
      (*base as u64)
    } else {
      1 // Default to 1 for compatibility
    }
  } else {
    anyhow::bail!("Failed to extract metadata from journal");
  };

  // Find the maximum version by scanning all entries
  let mut max_version = base_version;
  if let Value::Array(entries) = array {
    for (index, entry) in entries.iter().enumerate() {
      if index == 0 {
        continue; // Skip metadata
      }

      if let Value::Object(obj) = entry {
        if let Some(Value::Unsigned(v)) = obj.get("v") {
          max_version = max_version.max(*v);
        } else if let Some(Value::Signed(v)) = obj.get("v") {
          #[allow(clippy::cast_sign_loss)]
          {
            max_version = max_version.max(*v as u64);
          }
        }
      }
    }
  }

  Ok((base_version, max_version))
}

/// Replay journal entries up to a target version.
fn replay_journal_to_version(
  buffer: &[u8],
  target_version: u64,
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
        // Check version
        let entry_version = if let Some(Value::Unsigned(v)) = obj.get("v") {
          *v
        } else if let Some(Value::Signed(v)) = obj.get("v") {
          #[allow(clippy::cast_sign_loss)]
          (*v as u64)
        } else {
          continue; // Skip entries without version
        };

        // Only apply entries up to target version
        if entry_version > target_version {
          break;
        }

        // Extract timestamp
        let timestamp = if let Some(Value::Unsigned(t)) = obj.get("t") {
          *t
        } else if let Some(Value::Signed(t)) = obj.get("t") {
          #[allow(clippy::cast_sign_loss)]
          (*t as u64)
        } else {
          0 // Default to 0 if not found (shouldn't happen in v2 format)
        };

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
                timestamp,
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
