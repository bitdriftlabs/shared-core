// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]
#![allow(clippy::case_sensitive_file_extension_comparisons)]

use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::framing::Frame;
use crate::versioned_kv_journal::recovery::VersionedRecovery;
use crate::versioned_kv_journal::retention::RetentionRegistry;
use crate::versioned_kv_journal::store::PersistentStoreConfig;
use crate::versioned_kv_journal::{HEADER_SIZE, make_string_value};
use crate::{
  MAX_COMPRESSED_STATE_SNAPSHOT_BYTES,
  MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
  Scope,
  StateValue,
  Value_type,
  VersionedKVStore,
  decode_compressed_journal,
  decode_journal,
  extract_non_empty_string_values_from_compressed_journal,
};
use bd_time::TestTimeProvider;
use crc32fast::Hasher;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use protobuf::Message;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::macros::datetime;

fn compress(data: &[u8]) -> Vec<u8> {
  let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
  encoder.write_all(data).unwrap();
  encoder.finish().unwrap()
}

fn encode_frame(scope: Scope, key: &str, timestamp_micros: u64, value: StateValue) -> Vec<u8> {
  let frame = Frame::new(scope, key, timestamp_micros, value);
  let mut encoded = vec![0; frame.encoded_size()];
  let encoded_len = frame.encode(&mut encoded).unwrap();
  encoded.truncate(encoded_len);
  encoded
}

fn append_varint(mut value: u64, output: &mut Vec<u8>) {
  while value >= 0x80 {
    output.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
    value >>= 7;
  }
  output.push(u8::try_from(value).unwrap());
}

fn encode_raw_frame(scope: Scope, key: &str, timestamp_micros: u64, payload: &[u8]) -> Vec<u8> {
  let mut key_len = Vec::new();
  append_varint(key.len().try_into().unwrap(), &mut key_len);
  let mut timestamp = Vec::new();
  append_varint(timestamp_micros, &mut timestamp);
  encode_raw_frame_with_inner_varints(scope, key, &key_len, &timestamp, payload)
}

fn encode_raw_frame_with_inner_varints(
  scope: Scope,
  key: &str,
  key_len: &[u8],
  timestamp: &[u8],
  payload: &[u8],
) -> Vec<u8> {
  let mut frame_data = vec![scope.to_u8()];
  frame_data.extend_from_slice(key_len);
  frame_data.extend_from_slice(key.as_bytes());
  frame_data.extend_from_slice(timestamp);
  frame_data.extend_from_slice(payload);

  let mut hasher = Hasher::new();
  hasher.update(&frame_data);
  frame_data.extend_from_slice(&hasher.finalize().to_le_bytes());

  let mut encoded = Vec::new();
  append_varint(frame_data.len().try_into().unwrap(), &mut encoded);
  encoded.extend_from_slice(&frame_data);
  encoded
}

fn compressed_journal(encoded_frames: Vec<Vec<u8>>, padding_bytes: usize) -> Vec<u8> {
  let encoded_frames_len: usize = encoded_frames.iter().map(Vec::len).sum();
  let position = HEADER_SIZE + encoded_frames_len;
  let mut journal = vec![0; position + padding_bytes];
  journal[0] = 1;
  journal[1 .. 9].copy_from_slice(&(position as u64).to_le_bytes());
  let mut offset = HEADER_SIZE;
  for frame in encoded_frames {
    let end = offset + frame.len();
    journal[offset .. end].copy_from_slice(&frame);
    offset = end;
  }
  compress(&journal)
}

fn scan_session_ids(
  compressed: &[u8],
  max_decompressed_bytes: usize,
  max_values: usize,
) -> anyhow::Result<Vec<String>> {
  extract_non_empty_string_values_from_compressed_journal(
    compressed,
    max_decompressed_bytes,
    Scope::System,
    "sid",
    max_values,
  )
}

#[test]
fn test_recovery_buffer_too_small() {
  // Create a buffer that's smaller than the header size (9 bytes)
  let buffer = vec![0u8; 8];

  let recovery = VersionedRecovery::new(vec![(&buffer, 1000)]).unwrap();
  let result = recovery.recover_current();
  assert!(result.is_err());
  let err = result.unwrap_err();
  assert!(err.to_string().contains("Buffer too small"));
}

#[test]
fn test_recovery_invalid_position_less_than_header() {
  // Create a buffer with a position field that's less than HEADER_SIZE (9)
  let mut buffer = vec![0u8; 100];

  // Write version (1 byte)
  buffer[0] = 1;

  // Write position at bytes 1-8 (u64, little-endian)
  // Set position to 5, which is less than HEADER_SIZE (9)
  let invalid_position: u64 = 5;
  buffer[1 .. 9].copy_from_slice(&invalid_position.to_le_bytes());

  let recovery = VersionedRecovery::new(vec![(&buffer, 1000)]).unwrap();
  let result = recovery.recover_current();
  assert!(result.is_err());
  let err = result.unwrap_err();
  assert!(
    err.to_string().contains("Invalid position"),
    "Expected 'Invalid position' error, got: {}",
    err
  );
}

#[test]
fn test_recovery_position_exceeds_buffer_length() {
  // Create a buffer where position > buffer.len()
  let mut buffer = vec![0u8; 50];

  // Write version (1 byte)
  buffer[0] = 1;

  // Write position at bytes 1-8 (u64, little-endian)
  // Set position to 100, which exceeds buffer length of 50
  let invalid_position: u64 = 100;
  buffer[1 .. 9].copy_from_slice(&invalid_position.to_le_bytes());

  let recovery = VersionedRecovery::new(vec![(&buffer, 1000)]).unwrap();
  let result = recovery.recover_current();
  assert!(result.is_err());
  let err = result.unwrap_err();
  assert!(
    err.to_string().contains("Invalid position"),
    "Expected 'Invalid position' error, got: {}",
    err
  );
}

#[tokio::test]
async fn test_recovery_with_deletions() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));
  let handle = registry.create_handle().await; // Retain all snapshots
  handle.update_retention_micros(0);

  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 4096,
      ..Default::default()
    },
    time_provider.clone(),
    registry,
    &stats,
  )
  .await?;

  let (ts1, _) = store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  time_provider.advance(10_i64.milliseconds());

  let (ts2, _) = store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  time_provider.advance(10_i64.milliseconds());

  // Delete key1
  let (ts3, _) = store
    .remove(Scope::FeatureFlagExposure, "key1")
    .await?
    .unwrap();

  store.sync()?;

  // Rotate to create snapshot
  let rotation = store.rotate_journal().await?;

  // Read the snapshot
  let compressed_data = std::fs::read(rotation.snapshot_path.as_ref().unwrap())?;
  let decompressed_data = decompress_zlib(&compressed_data)?;

  // Use u64::MAX as snapshot timestamp since we're only checking the latest state
  let recovery = VersionedRecovery::new(vec![(&decompressed_data, u64::MAX)])?;

  // At ts1, only key1 should exist
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert!(state_ts1.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));

  // At ts2, both keys should exist
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert!(state_ts2.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));
  assert!(state_ts2.contains_key(&(Scope::FeatureFlagExposure, "key2".to_string())));

  // At ts3 (after deletion), only key2 should exist
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 1);
  assert!(
    !state_ts3.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())),
    "key1 should be deleted"
  );
  assert!(state_ts3.contains_key(&(Scope::FeatureFlagExposure, "key2".to_string())));

  Ok(())
}

#[test]
fn test_recovery_with_corrupted_frame() {
  // Create a valid header followed by corrupted frame data
  let mut buffer = vec![0u8; 100];

  // Write version (1 byte)
  buffer[0] = 1;

  // Write valid position at bytes 1-8 (u64, little-endian)
  let position: u64 = 50;
  buffer[1 .. 9].copy_from_slice(&position.to_le_bytes());

  // Fill data area with corrupted/invalid frame data
  // (random bytes that won't decode as a valid frame)
  buffer[9 .. 50].fill(0xFF);

  // Local recovery remains tolerant of an incomplete crash-time journal.
  let recovery = VersionedRecovery::new(vec![(&buffer, 1000)]).unwrap();
  assert!(recovery.recover_current().is_ok());

  // Remote artifact ingestion uses the strict reader instead.
  assert!(decode_journal(&buffer).is_err());
}

#[test]
fn test_strict_decoder_rejects_truncated_frame() {
  let frame = Frame::new(
    Scope::FeatureFlagExposure,
    "flag",
    1,
    make_string_value("enabled"),
  );
  let mut encoded_frame = vec![0; frame.encoded_size()];
  let encoded_len = frame.encode(&mut encoded_frame).unwrap();

  let mut buffer = vec![0; HEADER_SIZE + encoded_len - 1];
  let position = buffer.len() as u64;
  buffer[0] = 1;
  buffer[1 .. 9].copy_from_slice(&position.to_le_bytes());
  buffer[HEADER_SIZE ..].copy_from_slice(&encoded_frame[.. encoded_len - 1]);

  let state = decode_journal(&buffer);
  assert!(state.is_err());
  assert!(
    state
      .unwrap_err()
      .to_string()
      .contains("Invalid journal frame")
  );
}

#[test]
fn test_compressed_recovery_rejects_oversized_snapshot() {
  let compressed = compress(&vec![0; 1_024]);

  let result = decode_compressed_journal(&compressed, 1_023);
  assert!(result.is_err());
  assert!(
    result
      .unwrap_err()
      .to_string()
      .contains("decompressed-size limit")
  );
}

#[test]
fn test_compressed_recovery_rejects_oversized_compressed_snapshot() {
  let compressed = vec![0; MAX_COMPRESSED_STATE_SNAPSHOT_BYTES + 1];

  let result = decode_compressed_journal(&compressed, 1);
  assert!(result.is_err());
  assert!(
    result
      .unwrap_err()
      .to_string()
      .contains("compressed-size limit")
  );
}

#[test]
fn compressed_decoder_preserves_typed_changes_and_tombstones() {
  let flag_value = make_string_value("treatment");
  let compressed = compressed_journal(
    vec![
      encode_frame(
        Scope::FeatureFlagExposure,
        "checkout-experiment",
        10,
        flag_value.clone(),
      ),
      encode_frame(
        Scope::FeatureFlagExposure,
        "checkout-experiment",
        20,
        StateValue::default(),
      ),
    ],
    0,
  );

  let changes =
    decode_compressed_journal(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES).unwrap();

  assert_eq!(changes.len(), 2);
  assert_eq!(changes[0].scope, Scope::FeatureFlagExposure);
  assert_eq!(changes[0].key, "checkout-experiment");
  assert_eq!(changes[0].timestamp_micros, 10);
  assert_eq!(changes[0].value, flag_value);
  assert_eq!(changes[1].timestamp_micros, 20);
  assert!(changes[1].value.value_type.is_none());
}

#[test]
fn streaming_scanner_selects_requested_values_and_deduplicates_them() {
  let compressed = compressed_journal(
    vec![
      encode_frame(
        Scope::System,
        "other",
        1,
        make_string_value("not-a-session"),
      ),
      encode_frame(
        Scope::FeatureFlagExposure,
        "sid",
        2,
        make_string_value("not-a-session"),
      ),
      encode_frame(Scope::System, "sid", 3, make_string_value("")),
      encode_frame(Scope::System, "sid", 4, make_string_value("session-a")),
      encode_frame(Scope::System, "sid", 5, make_string_value("session-a")),
      encode_frame(Scope::System, "sid", 6, make_string_value("session-b")),
    ],
    0,
  );

  assert_eq!(
    scan_session_ids(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 2).unwrap(),
    ["session-a", "session-b"]
  );
}

#[test]
fn streaming_scanner_ignores_non_string_and_empty_matching_values() {
  let compressed = compressed_journal(
    vec![
      encode_frame(Scope::System, "sid", 1, StateValue::default()),
      encode_frame(
        Scope::System,
        "sid",
        2,
        StateValue {
          value_type: Some(Value_type::BoolValue(true)),
          ..Default::default()
        },
      ),
      encode_frame(Scope::System, "sid", 3, make_string_value("")),
    ],
    0,
  );

  assert!(
    scan_session_ids(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1)
      .unwrap()
      .is_empty()
  );
}

#[test]
fn streaming_scanner_only_requires_valid_protobuf_for_the_selected_state_key() {
  let compressed = compressed_journal(
    vec![
      encode_frame(Scope::System, "sid", 1, make_string_value("session-a")),
      encode_raw_frame(Scope::FeatureFlagExposure, "flag", 2, &[0x0a]),
    ],
    0,
  );

  assert_eq!(
    scan_session_ids(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1).unwrap(),
    ["session-a"]
  );
}

#[test]
fn streaming_scanner_rejects_invalid_selected_state_value() {
  let compressed = compressed_journal(vec![encode_raw_frame(Scope::System, "sid", 1, &[0x0a])], 0);

  assert!(scan_session_ids(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1).is_err());
}

#[test]
fn state_decoders_reject_corrupt_or_truncated_zlib_streams() {
  let compressed = compressed_journal(
    vec![encode_frame(
      Scope::System,
      "sid",
      1,
      make_string_value("session-a"),
    )],
    0,
  );
  let mut corrupt = compressed.clone();
  *corrupt.last_mut().unwrap() ^= 0xff;
  let mut trailing_data = compressed.clone();
  trailing_data.push(0);

  assert!(scan_session_ids(&corrupt, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1).is_err());
  assert!(scan_session_ids(&trailing_data, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1).is_err());
  assert!(
    scan_session_ids(
      &compressed[.. compressed.len() - 1],
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1,
    )
    .is_err()
  );
  assert!(decode_compressed_journal(&corrupt, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES).is_err());
  assert!(
    decode_compressed_journal(&trailing_data, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,).is_err()
  );
  assert!(
    decode_compressed_journal(
      &compressed[.. compressed.len() - 1],
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
    )
    .is_err()
  );
}

#[test]
fn streaming_scanner_enforces_the_decompressed_limit_while_draining_padding() {
  let frame = encode_frame(Scope::System, "sid", 1, make_string_value("session-a"));
  let journal_size = HEADER_SIZE + frame.len();
  let compressed = compressed_journal(vec![frame], 1);

  assert!(scan_session_ids(&compressed, journal_size, 1).is_err());
}

#[test]
fn streaming_scanner_rejects_invalid_frame_lengths() {
  let malformed_varint = compressed_journal(vec![vec![0x80; 10]], 0);
  let frame_past_journal_end = compressed_journal(vec![vec![0x7f]], 0);

  assert!(scan_session_ids(&malformed_varint, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1).is_err());
  assert!(
    scan_session_ids(
      &frame_past_journal_end,
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1,
    )
    .is_err()
  );
}

#[test]
fn streaming_scanner_rejects_overlong_inner_varints() {
  let payload = make_string_value("session-a").write_to_bytes().unwrap();
  let overlong_key_length = compressed_journal(
    vec![encode_raw_frame_with_inner_varints(
      Scope::System,
      "sid",
      &[0x82, 0x00],
      &[0x01],
      &payload,
    )],
    0,
  );
  let overlong_timestamp = compressed_journal(
    vec![encode_raw_frame_with_inner_varints(
      Scope::System,
      "sid",
      &[0x03],
      &[0x81, 0x00],
      &payload,
    )],
    0,
  );

  assert!(
    scan_session_ids(
      &overlong_key_length,
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1
    )
    .is_err()
  );
  assert!(
    scan_session_ids(
      &overlong_timestamp,
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1
    )
    .is_err()
  );
}

#[test]
fn streaming_scanner_rejects_invalid_headers_and_frame_checksums() {
  let mut invalid_version = vec![0; HEADER_SIZE];
  invalid_version[0] = 2;
  invalid_version[1 .. 9].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());

  let mut corrupt_checksum = encode_frame(Scope::System, "sid", 1, make_string_value("session-a"));
  *corrupt_checksum.last_mut().unwrap() ^= 0xff;

  assert!(
    scan_session_ids(
      &compress(&invalid_version),
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1,
    )
    .is_err()
  );
  assert!(
    scan_session_ids(
      &compressed_journal(vec![corrupt_checksum], 0),
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1,
    )
    .is_err()
  );
}

#[test]
fn streaming_scanner_enforces_value_and_compressed_size_limits() {
  let compressed = compressed_journal(
    vec![
      encode_frame(Scope::System, "sid", 1, make_string_value("session-a")),
      encode_frame(Scope::System, "sid", 2, make_string_value("session-b")),
    ],
    0,
  );

  assert!(scan_session_ids(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 0).is_err());
  assert!(scan_session_ids(&compressed, MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES, 1).is_err());
  assert!(
    scan_session_ids(
      &vec![0; MAX_COMPRESSED_STATE_SNAPSHOT_BYTES + 1],
      MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES,
      1,
    )
    .is_err()
  );
}

#[tokio::test]
async fn test_recovery_current_with_empty_snapshots() -> anyhow::Result<()> {
  // Test recover_current when there are no snapshots at all
  let recovery = VersionedRecovery::new(vec![])?;

  let state = recovery.recover_current()?;
  assert_eq!(
    state.len(),
    0,
    "Should return empty state with no snapshots"
  );

  Ok(())
}
