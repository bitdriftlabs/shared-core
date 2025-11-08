// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::VersionedKVStore;
use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::make_string_value;
use crate::versioned_kv_journal::recovery::VersionedRecovery;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::macros::datetime;


#[test]
fn test_recovery_buffer_too_small() {
  // Create a buffer that's smaller than the header size (17 bytes)
  let buffer = vec![0u8; 10];

  let recovery = VersionedRecovery::new(vec![(&buffer, 1000)]).unwrap();
  let result = recovery.recover_current();
  assert!(result.is_err());
  let err = result.unwrap_err();
  assert!(err.to_string().contains("Buffer too small"));
}

#[test]
fn test_recovery_invalid_position_less_than_header() {
  // Create a buffer with a position field that's less than HEADER_SIZE (17)
  let mut buffer = vec![0u8; 100];

  // Write version (1 byte)
  buffer[0] = 1;

  // Write position at bytes 8-15 (u64, little-endian)
  // Set position to 10, which is less than HEADER_SIZE (17)
  let invalid_position: u64 = 10;
  buffer[8 .. 16].copy_from_slice(&invalid_position.to_le_bytes());

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

  // Write position at bytes 8-15 (u64, little-endian)
  // Set position to 100, which exceeds buffer length of 50
  let invalid_position: u64 = 100;
  buffer[8 .. 16].copy_from_slice(&invalid_position.to_le_bytes());

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
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  let (mut store, _) =
    VersionedKVStore::new(temp_dir.path(), "test", 4096, None, time_provider.clone())?;

  // Insert a key
  store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  // Insert another key
  store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  // Delete key1
  store.remove("key1").await?;
  // ts3 should be greater than ts2 due to the sleep and time passage
  // We can't get the exact deletion timestamp, so we use ts2 + margin
  let ts3 = ts2 + 20_000; // Add 20ms in microseconds as a safe margin

  store.sync()?;

  // Rotate to create snapshot
  let snapshot = store.rotate_journal().await?;

  // Read the snapshot
  let compressed_data = std::fs::read(&snapshot)?;
  let decompressed_data = decompress_zlib(&compressed_data)?;

  // Use u64::MAX as snapshot timestamp since we're only checking the latest state
  let recovery = VersionedRecovery::new(vec![(&decompressed_data, u64::MAX)])?;

  // At ts1, only key1 should exist
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert!(state_ts1.contains_key("key1"));

  // At ts2, both keys should exist
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert!(state_ts2.contains_key("key1"));
  assert!(state_ts2.contains_key("key2"));

  // At ts3 (after deletion), only key2 should exist
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 1);
  assert!(!state_ts3.contains_key("key1"), "key1 should be deleted");
  assert!(state_ts3.contains_key("key2"));

  Ok(())
}

#[test]
fn test_recovery_with_corrupted_frame() {
  // Create a valid header followed by corrupted frame data
  let mut buffer = vec![0u8; 100];

  // Write version (1 byte)
  buffer[0] = 1;

  // Write valid position at bytes 8-15 (u64, little-endian)
  let position: u64 = 50;
  buffer[8 .. 16].copy_from_slice(&position.to_le_bytes());

  // Fill data area with corrupted/invalid frame data
  // (random bytes that won't decode as a valid frame)
  buffer[17 .. 50].fill(0xFF);

  // This should not panic, but should handle the corrupted frame gracefully
  let result = VersionedRecovery::new(vec![(&buffer, 1000)]);
  // The recovery should succeed even with corrupted frames (it will just stop reading)
  assert!(result.is_ok());

  let recovery = result.unwrap();
  let state = recovery.recover_current();
  // Should return empty state since frames are corrupted
  assert!(state.is_ok());
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
