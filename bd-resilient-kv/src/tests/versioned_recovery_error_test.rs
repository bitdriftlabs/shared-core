// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]
#![allow(clippy::case_sensitive_file_extension_comparisons)]

use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::make_string_value;
use crate::versioned_kv_journal::recovery::VersionedRecovery;
use crate::versioned_kv_journal::retention::RetentionRegistry;
use crate::versioned_kv_journal::store::PersistentStoreConfig;
use crate::{Scope, VersionedKVStore};
use bd_time::TestTimeProvider;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::macros::datetime;


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
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let _handle = registry.create_handle().await; // Retain all snapshots

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
  let compressed_data = std::fs::read(&rotation.snapshot_path)?;
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
