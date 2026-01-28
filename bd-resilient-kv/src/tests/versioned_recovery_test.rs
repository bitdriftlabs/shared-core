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

/// Helper function to find archived journal files in a directory.
/// Returns sorted paths to all `.zz` compressed journal archives in the snapshots subdirectory.
fn find_archived_journals(dir: &std::path::Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
  let snapshots_dir = dir.join("snapshots");

  // If snapshots directory doesn't exist, return empty vec
  if !snapshots_dir.exists() {
    return Ok(Vec::new());
  }

  let mut archived_files = std::fs::read_dir(snapshots_dir)?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      let filename = path.file_name()?.to_str()?;
      if filename.ends_with(".zz") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();
  Ok(archived_files)
}

/// Helper function to extract rotation timestamp from an archived journal filename.
/// Archived journals have the format: `{name}.jrn.g{generation}.t{timestamp}.zz`
fn extract_rotation_timestamp(path: &std::path::Path) -> anyhow::Result<u64> {
  let filename = path
    .file_name()
    .and_then(|f| f.to_str())
    .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;

  let timestamp = filename
    .split('.')
    .find(|part| {
      part.starts_with('t') && part.len() > 1 && part[1 ..].chars().all(|c| c.is_ascii_digit())
    })
    .and_then(|part| part.strip_prefix('t'))
    .ok_or_else(|| anyhow::anyhow!("No timestamp found in filename: {}", filename))?
    .parse::<u64>()?;

  Ok(timestamp)
}

#[tokio::test]
async fn test_recovery_multiple_journals_with_rotation() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;

  let time_provider = Arc::new(TestTimeProvider::new(datetime!(
    2024-01-01 00:00:00 UTC
  )));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let _handle = registry.create_handle().await; // Retain all snapshots

  // Create a store with larger buffer to avoid BufferFull errors during test
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 2048,
      ..Default::default()
    },
    time_provider.clone(),
    registry,
    &stats,
  )
  .await?;

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  let ts1 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  // Write more data to trigger rotation
  for i in 0 .. 20 {
    store
      .insert(
        Scope::FeatureFlagExposure,
        format!("key{i}"),
        make_string_value("foo"),
      )
      .await?;
  }

  let ts_middle = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key19")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  // Write more after rotation
  store
    .insert(
      Scope::FeatureFlagExposure,
      "final".to_string(),
      make_string_value("final_value"),
    )
    .await?;
  let ts_final = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "final")
    .map(|tv| tv.timestamp)
    .unwrap();
  store.sync()?;

  // Rotate to create final snapshot
  store.rotate_journal().await?;

  // Read all snapshots (archived journals)
  let mut all_journals = Vec::new();

  for archived_path in find_archived_journals(temp_dir.path())? {
    let rotation_ts = extract_rotation_timestamp(&archived_path)?;
    let compressed_data = std::fs::read(&archived_path)?;
    let decompressed_data = decompress_zlib(&compressed_data)?;
    all_journals.push((decompressed_data, rotation_ts));
  }

  // Create recovery utility with all journals
  let journal_refs: Vec<(&[u8], u64)> = all_journals
    .iter()
    .map(|(data, ts)| (data.as_slice(), *ts))
    .collect();
  let recovery = VersionedRecovery::new(journal_refs)?;

  // Verify we can recover at early timestamp
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert!(state_ts1.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));

  // Verify we can recover at middle timestamp (after rotation)
  let state_middle = recovery.recover_at_timestamp(ts_middle)?;
  assert!(state_middle.len() > 2);
  assert!(state_middle.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));
  assert!(state_middle.contains_key(&(Scope::FeatureFlagExposure, "key2".to_string())));

  // Verify we can recover at final timestamp
  let state_final = recovery.recover_at_timestamp(ts_final)?;
  assert!(state_final.contains_key(&(Scope::FeatureFlagExposure, "final".to_string())));
  assert_eq!(
    state_final
      .get(&(Scope::FeatureFlagExposure, "final".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("final_value"))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_empty_journal() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(
    2024-01-01 00:00:00 UTC
  )));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let _handle = registry.create_handle().await; // Retain all snapshots

  // Create an empty store
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 4096,
      ..Default::default()
    },
    time_provider,
    registry,
    &stats,
  )
  .await?;
  store.sync()?;

  // Rotate to create snapshot
  store.rotate_journal().await?;

  // Read the snapshot
  let archived_files = find_archived_journals(temp_dir.path())?;
  assert_eq!(archived_files.len(), 1);
  let compressed_data = std::fs::read(&archived_files[0])?;
  let decompressed_data = decompress_zlib(&compressed_data)?;
  let snapshot_ts = extract_rotation_timestamp(&archived_files[0])?;

  let recovery = VersionedRecovery::new(vec![(&decompressed_data, snapshot_ts)])?;

  // Recovering current should return empty map
  let state = recovery.recover_current()?;
  assert_eq!(state.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_recovery_with_overwrites() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(
    2024-01-01 00:00:00 UTC
  )));
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
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key".to_string(),
      make_string_value("1"),
    )
    .await?;
  let ts1 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key".to_string(),
      make_string_value("2"),
    )
    .await?;
  let ts2 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key".to_string(),
      make_string_value("3"),
    )
    .await?;
  let ts3 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Rotate to create snapshot
  store.rotate_journal().await?;

  // Read the snapshot
  let archived_files = find_archived_journals(temp_dir.path())?;
  assert_eq!(archived_files.len(), 1);
  let compressed_data = std::fs::read(&archived_files[0])?;
  let decompressed_data = decompress_zlib(&compressed_data)?;
  let snapshot_ts = extract_rotation_timestamp(&archived_files[0])?;

  let recovery = VersionedRecovery::new(vec![(&decompressed_data, snapshot_ts)])?;

  // Each timestamp should show the value at that time
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(
    state_ts1
      .get(&(Scope::FeatureFlagExposure, "key".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("1"))
  );

  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(
    state_ts2
      .get(&(Scope::FeatureFlagExposure, "key".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("2"))
  );

  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(
    state_ts3
      .get(&(Scope::FeatureFlagExposure, "key".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("3"))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_at_timestamp() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(
    2024-01-01 00:00:00 UTC
  )));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let _handle = registry.create_handle().await; // Retain all snapshots

  // Create a store and write some timestamped data
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

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  let ts1 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Advance time to ensure different timestamps
  time_provider.advance(10.milliseconds());

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;
  let ts2 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Advance time again
  time_provider.advance(10.milliseconds());

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("updated1"),
    )
    .await?;
  let ts3 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Rotate to create snapshot
  store.rotate_journal().await?;

  // Read the snapshot
  let archived_files = find_archived_journals(temp_dir.path())?;
  assert_eq!(archived_files.len(), 1);
  let compressed_data = std::fs::read(&archived_files[0])?;
  let decompressed_data = decompress_zlib(&compressed_data)?;
  let snapshot_ts = extract_rotation_timestamp(&archived_files[0])?;

  // Create recovery utility
  let recovery = VersionedRecovery::new(vec![(&decompressed_data, snapshot_ts)])?;

  // Recover at ts1: should have only key1=value1
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert_eq!(
    state_ts1
      .get(&(Scope::FeatureFlagExposure, "key1".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("value1"))
  );

  // Recover at ts2: should have key1=value1, key2=value2
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert_eq!(
    state_ts2
      .get(&(Scope::FeatureFlagExposure, "key1".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    state_ts2
      .get(&(Scope::FeatureFlagExposure, "key2".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("value2"))
  );

  // Recover at ts3: should have key1=updated1, key2=value2
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 2);
  assert_eq!(
    state_ts3
      .get(&(Scope::FeatureFlagExposure, "key1".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("updated1"))
  );
  assert_eq!(
    state_ts3
      .get(&(Scope::FeatureFlagExposure, "key2".to_string()))
      .map(|tv| &tv.value),
    Some(&make_string_value("value2"))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_at_timestamp_with_rotation() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(
    2024-01-01 00:00:00 UTC
  )));
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

  // Write some data before rotation
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  let ts1 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  time_provider.advance(10.milliseconds());

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;
  let ts2 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate journal
  store.rotate_journal().await?;

  time_provider.advance(10.milliseconds());

  // Write data after rotation
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("value3"),
    )
    .await?;
  let ts3 = store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Rotate again to create final snapshot
  store.rotate_journal().await?;

  // Read all snapshots
  let archived_files = find_archived_journals(temp_dir.path())?;
  assert_eq!(archived_files.len(), 2, "Should have two snapshots");

  let mut all_snapshots = Vec::new();
  for archived_path in &archived_files {
    let compressed_data = std::fs::read(archived_path)?;
    let decompressed_data = decompress_zlib(&compressed_data)?;
    let rotation_ts = extract_rotation_timestamp(archived_path)?;
    all_snapshots.push((decompressed_data, rotation_ts));
  }

  // Create recovery from all snapshots
  let snapshot_refs: Vec<(&[u8], u64)> = all_snapshots
    .iter()
    .map(|(data, ts)| (data.as_slice(), *ts))
    .collect();
  let recovery = VersionedRecovery::new(snapshot_refs)?;

  // Verify we can recover at any timestamp across all snapshots
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert!(state_ts1.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));

  // Recover at ts2 (should be in first snapshot)
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert!(state_ts2.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));
  assert!(state_ts2.contains_key(&(Scope::FeatureFlagExposure, "key2".to_string())));

  // Recover at ts3 (should include all data from both snapshots)
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 3);
  assert!(state_ts3.contains_key(&(Scope::FeatureFlagExposure, "key1".to_string())));
  assert!(state_ts3.contains_key(&(Scope::FeatureFlagExposure, "key2".to_string())));
  assert!(state_ts3.contains_key(&(Scope::FeatureFlagExposure, "key3".to_string())));

  Ok(())
}
