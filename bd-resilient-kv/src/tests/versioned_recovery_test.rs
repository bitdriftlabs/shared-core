// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]
#![allow(clippy::case_sensitive_file_extension_comparisons)]

use crate::VersionedKVStore;
use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::make_string_value;
use crate::versioned_kv_journal::recovery::VersionedRecovery;
use tempfile::TempDir;

/// Helper function to find archived journal files in a directory.
/// Returns sorted paths to all `.zz` compressed journal archives.
fn find_archived_journals(dir: &std::path::Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
  let mut archived_files = std::fs::read_dir(dir)?
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
/// Archived journals have the format: `{name}.jrn.t{timestamp}.zz`
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
  let temp_dir = TempDir::new()?;


  // Create a store with larger buffer to avoid BufferFull errors during test
  let (mut store, _) = VersionedKVStore::new(temp_dir.path(), "test", 2048, None)?;

  store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;

  // Write more data to trigger rotation
  for i in 0 .. 20 {
    store
      .insert(format!("key{i}"), make_string_value("foo"))
      .await?;
  }

  let ts_middle = store
    .get_with_timestamp("key19")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  // Write more after rotation
  store
    .insert("final".to_string(), make_string_value("final_value"))
    .await?;
  let ts_final = store
    .get_with_timestamp("final")
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
  assert!(state_ts1.contains_key("key1"));

  // Verify we can recover at middle timestamp (after rotation)
  let state_middle = recovery.recover_at_timestamp(ts_middle)?;
  assert!(state_middle.len() > 2);
  assert!(state_middle.contains_key("key1"));
  assert!(state_middle.contains_key("key2"));

  // Verify we can recover at final timestamp
  let state_final = recovery.recover_at_timestamp(ts_final)?;
  assert!(state_final.contains_key("final"));
  assert_eq!(
    state_final.get("final").map(|tv| &tv.value),
    Some(&make_string_value("final_value"))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_empty_journal() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create an empty store
  let (mut store, _) = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
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
  let temp_dir = TempDir::new()?;


  let (mut store, _) = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store
    .insert("key".to_string(), make_string_value("1"))
    .await?;
  let ts1 = store
    .get_with_timestamp("key")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key".to_string(), make_string_value("2"))
    .await?;
  let ts2 = store
    .get_with_timestamp("key")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key".to_string(), make_string_value("3"))
    .await?;
  let ts3 = store
    .get_with_timestamp("key")
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
    state_ts1.get("key").map(|tv| &tv.value),
    Some(&make_string_value("1"))
  );

  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(
    state_ts2.get("key").map(|tv| &tv.value),
    Some(&make_string_value("2"))
  );

  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(
    state_ts3.get("key").map(|tv| &tv.value),
    Some(&make_string_value("3"))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_at_timestamp() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  // Create a store and write some timestamped data
  let (mut store, _) = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Small sleep to ensure different timestamps
  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Small sleep to ensure different timestamps
  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key1".to_string(), make_string_value("updated1"))
    .await?;
  let ts3 = store
    .get_with_timestamp("key1")
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
    state_ts1.get("key1").map(|tv| &tv.value),
    Some(&make_string_value("value1"))
  );

  // Recover at ts2: should have key1=value1, key2=value2
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert_eq!(
    state_ts2.get("key1").map(|tv| &tv.value),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    state_ts2.get("key2").map(|tv| &tv.value),
    Some(&make_string_value("value2"))
  );

  // Recover at ts3: should have key1=updated1, key2=value2
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 2);
  assert_eq!(
    state_ts3.get("key1").map(|tv| &tv.value),
    Some(&make_string_value("updated1"))
  );
  assert_eq!(
    state_ts3.get("key2").map(|tv| &tv.value),
    Some(&make_string_value("value2"))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_at_timestamp_with_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let (mut store, _) = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Write some data before rotation
  store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate journal
  store.rotate_journal().await?;

  std::thread::sleep(std::time::Duration::from_millis(10));

  // Write data after rotation
  store
    .insert("key3".to_string(), make_string_value("value3"))
    .await?;
  let ts3 = store
    .get_with_timestamp("key3")
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
  assert!(state_ts1.contains_key("key1"));

  // Recover at ts2 (should be in first snapshot)
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert!(state_ts2.contains_key("key1"));
  assert!(state_ts2.contains_key("key2"));

  // Recover at ts3 (should include all data from both snapshots)
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 3);
  assert!(state_ts3.contains_key("key1"));
  assert!(state_ts3.contains_key("key2"));
  assert!(state_ts3.contains_key("key3"));

  Ok(())
}
