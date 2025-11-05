// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::VersionedKVStore;
use crate::versioned_recovery::VersionedRecovery;
use bd_bonjson::Value;
use tempfile::TempDir;

#[test]
fn test_recovery_single_journal() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create a store and write some versioned data
  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  let v3 = store.insert("key1".to_string(), Value::String("updated1".to_string()))?;
  store.sync()?;

  // Read the journal data
  let journal_data = std::fs::read(&file_path)?;

  // Create recovery utility
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // Verify version range
  let version_range = recovery.version_range();
  assert!(version_range.is_some());
  #[allow(clippy::unwrap_used)]
  let (min, max) = version_range.unwrap();
  assert_eq!(min, 1);
  assert_eq!(max, v3);

  // Recover at v1: should have only key1=value1
  let state_v1 = recovery.recover_at_version(v1)?;
  assert_eq!(state_v1.len(), 1);
  assert_eq!(
    state_v1.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );

  // Recover at v2: should have key1=value1, key2=value2
  let state_v2 = recovery.recover_at_version(v2)?;
  assert_eq!(state_v2.len(), 2);
  assert_eq!(
    state_v2.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(
    state_v2.get("key2").map(|tv| &tv.value),
    Some(&Value::String("value2".to_string()))
  );

  // Recover at v3: should have key1=updated1, key2=value2
  let state_v3 = recovery.recover_at_version(v3)?;
  assert_eq!(state_v3.len(), 2);
  assert_eq!(
    state_v3.get("key1").map(|tv| &tv.value),
    Some(&Value::String("updated1".to_string()))
  );
  assert_eq!(
    state_v3.get("key2").map(|tv| &tv.value),
    Some(&Value::String("value2".to_string()))
  );

  // Recover current should match v3
  let current = recovery.recover_current()?;
  assert_eq!(current, state_v3);

  Ok(())
}

#[test]
fn test_recovery_with_deletions() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create a store with deletions
  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  let v3_opt = store.remove("key1")?;
  assert!(v3_opt.is_some());
  #[allow(clippy::unwrap_used)]
  let v3 = v3_opt.unwrap();
  store.sync()?;

  // Read the journal data
  let journal_data = std::fs::read(&file_path)?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // At v1: key1 exists
  let state_v1 = recovery.recover_at_version(v1)?;
  assert_eq!(state_v1.len(), 1);
  assert!(state_v1.contains_key("key1"));

  // At v2: both keys exist
  let state_v2 = recovery.recover_at_version(v2)?;
  assert_eq!(state_v2.len(), 2);

  // At v3: only key2 exists (key1 deleted)
  let state_v3 = recovery.recover_at_version(v3)?;
  assert_eq!(state_v3.len(), 1);
  assert!(!state_v3.contains_key("key1"));
  assert!(state_v3.contains_key("key2"));

  Ok(())
}

#[test]
fn test_recovery_multiple_journals_with_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create a store with larger buffer to avoid BufferFull errors during test
  let mut store = VersionedKVStore::new(&file_path, 2048, None)?;

  // Write data that will trigger rotation
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::String("value2".to_string()))?;

  // Write more data to trigger rotation
  for i in 0 .. 20 {
    store.insert(format!("key{i}"), Value::Signed(i))?;
  }

  let v_middle = store.current_version();

  // Write more after rotation
  let v_final = store.insert(
    "final".to_string(),
    Value::String("final_value".to_string()),
  )?;
  store.sync()?;

  // Read all journal files
  let mut all_journals = Vec::new();

  // Read archived journals
  let archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.v") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();

  for archived_path in archived_files {
    all_journals.push(std::fs::read(archived_path)?);
  }

  // Read active journal
  all_journals.push(std::fs::read(&file_path)?);

  // Create recovery utility with all journals
  let journal_refs: Vec<&[u8]> = all_journals.iter().map(std::vec::Vec::as_slice).collect();
  let recovery = VersionedRecovery::new(journal_refs)?;

  // Verify we can recover at early version
  let state_v1 = recovery.recover_at_version(v1)?;
  assert_eq!(state_v1.len(), 1);
  assert!(state_v1.contains_key("key1"));

  // Verify we can recover at middle version (after rotation)
  let state_middle = recovery.recover_at_version(v_middle)?;
  assert!(state_middle.len() > 2);
  assert!(state_middle.contains_key("key1"));
  assert!(state_middle.contains_key("key2"));

  // Verify we can recover at final version
  let state_final = recovery.recover_at_version(v_final)?;
  assert!(state_final.contains_key("final"));
  assert_eq!(
    state_final.get("final").map(|tv| &tv.value),
    Some(&Value::String("final_value".to_string()))
  );

  Ok(())
}

#[test]
fn test_recovery_empty_journal() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create an empty store
  let store = VersionedKVStore::new(&file_path, 4096, None)?;
  store.sync()?;

  let journal_data = std::fs::read(&file_path)?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // Should have version range starting at 1
  let version_range = recovery.version_range();
  assert!(version_range.is_some());
  #[allow(clippy::unwrap_used)]
  let (min, _max) = version_range.unwrap();
  assert_eq!(min, 1);

  // Recovering at any version should return empty map
  let state = recovery.recover_at_version(1)?;
  assert_eq!(state.len(), 0);

  Ok(())
}

#[test]
fn test_recovery_version_range() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  let v3 = store.insert("key3".to_string(), Value::String("value3".to_string()))?;
  store.sync()?;

  let journal_data = std::fs::read(&file_path)?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  let version_range = recovery.version_range();
  assert!(version_range.is_some());
  #[allow(clippy::unwrap_used)]
  let (min, max) = version_range.unwrap();
  assert_eq!(min, 1); // base_version defaults to 1 for new stores
  assert_eq!(max, v3);

  Ok(())
}

#[test]
fn test_recovery_with_overwrites() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let v1 = store.insert("key".to_string(), Value::Signed(1))?;
  let v2 = store.insert("key".to_string(), Value::Signed(2))?;
  let v3 = store.insert("key".to_string(), Value::Signed(3))?;
  store.sync()?;

  let journal_data = std::fs::read(&file_path)?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // Each version should show the value at that time
  let state_v1 = recovery.recover_at_version(v1)?;
  assert_eq!(state_v1.get("key").map(|tv| &tv.value), Some(&Value::Signed(1)));

  let state_v2 = recovery.recover_at_version(v2)?;
  assert_eq!(state_v2.get("key").map(|tv| &tv.value), Some(&Value::Signed(2)));

  let state_v3 = recovery.recover_at_version(v3)?;
  assert_eq!(state_v3.get("key").map(|tv| &tv.value), Some(&Value::Signed(3)));

  Ok(())
}

#[test]
fn test_recovery_various_value_types() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  store.insert("string".to_string(), Value::String("hello".to_string()))?;
  store.insert("number".to_string(), Value::Signed(42))?;
  store.insert("float".to_string(), Value::Float(3.14))?;
  store.insert("bool".to_string(), Value::Bool(true))?;
  let v_final = store.current_version();
  store.sync()?;

  let journal_data = std::fs::read(&file_path)?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  let state = recovery.recover_at_version(v_final)?;
  assert_eq!(state.len(), 4);
  assert_eq!(
    state.get("string").map(|tv| &tv.value),
    Some(&Value::String("hello".to_string()))
  );
  assert_eq!(state.get("number").map(|tv| &tv.value), Some(&Value::Signed(42)));
  assert_eq!(state.get("float").map(|tv| &tv.value), Some(&Value::Float(3.14)));
  assert_eq!(state.get("bool").map(|tv| &tv.value), Some(&Value::Bool(true)));

  Ok(())
}

#[test]
fn test_recovery_from_compressed_archive() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create a store and write some data
  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  store.sync()?;

  // Get the current version before rotation (this will be used in the archive name)
  let archive_version = store.current_version();
  
  // Rotate to create compressed archive
  store.rotate_journal()?;

  // Add more data to active journal
  let v3 = store.insert("key3".to_string(), Value::String("value3".to_string()))?;
  store.sync()?;

  // Find the compressed archive (using the version at the time of rotation)
  let archived_path = temp_dir.path().join(format!("test.jrn.v{}.zz", archive_version));
  assert!(archived_path.exists(), "Compressed archive should exist");

  // Read both journals
  let compressed_data = std::fs::read(&archived_path)?;
  let active_data = std::fs::read(&file_path)?;

  // Create recovery from both journals (compressed first, then active)
  let recovery = VersionedRecovery::new(vec![&compressed_data, &active_data])?;

  // Verify version range spans both journals
  let version_range = recovery.version_range();
  assert!(version_range.is_some());
  #[allow(clippy::unwrap_used)]
  let (min, max) = version_range.unwrap();
  assert_eq!(min, 1);
  assert_eq!(max, v3);

  // Recover at v1 (should be in compressed archive)
  let state_v1 = recovery.recover_at_version(v1)?;
  assert_eq!(state_v1.len(), 1);
  assert_eq!(
    state_v1.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );

  // Recover at v2 (should be in compressed archive)
  let state_v2 = recovery.recover_at_version(v2)?;
  assert_eq!(state_v2.len(), 2);

  // Recover at v3 (should include data from both archives and active journal)
  let state_v3 = recovery.recover_at_version(v3)?;
  assert_eq!(state_v3.len(), 3);
  assert_eq!(
    state_v3.get("key3").map(|tv| &tv.value),
    Some(&Value::String("value3".to_string()))
  );

  Ok(())
}

#[test]
fn test_recovery_from_multiple_compressed_archives() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create a store and perform multiple rotations
  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let archive1_version = store.current_version();
  store.rotate_journal()?;

  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  let archive2_version = store.current_version();
  store.rotate_journal()?;

  let v3 = store.insert("key3".to_string(), Value::String("value3".to_string()))?;
  store.sync()?;

  // Collect all journal data (2 compressed + 1 active)
  let archive1_path = temp_dir.path().join(format!("test.jrn.v{}.zz", archive1_version));
  let archive2_path = temp_dir.path().join(format!("test.jrn.v{}.zz", archive2_version));

  let archive1_data = std::fs::read(&archive1_path)?;
  let archive2_data = std::fs::read(&archive2_path)?;
  let active_data = std::fs::read(&file_path)?;

  // Create recovery from all journals
  let recovery = VersionedRecovery::new(vec![&archive1_data, &archive2_data, &active_data])?;

  // Verify we can recover at any version
  let state_v1 = recovery.recover_at_version(v1)?;
  assert_eq!(state_v1.len(), 1);
  assert!(state_v1.contains_key("key1"));

  let state_v2 = recovery.recover_at_version(v2)?;
  assert_eq!(state_v2.len(), 2);
  assert!(state_v2.contains_key("key1"));
  assert!(state_v2.contains_key("key2"));

  let state_v3 = recovery.recover_at_version(v3)?;
  assert_eq!(state_v3.len(), 3);
  assert!(state_v3.contains_key("key1"));
  assert!(state_v3.contains_key("key2"));
  assert!(state_v3.contains_key("key3"));

  Ok(())
}

#[test]
fn test_recovery_mixed_compressed_and_uncompressed() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create initial store and archive (will be compressed)
  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let _v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.sync()?;
  let archive_version = store.current_version();
  store.rotate_journal()?;

  // Get compressed archive
  let compressed_archive_path = temp_dir.path().join(format!("test.jrn.v{}.zz", archive_version));
  let compressed_data = std::fs::read(&compressed_archive_path)?;

  // Create uncompressed journal data manually
  let mut uncompressed_store = VersionedKVStore::new(&file_path, 4096, None)?;
  let v2 = uncompressed_store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  uncompressed_store.sync()?;
  let uncompressed_data = std::fs::read(&file_path)?;

  // Recovery should handle both compressed and uncompressed
  let recovery = VersionedRecovery::new(vec![&compressed_data, &uncompressed_data])?;

  let state_final = recovery.recover_at_version(v2)?;
  assert_eq!(state_final.len(), 2);
  assert!(state_final.contains_key("key1"));
  assert!(state_final.contains_key("key2"));

  Ok(())
}

#[test]
fn test_recovery_decompression_transparent() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Create store with compressible data
  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
  let compressible = "A".repeat(500);
  let v1 = store.insert("data".to_string(), Value::String(compressible.clone()))?;
  store.sync()?;

  // Create uncompressed recovery baseline
  let uncompressed_data = std::fs::read(&file_path)?;
  let recovery_uncompressed = VersionedRecovery::new(vec![&uncompressed_data])?;
  let state_uncompressed = recovery_uncompressed.recover_at_version(v1)?;

  // Get archive version before rotation
  let archive_version = store.current_version();
  
  // Rotate to compress
  store.rotate_journal()?;

  // Read compressed archive
  let compressed_path = temp_dir.path().join(format!("test.jrn.v{}.zz", archive_version));
  let compressed_data = std::fs::read(&compressed_path)?;

  // Verify it's actually compressed (smaller)
  assert!(compressed_data.len() < uncompressed_data.len());

  // Create recovery from compressed data
  let recovery_compressed = VersionedRecovery::new(vec![&compressed_data])?;
  let state_compressed = recovery_compressed.recover_at_version(v1)?;

  // Both should produce identical results
  assert_eq!(state_uncompressed.len(), state_compressed.len());
  assert_eq!(
    state_uncompressed.get("data").map(|tv| &tv.value),
    state_compressed.get("data").map(|tv| &tv.value)
  );
  assert_eq!(
    state_uncompressed.get("data").map(|tv| &tv.value),
    Some(&Value::String(compressible))
  );

  Ok(())
}
