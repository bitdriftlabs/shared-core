// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::VersionedKVStore;
use crate::versioned_recovery::VersionedRecovery;
use bd_bonjson::Value;
use tempfile::TempDir;

#[tokio::test]
async fn test_recover_current_only_needs_last_journal() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  // Create a store with multiple rotations to build up history
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Add initial data
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  // First rotation
  store.rotate_journal().await?;

  // Update key1 and add key3
  store
    .insert("key1".to_string(), Value::String("updated1".to_string()))
    .await?;
  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;

  // Second rotation
  store.rotate_journal().await?;

  // Add more data and delete key2
  store
    .insert("key4".to_string(), Value::String("value4".to_string()))
    .await?;
  store.remove("key2").await?;

  store.sync()?;

  // Get expected current state using the store's hashmap
  let expected_state = store.as_hashmap();

  // Read ALL journals
  let mut all_journals = Vec::new();
  let mut archived_paths = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();

  // Sort to ensure chronological order
  archived_paths.sort();

  for archived_path in &archived_paths {
    all_journals.push(std::fs::read(archived_path)?);
  }

  // Read active journal (the last one)
  let active_journal = std::fs::read(temp_dir.path().join("test.jrn"))?;
  all_journals.push(active_journal.clone());

  // Test 1: Verify recover_current() with ALL journals gives correct state
  let all_journal_refs: Vec<&[u8]> = all_journals.iter().map(Vec::as_slice).collect();
  let recovery_all = VersionedRecovery::new(all_journal_refs)?;
  let state_all = recovery_all.recover_current()?;

  // Convert to comparable format (Value only, not TimestampedValue)
  let state_all_values: ahash::AHashMap<String, Value> =
    state_all.into_iter().map(|(k, tv)| (k, tv.value)).collect();

  assert_eq!(state_all_values, expected_state);

  // Test 2: Verify recover_current() with ONLY the last journal gives the same state
  // This is the optimization we want to prove works!
  let recovery_last = VersionedRecovery::new(vec![&active_journal])?;
  let state_last = recovery_last.recover_current()?;

  let state_last_values: ahash::AHashMap<String, Value> = state_last
    .into_iter()
    .map(|(k, tv)| (k, tv.value))
    .collect();

  // The last journal alone should give us the same current state
  assert_eq!(state_last_values, expected_state);

  // Verify the expected final state has the right keys
  assert!(state_last_values.contains_key("key1"));
  assert!(!state_last_values.contains_key("key2")); // deleted
  assert!(state_last_values.contains_key("key3"));
  assert!(state_last_values.contains_key("key4"));
  assert_eq!(
    state_last_values.get("key1"),
    Some(&Value::String("updated1".to_string()))
  );

  Ok(())
}

#[tokio::test]
async fn test_detection_compressed_journal() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create and rotate to get compressed archive
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store.rotate_journal().await?;

  // Find the archived file
  let mut archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();
  let archived_path = archived_files.first().unwrap();
  let compressed_data = std::fs::read(archived_path)?;

  // Verify it starts with zlib magic bytes (0x78)
  assert_eq!(
    compressed_data[0], 0x78,
    "Compressed data should start with zlib magic byte"
  );

  // Should successfully detect and decompress
  let recovery = VersionedRecovery::new(vec![&compressed_data])?;
  let state = recovery.recover_current()?;
  assert_eq!(state.len(), 1);
  assert_eq!(
    state.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );

  Ok(())
}

#[tokio::test]
async fn test_detection_invalid_journal_data() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  // Create valid journal for mixed test
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store.sync()?;
  let valid_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Test 1: Invalid format version
  let mut invalid_version = vec![0u8; 32];
  let version_bytes = 999u64.to_le_bytes();
  invalid_version[0 .. 8].copy_from_slice(&version_bytes);
  let result = VersionedRecovery::new(vec![&invalid_version]);
  assert!(result.is_err());
  assert!(
    result.unwrap_err().to_string().contains("Invalid journal format version"),
    "Should fail with invalid version error"
  );

  // Test 2: Data too small (smaller than header)
  let small_data = vec![0u8; 8];
  let result = VersionedRecovery::new(vec![&small_data]);
  assert!(result.is_err());
  assert!(
    result.unwrap_err().to_string().contains("Data too small"),
    "Should fail with data too small error"
  );

  // Test 3: Empty data
  let empty_data = vec![];
  let result = VersionedRecovery::new(vec![&empty_data]);
  assert!(result.is_err());
  assert!(
    result.unwrap_err().to_string().contains("Data too small"),
    "Should fail with data too small error"
  );

  // Test 4: Corrupted zlib header
  let mut fake_zlib = vec![0x78, 0x9C]; // Valid zlib magic bytes
  fake_zlib.extend_from_slice(&[0xFF; 100]); // But garbage data
  let result = VersionedRecovery::new(vec![&fake_zlib]);
  assert!(result.is_err(), "Should fail with corrupted zlib data");

  // Test 5: Random garbage
  let garbage = vec![0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90];
  let result = VersionedRecovery::new(vec![&garbage]);
  assert!(result.is_err());
  let err_msg = result.unwrap_err().to_string();
  assert!(
    err_msg.contains("Data too small") || err_msg.contains("corrupt"),
    "Should fail with appropriate error"
  );

  // Test 6: Mixed valid and invalid journals
  let mut invalid_mixed = vec![0u8; 32];
  let version_bytes = 999u64.to_le_bytes();
  invalid_mixed[0 .. 8].copy_from_slice(&version_bytes);
  let result = VersionedRecovery::new(vec![&valid_data, &invalid_mixed]);
  assert!(result.is_err(), "Should fail if any journal is invalid");

  Ok(())
}

#[test]
fn test_detection_all_zlib_compression_levels() {
  use flate2::Compression;
  use flate2::write::ZlibEncoder;
  use std::io::Write;

  // Create some uncompressed journal-like data
  let mut uncompressed = vec![0u8; 64];
  // Version 2
  uncompressed[0 .. 8].copy_from_slice(&2u64.to_le_bytes());
  // Position at end
  uncompressed[8 .. 16].copy_from_slice(&64u64.to_le_bytes());
  // Some data
  uncompressed[16 .. 32].copy_from_slice(b"[{\"base_version\"");

  // Test different compression levels
  for level in [
    Compression::none(),
    Compression::fast(),
    Compression::default(),
    Compression::best(),
  ] {
    let mut encoder = ZlibEncoder::new(Vec::new(), level);
    encoder.write_all(&uncompressed).unwrap();
    let compressed = encoder.finish().unwrap();

    // Verify it starts with 0x78
    assert_eq!(compressed[0], 0x78);

    // Should be able to detect and decompress
    let result = VersionedRecovery::new(vec![&compressed]);
    // May succeed or fail depending on whether the data is valid bonjson,
    // but should at least attempt decompression without panicking
    let _ = result;
  }
}


#[tokio::test]
async fn test_recovery_multiple_journals_with_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create a store with larger buffer to avoid BufferFull errors during test
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 2048, None)?;

  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  // Write more data to trigger rotation
  for i in 0 .. 20 {
    store.insert(format!("key{i}"), Value::Signed(i)).await?;
  }

  let ts_middle = store
    .get_with_timestamp("key19")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  // Write more after rotation
  store
    .insert(
      "final".to_string(),
      Value::String("final_value".to_string()),
    )
    .await?;
  let ts_final = store
    .get_with_timestamp("final")
    .map(|tv| tv.timestamp)
    .unwrap();
  store.sync()?;

  // Read all journal files
  let mut all_journals = Vec::new();

  // Read archived journals
  let archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
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
  all_journals.push(std::fs::read(temp_dir.path().join("test.jrn"))?);

  // Create recovery utility with all journals
  let journal_refs: Vec<&[u8]> = all_journals.iter().map(std::vec::Vec::as_slice).collect();
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
    Some(&Value::String("final_value".to_string()))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_empty_journal() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create an empty store
  let store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store.sync()?;

  let journal_data = std::fs::read(temp_dir.path().join("test.jrn"))?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // Should have timestamp range starting at base
  let timestamp_range = recovery.timestamp_range();
  assert!(timestamp_range.is_some());

  // Recovering current should return empty map
  let state = recovery.recover_current()?;
  assert_eq!(state.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_recovery_with_overwrites() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store.insert("key".to_string(), Value::Signed(1)).await?;
  let ts1 = store
    .get_with_timestamp("key")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store.insert("key".to_string(), Value::Signed(2)).await?;
  let ts2 = store
    .get_with_timestamp("key")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store.insert("key".to_string(), Value::Signed(3)).await?;
  let ts3 = store
    .get_with_timestamp("key")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  let journal_data = std::fs::read(temp_dir.path().join("test.jrn"))?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // Each timestamp should show the value at that time
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(
    state_ts1.get("key").map(|tv| &tv.value),
    Some(&Value::Signed(1))
  );

  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(
    state_ts2.get("key").map(|tv| &tv.value),
    Some(&Value::Signed(2))
  );

  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(
    state_ts3.get("key").map(|tv| &tv.value),
    Some(&Value::Signed(3))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_various_value_types() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store
    .insert("string".to_string(), Value::String("hello".to_string()))
    .await?;
  store
    .insert("number".to_string(), Value::Signed(42))
    .await?;
  store
    .insert("float".to_string(), Value::Float(3.14))
    .await?;
  store.insert("bool".to_string(), Value::Bool(true)).await?;
  store.sync()?;

  let journal_data = std::fs::read(temp_dir.path().join("test.jrn"))?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  let state = recovery.recover_current()?;
  assert_eq!(state.len(), 4);
  assert_eq!(
    state.get("string").map(|tv| &tv.value),
    Some(&Value::String("hello".to_string()))
  );
  assert_eq!(
    state.get("number").map(|tv| &tv.value),
    Some(&Value::Signed(42))
  );
  assert_eq!(
    state.get("float").map(|tv| &tv.value),
    Some(&Value::Float(3.14))
  );
  assert_eq!(
    state.get("bool").map(|tv| &tv.value),
    Some(&Value::Bool(true))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_from_compressed_archive() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create a store and write some data
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Rotate to create compressed archive
  store.rotate_journal().await?;

  // Add more data to active journal
  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  let ts3 = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Find the compressed archive
  let mut archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();
  let archived_path = archived_files.first().unwrap();
  assert!(archived_path.exists(), "Compressed archive should exist");

  // Read both journals
  let compressed_data = std::fs::read(archived_path)?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Create recovery from both journals (compressed first, then active)
  let recovery = VersionedRecovery::new(vec![&compressed_data, &active_data])?;

  // Verify timestamp range spans both journals
  let timestamp_range = recovery.timestamp_range();
  assert!(timestamp_range.is_some());
  let (min, max) = timestamp_range.unwrap();
  assert!(min <= ts1);
  assert!(max >= ts3);

  // Recover at ts1 (should be in compressed archive)
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert_eq!(
    state_ts1.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );

  // Recover at ts2 (should be in compressed archive)
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);

  // Recover at ts3 (should include data from both archives and active journal)
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 3);
  assert_eq!(
    state_ts3.get("key3").map(|tv| &tv.value),
    Some(&Value::String("value3".to_string()))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_from_multiple_compressed_archives() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create a store and perform multiple rotations
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.rotate_journal().await?;

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.rotate_journal().await?;

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  let ts3 = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Collect all journal data (2 compressed + 1 active)
  let mut archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();

  let archive1_path = &archived_files[0];
  let archive2_path = &archived_files[1];

  let archive1_data = std::fs::read(archive1_path)?;
  let archive2_data = std::fs::read(archive2_path)?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Create recovery from all journals
  let recovery = VersionedRecovery::new(vec![&archive1_data, &archive2_data, &active_data])?;

  // Verify we can recover at any timestamp
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert!(state_ts1.contains_key("key1"));

  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert!(state_ts2.contains_key("key1"));
  assert!(state_ts2.contains_key("key2"));

  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 3);
  assert!(state_ts3.contains_key("key1"));
  assert!(state_ts3.contains_key("key2"));
  assert!(state_ts3.contains_key("key3"));

  Ok(())
}

#[tokio::test]
async fn test_recovery_mixed_compressed_and_uncompressed() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create initial store and archive (will be compressed)
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  let _ts1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store.sync()?;
  store.rotate_journal().await?;

  // Get compressed archive
  let mut archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();
  let compressed_archive_path = archived_files.first().unwrap();
  let compressed_data = std::fs::read(compressed_archive_path)?;

  // Create uncompressed journal data manually
  let mut uncompressed_store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  uncompressed_store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  let ts2 = uncompressed_store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();
  uncompressed_store.sync()?;
  let uncompressed_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Recovery should handle both compressed and uncompressed
  let recovery = VersionedRecovery::new(vec![&compressed_data, &uncompressed_data])?;

  let state_final = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_final.len(), 2);
  assert!(state_final.contains_key("key1"));
  assert!(state_final.contains_key("key2"));

  Ok(())
}

#[tokio::test]
async fn test_recovery_at_timestamp() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  // Create a store and write some timestamped data
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Small sleep to ensure different timestamps
  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Small sleep to ensure different timestamps
  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key1".to_string(), Value::String("updated1".to_string()))
    .await?;
  let ts3 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Read the journal data
  let journal_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Create recovery utility
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  // Verify timestamp range
  let timestamp_range = recovery.timestamp_range();
  assert!(timestamp_range.is_some());
  let (min, max) = timestamp_range.unwrap();
  assert!(min <= ts1);
  assert!(max >= ts3);

  // Recover at ts1: should have only key1=value1
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert_eq!(
    state_ts1.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );

  // Recover at ts2: should have key1=value1, key2=value2
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert_eq!(
    state_ts2.get("key1").map(|tv| &tv.value),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(
    state_ts2.get("key2").map(|tv| &tv.value),
    Some(&Value::String("value2".to_string()))
  );

  // Recover at ts3: should have key1=updated1, key2=value2
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 2);
  assert_eq!(
    state_ts3.get("key1").map(|tv| &tv.value),
    Some(&Value::String("updated1".to_string()))
  );
  assert_eq!(
    state_ts3.get("key2").map(|tv| &tv.value),
    Some(&Value::String("value2".to_string()))
  );

  Ok(())
}

#[tokio::test]
async fn test_recovery_at_timestamp_with_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Write some data before rotation
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate journal
  let rotation_ts = ts2;
  store.rotate_journal().await?;

  std::thread::sleep(std::time::Duration::from_millis(10));

  // Write data after rotation
  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  let ts3 = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  // Read both journals
  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.t{}.zz", rotation_ts));
  let archived_data = std::fs::read(&archived_path)?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Create recovery from both journals
  let recovery = VersionedRecovery::new(vec![&archived_data, &active_data])?;

  // Verify timestamp range spans both journals
  let timestamp_range = recovery.timestamp_range();
  assert!(timestamp_range.is_some());
  let (min, max) = timestamp_range.unwrap();
  assert!(min <= ts1);
  assert!(max >= ts3);

  // Recover at ts1 (should be in archived journal)
  let state_ts1 = recovery.recover_at_timestamp(ts1)?;
  assert_eq!(state_ts1.len(), 1);
  assert!(state_ts1.contains_key("key1"));

  // Recover at ts2 (should be in archived journal)
  let state_ts2 = recovery.recover_at_timestamp(ts2)?;
  assert_eq!(state_ts2.len(), 2);
  assert!(state_ts2.contains_key("key1"));
  assert!(state_ts2.contains_key("key2"));

  // Recover at ts3 (should include all data)
  let state_ts3 = recovery.recover_at_timestamp(ts3)?;
  assert_eq!(state_ts3.len(), 3);
  assert!(state_ts3.contains_key("key1"));
  assert!(state_ts3.contains_key("key2"));
  assert!(state_ts3.contains_key("key3"));

  Ok(())
}

#[tokio::test]
async fn test_timestamp_range() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  let ts3 = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  store.sync()?;

  let journal_data = std::fs::read(temp_dir.path().join("test.jrn"))?;
  let recovery = VersionedRecovery::new(vec![&journal_data])?;

  let timestamp_range = recovery.timestamp_range();
  assert!(timestamp_range.is_some());
  let (min, max) = timestamp_range.unwrap();

  // Min should be <= first timestamp, max should be >= last timestamp
  assert!(min <= ts1);
  assert!(max >= ts3);

  // Timestamps should be ordered
  assert!(ts3 > ts1);

  Ok(())
}

#[tokio::test]
async fn test_recovery_decompression_transparent() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create store with compressible data
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
  let compressible = "A".repeat(500);
  store
    .insert("data".to_string(), Value::String(compressible.clone()))
    .await?;
  let ts1 = store
    .get_with_timestamp("data")
    .map(|tv| tv.timestamp)
    .unwrap();
  store.sync()?;

  // Create uncompressed recovery baseline
  let uncompressed_data = std::fs::read(temp_dir.path().join("test.jrn"))?;
  let recovery_uncompressed = VersionedRecovery::new(vec![&uncompressed_data])?;
  let state_uncompressed = recovery_uncompressed.recover_at_timestamp(ts1)?;

  // Rotate to compress
  store.rotate_journal().await?;

  // Read compressed archive
  let mut archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();
  let compressed_path = archived_files.first().unwrap();
  let compressed_data = std::fs::read(compressed_path)?;

  // Verify it's actually compressed (smaller)
  assert!(compressed_data.len() < uncompressed_data.len());

  // Create recovery from compressed data
  let recovery_compressed = VersionedRecovery::new(vec![&compressed_data])?;
  let state_compressed = recovery_compressed.recover_at_timestamp(ts1)?;

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

#[tokio::test]
async fn test_base_timestamp_validation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  // Create a store and perform rotation to get proper sequential journals
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Add initial data
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store.sync()?;

  // Rotate to create first archived journal
  store.rotate_journal().await?;

  std::thread::sleep(std::time::Duration::from_millis(50));

  // Add more data after rotation
  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  store.sync()?;

  // Read both journals (archived + active)
  let mut archived_files = std::fs::read_dir(temp_dir.path())?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path();
      if path.file_name()?.to_str()?.starts_with("test.jrn.t") {
        Some(path)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  archived_files.sort();

  let archived_data = std::fs::read(archived_files.first().unwrap())?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Should succeed when journals are in correct chronological order (archived, then active)
  let recovery = VersionedRecovery::new(vec![&archived_data, &active_data]);
  assert!(recovery.is_ok(), "Should succeed with correct ordering");

  // Should fail when journals are in wrong order (active before archived)
  let recovery_reversed = VersionedRecovery::new(vec![&active_data, &archived_data]);
  assert!(
    recovery_reversed.is_err(),
    "Should fail when base_timestamp ordering is violated"
  );

  let err = recovery_reversed.unwrap_err();
  let err_msg = err.to_string();
  assert!(
    err_msg.contains("base_timestamp") && err_msg.contains("max_timestamp"),
    "Error should mention base_timestamp and max_timestamp validation, got: {}",
    err_msg
  );

  Ok(())
}
