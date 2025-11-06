// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]
#![allow(clippy::case_sensitive_file_extension_comparisons)]

use crate::VersionedKVStore;
use crate::versioned_recovery::VersionedRecovery;
use bd_bonjson::Value;
use tempfile::TempDir;

/// Helper function to decompress zlib-compressed data.
/// The `VersionedRecovery` no longer handles compression, so tests must decompress manually.
fn decompress_zlib(data: &[u8]) -> anyhow::Result<Vec<u8>> {
  use flate2::read::ZlibDecoder;
  use std::io::Read;

  let mut decoder = ZlibDecoder::new(data);
  let mut decompressed = Vec::new();
  decoder.read_to_end(&mut decompressed)?;
  Ok(decompressed)
}

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

  // Read ALL journals (archived + active)
  let mut all_journals = Vec::new();

  for archived_path in find_archived_journals(temp_dir.path())? {
    let rotation_ts = extract_rotation_timestamp(&archived_path)?;
    all_journals.push((std::fs::read(archived_path)?, rotation_ts));
  }

  // Read active journal (the last one) - use u64::MAX for active journal
  let active_journal = std::fs::read(temp_dir.path().join("test.jrn"))?;
  all_journals.push((active_journal.clone(), u64::MAX));

  // Test 1: Verify recover_current() with ALL journals gives correct state
  let all_journal_refs: Vec<(&[u8], u64)> = all_journals
    .iter()
    .map(|(data, ts)| (data.as_slice(), *ts))
    .collect();
  let recovery_all = VersionedRecovery::new(all_journal_refs)?;
  let state_all = recovery_all.recover_current()?;

  // Convert to comparable format (Value only, not TimestampedValue)
  let state_all_values: ahash::AHashMap<String, Value> =
    state_all.into_iter().map(|(k, tv)| (k, tv.value)).collect();

  assert_eq!(state_all_values, expected_state);

  // Test 2: Verify recover_current() with ONLY the last journal gives the same state
  // This is the optimization we want to prove works!
  let recovery_last = VersionedRecovery::new(vec![(&active_journal, u64::MAX)])?;
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
  let archived_files = find_archived_journals(temp_dir.path())?;
  let archived_path = archived_files.first().unwrap();
  let compressed_data = std::fs::read(archived_path)?;

  // Verify it starts with zlib magic bytes (0x78)
  assert_eq!(
    compressed_data[0], 0x78,
    "Compressed data should start with zlib magic byte"
  );

  // Decompress manually since VersionedRecovery no longer handles compression
  let decompressed_data = decompress_zlib(&compressed_data)?;

  // Should successfully recover from decompressed data
  let recovery = VersionedRecovery::new(vec![(&decompressed_data, u64::MAX)])?;
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
  // Since VersionedRecovery no longer validates during construction,
  // errors will occur when trying to recover data
  let mut invalid_version = vec![0u8; 32];
  let version_bytes = 999u64.to_le_bytes();
  invalid_version[0 .. 8].copy_from_slice(&version_bytes);
  let recovery = VersionedRecovery::new(vec![(&invalid_version, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_err(),
    "Should fail when recovering with invalid version"
  );

  // Test 2: Data too small (smaller than header)
  let small_data = vec![0u8; 8];
  let recovery = VersionedRecovery::new(vec![(&small_data, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_err(),
    "Should fail when recovering with data too small"
  );

  // Test 3: Empty data
  let empty_data = vec![];
  let recovery = VersionedRecovery::new(vec![(&empty_data, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_err(),
    "Should fail when recovering with empty data"
  );

  // Test 4: Corrupted zlib data (caller should decompress before passing)
  // If caller accidentally passes compressed data, it will fail during recovery
  let mut fake_zlib = vec![0x78, 0x9C]; // Valid zlib magic bytes
  fake_zlib.extend_from_slice(&[0xFF; 100]); // But garbage data
  let recovery = VersionedRecovery::new(vec![(&fake_zlib, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_err(),
    "Should fail when recovering with compressed data"
  );

  // Test 5: Random garbage
  let garbage = vec![0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90];
  let recovery = VersionedRecovery::new(vec![(&garbage, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_err(),
    "Should fail when recovering with garbage data"
  );

  // Test 6: Mixed valid and invalid journals
  // When the last journal is invalid, recover_current() should fail
  let mut invalid_mixed = vec![0u8; 32];
  let version_bytes = 999u64.to_le_bytes();
  invalid_mixed[0 .. 8].copy_from_slice(&version_bytes);
  let recovery = VersionedRecovery::new(vec![(&valid_data, u64::MAX), (&invalid_mixed, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_err(),
    "Should fail because last journal is invalid"
  );

  // Test 7: Mixed invalid and valid journals
  // When the last journal is valid, recover_current() should succeed
  let recovery = VersionedRecovery::new(vec![(&invalid_mixed, u64::MAX), (&valid_data, u64::MAX)])?;
  let result = recovery.recover_current();
  assert!(
    result.is_ok(),
    "Should succeed because last journal is valid"
  );

  Ok(())
}

#[test]
fn test_detection_zlib_compression_level_5() {
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

  // Test compression level 5 (what we use in production)
  let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(5));
  encoder.write_all(&uncompressed).unwrap();
  let compressed = encoder.finish().unwrap();

  // Verify it starts with 0x78 (zlib magic byte)
  assert_eq!(compressed[0], 0x78);

  // Decompress manually since VersionedRecovery no longer handles compression
  let decompressed = decompress_zlib(&compressed).unwrap();

  // Should be able to process the decompressed data
  let result = VersionedRecovery::new(vec![(&decompressed, u64::MAX)]);
  // May succeed or fail depending on whether the data is valid bonjson,
  // but should at least attempt to parse without panicking
  let _ = result;
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
  for archived_path in find_archived_journals(temp_dir.path())? {
    let rotation_ts = extract_rotation_timestamp(&archived_path)?;
    all_journals.push((std::fs::read(archived_path)?, rotation_ts));
  }

  // Read active journal
  all_journals.push((std::fs::read(temp_dir.path().join("test.jrn"))?, u64::MAX));

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
  let recovery = VersionedRecovery::new(vec![(&journal_data, u64::MAX)])?;

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
  let recovery = VersionedRecovery::new(vec![(&journal_data, u64::MAX)])?;

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
  let recovery = VersionedRecovery::new(vec![(&journal_data, u64::MAX)])?;

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
  let archived_files = find_archived_journals(temp_dir.path())?;
  let archived_path = archived_files.first().unwrap();
  assert!(archived_path.exists(), "Compressed archive should exist");

  // Read both journals
  let compressed_data = std::fs::read(archived_path)?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Decompress the archived journal manually
  let decompressed_data = decompress_zlib(&compressed_data)?;

  // Extract rotation timestamp from archived filename
  let rotation_ts = extract_rotation_timestamp(archived_path)?;

  // Create recovery from both journals (decompressed first, then active)
  let recovery = VersionedRecovery::new(vec![
    (&decompressed_data, rotation_ts),
    (&active_data, u64::MAX),
  ])?;

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
  let archived_files = find_archived_journals(temp_dir.path())?;

  let archive1_path = &archived_files[0];
  let archive2_path = &archived_files[1];

  let archive1_data = std::fs::read(archive1_path)?;
  let archive2_data = std::fs::read(archive2_path)?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Decompress archived journals manually
  let decompressed1 = decompress_zlib(&archive1_data)?;
  let decompressed2 = decompress_zlib(&archive2_data)?;

  // Extract rotation timestamps
  let archive1_ts = extract_rotation_timestamp(archive1_path)?;
  let archive2_ts = extract_rotation_timestamp(archive2_path)?;

  // Create recovery from all journals (all decompressed)
  let recovery = VersionedRecovery::new(vec![
    (&decompressed1, archive1_ts),
    (&decompressed2, archive2_ts),
    (&active_data, u64::MAX),
  ])?;

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
  let archived_files = find_archived_journals(temp_dir.path())?;
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

  // Extract rotation timestamp from compressed archive
  let compressed_ts = extract_rotation_timestamp(compressed_archive_path)?;

  // Decompress the compressed archive manually
  let decompressed_data = decompress_zlib(&compressed_data)?;

  // Recovery now requires both journals to be decompressed
  let recovery = VersionedRecovery::new(vec![
    (&decompressed_data, compressed_ts),
    (&uncompressed_data, u64::MAX),
  ])?;

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
  let recovery = VersionedRecovery::new(vec![(&journal_data, u64::MAX)])?;

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

  // Decompress the archived journal manually
  let decompressed_archived = decompress_zlib(&archived_data)?;

  // Create recovery from both journals
  let recovery = VersionedRecovery::new(vec![
    (&decompressed_archived, rotation_ts),
    (&active_data, u64::MAX),
  ])?;

  // Verify we can recover at any timestamp across both journals
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
  let recovery_uncompressed = VersionedRecovery::new(vec![(&uncompressed_data, u64::MAX)])?;
  let state_uncompressed = recovery_uncompressed.recover_at_timestamp(ts1)?;

  // Rotate to compress
  store.rotate_journal().await?;

  // Read compressed archive
  let archived_files = find_archived_journals(temp_dir.path())?;
  let compressed_path = archived_files.first().unwrap();
  let compressed_data = std::fs::read(compressed_path)?;

  // Extract rotation timestamp from compressed archive filename
  let compressed_ts = extract_rotation_timestamp(compressed_path)?;

  // Verify it's actually compressed (smaller)
  assert!(compressed_data.len() < uncompressed_data.len());

  // Decompress the archived journal manually
  let decompressed_data = decompress_zlib(&compressed_data)?;

  // Create recovery from decompressed data
  let recovery_compressed = VersionedRecovery::new(vec![(&decompressed_data, compressed_ts)])?;
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
async fn test_journal_ordering_requirement() -> anyhow::Result<()> {
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
  let archived_files = find_archived_journals(temp_dir.path())?;

  let archived_data = std::fs::read(archived_files.first().unwrap())?;
  let active_data = std::fs::read(temp_dir.path().join("test.jrn"))?;

  // Extract rotation timestamp from archived filename
  let archived_ts = extract_rotation_timestamp(archived_files.first().unwrap())?;

  // Should succeed when journals are in correct chronological order (archived, then active)
  let recovery = VersionedRecovery::new(vec![
    (&archived_data, archived_ts),
    (&active_data, u64::MAX),
  ]);
  assert!(recovery.is_ok(), "Should succeed with correct ordering");

  // Verify correct ordering produces expected results
  let state = recovery?.recover_current()?;
  assert_eq!(state.len(), 2);
  assert!(state.contains_key("key1"));
  assert!(state.contains_key("key2"));

  // Note: Journals with reversed order may not produce correct results
  // because recovery replays journals sequentially. Users must provide
  // journals in chronological order (oldest to newest).
  // The removal of base_timestamp metadata field doesn't change this requirement -
  // chronological order is determined by filename timestamps (e.g., store.jrn.t300.zz)

  Ok(())
}
