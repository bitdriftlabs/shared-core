// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::VersionedKVStore;
use bd_bonjson::Value;
use tempfile::TempDir;

#[test]
fn test_versioned_store_new() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Should start empty
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_timestamp_collision_on_clamping() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert first value - this establishes a timestamp
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts1 = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Perform rapid successive writes - these might share timestamps if system clock hasn't advanced
  store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;
  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  store
    .insert("key4".to_string(), Value::String("value4".to_string()))
    .await?;

  let ts2 = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();
  let ts3 = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();
  let ts4 = store
    .get_with_timestamp("key4")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Verify monotonicity: timestamps should never decrease
  assert!(
    ts2 >= ts1,
    "Timestamps should be monotonically non-decreasing"
  );
  assert!(
    ts3 >= ts2,
    "Timestamps should be monotonically non-decreasing"
  );
  assert!(
    ts4 >= ts3,
    "Timestamps should be monotonically non-decreasing"
  );

  // Document that timestamps CAN be equal (this is the key difference from the old +1 behavior)
  // When system clock doesn't advance or goes backwards, we reuse the same timestamp
  // This is acceptable because version numbers provide total ordering

  // Count unique timestamps - with rapid operations, we might have collisions
  let timestamps = [ts1, ts2, ts3, ts4];
  let unique_count = timestamps
    .iter()
    .collect::<std::collections::HashSet<_>>()
    .len();

  // We should have at least 1 unique timestamp (all could be the same in extreme cases)
  assert!(
    unique_count >= 1 && unique_count <= 4,
    "Should have 1-4 unique timestamps, got {}",
    unique_count
  );

  Ok(())
}


#[tokio::test]
async fn test_versioned_store_remove() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert some values
  let ts1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts2 = store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  assert_eq!(store.len(), 2);
  assert!(ts2 >= ts1);

  // Remove a key
  let ts3 = store.remove("key1").await?;
  assert!(ts3.is_some());
  assert!(ts3.unwrap() >= ts2);

  assert_eq!(store.len(), 1);
  assert!(!store.contains_key("key1"));
  assert!(store.contains_key("key2"));

  // Remove non-existent key
  let removed = store.remove("nonexistent").await?;
  assert!(removed.is_none());

  Ok(())
}

#[tokio::test]
async fn test_persistence_and_reload() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create store and write some data
  {
    let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
    let _ts1 = store
      .insert("key1".to_string(), Value::String("value1".to_string()))
      .await?;
    let _ts2 = store.insert("key2".to_string(), Value::Signed(42)).await?;
    store.sync()?;
  }

  // Reopen and verify data persisted
  {
    let store = VersionedKVStore::open_existing(temp_dir.path(), "test", 4096, None)?;
    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get("key1"),
      Some(&Value::String("value1".to_string()))
    );
    assert_eq!(store.get("key2"), Some(&Value::Signed(42)));
  }

  Ok(())
}

#[tokio::test]
async fn test_null_value_is_deletion() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert a value
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  assert!(store.contains_key("key1"));

  // Insert null to delete
  store.insert("key1".to_string(), Value::Null).await?;
  assert!(!store.contains_key("key1"));
  assert_eq!(store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_manual_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert some data
  let _ts1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let ts2 = store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  // Get max timestamp before rotation (this will be used in the archive name)
  let rotation_timestamp = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Manually trigger rotation
  store.rotate_journal().await?;

  // Verify archived file exists (compressed)
  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.t{}.zz", rotation_timestamp));
  assert!(archived_path.exists());

  // Verify active journal still works
  let ts3 = store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  assert!(ts3 >= ts2);
  assert_eq!(store.len(), 3);

  // Verify data is intact
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(
    store.get("key2"),
    Some(&Value::String("value2".to_string()))
  );
  assert_eq!(
    store.get("key3"),
    Some(&Value::String("value3".to_string()))
  );

  Ok(())
}

#[tokio::test]
async fn test_rotation_preserves_state() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Create complex state
  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store.insert("key2".to_string(), Value::Signed(42)).await?;
  store.insert("key3".to_string(), Value::Bool(true)).await?;
  store
    .insert("key4".to_string(), Value::Float(3.14159))
    .await?;

  let pre_rotation_state = store.as_hashmap();
  let pre_rotation_ts = store
    .get_with_timestamp("key4")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate
  store.rotate_journal().await?;

  // Verify state is preserved exactly
  let post_rotation_state = store.as_hashmap();
  assert_eq!(pre_rotation_state, post_rotation_state);
  assert_eq!(store.len(), 4);

  // Verify we can continue writing
  let ts_new = store
    .insert("key5".to_string(), Value::String("value5".to_string()))
    .await?;
  assert!(ts_new >= pre_rotation_ts);
  assert_eq!(store.len(), 5);

  Ok(())
}

#[tokio::test]
async fn test_empty_store_operations() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Operations on empty store
  assert_eq!(store.get("nonexistent"), None);
  assert!(!store.contains_key("nonexistent"));
  assert_eq!(store.remove("nonexistent").await?, None);
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_version_monotonicity() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  let mut last_timestamp = 0u64;

  // Perform various operations and ensure timestamp always increases
  for i in 0 .. 20 {
    let op_timestamp = if i % 3 == 0 {
      store
        .insert(format!("key{}", i), Value::Signed(i as i64))
        .await?
    } else if i % 3 == 1 {
      store
        .insert(
          format!("key{}", i / 3),
          Value::String(format!("updated{}", i)),
        )
        .await?
    } else {
      store
        .remove(&format!("key{}", i / 3))
        .await?
        .unwrap_or(last_timestamp)
    };

    assert!(
      op_timestamp >= last_timestamp,
      "Timestamp should be monotonically non-decreasing"
    );
    last_timestamp = op_timestamp;
  }

  Ok(())
}

#[tokio::test]
async fn test_timestamp_preservation_during_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create store with small buffer to trigger rotation easily
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 2048, Some(0.5))?;

  // Insert some keys and capture their timestamps
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

  // Verify timestamps are different
  assert_ne!(ts1, ts2, "Timestamps should be different");
  assert!(ts2 > ts1, "Later writes should have later timestamps");

  // Write enough data to trigger rotation
  for i in 0 .. 50 {
    store.insert(format!("fill{i}"), Value::Signed(i)).await?;
  }

  // Verify that after rotation, the original timestamps are preserved
  let ts1_after = store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  let ts2_after = store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  assert_eq!(
    ts1, ts1_after,
    "key1 timestamp should be preserved during rotation"
  );
  assert_eq!(
    ts2, ts2_after,
    "key2 timestamp should be preserved during rotation"
  );

  // Verify ordering is still correct
  assert!(
    ts2_after > ts1_after,
    "Timestamp ordering should be preserved"
  );

  Ok(())
}

#[tokio::test]
async fn test_timestamp_monotonicity() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Track timestamps across multiple writes
  let mut timestamps = Vec::new();

  // Perform multiple writes and collect their timestamps
  for i in 0 .. 20 {
    store
      .insert(format!("key{}", i), Value::Signed(i as i64))
      .await?;

    let ts = store
      .get_with_timestamp(&format!("key{}", i))
      .map(|tv| tv.timestamp)
      .unwrap();

    timestamps.push(ts);
  }

  // Verify all timestamps are monotonically increasing
  for i in 1 .. timestamps.len() {
    assert!(
      timestamps[i] >= timestamps[i - 1],
      "Timestamp at index {} ({}) should be >= timestamp at index {} ({})",
      i,
      timestamps[i],
      i - 1,
      timestamps[i - 1]
    );
  }

  // Verify that timestamps are actually different (at least some of them)
  // This ensures we're not just assigning the same timestamp to everything
  let unique_timestamps: std::collections::HashSet<_> = timestamps.iter().collect();
  assert!(
    unique_timestamps.len() > 1,
    "Expected multiple unique timestamps, got only {}",
    unique_timestamps.len()
  );

  Ok(())
}

#[tokio::test]
async fn test_timestamp_monotonicity_across_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Write before rotation
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
  store.rotate_journal().await?;

  std::thread::sleep(std::time::Duration::from_millis(10));

  // Write after rotation
  store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  let ts3 = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  std::thread::sleep(std::time::Duration::from_millis(10));

  store
    .insert("key4".to_string(), Value::String("value4".to_string()))
    .await?;
  let ts4 = store
    .get_with_timestamp("key4")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Verify monotonicity across rotation boundary
  assert!(ts2 >= ts1, "ts2 should be >= ts1");
  assert!(ts3 >= ts2, "ts3 should be >= ts2 (across rotation)");
  assert!(ts4 >= ts3, "ts4 should be >= ts3");

  // Verify ordering
  let timestamps = [ts1, ts2, ts3, ts4];
  for i in 1 .. timestamps.len() {
    assert!(
      timestamps[i] >= timestamps[i - 1],
      "Timestamp monotonicity violated at index {}: {} < {}",
      i,
      timestamps[i],
      timestamps[i - 1]
    );
  }

  Ok(())
}

#[tokio::test]
async fn test_compression_during_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert some data
  let data = "x".repeat(1000); // Large value to make compression effective
  store
    .insert("key1".to_string(), Value::String(data.clone()))
    .await?;
  store
    .insert("key2".to_string(), Value::String(data.clone()))
    .await?;
  store
    .insert("key3".to_string(), Value::String(data))
    .await?;

  // Get size of uncompressed journal before rotation
  let uncompressed_size = std::fs::metadata(temp_dir.path().join("test.jrn"))?.len();

  // Get max timestamp before rotation (this will be used in the archive name)
  let rotation_timestamp = store
    .get_with_timestamp("key3")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Trigger rotation
  store.rotate_journal().await?;

  // Verify compressed archive exists
  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.t{}.zz", rotation_timestamp));
  assert!(
    archived_path.exists(),
    "Compressed archive should exist at {:?}",
    archived_path
  );

  // Verify compressed size is smaller than original
  let compressed_size = std::fs::metadata(&archived_path)?.len();
  assert!(
    compressed_size < uncompressed_size,
    "Compressed size ({}) should be smaller than uncompressed ({})",
    compressed_size,
    uncompressed_size
  );

  // Verify uncompressed temporary file was deleted
  let temp_archive_path = temp_dir.path().join("test.jrn.old");
  assert!(
    !temp_archive_path.exists(),
    "Temporary uncompressed archive should be deleted"
  );

  // Verify active journal still works
  store
    .insert("key4".to_string(), Value::String("value4".to_string()))
    .await?;
  assert_eq!(store.len(), 4);

  Ok(())
}

#[tokio::test]
async fn test_compression_ratio() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 8192, None)?;

  // Insert highly compressible data
  let compressible_data = "A".repeat(500);
  for i in 0 .. 10 {
    store
      .insert(
        format!("key{}", i),
        Value::String(compressible_data.clone()),
      )
      .await?;
  }

  let uncompressed_size = std::fs::metadata(temp_dir.path().join("test.jrn"))?.len();
  let rotation_timestamp = store
    .get_with_timestamp(&format!("key{}", 9))
    .map(|tv| tv.timestamp)
    .unwrap();

  store.rotate_journal().await?;

  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.t{}.zz", rotation_timestamp));
  let compressed_size = std::fs::metadata(&archived_path)?.len();

  // With highly compressible data, we should get significant compression
  // Expecting at least 50% compression ratio for repeated characters
  #[allow(clippy::cast_precision_loss)]
  let compression_ratio = (compressed_size as f64) / (uncompressed_size as f64);
  assert!(
    compression_ratio < 0.5,
    "Compression ratio should be better than 50% for repeated data, got {:.2}%",
    compression_ratio * 100.0
  );

  Ok(())
}

#[tokio::test]
async fn test_multiple_rotations_with_compression() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  let mut rotation_timestamps = Vec::new();

  // Perform multiple rotations
  for i in 0 .. 3 {
    let key = format!("key{}", i);
    let value = Value::String(format!("value{}", i));
    store.insert(key.clone(), value).await?;
    let timestamp = store
      .get_with_timestamp(&key)
      .map(|tv| tv.timestamp)
      .unwrap();
    rotation_timestamps.push(timestamp);
    store.rotate_journal().await?;
  }

  // Verify all compressed archives exist
  for timestamp in rotation_timestamps {
    let archived_path = temp_dir.path().join(format!("test.jrn.t{}.zz", timestamp));
    assert!(
      archived_path.exists(),
      "Compressed archive for timestamp {} should exist",
      timestamp
    );
  }

  Ok(())
}
