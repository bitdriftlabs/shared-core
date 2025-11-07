// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::VersionedKVStore;
use crate::kv_journal::TimestampedValue;
use crate::tests::decompress_zlib;
use bd_bonjson::Value;
use tempfile::TempDir;

#[test]
fn empty_store() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Should start empty
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);

  assert!(temp_dir.path().join("test.jrn").exists());

  Ok(())
}

#[tokio::test]
async fn basic_crud() -> anyhow::Result<()> {
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

  // Read back existing key
  let val = store.get("key2");
  assert_eq!(val, Some(&Value::String("value2".to_string())));

  // Read non-existent key
  let val = store.get("key1");
  assert_eq!(val, None);

  Ok(())
}

#[tokio::test]
async fn test_persistence_and_reload() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create store and write some data
  let (ts1, ts2) = {
    let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
    let ts1 = store
      .insert("key1".to_string(), Value::String("value1".to_string()))
      .await?;
    let ts2 = store.insert("key2".to_string(), Value::Signed(42)).await?;
    store.sync()?;

    (ts1, ts2)
  };

  // Reopen and verify data persisted
  {
    let store = VersionedKVStore::open_existing(temp_dir.path(), "test", 4096, None)?;
    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get_with_timestamp("key1"),
      Some(&TimestampedValue {
        value: Value::String("value1".to_string()),
        timestamp: ts1,
      })
    );
    assert_eq!(
      store.get_with_timestamp("key2"),
      Some(&TimestampedValue {
        value: Value::Signed(42),
        timestamp: ts2,
      })
    );
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

  // Decompress the archive and load it as a Store to verify that it contains the old state.
  let snapshot_store = make_store_from_snapshot_file(&temp_dir, &archived_path)?;
  assert_eq!(
    snapshot_store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(
    snapshot_store.get("key2"),
    Some(&Value::String("value2".to_string()))
  );
  assert_eq!(snapshot_store.len(), 2);

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

  let pre_rotation_state = store.as_hashmap().clone();
  let pre_rotation_ts = store
    .get_with_timestamp("key4")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate
  store.rotate_journal().await?;

  // Verify state is preserved exactly
  let post_rotation_state = store.as_hashmap();
  assert_eq!(pre_rotation_state, *post_rotation_state);
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
async fn test_timestamp_preservation_during_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Create store with small buffer to trigger rotation easily
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 2048, Some(0.5))?;

  // Insert some keys and capture their timestamps
  let ts1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;

  // Small sleep to ensure different timestamps
  std::thread::sleep(std::time::Duration::from_millis(10));

  let ts2 = store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

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
async fn test_multiple_rotations() -> anyhow::Result<()> {
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

fn make_store_from_snapshot_file(
  temp_dir: &TempDir,
  snapshot_path: &std::path::Path,
) -> anyhow::Result<VersionedKVStore> {
  // Decompress the snapshot and journal files into the temp directory
  // so we can open them as a store.
  let data = std::fs::read(snapshot_path)?;
  let decompressed_snapshot = decompress_zlib(&data)?;
  std::fs::write(temp_dir.path().join("snapshot.jrn"), decompressed_snapshot)?;

  let store = VersionedKVStore::open_existing(temp_dir.path(), "snapshot", 4096, None)?;

  Ok(store)
}
