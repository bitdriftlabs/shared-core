// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::VersionedKVStore;
use bd_bonjson::Value;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[test]
fn test_versioned_store_new() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Should start empty
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);
  assert_eq!(store.base_version(), 1); // Base version starts at 1
  assert_eq!(store.current_version(), 1);

  Ok(())
}

#[tokio::test]
async fn test_versioned_store_basic_operations() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;

  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Test insert with version tracking
  let v1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  assert_eq!(v1, 2); // First write is version 2 (base is 1)

  let retrieved = store.get("key1");
  assert_eq!(retrieved, Some(&Value::String("value1".to_string())));

  // Test overwrite
  let v2 = store
    .insert("key1".to_string(), Value::String("value2".to_string()))
    .await?;
  assert_eq!(v2, 3); // Second write is version 3
  assert!(v2 > v1);

  let retrieved = store.get("key1");
  assert_eq!(retrieved, Some(&Value::String("value2".to_string())));

  // Test contains_key
  assert!(store.contains_key("key1"));
  assert!(!store.contains_key("nonexistent"));

  // Test len and is_empty
  assert_eq!(store.len(), 1);
  assert!(!store.is_empty());

  // Current version should track latest write
  assert_eq!(store.current_version(), v2);

  Ok(())
}

#[tokio::test]
async fn test_versioned_store_remove() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert some values
  let v1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let v2 = store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  assert_eq!(store.len(), 2);
  assert!(v2 > v1);

  // Remove a key
  let v3 = store.remove("key1").await?;
  assert!(v3.is_some());
  assert!(v3.unwrap() > v2);

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


  let v2;

  // Create store and write some data
  {
    let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;
    let _v1 = store
      .insert("key1".to_string(), Value::String("value1".to_string()))
      .await?;
    v2 = store.insert("key2".to_string(), Value::Signed(42)).await?;
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

    // Version numbers should be preserved
    assert_eq!(store.current_version(), v2);
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
async fn test_rotation_callback() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  // Use a small buffer and low high water mark to trigger rotation easily
  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 1024, Some(0.3))?;

  // Set up callback to track rotation events
  let callback_data = Arc::new(Mutex::new(Vec::new()));
  let callback_data_clone = Arc::clone(&callback_data);

  store.set_rotation_callback(Box::new(move |old_path, new_path, version| {
    let mut data = callback_data_clone.lock().unwrap();
    data.push((old_path.to_path_buf(), new_path.to_path_buf(), version));
  }));

  // Write enough data to trigger rotation
  let mut last_version = 0;
  for i in 0 .. 100 {
    let key = format!("key{}", i);
    let value = Value::String(format!("value_{}_with_some_extra_padding", i));
    last_version = store.insert(key, value).await?;

    // Rotation happens automatically inside insert when high water mark is triggered
    let data = callback_data.lock().unwrap();
    if !data.is_empty() {
      break;
    }
  }

  // Check that callback was invoked
  let data = callback_data.lock().unwrap();
  assert!(data.len() >= 1, "Expected at least one rotation event");

  let (old_path, new_path, rotation_version) = &data[0];
  assert!(old_path.to_string_lossy().contains(".v"));
  assert_eq!(new_path, &temp_dir.path().join("test.jrn"));
  assert!(*rotation_version <= last_version);

  Ok(())
}

#[tokio::test]
async fn test_manual_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  // Insert some data
  let _v1 = store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  let v2 = store
    .insert("key2".to_string(), Value::String("value2".to_string()))
    .await?;

  // Manually trigger rotation
  let rotation_version = store.current_version();
  store.rotate_journal().await?;

  // Verify archived file exists (compressed)
  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.v{}.zz", rotation_version));
  assert!(archived_path.exists());

  // Verify active journal still works
  let v3 = store
    .insert("key3".to_string(), Value::String("value3".to_string()))
    .await?;
  assert!(v3 > v2);
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

  // New journal should have base version at rotation point
  assert_eq!(store.base_version(), rotation_version);

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
  let pre_rotation_version = store.current_version();

  // Rotate
  store.rotate_journal().await?;

  // Verify state is preserved exactly
  let post_rotation_state = store.as_hashmap();
  assert_eq!(pre_rotation_state, post_rotation_state);
  assert_eq!(store.len(), 4);

  // Verify we can continue writing
  let v_new = store
    .insert("key5".to_string(), Value::String("value5".to_string()))
    .await?;
  assert!(v_new > pre_rotation_version);
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

  let mut last_version = store.current_version();

  // Perform various operations and ensure version always increases
  for i in 0 .. 20 {
    let op_version = if i % 3 == 0 {
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
        .unwrap_or(last_version)
    };

    assert!(
      op_version >= last_version,
      "Version should be monotonically increasing"
    );
    last_version = op_version;
  }

  assert_eq!(store.current_version(), last_version);

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

  // Get current version before rotation (this is what will be used in the archive name)
  let rotation_version = store.current_version();

  // Trigger rotation
  store.rotate_journal().await?;

  // Verify compressed archive exists
  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.v{}.zz", rotation_version));
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
  let rotation_version = store.current_version();

  store.rotate_journal().await?;

  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.v{}.zz", rotation_version));
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

  let mut rotation_versions = Vec::new();

  // Perform multiple rotations
  for i in 0 .. 3 {
    let key = format!("key{}", i);
    let value = Value::String(format!("value{}", i));
    let version = store.insert(key, value).await?;
    rotation_versions.push(version);
    store.rotate_journal().await?;
  }

  // Verify all compressed archives exist
  for version in rotation_versions {
    let archived_path = temp_dir.path().join(format!("test.jrn.v{}.zz", version));
    assert!(
      archived_path.exists(),
      "Compressed archive for version {} should exist",
      version
    );
  }

  Ok(())
}

#[tokio::test]
async fn test_rotation_callback_receives_compressed_path() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;


  let mut store = VersionedKVStore::new(temp_dir.path(), "test", 4096, None)?;

  let callback_data = Arc::new(Mutex::new(None));
  let callback_data_clone = Arc::clone(&callback_data);

  store.set_rotation_callback(Box::new(move |old_path, _new_path, _version| {
    let mut data = callback_data_clone.lock().unwrap();
    *data = Some(old_path.to_path_buf());
  }));

  store
    .insert("key1".to_string(), Value::String("value1".to_string()))
    .await?;
  store.rotate_journal().await?;

  // Verify callback received compressed path
  let data = callback_data.lock().unwrap();
  let archived_path = data.as_ref().unwrap();

  assert!(
    archived_path.to_string_lossy().ends_with(".zz"),
    "Callback should receive compressed archive path ending with .zz, got: {:?}",
    archived_path
  );

  // Verify the file actually exists
  assert!(
    archived_path.exists(),
    "Compressed archive passed to callback should exist"
  );

  Ok(())
}
