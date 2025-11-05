// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::VersionedKVStore;
use bd_bonjson::Value;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[test]
fn test_versioned_store_new() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Should start empty
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);
  assert_eq!(store.base_version(), 1); // Base version starts at 1
  assert_eq!(store.current_version(), 1);

  Ok(())
}

#[test]
fn test_versioned_store_basic_operations() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Test insert with version tracking
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  assert_eq!(v1, 2); // First write is version 2 (base is 1)

  let retrieved = store.get("key1");
  assert_eq!(retrieved, Some(&Value::String("value1".to_string())));

  // Test overwrite
  let v2 = store.insert("key1".to_string(), Value::String("value2".to_string()))?;
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

#[test]
fn test_versioned_store_remove() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Insert some values
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;

  assert_eq!(store.len(), 2);
  assert!(v2 > v1);

  // Remove a key
  let v3 = store.remove("key1")?;
  assert!(v3.is_some());
  assert!(v3.unwrap() > v2);

  assert_eq!(store.len(), 1);
  assert!(!store.contains_key("key1"));
  assert!(store.contains_key("key2"));

  // Remove non-existent key
  let removed = store.remove("nonexistent")?;
  assert!(removed.is_none());

  Ok(())
}

#[test]
fn test_point_in_time_recovery() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Create a sequence of writes
  let v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  let v3 = store.insert("key1".to_string(), Value::String("updated1".to_string()))?;
  let v4 = store.remove("key2")?;

  // Current state should have key1=updated1, key2 deleted
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("updated1".to_string()))
  );
  assert_eq!(store.get("key2"), None);
  assert_eq!(store.len(), 1);

  // Recover at v1: should have key1=value1
  let state_v1 = store.as_hashmap_at_version(v1)?;
  assert_eq!(state_v1.len(), 1);
  assert_eq!(
    state_v1.get("key1"),
    Some(&Value::String("value1".to_string()))
  );

  // Recover at v2: should have key1=value1, key2=value2
  let state_v2 = store.as_hashmap_at_version(v2)?;
  assert_eq!(state_v2.len(), 2);
  assert_eq!(
    state_v2.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(
    state_v2.get("key2"),
    Some(&Value::String("value2".to_string()))
  );

  // Recover at v3: should have key1=updated1, key2=value2
  let state_v3 = store.as_hashmap_at_version(v3)?;
  assert_eq!(state_v3.len(), 2);
  assert_eq!(
    state_v3.get("key1"),
    Some(&Value::String("updated1".to_string()))
  );
  assert_eq!(
    state_v3.get("key2"),
    Some(&Value::String("value2".to_string()))
  );

  // Recover at v4: should have key1=updated1, key2 deleted
  let state_v4 = store.as_hashmap_at_version(v4.unwrap())?;
  assert_eq!(state_v4.len(), 1);
  assert_eq!(
    state_v4.get("key1"),
    Some(&Value::String("updated1".to_string()))
  );
  assert!(!state_v4.contains_key("key2"));

  Ok(())
}

#[test]
fn test_persistence_and_reload() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let v1;
  let v2;

  // Create store and write some data
  {
    let mut store = VersionedKVStore::new(&file_path, 4096, None)?;
    v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    v2 = store.insert("key2".to_string(), Value::Signed(42))?;
    store.sync()?;
  }

  // Reopen and verify data persisted
  {
    let store = VersionedKVStore::open_existing(&file_path, 4096, None)?;
    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get("key1"),
      Some(&Value::String("value1".to_string()))
    );
    assert_eq!(store.get("key2"), Some(&Value::Signed(42)));

    // Version numbers should be preserved
    assert_eq!(store.current_version(), v2);

    // Point-in-time recovery should still work
    let state_v1 = store.as_hashmap_at_version(v1)?;
    assert_eq!(state_v1.len(), 1);
    assert_eq!(
      state_v1.get("key1"),
      Some(&Value::String("value1".to_string()))
    );
  }

  Ok(())
}

#[test]
fn test_null_value_is_deletion() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Insert a value
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  assert!(store.contains_key("key1"));

  // Insert null to delete
  store.insert("key1".to_string(), Value::Null)?;
  assert!(!store.contains_key("key1"));
  assert_eq!(store.len(), 0);

  Ok(())
}

#[test]
fn test_rotation_callback() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  // Use a small buffer and low high water mark to trigger rotation easily
  let mut store = VersionedKVStore::new(&file_path, 1024, Some(0.3))?;

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
    last_version = store.insert(key, value)?;

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
  assert_eq!(new_path, &file_path);
  assert!(*rotation_version <= last_version);

  Ok(())
}

#[test]
fn test_manual_rotation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Insert some data
  let _v1 = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  let v2 = store.insert("key2".to_string(), Value::String("value2".to_string()))?;

  // Manually trigger rotation
  let rotation_version = store.current_version();
  store.rotate_journal()?;

  // Verify archived file exists
  let archived_path = temp_dir
    .path()
    .join(format!("test.jrn.v{}", rotation_version));
  assert!(archived_path.exists());

  // Verify active journal still works
  let v3 = store.insert("key3".to_string(), Value::String("value3".to_string()))?;
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

#[test]
fn test_rotation_preserves_state() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Create complex state
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::Signed(42))?;
  store.insert("key3".to_string(), Value::Bool(true))?;
  store.insert("key4".to_string(), Value::Float(3.14159))?;

  let pre_rotation_state = store.as_hashmap().clone();
  let pre_rotation_version = store.current_version();

  // Rotate
  store.rotate_journal()?;

  // Verify state is preserved exactly
  let post_rotation_state = store.as_hashmap();
  assert_eq!(&pre_rotation_state, post_rotation_state);
  assert_eq!(store.len(), 4);

  // Verify we can continue writing
  let v_new = store.insert("key5".to_string(), Value::String("value5".to_string()))?;
  assert!(v_new > pre_rotation_version);
  assert_eq!(store.len(), 5);

  Ok(())
}

#[test]
fn test_empty_store_operations() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  // Operations on empty store
  assert_eq!(store.get("nonexistent"), None);
  assert!(!store.contains_key("nonexistent"));
  assert_eq!(store.remove("nonexistent")?, None);
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);

  // Point-in-time recovery of empty state
  let state = store.as_hashmap_at_version(1)?;
  assert!(state.is_empty());

  Ok(())
}

#[test]
fn test_version_monotonicity() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("test.jrn");

  let mut store = VersionedKVStore::new(&file_path, 4096, None)?;

  let mut last_version = store.current_version();

  // Perform various operations and ensure version always increases
  for i in 0 .. 20 {
    let op_version = if i % 3 == 0 {
      store.insert(format!("key{}", i), Value::Signed(i as i64))?
    } else if i % 3 == 1 {
      store.insert(
        format!("key{}", i / 3),
        Value::String(format!("updated{}", i)),
      )?
    } else {
      store
        .remove(&format!("key{}", i / 3))?
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
