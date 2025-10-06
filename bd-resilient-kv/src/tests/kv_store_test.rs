// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::KVStore;
use bd_bonjson::Value;
use std::collections::HashMap;
use tempfile::TempDir;

#[test]
fn test_kv_store_new() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let store = KVStore::new(&base_path, 4096, None, None)?;

  // Should start empty
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);

  Ok(())
}

#[test]
fn test_kv_store_basic_operations() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Test insert and get
  let old_value = store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  assert!(old_value.is_none());

  let retrieved = store.get("key1");
  assert_eq!(retrieved, Some(&Value::String("value1".to_string())));

  // Test overwrite
  let old_value = store.insert("key1".to_string(), Value::String("value2".to_string()))?;
  assert_eq!(old_value, Some(Value::String("value1".to_string())));

  let retrieved = store.get("key1");
  assert_eq!(retrieved, Some(&Value::String("value2".to_string())));

  // Test contains_key
  assert!(store.contains_key("key1"));
  assert!(!store.contains_key("nonexistent"));

  // Test len and is_empty
  assert_eq!(store.len(), 1);
  assert!(!store.is_empty());

  Ok(())
}

#[test]
fn test_kv_store_remove() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Insert some values
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::String("value2".to_string()))?;

  assert_eq!(store.len(), 2);

  // Remove a key
  let removed = store.remove("key1")?;
  assert_eq!(removed, Some(Value::String("value1".to_string())));

  assert_eq!(store.len(), 1);
  assert!(!store.contains_key("key1"));
  assert!(store.contains_key("key2"));

  // Remove non-existent key
  let removed = store.remove("nonexistent")?;
  assert!(removed.is_none());

  Ok(())
}

#[test]
fn test_kv_store_insert_multiple() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Create multiple entries to insert
  let mut entries = HashMap::new();
  entries.insert("key1".to_string(), Value::String("value1".to_string()));
  entries.insert("key2".to_string(), Value::Signed(42));
  entries.insert("key3".to_string(), Value::Bool(true));
  entries.insert("key4".to_string(), Value::Float(3.14159));

  // Insert all entries at once
  store.insert_multiple(&entries)?;

  // Verify all entries were inserted
  assert_eq!(store.len(), 4);
  assert_eq!(store.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(store.get("key2"), Some(&Value::Signed(42)));
  assert_eq!(store.get("key3"), Some(&Value::Bool(true)));
  assert_eq!(store.get("key4"), Some(&Value::Float(3.14159)));

  // Test inserting more entries, including one with Null (deletion)
  let mut more_entries = HashMap::new();
  more_entries.insert("key5".to_string(), Value::String("value5".to_string()));
  more_entries.insert("key2".to_string(), Value::Null); // This should delete key2
  more_entries.insert("key1".to_string(), Value::String("updated_value1".to_string())); // Update key1

  store.insert_multiple(&more_entries)?;

  // Verify the changes
  assert_eq!(store.len(), 4); // key1, key3, key4, key5 (key2 deleted)
  assert_eq!(store.get("key1"), Some(&Value::String("updated_value1".to_string())));
  assert_eq!(store.get("key2"), None); // Deleted
  assert_eq!(store.get("key3"), Some(&Value::Bool(true))); // Unchanged
  assert_eq!(store.get("key4"), Some(&Value::Float(3.14159))); // Unchanged
  assert_eq!(store.get("key5"), Some(&Value::String("value5".to_string()))); // New

  Ok(())
}

#[test]
fn test_kv_store_clear() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Insert some values
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  store.insert("key3".to_string(), Value::Signed(42))?;

  assert_eq!(store.len(), 3);

  // Clear all
  store.clear()?;

  assert_eq!(store.len(), 0);
  assert!(store.is_empty());
  assert!(!store.contains_key("key1"));
  assert!(!store.contains_key("key2"));
  assert!(!store.contains_key("key3"));

  Ok(())
}

#[test]
fn test_kv_store_clear_efficiency() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Insert many key-value pairs
  for i in 0 .. 100 {
    store.insert(format!("key{}", i), Value::Signed(i as i64))?;
  }

  assert_eq!(store.len(), 100);

  // Check buffer usage before clear
  let buffer_usage_before = store.buffer_usage_ratio();

  // Clear all - this should be efficient with the new implementation
  store.clear()?;

  // Verify everything is cleared
  assert_eq!(store.len(), 0);
  assert!(store.is_empty());

  // Check buffer usage after clear - should be much lower than before
  let buffer_usage_after = store.buffer_usage_ratio();

  // The buffer usage should be significantly reduced after clearing
  // (not just from deletion entries, but from actual reinitialization)
  assert!(buffer_usage_after < buffer_usage_before);

  // Verify we can still insert after clearing
  store.insert(
    "new_key".to_string(),
    Value::String("new_value".to_string()),
  )?;
  assert_eq!(store.len(), 1);
  assert_eq!(
    store.get("new_key"),
    Some(&Value::String("new_value".to_string()))
  );

  Ok(())
}

#[test]
fn test_kv_store_hashmap_access() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Insert some values
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::Signed(42))?;
  store.insert("key3".to_string(), Value::Bool(true))?;

  // Test accessing keys via as_hashmap
  let map = store.as_hashmap();
  let mut key_strs: Vec<&str> = map.keys().map(std::string::String::as_str).collect();
  key_strs.sort();
  assert_eq!(key_strs, vec!["key1", "key2", "key3"]);

  // Test accessing values via as_hashmap
  let values: Vec<&Value> = map.values().collect();
  assert_eq!(values.len(), 3);
  assert!(
    values
      .iter()
      .any(|v| **v == Value::String("value1".to_string()))
  );
  assert!(values.iter().any(|v| **v == Value::Signed(42)));
  assert!(values.iter().any(|v| **v == Value::Bool(true)));

  // Test direct access via as_hashmap
  let map = store.as_hashmap();
  assert_eq!(map.len(), 3);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(42)));
  assert_eq!(map.get("key3"), Some(&Value::Bool(true)));

  Ok(())
}

#[test]
fn test_kv_store_persistence() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store and add data
  {
    let mut store = KVStore::new(&base_path, 4096, None, None)?;
    store.insert(
      "persistent_key".to_string(),
      Value::String("persistent_value".to_string()),
    )?;
    store.insert("number".to_string(), Value::Signed(123))?;
    store.sync()?;
  }

  // Verify files were created
  assert!(base_path.with_extension("jrna").exists());
  assert!(base_path.with_extension("jrnb").exists());

  // Open store again and verify data persisted
  {
    let store = KVStore::new(&base_path, 4096, None, None)?;

    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get("persistent_key"),
      Some(&Value::String("persistent_value".to_string()))
    );
    assert_eq!(store.get("number"), Some(&Value::Signed(123)));
  }

  Ok(())
}

#[test]
fn test_kv_store_file_resizing() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store with small size
  {
    let mut store = KVStore::new(&base_path, 1024, None, None)?;
    store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    store.sync()?;
  }

  // Reopen with larger size
  {
    let mut store = KVStore::new(&base_path, 4096, None, None)?;

    // Data should still be there
    assert_eq!(
      store.get("key1"),
      Some(&Value::String("value1".to_string()))
    );

    // Should be able to add more data with the larger size
    store.insert("key2".to_string(), Value::String("value2".to_string()))?;
    assert_eq!(store.len(), 2);
  }

  Ok(())
}

#[test]
fn test_kv_store_compress() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, Some(0.5), None)?;

  // Add some data
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::String("value2".to_string()))?;
  store.insert("key3".to_string(), Value::String("value3".to_string()))?;

  let _before_usage = store.buffer_usage_ratio();

  // Force compression
  store.compress()?;

  // Data should still be there
  assert_eq!(store.len(), 3);
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

  // Usage ratio might be different after compression
  let after_usage = store.buffer_usage_ratio();
  // Just verify it's still a valid ratio
  assert!(after_usage >= 0.0 && after_usage <= 1.0);

  Ok(())
}

#[test]
fn test_kv_store_mixed_value_types() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Insert different value types
  store.insert("string".to_string(), Value::String("hello".to_string()))?;
  store.insert("signed".to_string(), Value::Signed(-42))?;
  store.insert("unsigned".to_string(), Value::Signed(42))?; // Use Signed since BONJSON might convert
  store.insert("float".to_string(), Value::Float(3.14))?;
  store.insert("boolean".to_string(), Value::Bool(true))?;
  // Note: inserting Value::Null is equivalent to deletion, so we'll test that separately

  // Verify all types
  assert_eq!(
    store.get("string"),
    Some(&Value::String("hello".to_string()))
  );
  assert_eq!(store.get("signed"), Some(&Value::Signed(-42)));
  assert_eq!(store.get("unsigned"), Some(&Value::Signed(42)));
  assert_eq!(store.get("float"), Some(&Value::Float(3.14)));
  assert_eq!(store.get("boolean"), Some(&Value::Bool(true)));

  assert_eq!(store.len(), 5);

  Ok(())
}

#[test]
fn test_insert_null_is_deletion() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_null");

  let mut store = KVStore::new(base_path, 1024, None, None)?;

  // Insert a value
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(store.len(), 1);

  // Insert null should delete the key
  let old_value = store.insert("key1".to_string(), Value::Null)?;
  assert_eq!(old_value, Some(Value::String("value1".to_string())));
  assert_eq!(store.get("key1"), None);
  assert_eq!(store.len(), 0);

  // Insert null for non-existent key should be no-op
  let old_value = store.insert("key2".to_string(), Value::Null)?;
  assert_eq!(old_value, None);
  assert_eq!(store.len(), 0);

  Ok(())
}

#[test]
fn test_kv_store_caching_behavior() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Add some data
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::Signed(42))?;
  store.insert("key3".to_string(), Value::Bool(true))?;

  // Multiple reads should work correctly (using cache)
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(store.get("key2"), Some(&Value::Signed(42)));
  assert_eq!(store.get("key3"), Some(&Value::Bool(true)));

  // Operations that read the entire map should work
  assert_eq!(store.len(), 3);
  assert!(store.contains_key("key1"));
  assert!(!store.contains_key("key4"));

  let map = store.as_hashmap();
  assert_eq!(map.len(), 3);
  assert!(map.keys().any(|k| k.as_str() == "key1"));
  assert!(map.keys().any(|k| k.as_str() == "key2"));
  assert!(map.keys().any(|k| k.as_str() == "key3"));

  assert_eq!(map.values().len(), 3);

  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));

  // Modify data (should invalidate cache)
  store.insert("key4".to_string(), Value::String("value4".to_string()))?;

  // Verify cache was properly invalidated and new data is visible
  assert_eq!(store.len(), 4);
  assert!(store.contains_key("key4"));
  assert_eq!(
    store.get("key4"),
    Some(&Value::String("value4".to_string()))
  );

  // Remove data (should also invalidate cache)
  let removed = store.remove("key2")?;
  assert_eq!(removed, Some(Value::Signed(42)));

  // Verify cache was invalidated
  assert_eq!(store.len(), 3);
  assert!(!store.contains_key("key2"));
  assert_eq!(store.get("key2"), None);

  // Clear all data (should invalidate cache)
  store.clear()?;

  // Verify cache was invalidated
  assert_eq!(store.len(), 0);
  assert!(store.is_empty());
  assert!(!store.contains_key("key1"));

  Ok(())
}

#[test]
fn test_kv_store_constructor_cache_coherency_empty() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create a new store - should have coherent empty cache
  let store = KVStore::new(&base_path, 4096, None, None)?;

  // Cache should be empty and coherent with journal
  assert!(store.is_empty());
  assert_eq!(store.len(), 0);
  let map = store.as_hashmap();
  assert_eq!(map.len(), 0);
  assert_eq!(map.keys().count(), 0);
  assert_eq!(map.values().count(), 0);

  Ok(())
}

#[test]
fn test_kv_store_constructor_cache_coherency_with_existing_data() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store and add data
  {
    let mut store = KVStore::new(&base_path, 4096, None, None)?;
    store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    store.insert("key2".to_string(), Value::Signed(42))?;
    store.insert("key3".to_string(), Value::Bool(true))?;
    store.sync()?;
  }

  // Re-open the store - cache should be coherent with persisted data
  let store = KVStore::new(&base_path, 4096, None, None)?;

  // Verify cache is coherent with journal data
  assert_eq!(store.len(), 3);
  assert!(!store.is_empty());

  // Check each key exists in cache
  assert!(store.contains_key("key1"));
  assert!(store.contains_key("key2"));
  assert!(store.contains_key("key3"));

  // Check values are correct in cache
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(store.get("key2"), Some(&Value::Signed(42)));
  assert_eq!(store.get("key3"), Some(&Value::Bool(true)));

  // Check as_hashmap returns coherent data
  let map = store.as_hashmap();
  assert_eq!(map.len(), 3);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(42)));
  assert_eq!(map.get("key3"), Some(&Value::Bool(true)));

  // Check keys and values are coherent
  let keys: Vec<&String> = map.keys().collect();
  assert_eq!(keys.len(), 3);
  assert!(keys.contains(&&"key1".to_string()));
  assert!(keys.contains(&&"key2".to_string()));
  assert!(keys.contains(&&"key3".to_string()));

  assert_eq!(map.values().count(), 3);

  Ok(())
}

#[test]
fn test_kv_store_constructor_cache_coherency_with_file_resize() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store with small buffer and add data
  {
    let mut store = KVStore::new(&base_path, 512, None, None)?;
    store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    store.sync()?;
  }

  // Re-open with larger buffer - cache should be coherent with existing data
  let store = KVStore::new(&base_path, 4096, None, None)?;

  // Verify cache is coherent after file resize
  assert_eq!(store.len(), 1);
  assert!(store.contains_key("key1"));
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );

  Ok(())
}

#[test]
fn test_kv_store_constructor_cache_coherency_with_corrupted_data() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store and add data
  {
    let mut store = KVStore::new(&base_path, 4096, None, None)?;
    store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    store.sync()?;
  }

  // Corrupt one of the journal files
  let file_a = base_path.with_extension("jrna");
  std::fs::write(&file_a, b"corrupted data")?;

  // Re-open store - should handle corruption and have coherent cache
  let store = KVStore::new(&base_path, 4096, None, None)?;

  // Cache should be coherent (either empty for fresh journal or with data from non-corrupted
  // journal)
  let len = store.len();
  let map = store.as_hashmap();
  assert_eq!(map.len(), len);
  assert_eq!(map.keys().count(), len);
  assert_eq!(map.values().count(), len);

  // All operations should work consistently
  for key in map.keys() {
    assert!(store.contains_key(key));
    assert!(store.get(key).is_some());
  }

  Ok(())
}

#[test]
fn test_kv_store_constructor_cache_coherency_different_high_water_marks() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store with specific high water mark and add data
  {
    let mut store = KVStore::new(&base_path, 4096, Some(0.5), None)?;
    store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    store.insert("key2".to_string(), Value::String("value2".to_string()))?;
    store.sync()?;
  }

  // Re-open with different high water mark - cache should be coherent
  let store = KVStore::new(&base_path, 4096, Some(0.8), None)?;

  // Verify cache is coherent regardless of high water mark setting
  assert_eq!(store.len(), 2);
  assert!(store.contains_key("key1"));
  assert!(store.contains_key("key2"));
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(
    store.get("key2"),
    Some(&Value::String("value2".to_string()))
  );

  Ok(())
}

#[test]
fn test_kv_store_constructor_cache_coherency_after_journal_switch() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create store and force journal switching by filling one journal
  {
    let mut store = KVStore::new(&base_path, 512, Some(0.7), None)?;

    // Add enough data to trigger high water mark and journal switching
    for i in 0 .. 20 {
      store.insert(format!("key{}", i), Value::String(format!("value{}", i)))?;
    }
    store.sync()?;
  }

  // Re-open store - cache should be coherent with the active journal data
  let store = KVStore::new(&base_path, 512, Some(0.7), None)?;

  // Verify cache is coherent
  let len = store.len();
  assert!(len > 0); // Should have some data
  let map = store.as_hashmap();
  assert_eq!(map.len(), len);
  assert_eq!(map.keys().count(), len);
  assert_eq!(map.values().count(), len);

  // All cached keys should be valid
  for key in map.keys() {
    assert!(store.contains_key(key));
    assert!(store.get(key).is_some());
  }

  Ok(())
}

#[test]
fn test_kv_store_comprehensive_cache_coherency() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Helper function to verify cache coherency at any point
  let verify_coherency = |store: &KVStore, base_path: &std::path::Path| -> anyhow::Result<()> {
    // Test TRUE coherency: create a fresh store from the same journal files
    // and verify it loads the exact same data as what's in the current cache
    store.sync()?; // Ensure all data is written to disk

    let fresh_store = KVStore::new(base_path, 4096, None, None)?;
    let fresh_hashmap = fresh_store.as_hashmap();
    let current_hashmap = store.as_hashmap();

    // The fresh store should have exactly the same data as the current cache
    assert_eq!(
      fresh_store.len(),
      store.len(),
      "Fresh store length doesn't match cached length"
    );
    assert_eq!(
      fresh_hashmap.len(),
      current_hashmap.len(),
      "Fresh store hashmap length doesn't match"
    );

    // Every key-value pair in the cache should exist in the fresh store
    for (key, value) in current_hashmap {
      assert!(
        fresh_store.contains_key(key),
        "Fresh store missing key: {}",
        key
      );
      let fresh_value = fresh_store.get(key);
      assert_eq!(
        fresh_value,
        Some(value),
        "Fresh store has different value for key: {}",
        key
      );
    }

    // Every key-value pair in the fresh store should exist in the cache
    for (key, value) in fresh_hashmap {
      assert!(store.contains_key(key), "Cache missing key: {}", key);
      let cached_value = store.get(key);
      assert_eq!(
        cached_value,
        Some(value),
        "Cache has different value for key: {}",
        key
      );
    }

    Ok(())
  };

  let mut store = KVStore::new(&base_path, 4096, None, None)?;

  // Verify coherency on empty store
  verify_coherency(&store, &base_path)?;

  // Add data and verify coherency after each operation
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  verify_coherency(&store, &base_path)?;

  store.insert("key2".to_string(), Value::Signed(42))?;
  verify_coherency(&store, &base_path)?;

  store.insert("key3".to_string(), Value::Bool(true))?;
  verify_coherency(&store, &base_path)?;

  // Overwrite existing key
  store.insert("key1".to_string(), Value::String("new_value1".to_string()))?;
  verify_coherency(&store, &base_path)?;
  assert_eq!(
    store.get("key1"),
    Some(&Value::String("new_value1".to_string()))
  );

  // Remove a key
  store.remove("key2")?;
  verify_coherency(&store, &base_path)?;
  assert!(!store.contains_key("key2"));

  // Insert null (equivalent to deletion)
  store.insert("key3".to_string(), Value::Null)?;
  verify_coherency(&store, &base_path)?;
  assert!(!store.contains_key("key3"));

  // Add many keys
  for i in 10 .. 20 {
    store.insert(format!("key{}", i), Value::Signed(i as i64))?;
    verify_coherency(&store, &base_path)?;
  }

  // Clear all and verify
  store.clear()?;
  verify_coherency(&store, &base_path)?;
  assert!(store.is_empty());

  // Add data again and compress
  for i in 0 .. 5 {
    store.insert(
      format!("compress_key{}", i),
      Value::String(format!("compress_value{}", i)),
    )?;
  }
  verify_coherency(&store, &base_path)?;

  store.compress()?;
  verify_coherency(&store, &base_path)?;

  // Verify data is still there after compression
  for i in 0 .. 5 {
    assert_eq!(
      store.get(&format!("compress_key{}", i)),
      Some(&Value::String(format!("compress_value{}", i)))
    );
  }

  store.sync()?;
  verify_coherency(&store, &base_path)?;

  Ok(())
}

#[test]
fn test_cache_vs_journal_coherency_validation() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");

  // Create a store and add some data
  {
    let mut store = KVStore::new(&base_path, 4096, None, None)?;
    store.insert(
      "test_key1".to_string(),
      Value::String("test_value1".to_string()),
    )?;
    store.insert("test_key2".to_string(), Value::Signed(123))?;
    store.insert("test_key3".to_string(), Value::Bool(false))?;
    store.sync()?;
  }

  // Now open the store and verify its cache matches what's actually persisted
  let store = KVStore::new(&base_path, 4096, None, None)?;

  // The cache should reflect the persisted state
  assert_eq!(store.len(), 3);
  assert_eq!(
    store.get("test_key1"),
    Some(&Value::String("test_value1".to_string()))
  );
  assert_eq!(store.get("test_key2"), Some(&Value::Signed(123)));
  assert_eq!(store.get("test_key3"), Some(&Value::Bool(false)));

  // Create another store instance to verify the journal data matches the cache
  let verification_store = KVStore::new(&base_path, 4096, None, None)?;

  // Both stores should have identical data
  assert_eq!(store.len(), verification_store.len());
  assert_eq!(store.as_hashmap(), verification_store.as_hashmap());

  // Every key-value pair should match
  let map = store.as_hashmap();
  for key in map.keys() {
    assert_eq!(
      store.get(key),
      verification_store.get(key),
      "Mismatch for key: {}",
      key
    );
  }

  // Test that cache remains coherent after operations
  let mut mutable_store = store;
  mutable_store.insert(
    "new_key".to_string(),
    Value::String("new_value".to_string()),
  )?;
  mutable_store.remove("test_key2")?;
  mutable_store.sync()?;

  // Create yet another store to verify the changes are persisted correctly
  let final_verification_store = KVStore::new(&base_path, 4096, None, None)?;

  // Should have the updated data (3 original - 1 removed + 1 added = 3)
  assert_eq!(final_verification_store.len(), 3);
  assert!(final_verification_store.contains_key("new_key"));
  assert!(!final_verification_store.contains_key("test_key2"));
  assert_eq!(
    final_verification_store.get("new_key"),
    Some(&Value::String("new_value".to_string()))
  );
  assert_eq!(
    final_verification_store.get("test_key1"),
    Some(&Value::String("test_value1".to_string()))
  );
  assert_eq!(
    final_verification_store.get("test_key3"),
    Some(&Value::Bool(false))
  );

  Ok(())
}
