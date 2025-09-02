// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
use super::*;
use bd_bonjson::Value;
use tempfile::TempDir;

#[test]
fn test_kv_store_new() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");
  
  let mut store = KVStore::new(&base_path, 4096, None, None)?;
  
  // Should start empty
  assert!(store.is_empty()?);
  assert_eq!(store.len()?, 0);
  
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
  
  let retrieved = store.get("key1")?;
  assert_eq!(retrieved, Some(Value::String("value1".to_string())));
  
  // Test overwrite
  let old_value = store.insert("key1".to_string(), Value::String("value2".to_string()))?;
  assert_eq!(old_value, Some(Value::String("value1".to_string())));
  
  let retrieved = store.get("key1")?;
  assert_eq!(retrieved, Some(Value::String("value2".to_string())));
  
  // Test contains_key
  assert!(store.contains_key("key1")?);
  assert!(!store.contains_key("nonexistent")?);
  
  // Test len
  assert_eq!(store.len()?, 1);
  assert!(!store.is_empty()?);
  
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
  
  assert_eq!(store.len()?, 2);
  
  // Remove a key
  let removed = store.remove("key1")?;
  assert_eq!(removed, Some(Value::String("value1".to_string())));
  
  assert_eq!(store.len()?, 1);
  assert!(!store.contains_key("key1")?);
  assert!(store.contains_key("key2")?);
  
  // Remove non-existent key
  let removed = store.remove("nonexistent")?;
  assert!(removed.is_none());
  
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
  
  assert_eq!(store.len()?, 3);
  
  // Clear all
  store.clear()?;
  
  assert_eq!(store.len()?, 0);
  assert!(store.is_empty()?);
  assert!(!store.contains_key("key1")?);
  assert!(!store.contains_key("key2")?);
  assert!(!store.contains_key("key3")?);
  
  Ok(())
}

#[test]
fn test_kv_store_clear_efficiency() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");
  
  let mut store = KVStore::new(&base_path, 4096, None, None)?;
  
  // Insert many key-value pairs
  for i in 0..100 {
    store.insert(format!("key{}", i), Value::Signed(i as i64))?;
  }
  
  assert_eq!(store.len()?, 100);
  
  // Check buffer usage before clear
  let buffer_usage_before = store.buffer_usage_ratio();
  
  // Clear all - this should be efficient with the new implementation
  store.clear()?;
  
  // Verify everything is cleared
  assert_eq!(store.len()?, 0);
  assert!(store.is_empty()?);
  
  // Check buffer usage after clear - should be much lower than before
  let buffer_usage_after = store.buffer_usage_ratio();
  
  // The buffer usage should be significantly reduced after clearing
  // (not just from deletion entries, but from actual reinitialization)
  assert!(buffer_usage_after < buffer_usage_before);
  
  // Verify we can still insert after clearing
  store.insert("new_key".to_string(), Value::String("new_value".to_string()))?;
  assert_eq!(store.len()?, 1);
  assert_eq!(store.get("new_key")?, Some(Value::String("new_value".to_string())));
  
  Ok(())
}

#[test]
fn test_kv_store_keys_and_values() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_store");
  
  let mut store = KVStore::new(&base_path, 4096, None, None)?;
  
  // Insert some values
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  store.insert("key2".to_string(), Value::Signed(42))?;
  store.insert("key3".to_string(), Value::Bool(true))?;
  
  // Test keys
  let mut keys = store.keys()?;
  keys.sort();
  assert_eq!(keys, vec!["key1", "key2", "key3"]);
  
  // Test values
  let values = store.values()?;
  assert_eq!(values.len(), 3);
  assert!(values.contains(&Value::String("value1".to_string())));
  assert!(values.contains(&Value::Signed(42)));
  assert!(values.contains(&Value::Bool(true)));
  
  // Test as_hashmap
  let map = store.as_hashmap()?;
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
    store.insert("persistent_key".to_string(), Value::String("persistent_value".to_string()))?;
    store.insert("number".to_string(), Value::Signed(123))?;
    store.sync()?;
  }
  
  // Verify files were created
  assert!(base_path.with_extension("jrna").exists());
  assert!(base_path.with_extension("jrnb").exists());
  
  // Open store again and verify data persisted
  {
    let mut store = KVStore::new(&base_path, 4096, None, None)?;
    
    assert_eq!(store.len()?, 2);
    assert_eq!(store.get("persistent_key")?, Some(Value::String("persistent_value".to_string())));
    assert_eq!(store.get("number")?, Some(Value::Signed(123)));
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
    assert_eq!(store.get("key1")?, Some(Value::String("value1".to_string())));
    
    // Should be able to add more data with the larger size
    store.insert("key2".to_string(), Value::String("value2".to_string()))?;
    assert_eq!(store.len()?, 2);
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
  assert_eq!(store.len()?, 3);
  assert_eq!(store.get("key1")?, Some(Value::String("value1".to_string())));
  assert_eq!(store.get("key2")?, Some(Value::String("value2".to_string())));
  assert_eq!(store.get("key3")?, Some(Value::String("value3".to_string())));
  
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
  assert_eq!(store.get("string")?, Some(Value::String("hello".to_string())));
  assert_eq!(store.get("signed")?, Some(Value::Signed(-42)));
  assert_eq!(store.get("unsigned")?, Some(Value::Signed(42)));
  assert_eq!(store.get("float")?, Some(Value::Float(3.14)));
  assert_eq!(store.get("boolean")?, Some(Value::Bool(true)));
  
  assert_eq!(store.len()?, 5);
  
  Ok(())
}

#[test]
fn test_insert_null_is_deletion() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_null");
  
  let mut store = KVStore::new(base_path, 1024, None, None)?;
  
  // Insert a value
  store.insert("key1".to_string(), Value::String("value1".to_string()))?;
  assert_eq!(store.get("key1")?, Some(Value::String("value1".to_string())));
  assert_eq!(store.len()?, 1);
  
  // Insert null should delete the key
  let old_value = store.insert("key1".to_string(), Value::Null)?;
  assert_eq!(old_value, Some(Value::String("value1".to_string())));
  assert_eq!(store.get("key1")?, None);
  assert_eq!(store.len()?, 0);
  
  // Insert null for non-existent key should be no-op
  let old_value = store.insert("key2".to_string(), Value::Null)?;
  assert_eq!(old_value, None);
  assert_eq!(store.len()?, 0);
  
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
  assert_eq!(store.get("key1")?, Some(Value::String("value1".to_string())));
  assert_eq!(store.get("key2")?, Some(Value::Signed(42)));
  assert_eq!(store.get("key3")?, Some(Value::Bool(true)));
  
  // Operations that read the entire map should work
  assert_eq!(store.len()?, 3);
  assert!(store.contains_key("key1")?);
  assert!(!store.contains_key("key4")?);
  
  let keys = store.keys()?;
  assert_eq!(keys.len(), 3);
  assert!(keys.contains(&"key1".to_string()));
  assert!(keys.contains(&"key2".to_string()));
  assert!(keys.contains(&"key3".to_string()));
  
  let values = store.values()?;
  assert_eq!(values.len(), 3);
  
  let map = store.as_hashmap()?;
  assert_eq!(map.len(), 3);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  
  // Modify data (should invalidate cache)
  store.insert("key4".to_string(), Value::String("value4".to_string()))?;
  
  // Verify cache was properly invalidated and new data is visible
  assert_eq!(store.len()?, 4);
  assert!(store.contains_key("key4")?);
  assert_eq!(store.get("key4")?, Some(Value::String("value4".to_string())));
  
  // Remove data (should also invalidate cache)
  let removed = store.remove("key2")?;
  assert_eq!(removed, Some(Value::Signed(42)));
  
  // Verify cache was invalidated
  assert_eq!(store.len()?, 3);
  assert!(!store.contains_key("key2")?);
  assert_eq!(store.get("key2")?, None);
  
  // Clear all data (should invalidate cache)
  store.clear()?;
  
  // Verify cache was invalidated
  assert_eq!(store.len()?, 0);
  assert!(store.is_empty()?);
  assert!(!store.contains_key("key1")?);
  
  Ok(())
}
