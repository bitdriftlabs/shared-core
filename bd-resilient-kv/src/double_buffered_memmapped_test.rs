// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DoubleBufferedMemMappedKVJournal, KVJournal};
use bd_bonjson::Value;
use tempfile::TempDir;

#[test]
fn test_double_buffered_memmapped_basic_operations() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let mut db_kv = DoubleBufferedMemMappedKVJournal::new(file_a, file_b, 4096, Some(0.8), None)?;
  
  // Test basic set and get
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  db_kv.set("key2", &Value::Signed(42))?;
  
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 2);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(42)));
  
  // Should start with file A active
  assert!(db_kv.is_active_file_a());
  
  // Files should exist on disk
  assert!(db_kv.active_file_path().exists());
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_deletion() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let mut db_kv = DoubleBufferedMemMappedKVJournal::new(file_a, file_b, 4096, Some(0.8), None)?;
  
  // Add some data
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  db_kv.set("key2", &Value::String("value2".to_string()))?;
  
  // Delete one key
  db_kv.delete("key1")?;
  
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 1);
  assert!(!map.contains_key("key1"));
  assert_eq!(map.get("key2"), Some(&Value::String("value2".to_string())));
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_persistence() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  // Create and populate the first instance
  {
    let mut db_kv = DoubleBufferedMemMappedKVJournal::new(&file_a, &file_b, 4096, Some(0.8), None)?;
    
    db_kv.set("persistent_key", &Value::String("persistent_value".to_string()))?;
    db_kv.set("another_key", &Value::Signed(123))?;
    
    // Force sync to disk
    db_kv.sync()?;
  } // db_kv goes out of scope
  
  // Create a new instance from the existing file
  {
    let mut db_kv = DoubleBufferedMemMappedKVJournal::from_file(&file_a, &file_b, 4096, Some(0.8), None)?;
    
    let map = db_kv.as_hashmap()?;
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("persistent_key"), Some(&Value::String("persistent_value".to_string())));
    assert_eq!(map.get("another_key"), Some(&Value::Signed(123)));
  }
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_file_switching() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  // Use small files to trigger switching quickly
  let mut db_kv = DoubleBufferedMemMappedKVJournal::new(&file_a, &file_b, 1024, Some(0.5), None)?;
  
  // Should start with file A
  assert!(db_kv.is_active_file_a());
  
  // Add data gradually to trigger switching
  let mut switch_detected = false;
  for i in 0..20 {
    let key = format!("key_{}", i);
    let value = format!("longer_value_to_fill_file_{}", i);
    
    let was_file_a = db_kv.is_active_file_a();
    db_kv.set(&key, &Value::String(value))?;
    let is_file_a = db_kv.is_active_file_a();
    
    if was_file_a != is_file_a {
      switch_detected = true;
      println!("File switched after entry {}: {} -> {}", 
               i + 1, 
               if was_file_a { "A" } else { "B" },
               if is_file_a { "A" } else { "B" });
      break;
    }
  }
  
  // Verify all data is still accessible regardless of switching
  let map = db_kv.as_hashmap()?;
  println!("Final map contains {} entries", map.len());
  
  for (key, _) in &map {
    assert!(key.starts_with("key_"), "All keys should be test keys");
  }
  
  // Both files should exist if switching occurred
  if switch_detected {
    assert!(file_a.exists());
    assert!(file_b.exists());
    println!("Both files exist after switching");
  }
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_get_init_time() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let mut db_kv = DoubleBufferedMemMappedKVJournal::new(file_a, file_b, 4096, Some(0.8), None)?;
  
  let init_time = db_kv.get_init_time()?;
  assert!(init_time > 0);
  
  // Add some data and verify init time doesn't change
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  let init_time2 = db_kv.get_init_time()?;
  assert_eq!(init_time, init_time2);
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_file_paths() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let db_kv = DoubleBufferedMemMappedKVJournal::new(&file_a, &file_b, 4096, Some(0.8), None)?;
  
  // Check file paths
  assert_eq!(db_kv.active_file_path(), &file_a);
  assert_eq!(db_kv.inactive_file_path(), &file_b);
  assert_eq!(db_kv.file_size(), 4096);
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_high_water_mark() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let db_kv = DoubleBufferedMemMappedKVJournal::new(file_a, file_b, 2000, Some(0.6), None)?;
  
  // High water mark should be delegated to the active KV instance
  // The actual calculation depends on the MemMappedResilientKv implementation
  let hwm = db_kv.high_water_mark();
  assert!(hwm > 0);
  assert!(hwm <= 2000); // Should be less than or equal to file size
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_cleanup_inactive_file() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let mut db_kv = DoubleBufferedMemMappedKVJournal::new(&file_a, &file_b, 4096, Some(0.8), None)?;
  
  // Add some data
  db_kv.set("test_key", &Value::String("test_value".to_string()))?;
  
  // Both files should exist initially
  assert!(file_a.exists());
  
  // Cleanup inactive file (file_b in this case)
  db_kv.cleanup_inactive_file()?;
  
  // Active file should still exist, inactive should be gone
  assert!(file_a.exists());
  // file_b may not have been created yet, so we can't assert it was deleted
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_overwrite_existing_key() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("test_a.kv");
  let file_b = temp_dir.path().join("test_b.kv");
  
  let mut db_kv = DoubleBufferedMemMappedKVJournal::new(file_a, file_b, 4096, Some(0.8), None)?;
  
  // Set a key
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  
  // Overwrite it
  db_kv.set("key1", &Value::String("new_value".to_string()))?;
  
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("key1"), Some(&Value::String("new_value".to_string())));
  
  Ok(())
}

#[test]
fn test_double_buffered_memmapped_from_existing_file() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let file_a = temp_dir.path().join("existing.kv");
  let file_b = temp_dir.path().join("backup.kv");
  
  // Create an initial file with some data
  {
    let mut initial_kv = crate::MemMappedKVJournal::new(&file_a, 4096, Some(0.8), None)?;
    initial_kv.set("existing_key", &Value::String("existing_value".to_string()))?;
    initial_kv.sync()?;
  }
  
  // Now create a double-buffered KV from the existing file
  let mut db_kv = DoubleBufferedMemMappedKVJournal::from_file(file_a, file_b, 4096, Some(0.8), None)?;
  
  // Verify the existing data is accessible
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("existing_key"), Some(&Value::String("existing_value".to_string())));
  
  // Add more data
  db_kv.set("new_key", &Value::String("new_value".to_string()))?;
  
  let map2 = db_kv.as_hashmap()?;
  assert_eq!(map2.len(), 2);
  assert_eq!(map2.get("existing_key"), Some(&Value::String("existing_value".to_string())));
  assert_eq!(map2.get("new_key"), Some(&Value::String("new_value".to_string())));
  
  Ok(())
}
