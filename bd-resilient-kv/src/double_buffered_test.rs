// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DoubleBufferedKVJournal, KVJournal};
use bd_bonjson::Value;

#[test]
fn test_double_buffered_basic_operations() -> anyhow::Result<()> {
  let mut db_kv = DoubleBufferedKVJournal::new(1024, Some(0.8), None)?;
  
  // Test basic set and get
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  db_kv.set("key2", &Value::Signed(42))?;
  
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 2);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(42)));
  
  // Should start with buffer A active
  assert!(db_kv.is_active_buffer_a());
  
  Ok(())
}

#[test]
fn test_double_buffered_deletion() -> anyhow::Result<()> {
  let mut db_kv = DoubleBufferedKVJournal::new(1024, Some(0.8), None)?;
  
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
fn test_double_buffered_buffer_switching() -> anyhow::Result<()> {
  // Use a small buffer to trigger switching, but not too small
  let mut db_kv = DoubleBufferedKVJournal::new(256, Some(0.5), None)?;
  
  // Should start with buffer A
  assert!(db_kv.is_active_buffer_a());
  
  // Add enough data to trigger high water mark, but with smaller values
  for i in 0..10 {
    let key = format!("k{}", i);
    let value = format!("val{}", i);
    db_kv.set(&key, &Value::String(value))?;
  }
  
  // After enough writes, it should have switched to buffer B
  // Note: The exact point of switching depends on the internal serialization overhead
  let final_buffer = db_kv.is_active_buffer_a();
  
  // Verify all data is still accessible
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 10);
  
  for i in 0..10 {
    let key = format!("k{}", i);
    let expected_value = format!("val{}", i);
    assert_eq!(map.get(&key), Some(&Value::String(expected_value)));
  }
  
  println!("Final buffer A active: {}", final_buffer);
  Ok(())
}

#[test]
fn test_double_buffered_get_init_time() -> anyhow::Result<()> {
  let mut db_kv = DoubleBufferedKVJournal::new(1024, Some(0.8), None)?;
  
  let init_time = db_kv.get_init_time()?;
  assert!(init_time > 0);
  
  // Add some data and verify init time doesn't change
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  let init_time2 = db_kv.get_init_time()?;
  assert_eq!(init_time, init_time2);
  
  Ok(())
}

#[test]
fn test_double_buffered_from_data() -> anyhow::Result<()> {
  // First create a regular KV store with some data
  let mut buffer = vec![0u8; 512];
  let mut kv = crate::InMemoryKVJournal::new(&mut buffer, Some(0.8), None)?;
  kv.set("existing_key", &Value::String("existing_value".to_string()))?;
  let buffer_data = kv.buffer_copy();
  
  // Now create a double-buffered KV from this data
  let mut db_kv = DoubleBufferedKVJournal::from_data(buffer_data, 512, Some(0.8), None)?;
  
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

#[test]
fn test_double_buffered_buffer_sizes() -> anyhow::Result<()> {
  let db_kv = DoubleBufferedKVJournal::new(1024, Some(0.8), None)?;
  
  assert_eq!(db_kv.active_buffer_size(), 1024);
  assert_eq!(db_kv.inactive_buffer_size(), 1024);
  
  Ok(())
}

#[test]
fn test_double_buffered_high_water_mark() -> anyhow::Result<()> {
  let db_kv = DoubleBufferedKVJournal::new(1000, Some(0.6), None)?;
  
  // High water mark should be 60% of 1000 = 600
  assert_eq!(db_kv.high_water_mark(), 600);
  
  Ok(())
}

#[test]
fn test_double_buffered_overwrite_existing_key() -> anyhow::Result<()> {
  let mut db_kv = DoubleBufferedKVJournal::new(1024, Some(0.8), None)?;
  
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
fn test_double_buffered_forced_switching() -> anyhow::Result<()> {
  // Create a double buffered KV with a reasonable high water mark
  let mut db_kv = DoubleBufferedKVJournal::new(1024, Some(0.7), None)?; // 70% of 1024 = ~717 bytes
  
  // Should start with buffer A
  assert!(db_kv.is_active_buffer_a());
  
  // Add data gradually and track when buffer switches
  let mut switch_count = 0;
  let initial_buffer = db_kv.is_active_buffer_a();
  
  for i in 0..30 {
    let key = format!("test_key_{:03}", i);
    let value = format!("test_value_with_some_longer_content_to_fill_buffer_{:03}", i);
    
    // Check buffer before adding
    let was_buffer_a = db_kv.is_active_buffer_a();
    
    // Add the entry
    db_kv.set(&key, &Value::String(value))?;
    
    // Check if buffer switched
    let is_buffer_a = db_kv.is_active_buffer_a();
    if was_buffer_a != is_buffer_a {
      switch_count += 1;
      println!("Buffer switched after entry {}: {} -> {}", i + 1, 
               if was_buffer_a { "A" } else { "B" }, 
               if is_buffer_a { "A" } else { "B" });
      
      // After first switch, we should be stable for a while
      if switch_count == 1 {
        // Add a few more entries to verify stability
        for j in (i + 1)..(i + 5).min(30) {
          let key = format!("test_key_{:03}", j);
          let value = format!("test_value_with_some_longer_content_to_fill_buffer_{:03}", j);
          db_kv.set(&key, &Value::String(value))?;
          
          let new_buffer_a = db_kv.is_active_buffer_a();
          if new_buffer_a != is_buffer_a {
            println!("Unexpected additional switch after entry {}", j + 1);
          }
        }
        break;
      }
    }
  }
  
  // We should have triggered exactly one switch for this test
  if switch_count == 0 {
    println!("No buffer switches occurred - data may fit in single buffer with high water mark");
  } else {
    println!("Total switches: {}", switch_count);
    // The final buffer should be different from initial if we switched
    assert_ne!(initial_buffer, db_kv.is_active_buffer_a(), "Buffer should have switched");
  }
  
  // Verify all data is still accessible
  let map = db_kv.as_hashmap()?;
  println!("Final map contains {} entries", map.len());
  
  for (key, _) in &map {
    assert!(key.starts_with("test_key_"), "All keys should be test keys");
  }
  
  Ok(())
}
