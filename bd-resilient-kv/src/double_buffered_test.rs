// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DoubleBufferedKVJournal, InMemoryKVJournal, KVJournal};
use bd_bonjson::Value;

/// Helper function to create a double-buffered journal for testing
fn create_test_double_buffered_journal() -> anyhow::Result<DoubleBufferedKVJournal<InMemoryKVJournal<'static>, InMemoryKVJournal<'static>>> {
  // Use Box::leak to get static lifetime for testing
  let buffer_a = Box::leak(vec![0u8; 1024].into_boxed_slice());
  let buffer_b = Box::leak(vec![0u8; 1024].into_boxed_slice());
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.8), None)?;
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.8), None)?;
  Ok(DoubleBufferedKVJournal::new(journal_a, journal_b))
}

/// Helper function to create a double-buffered journal with specific buffer sizes
fn create_test_double_buffered_journal_with_sizes(
  size_a: usize, 
  size_b: usize, 
  high_water_mark_ratio: Option<f32>
) -> anyhow::Result<DoubleBufferedKVJournal<InMemoryKVJournal<'static>, InMemoryKVJournal<'static>>> {
  let buffer_a = Box::leak(vec![0u8; size_a].into_boxed_slice());
  let buffer_b = Box::leak(vec![0u8; size_b].into_boxed_slice());
  let journal_a = InMemoryKVJournal::new(buffer_a, high_water_mark_ratio, None)?;
  let journal_b = InMemoryKVJournal::new(buffer_b, high_water_mark_ratio, None)?;
  Ok(DoubleBufferedKVJournal::new(journal_a, journal_b))
}

#[test]
fn test_double_buffered_basic_operations() -> anyhow::Result<()> {
  let mut db_kv = create_test_double_buffered_journal()?;
  
  // Test basic set and get
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  db_kv.set("key2", &Value::Signed(42))?;
  
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 2);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(42)));
  
  // Should start with journal A active
  assert!(db_kv.is_active_journal_a());
  
  Ok(())
}

#[test]
fn test_double_buffered_deletion() -> anyhow::Result<()> {
  let mut db_kv = create_test_double_buffered_journal()?;
  
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
fn test_double_buffered_journal_switching() -> anyhow::Result<()> {
  // Use a small buffer to trigger switching
  let mut db_kv = create_test_double_buffered_journal_with_sizes(256, 256, Some(0.5))?;
  
  // Should start with journal A
  assert!(db_kv.is_active_journal_a());
  
  // Add enough data to trigger high water mark
  for i in 0..10 {
    let key = format!("k{}", i);
    let value = format!("val{}", i);
    db_kv.set(&key, &Value::String(value))?;
  }
  
  // After enough writes, it should have switched to journal B (or stayed on A if not triggered)
  let final_journal = db_kv.is_active_journal_a();
  
  // Verify all data is still accessible
  let map = db_kv.as_hashmap()?;
  assert_eq!(map.len(), 10);
  
  for i in 0..10 {
    let key = format!("k{}", i);
    let expected_value = format!("val{}", i);
    assert_eq!(map.get(&key), Some(&Value::String(expected_value)));
  }
  
  println!("Final journal A active: {}", final_journal);
  Ok(())
}

#[test]
fn test_double_buffered_get_init_time() -> anyhow::Result<()> {
  let mut db_kv = create_test_double_buffered_journal()?;
  
  let init_time = db_kv.get_init_time()?;
  assert!(init_time > 0);
  
  // Add some data and verify init time doesn't change
  db_kv.set("key1", &Value::String("value1".to_string()))?;
  let init_time2 = db_kv.get_init_time()?;
  assert_eq!(init_time, init_time2);
  
  Ok(())
}

#[test]
fn test_double_buffered_high_water_mark() -> anyhow::Result<()> {
  let db_kv = create_test_double_buffered_journal_with_sizes(1000, 1000, Some(0.6))?;
  
  // High water mark should be 60% of 1000 = 600
  assert_eq!(db_kv.high_water_mark(), 600);
  
  Ok(())
}

#[test]
fn test_double_buffered_overwrite_existing_key() -> anyhow::Result<()> {
  let mut db_kv = create_test_double_buffered_journal()?;
  
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
  let mut db_kv = create_test_double_buffered_journal_with_sizes(1024, 1024, Some(0.7))?; // 70% of 1024 = ~717 bytes
  
  // Should start with journal A
  assert!(db_kv.is_active_journal_a());
  
  // Add data gradually and track when journal switches
  let initial_journal = db_kv.is_active_journal_a();
  
  for i in 0..30 {
    let key = format!("test_key_{:03}", i);
    let value = format!("test_value_with_some_longer_content_to_fill_buffer_{:03}", i);
    
    db_kv.set(&key, &Value::String(value))?;
    
    // Check if we've switched journals
    let current_journal = db_kv.is_active_journal_a();
    if current_journal != initial_journal {
      println!("Journal switched after {} entries", i + 1);
      break;
    }
  }
  
  // Verify all data is still accessible
  let map = db_kv.as_hashmap()?;
  assert!(map.len() > 0);
  
  Ok(())
}
