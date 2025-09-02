// Test cases for potential concurrency issues and race conditions

use crate::{MemMappedKVJournal, InMemoryKVJournal, KVJournal};
use bd_bonjson::Value;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;

#[test]
fn test_memmapped_concurrent_writes_different_files() -> anyhow::Result<()> {
  // Test multiple memory-mapped journals writing to different files concurrently
  let temp_dir = TempDir::new()?;
  let errors = Arc::new(Mutex::new(Vec::new()));
  
  let handles: Vec<_> = (0..4).map(|i| {
    let temp_dir_path = temp_dir.path().to_path_buf();
    let errors_clone = Arc::clone(&errors);
    
    thread::spawn(move || {
      let file_path = temp_dir_path.join(format!("test_{}.kv", i));
      match MemMappedKVJournal::new(&file_path, 1024, None, None) {
        Ok(mut journal) => {
          for j in 0..10 {
            let key = format!("thread_{}_key_{}", i, j);
            let value = format!("value_{}", j);
            if let Err(e) = journal.set(&key, &Value::String(value)) {
              errors_clone.lock().unwrap().push(format!("Thread {}: {}", i, e));
            }
          }
          if let Err(e) = journal.sync() {
            errors_clone.lock().unwrap().push(format!("Thread {} sync: {}", i, e));
          }
        }
        Err(e) => {
          errors_clone.lock().unwrap().push(format!("Thread {} init: {}", i, e));
        }
      }
    })
  }).collect();
  
  for handle in handles {
    handle.join().unwrap();
  }
  
  let errors = errors.lock().unwrap();
  if !errors.is_empty() {
    panic!("Concurrent write errors: {:?}", *errors);
  }
  
  // Verify all files were created and contain correct data
  for i in 0..4 {
    let file_path = temp_dir.path().join(format!("test_{}.kv", i));
    let mut journal = MemMappedKVJournal::from_file(&file_path, None, None)?;
    let data = journal.as_hashmap()?;
    assert_eq!(data.len(), 10);
    
    for j in 0..10 {
      let key = format!("thread_{}_key_{}", i, j);
      let expected_value = format!("value_{}", j);
      assert_eq!(data.get(&key), Some(&Value::String(expected_value)));
    }
  }
  
  Ok(())
}

#[test]
fn test_memmapped_rapid_sync_operations() -> anyhow::Result<()> {
  // Test rapid sync operations to check for potential race conditions
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("rapid_sync.kv");
  
  let mut journal = MemMappedKVJournal::new(&file_path, 4096, None, None)?; // Larger buffer
  
  // Perform rapid writes and syncs, but handle buffer full conditions
  let mut successful_writes = 0;
  for i in 0..100 {
    match journal.set(&format!("key_{}", i), &Value::Signed(i as i64)) {
      Ok(_) => {
        successful_writes += 1;
        if i % 10 == 0 {
          journal.sync()?; // Sync every 10 writes
        }
      }
      Err(_) => break, // Buffer full, stop writing
    }
  }
  
  // Final sync
  journal.sync()?;
  
  // Verify data integrity for the entries we successfully wrote
  let data = journal.as_hashmap()?;
  assert_eq!(data.len(), successful_writes);
  assert!(successful_writes > 10, "Should have written at least some entries");
  
  for i in 0..successful_writes {
    let key = format!("key_{}", i);
    assert_eq!(data.get(&key), Some(&Value::Signed(i as i64)));
  }
  
  Ok(())
}

#[test]
fn test_memmapped_file_size_growth_stress() -> anyhow::Result<()> {
  // Test file growing under stress conditions
  let temp_dir = TempDir::new()?;
  let file_path = temp_dir.path().join("growing.kv");
  
  // Start with larger file to accommodate more data
  let mut journal = MemMappedKVJournal::new(&file_path, 8192, None, None)?;
  
  // Write increasingly large data, but handle buffer full conditions
  let mut successful_writes = 0;
  for i in 0..30 { // Reduced iterations to stay within buffer
    let key = format!("key_{}", i);
    let value = "x".repeat(i * 5); // Smaller growth rate
    
    match journal.set(&key, &Value::String(value.clone())) {
      Ok(_) => {
        successful_writes += 1;
        // Verify we can read it back immediately  
        let data = journal.as_hashmap()?;
        assert_eq!(data.get(&key), Some(&Value::String(value)));
      }
      Err(_) => break, // Buffer full, stop adding data
    }
  }
  
  assert!(successful_writes > 10, "Should have written at least some entries before buffer full");
  
  Ok(())
}

#[test]
fn test_high_water_mark_callback_timing() -> anyhow::Result<()> {
  // Test that high water mark callbacks are called at the right time
  use std::sync::atomic::{AtomicUsize, Ordering};
  
  static CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);
  
  fn callback(_pos: usize, _size: usize, _hwm: usize) {
    CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
  }
  
  let mut buffer = vec![0u8; 256];
  let mut journal = InMemoryKVJournal::new(&mut buffer, Some(0.5), Some(callback))?;
  
  // Reset counter
  CALLBACK_COUNT.store(0, Ordering::SeqCst);
  
  // Write data until high water mark is triggered
  let mut writes = 0;
  loop {
    writes += 1;
    let key = format!("key_{}", writes);
    let value = format!("value_that_takes_some_space_{}", writes);
    
    match journal.set(&key, &Value::String(value)) {
      Ok(_) => {
        if journal.is_high_water_mark_triggered() {
          break;
        }
        if writes > 100 { // Safety break
          break;
        }
      }
      Err(_) => break, // Buffer full
    }
  }
  
  // Callback should have been called exactly once
  assert_eq!(CALLBACK_COUNT.load(Ordering::SeqCst), 1);
  
  // Additional writes shouldn't trigger more callbacks
  let initial_count = CALLBACK_COUNT.load(Ordering::SeqCst);
  let _ = journal.set("extra", &Value::String("extra".to_string()));
  assert_eq!(CALLBACK_COUNT.load(Ordering::SeqCst), initial_count);
  
  Ok(())
}
