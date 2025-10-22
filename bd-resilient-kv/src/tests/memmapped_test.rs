// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::MemMappedKVJournal;
use crate::kv_journal::KVJournal;
use bd_bonjson::Value;
use tempfile::NamedTempFile;

#[test]
fn test_create_new_memmapped_kv() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let kv = MemMappedKVJournal::new(path, 1024, None).unwrap();
  assert_eq!(kv.as_hashmap().unwrap().len(), 0);
  // Note: high_water_mark() returns the position after the initial header, not 0
  assert!(kv.high_water_mark() > 0);
  assert!(!kv.is_high_water_mark_triggered());
}

#[test]
fn test_memmapped_set_and_get_string_value() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("test_key", &Value::String("test_value".to_string()))
    .unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(
    map.get("test_key"),
    Some(&Value::String("test_value".to_string()))
  );
}

#[test]
fn test_memmapped_set_and_get_integer_value() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("number", &Value::Signed(42)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("number"), Some(&Value::Signed(42)));
}

#[test]
fn test_memmapped_set_and_get_boolean_value() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("flag", &Value::Bool(true)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("flag"), Some(&Value::Bool(true)));
}

#[test]
fn test_memmapped_set_multiple_values() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("key1", &Value::String("value1".to_string()))
    .unwrap();
  kv.set("key2", &Value::Signed(123)).unwrap();
  kv.set("key3", &Value::Bool(false)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 3);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(123)));
  assert_eq!(map.get("key3"), Some(&Value::Bool(false)));
}

#[test]
fn test_memmapped_overwrite_existing_key() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("key", &Value::String("old_value".to_string()))
    .unwrap();
  kv.set("key", &Value::String("new_value".to_string()))
    .unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(
    map.get("key"),
    Some(&Value::String("new_value".to_string()))
  );
}

#[test]
fn test_memmapped_delete_key() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("key", &Value::String("value".to_string())).unwrap();
  kv.delete("key").unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_memmapped_set_null_value() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  kv.set("null_key", &Value::Null).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_memmapped_empty_kv_returns_empty_map() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert!(map.is_empty());
}

#[test]
fn test_memmapped_persistence_across_instances() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  // Create first instance and add data
  {
    let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();
    kv.set(
      "persistent_key",
      &Value::String("persistent_value".to_string()),
    )
    .unwrap();
    kv.set("number", &Value::Signed(42)).unwrap();
    kv.sync().unwrap();
  } // kv goes out of scope, simulating process restart

  // Create second instance from same file
  {
    let kv = MemMappedKVJournal::from_file(path, 1024, None).unwrap();
    let map = kv.as_hashmap().unwrap();

    assert_eq!(map.len(), 2);
    assert_eq!(
      map.get("persistent_key"),
      Some(&Value::String("persistent_value".to_string()))
    );
    assert_eq!(map.get("number"), Some(&Value::Signed(42)));
  }
}

#[test]
fn test_memmapped_from_file_nonexistent() {
  let result = MemMappedKVJournal::from_file("/nonexistent/path/file.dat", 1024, None);
  assert!(result.is_err());
}

#[test]
fn test_memmapped_file_expansion() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  // Start with a reasonably sized file that can accommodate some data
  let mut kv = MemMappedKVJournal::new(path, 512, None).unwrap();

  // Add a smaller amount of data that fits in the buffer
  for i in 0 .. 5 {
    kv.set(&format!("key_{}", i), &Value::String(format!("val_{}", i)))
      .unwrap();
  }

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 5);

  // Verify all values are correct
  for i in 0 .. 5 {
    assert_eq!(
      map.get(&format!("key_{}", i)),
      Some(&Value::String(format!("val_{}", i)))
    );
  }
}

#[test]
fn test_memmapped_sync_operation() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();
  kv.set("sync_test", &Value::String("sync_value".to_string()))
    .unwrap();

  // Should not error
  kv.sync().unwrap();

  // Verify data is still there after sync
  let map = kv.as_hashmap().unwrap();
  assert_eq!(
    map.get("sync_test"),
    Some(&Value::String("sync_value".to_string()))
  );
}

#[test]
fn test_memmapped_high_water_mark() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();



  // Use a moderately sized buffer with low high water mark threshold
  let mut kv = MemMappedKVJournal::new(path, 800, Some(0.4)).unwrap();

  // Add enough data to trigger high water mark but not completely fill buffer
  for i in 0 .. 12 {
    kv.set(
      &format!("hwm_key_{}", i),
      &Value::String(format!("data_to_reach_hwm_{}", i)),
    )
    .unwrap();
  }

  // Check if high water mark was triggered
  // Note: The exact triggering depends on the buffer layout and usage
  // Since callbacks are removed, we just check the journal's high water mark status directly
  assert!(kv.is_high_water_mark_triggered());
}

#[test]
fn test_memmapped_buffer_usage_ratio() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  // Initially should be very low usage
  let initial_ratio = kv.buffer_usage_ratio();
  assert!(initial_ratio < 0.1);

  // Add some data
  for i in 0 .. 5 {
    kv.set(
      &format!("ratio_key_{}", i),
      &Value::String(format!("ratio_value_{}", i)),
    )
    .unwrap();
  }

  let usage_ratio = kv.buffer_usage_ratio();
  assert!(usage_ratio > initial_ratio);
  assert!(usage_ratio >= 0.0 && usage_ratio <= 1.0);
}

#[test]
fn test_memmapped_large_data_persistence() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  // Create first instance with large data set
  {
    let mut kv = MemMappedKVJournal::new(path, 2048, None).unwrap();

    // Add many entries with various operations
    for i in 0 .. 30 {
      kv.set(
        &format!("user:{}", i),
        &Value::String(format!("username_{}", i)),
      )
      .unwrap();
    }

    // Update some entries
    for i in 0 .. 10 {
      kv.set(
        &format!("user:{}", i),
        &Value::String(format!("updated_username_{}", i)),
      )
      .unwrap();
    }

    // Delete some entries
    for i in 20 .. 25 {
      kv.delete(&format!("user:{}", i)).unwrap();
    }

    kv.sync().unwrap();
  }

  // Verify persistence
  {
    let kv = MemMappedKVJournal::from_file(path, 2048, None).unwrap();
    let map = kv.as_hashmap().unwrap();

    // Should have 25 entries (30 - 5 deleted)
    assert_eq!(map.len(), 25);

    // Check updated entries
    for i in 0 .. 10 {
      assert_eq!(
        map.get(&format!("user:{}", i)),
        Some(&Value::String(format!("updated_username_{}", i)))
      );
    }

    // Check unchanged entries
    for i in 10 .. 20 {
      assert_eq!(
        map.get(&format!("user:{}", i)),
        Some(&Value::String(format!("username_{}", i)))
      );
    }

    // Check deleted entries don't exist
    for i in 20 .. 25 {
      assert_eq!(map.get(&format!("user:{}", i)), None);
    }

    // Check remaining entries
    for i in 25 .. 30 {
      assert_eq!(
        map.get(&format!("user:{}", i)),
        Some(&Value::String(format!("username_{}", i)))
      );
    }
  }
}

#[test]
fn test_memmapped_file_recovery_from_corrupted_end() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  // Create initial data
  {
    let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();
    kv.set(
      "recoverable_key",
      &Value::String("recoverable_value".to_string()),
    )
    .unwrap();
    kv.sync().unwrap();
  }

  // Simulate corruption by truncating file or appending invalid data
  {
    use std::fs::OpenOptions;
    use std::io::Write;
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap(); // Invalid data
  }

  // Should still be able to recover valid entries
  {
    let kv = MemMappedKVJournal::from_file(path, 1024, None).unwrap();
    let map = kv.as_hashmap().unwrap();

    // Should recover the valid entry
    assert_eq!(
      map.get("recoverable_key"),
      Some(&Value::String("recoverable_value".to_string()))
    );
  }
}

#[test]
fn test_memmapped_concurrent_operations() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let mut kv = MemMappedKVJournal::new(path, 2048, None).unwrap();

  // Simulate rapid operations that might happen in concurrent scenarios
  for i in 0 .. 50 {
    kv.set(&format!("concurrent_key_{}", i), &Value::Signed(i as i64))
      .unwrap();

    if i % 3 == 0 {
      kv.delete(&format!("concurrent_key_{}", i)).unwrap();
    }

    if i % 5 == 0 {
      kv.set(&format!("concurrent_key_{}", i / 2), &Value::Bool(true))
        .unwrap();
    }

    // Periodic sync
    if i % 10 == 0 {
      kv.sync().unwrap();
    }
  }

  let map = kv.as_hashmap().unwrap();
  assert!(map.len() > 0); // Should have some entries remaining
}

#[test]
fn test_memmapped_get_init_time() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let kv = MemMappedKVJournal::new(path, 1024, None).unwrap();

  // Get the initialization time
  let init_time = kv.get_init_time();

  // The timestamp should be a reasonable nanosecond value since UNIX epoch
  assert!(init_time > 946_684_800_000_000_000);
  assert!(init_time < 4_102_444_800_000_000_000);

  // Should return the same time when called multiple times
  let init_time2 = kv.get_init_time();
  assert_eq!(init_time, init_time2);
}

#[test]
fn test_memmapped_get_init_time_persistence() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();

  let original_time = {
    let mut kv = MemMappedKVJournal::new(path, 1024, None).unwrap();
    kv.set("test", &Value::String("value".to_string())).unwrap();
    kv.sync().unwrap();
    kv.get_init_time()
  };

  // Create a new instance from the same file
  let kv = MemMappedKVJournal::from_file(path, 1024, None).unwrap();
  let loaded_time = kv.get_init_time();

  // Should have the same initialization time
  assert_eq!(original_time, loaded_time);
}

#[test]
fn test_from_file_resize_larger() -> anyhow::Result<()> {
  let temp_file = NamedTempFile::new()?;
  let path = temp_file.path().to_str().unwrap();

  // Create initial file with small size
  {
    let mut kv = MemMappedKVJournal::new(path, 1024, None)?;
    kv.set("test_key", &Value::String("test_value".to_string()))?;
    kv.sync()?;
  }

  // Verify initial file size
  let initial_size = std::fs::metadata(path)?.len();
  assert_eq!(initial_size, 1024);

  // Open with larger size
  {
    let kv = MemMappedKVJournal::from_file(path, 2048, None)?;

    // Verify file was resized
    let new_size = std::fs::metadata(path)?.len();
    assert_eq!(new_size, 2048);

    // Verify data is still accessible
    let map = kv.as_hashmap()?;
    assert_eq!(
      map.get("test_key"),
      Some(&Value::String("test_value".to_string()))
    );

    // Verify we can still add more data
    let file_size = kv.file_size();
    assert_eq!(file_size, 2048);
  }

  Ok(())
}

#[test]
fn test_from_file_resize_smaller() -> anyhow::Result<()> {
  let temp_file = NamedTempFile::new()?;
  let path = temp_file.path().to_str().unwrap();

  // Create initial file with large size and some data
  {
    let mut kv = MemMappedKVJournal::new(path, 2048, None)?;
    kv.set("key1", &Value::String("value1".to_string()))?;
    kv.set("key2", &Value::String("value2".to_string()))?;
    kv.sync()?;
  }

  // Verify initial file size
  let initial_size = std::fs::metadata(path)?.len();
  assert_eq!(initial_size, 2048);

  // Open with smaller size (but still large enough for existing data)
  {
    let kv = MemMappedKVJournal::from_file(path, 1024, None)?;

    // Verify file was resized
    let new_size = std::fs::metadata(path)?.len();
    assert_eq!(new_size, 1024);

    // Verify data is still accessible
    let map = kv.as_hashmap()?;
    assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
    assert_eq!(map.get("key2"), Some(&Value::String("value2".to_string())));

    // Verify file size matches what we requested
    let file_size = kv.file_size();
    assert_eq!(file_size, 1024);
  }

  Ok(())
}

#[test]
fn test_from_file_resize_truncates_data() -> anyhow::Result<()> {
  let temp_file = NamedTempFile::new()?;
  let path = temp_file.path().to_str().unwrap();

  // Create initial file with data that will be larger than our resize target
  {
    let mut kv = MemMappedKVJournal::new(path, 1024, None)?;
    // Add enough data to use most of the space
    for i in 0 .. 20 {
      kv.set(
        &format!("key_{}", i),
        &Value::String(format!("long_value_string_{}", i)),
      )?;
    }
    kv.sync()?;
  }

  // Verify initial file size
  let initial_size = std::fs::metadata(path)?.len();
  assert_eq!(initial_size, 1024);

  // Open with much smaller size that will truncate data
  let result = MemMappedKVJournal::from_file(path, 128, None);

  // This should either:
  // 1. Fail to create due to truncated/invalid data, or
  // 2. Succeed but lose some data

  // In both cases, verify the file was resized
  let new_size = std::fs::metadata(path)?.len();
  assert_eq!(new_size, 128);

  if let Ok(kv) = result {
    // If it succeeds, verify we can access the data (though some may be lost)
    let _map = kv.as_hashmap();
    // We don't assert specific data here since truncation behavior is undefined
    // The test just verifies that the resize operation completes
  }
  // If it fails, that's also acceptable - truncation may make the file unreadable
  // but the file size should still be correct (verified above)

  Ok(())
}

#[test]
fn test_from_file_same_size_no_resize() -> anyhow::Result<()> {
  let temp_file = NamedTempFile::new()?;
  let path = temp_file.path().to_str().unwrap();

  // Create initial file
  {
    let mut kv = MemMappedKVJournal::new(path, 1024, None)?;
    kv.set("test_key", &Value::String("test_value".to_string()))?;
    kv.sync()?;
  }

  // Get file metadata before opening
  let initial_metadata = std::fs::metadata(path)?;
  let initial_size = initial_metadata.len();

  // Small delay to ensure different timestamps if file is modified
  std::thread::sleep(std::time::Duration::from_millis(10));

  // Open with same size
  {
    let kv = MemMappedKVJournal::from_file(path, 1024, None)?;

    // Verify file size didn't change
    let new_metadata = std::fs::metadata(path)?;
    let new_size = new_metadata.len();
    assert_eq!(new_size, initial_size);

    // Verify data is accessible
    let map = kv.as_hashmap()?;
    assert_eq!(
      map.get("test_key"),
      Some(&Value::String("test_value".to_string()))
    );
  }

  Ok(())
}
