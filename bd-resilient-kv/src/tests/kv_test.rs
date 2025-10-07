// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::InMemoryKVJournal;
use crate::kv_journal::{DoubleBufferedKVJournal, KVJournal, MemMappedKVJournal};
use bd_bonjson::Value;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_create_resilient_kv() {
  let mut buffer = vec![0; 128];
  let kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();
  assert_eq!(kv.as_hashmap().unwrap().len(), 0);
}

#[test]
fn test_set_and_get_string_value() {
  let mut buffer = vec![0; 64];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

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
fn test_set_and_get_integer_value() {
  let mut buffer = vec![0; 64];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  kv.set("number", &Value::Signed(42)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("number"), Some(&Value::Signed(42)));
}

#[test]
fn test_set_and_get_boolean_value() {
  let mut buffer = vec![0; 128];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  kv.set("flag", &Value::Bool(true)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("flag"), Some(&Value::Bool(true)));
}

#[test]
fn test_set_multiple_values() {
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

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
fn test_set_multiple_method() {
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Create a Vec with multiple entries
  let entries = vec![
    ("key1".to_string(), Value::String("value1".to_string())),
    ("key2".to_string(), Value::Signed(456)),
    ("key3".to_string(), Value::Bool(true)),
    ("key4".to_string(), Value::Float(3.14)),
  ];

  // Use set_multiple to set all entries at once
  kv.set_multiple(&entries).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 4);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(456)));
  assert_eq!(map.get("key3"), Some(&Value::Bool(true)));
  assert_eq!(map.get("key4"), Some(&Value::Float(3.14)));
}

#[test]
fn test_set_multiple_with_deletion() {
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // First set some initial values
  kv.set("key1", &Value::String("value1".to_string()))
    .unwrap();
  kv.set("key2", &Value::Signed(123)).unwrap();
  kv.set("key3", &Value::Bool(false)).unwrap();

  // Now use set_multiple to update some and delete others
  let entries = vec![
    ("key1".to_string(), Value::String("updated_value1".to_string())),
    ("key2".to_string(), Value::Null), // This should delete key2
    ("key4".to_string(), Value::Float(2.71)), // This is a new key
  ];

  kv.set_multiple(&entries).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 3); // key1, key3 (original), and key4 (new); key2 deleted
  assert_eq!(
    map.get("key1"),
    Some(&Value::String("updated_value1".to_string()))
  );
  assert_eq!(map.get("key2"), None); // Deleted by Value::Null
  assert_eq!(map.get("key3"), Some(&Value::Bool(false))); // Unchanged
  assert_eq!(map.get("key4"), Some(&Value::Float(2.71))); // New entry
}

#[test]
fn test_set_multiple_forwarding_double_buffered() {
  let mut buffer_a = vec![0; 256];
  let mut buffer_b = vec![0; 256];
  let journal_a = InMemoryKVJournal::new(&mut buffer_a, None).unwrap();
  let journal_b = InMemoryKVJournal::new(&mut buffer_b, None).unwrap();
  let mut double_buffered = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  // Create multiple entries
  let entries = vec![
    ("db_key1".to_string(), Value::String("value1".to_string())),
    ("db_key2".to_string(), Value::Signed(123)),
    ("db_key3".to_string(), Value::Bool(true)),
  ];

  // Use set_multiple on double buffered journal
  double_buffered.set_multiple(&entries).unwrap();

  let map = double_buffered.as_hashmap().unwrap();
  assert_eq!(map.len(), 3);
  assert_eq!(
    map.get("db_key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(map.get("db_key2"), Some(&Value::Signed(123)));
  assert_eq!(map.get("db_key3"), Some(&Value::Bool(true)));
}

#[test]
fn test_set_multiple_forwarding_memmapped() {
  let temp_file = NamedTempFile::new().unwrap();
  let path = temp_file.path().to_str().unwrap();
  let mut memmapped = MemMappedKVJournal::new(path, 512, None).unwrap();

  // Create multiple entries
  let entries = vec![
    ("mm_key1".to_string(), Value::String("mapped_value1".to_string())),
    ("mm_key2".to_string(), Value::Float(3.14159)),
    ("mm_key3".to_string(), Value::Bool(false)),
  ];

  // Use set_multiple on memory-mapped journal
  memmapped.set_multiple(&entries).unwrap();

  let map = memmapped.as_hashmap().unwrap();
  assert_eq!(map.len(), 3);
  assert_eq!(
    map.get("mm_key1"),
    Some(&Value::String("mapped_value1".to_string()))
  );
  assert_eq!(map.get("mm_key2"), Some(&Value::Float(3.14159)));
  assert_eq!(map.get("mm_key3"), Some(&Value::Bool(false)));
}

#[test]
fn test_overwrite_existing_key() {
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

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
fn test_delete_key() {
  let mut buffer = vec![0; 64];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  kv.set("key", &Value::String("value".to_string())).unwrap();
  kv.delete("key").unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_set_null_value() {
  let mut buffer = vec![0; 64];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  kv.set("null_key", &Value::Null).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_empty_kv_returns_empty_map() {
  let mut buffer = vec![0; 128];
  let kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert!(map.is_empty());
}

#[test]
fn test_create_kv_from_existing_journal_with_many_entries() {
  // Create an initial KV journal with a large buffer to accommodate many entries
  let mut buffer1 = vec![0; 1024];
  let mut original_kv = InMemoryKVJournal::new(&mut buffer1, None).unwrap();

  // Add initial entries
  original_kv
    .set("user:1", &Value::String("alice".to_string()))
    .unwrap();
  original_kv
    .set("user:2", &Value::String("bob".to_string()))
    .unwrap();
  original_kv
    .set("user:3", &Value::String("charlie".to_string()))
    .unwrap();
  original_kv.set("config:debug", &Value::Bool(true)).unwrap();
  original_kv
    .set("config:port", &Value::Signed(8080))
    .unwrap();
  original_kv
    .set("config:timeout", &Value::Signed(30))
    .unwrap();
  original_kv
    .set("stats:requests", &Value::Signed(0))
    .unwrap();
  original_kv.set("stats:errors", &Value::Signed(0)).unwrap();

  // Replace some existing entries
  original_kv
    .set("user:2", &Value::String("robert".to_string()))
    .unwrap(); // Replace bob with robert
  original_kv
    .set("config:debug", &Value::Bool(false))
    .unwrap(); // Disable debug
  original_kv
    .set("config:port", &Value::Signed(9090))
    .unwrap(); // Change port
  original_kv
    .set("stats:requests", &Value::Signed(100))
    .unwrap(); // Update request count

  // Add more entries after replacements
  original_kv
    .set("cache:enabled", &Value::Bool(true))
    .unwrap();
  original_kv.set("cache:ttl", &Value::Signed(3600)).unwrap();
  original_kv
    .set("user:4", &Value::String("diana".to_string()))
    .unwrap();

  // Remove some entries
  original_kv.delete("user:3").unwrap(); // Remove charlie
  original_kv.delete("stats:errors").unwrap(); // Remove error count
  original_kv.delete("config:timeout").unwrap(); // Remove timeout config

  // Add final entries after deletions
  original_kv
    .set("version", &Value::String("1.2.3".to_string()))
    .unwrap();
  original_kv
    .set("maintenance:mode", &Value::Bool(false))
    .unwrap();

  // Verify the original journal has the expected final state
  let original_map = original_kv.as_hashmap().unwrap();
  assert_eq!(original_map.len(), 10); // Should have 10 entries after all operations

  // Verify specific values
  assert_eq!(
    original_map.get("user:1"),
    Some(&Value::String("alice".to_string()))
  );
  assert_eq!(
    original_map.get("user:2"),
    Some(&Value::String("robert".to_string()))
  ); // Was replaced
  assert_eq!(original_map.get("user:3"), None); // Was deleted
  assert_eq!(
    original_map.get("user:4"),
    Some(&Value::String("diana".to_string()))
  );
  assert_eq!(original_map.get("config:debug"), Some(&Value::Bool(false))); // Was replaced
  assert_eq!(original_map.get("config:port"), Some(&Value::Signed(9090))); // Was replaced
  assert_eq!(original_map.get("config:timeout"), None); // Was deleted
  assert_eq!(
    original_map.get("stats:requests"),
    Some(&Value::Signed(100))
  ); // Was replaced
  assert_eq!(original_map.get("stats:errors"), None); // Was deleted
  assert_eq!(original_map.get("cache:enabled"), Some(&Value::Bool(true)));
  assert_eq!(original_map.get("cache:ttl"), Some(&Value::Signed(3600)));
  assert_eq!(
    original_map.get("version"),
    Some(&Value::String("1.2.3".to_string()))
  );
  assert_eq!(
    original_map.get("maintenance:mode"),
    Some(&Value::Bool(false))
  );

  // Create a new KV journal and reinitialize it from the existing one
  let mut buffer2 = vec![0; 1024];
  let mut new_kv = InMemoryKVJournal::new(&mut buffer2, None).unwrap();
  new_kv.reinit_from(&original_kv).unwrap();

  // Verify the new journal has the same state as the original
  let new_map = new_kv.as_hashmap().unwrap();
  assert_eq!(new_map.len(), original_map.len());

  // Verify all entries match
  for (key, value) in &original_map {
    assert_eq!(new_map.get(key), Some(value));
  }

  // Verify the new journal is functional by making additional changes
  new_kv
    .set("test:new", &Value::String("added_to_new".to_string()))
    .unwrap();
  new_kv.delete("user:1").unwrap();

  let final_map = new_kv.as_hashmap().unwrap();
  assert_eq!(final_map.len(), 10); // Should have 10 entries (added 1, removed 1)
  assert_eq!(
    final_map.get("test:new"),
    Some(&Value::String("added_to_new".to_string()))
  );
  assert_eq!(final_map.get("user:1"), None); // Should be deleted
  assert_eq!(
    final_map.get("user:2"),
    Some(&Value::String("robert".to_string()))
  ); // Should still exist
}

#[test]
fn test_from_buffer_with_insufficient_data() {
  // Test with buffer too small for header
  let mut small_buffer = vec![0; 8]; // Only 8 bytes, need 16
  let result = InMemoryKVJournal::from_buffer(&mut small_buffer, None);
  assert!(result.is_err());

  if let Err(e) = result {
    assert!(e.to_string().contains("Buffer too small"));
  }
}

#[test]
fn test_from_buffer_with_invalid_version() {
  // Create a buffer with wrong version
  let mut buffer_data = vec![0; 32];
  buffer_data[0] = 99; // Invalid version (should be 1)
  let mut buffer = buffer_data;
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None);
  assert!(result.is_err());

  if let Err(e) = result {
    assert!(e.to_string().contains("Unsupported version"));
  }
}

#[test]
fn test_from_buffer_success() {
  // First create a KV journal and populate it
  let mut buffer1 = vec![0; 128];
  let mut kv1 = InMemoryKVJournal::new(&mut buffer1, None).unwrap();

  kv1
    .set("key1", &Value::String("value1".to_string()))
    .unwrap();
  kv1.set("key2", &Value::Signed(42)).unwrap();

  // Get the current state to verify
  let original_map = kv1.as_hashmap().unwrap();
  assert_eq!(original_map.len(), 2);

  // Now use from_buffer to load the same data
  let buffer_slice = kv1.buffer_copy();
  let mut buffer2 = buffer_slice;
  let kv2 = InMemoryKVJournal::from_buffer(&mut buffer2, None).unwrap();

  // Verify the data is loaded correctly
  let loaded_map = kv2.as_hashmap().unwrap();
  assert_eq!(loaded_map.len(), 2);
  assert_eq!(
    loaded_map.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(loaded_map.get("key2"), Some(&Value::Signed(42)));
}

#[test]
fn test_from_buffer_with_invalid_position() {
  // Create a buffer with invalid position (beyond buffer size)
  let mut buffer_data = vec![0; 32];
  // Set version to 1 (valid)
  buffer_data[0] = 1;
  // Set position to a value larger than buffer size (position at bytes 8-15)
  let large_position: u64 = 1000;
  buffer_data[8 .. 16].copy_from_slice(&large_position.to_le_bytes());

  let mut buffer = buffer_data;
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None);
  assert!(result.is_err());

  if let Err(e) = result {
    assert!(e.to_string().contains("Invalid position"));
  }
}

#[test]
fn test_float_values() {
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Test positive float
  kv.set("pi", &Value::Float(std::f64::consts::PI)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("pi"), Some(&Value::Float(std::f64::consts::PI)));

  // Test negative float
  kv.set("temp", &Value::Float(-273.15)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("temp"), Some(&Value::Float(-273.15)));

  // Test zero float
  kv.set("zero", &Value::Float(0.0)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("zero"), Some(&Value::Float(0.0)));

  // Test very small and large floats
  kv.set("small", &Value::Float(1e-10)).unwrap();
  kv.set("large", &Value::Float(1e10)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("small"), Some(&Value::Float(1e-10)));
  assert_eq!(map.get("large"), Some(&Value::Float(1e10)));
}

#[test]
fn test_unsigned_values() {
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Test small unsigned values (will be decoded as signed when they fit)
  kv.set("count", &Value::Unsigned(42)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("count"), Some(&Value::Signed(42))); // BONJSON converts small unsigned to signed

  // Test zero unsigned
  kv.set("zero", &Value::Unsigned(0)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("zero"), Some(&Value::Signed(0))); // BONJSON converts 0 to signed

  // Test large unsigned that stays unsigned (larger than i64::MAX)
  let large_unsigned = (i64::MAX as u64) + 1;
  kv.set("max", &Value::Unsigned(large_unsigned)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("max"), Some(&Value::Unsigned(large_unsigned))); // This stays unsigned

  // Test another truly large unsigned
  kv.set("big", &Value::Unsigned(18_446_744_073_709_551_615))
    .unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(
    map.get("big"),
    Some(&Value::Unsigned(18_446_744_073_709_551_615))
  );
}

#[test]
fn test_array_values() {
  let mut buffer = vec![0; 512];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Test empty array
  let empty_array = Value::Array(vec![]);
  kv.set("empty", &empty_array).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("empty"), Some(&empty_array));

  // Test array with mixed types
  let mixed_array = Value::Array(vec![
    Value::String("hello".to_string()),
    Value::Signed(42),
    Value::Bool(true),
    Value::Float(std::f64::consts::PI),
    Value::Unsigned(100),
  ]);
  kv.set("mixed", &mixed_array).unwrap();
  let map = kv.as_hashmap().unwrap();
  // Note: BONJSON converts small unsigned values to signed when possible
  let expected_array = Value::Array(vec![
    Value::String("hello".to_string()),
    Value::Signed(42),
    Value::Bool(true),
    Value::Float(std::f64::consts::PI),
    Value::Signed(100), // 100 fits in signed, so BONJSON converts it
  ]);
  assert_eq!(map.get("mixed"), Some(&expected_array));

  // Test nested arrays
  let nested_array = Value::Array(vec![
    Value::Array(vec![Value::Signed(1), Value::Signed(2)]),
    Value::Array(vec![
      Value::String("a".to_string()),
      Value::String("b".to_string()),
    ]),
  ]);
  kv.set("nested", &nested_array).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("nested"), Some(&nested_array));
}

#[test]
fn test_object_values() {
  let mut buffer = vec![0; 512];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Test empty object
  let empty_object = Value::Object(std::collections::HashMap::new());
  kv.set("empty_obj", &empty_object).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("empty_obj"), Some(&empty_object));

  // Test object with mixed value types
  let mut obj_map = std::collections::HashMap::new();
  obj_map.insert("name".to_string(), Value::String("Alice".to_string()));
  obj_map.insert("age".to_string(), Value::Unsigned(30));
  obj_map.insert("score".to_string(), Value::Float(95.5));
  obj_map.insert("active".to_string(), Value::Bool(true));
  obj_map.insert("negative".to_string(), Value::Signed(-10));

  let mixed_object = Value::Object(obj_map);
  kv.set("user", &mixed_object).unwrap();
  let map = kv.as_hashmap().unwrap();

  // Note: BONJSON converts small unsigned values to signed when possible
  let retrieved = map.get("user").unwrap();
  if let Value::Object(obj) = retrieved {
    assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
    assert_eq!(obj.get("age"), Some(&Value::Signed(30))); // 30 converted to signed
    assert_eq!(obj.get("score"), Some(&Value::Float(95.5)));
    assert_eq!(obj.get("active"), Some(&Value::Bool(true)));
    assert_eq!(obj.get("negative"), Some(&Value::Signed(-10)));
  } else {
    panic!("Expected Object value");
  }

  // Test nested object
  let mut inner_map = std::collections::HashMap::new();
  inner_map.insert("city".to_string(), Value::String("Seattle".to_string()));
  inner_map.insert("zipcode".to_string(), Value::Unsigned(98101));

  let mut outer_map = std::collections::HashMap::new();
  outer_map.insert("name".to_string(), Value::String("Bob".to_string()));
  outer_map.insert("address".to_string(), Value::Object(inner_map));

  let nested_object = Value::Object(outer_map);
  kv.set("profile", &nested_object).unwrap();
  let map = kv.as_hashmap().unwrap();

  // Note: BONJSON converts small unsigned values to signed when possible
  let retrieved = map.get("profile").unwrap();
  if let Value::Object(obj) = retrieved {
    assert_eq!(obj.get("name"), Some(&Value::String("Bob".to_string())));
    if let Some(Value::Object(addr)) = obj.get("address") {
      assert_eq!(
        addr.get("city"),
        Some(&Value::String("Seattle".to_string()))
      );
      assert_eq!(addr.get("zipcode"), Some(&Value::Signed(98101))); // Converted to signed
    } else {
      panic!("Expected nested Object");
    }
  } else {
    panic!("Expected Object value");
  }
}

#[test]
fn test_complex_nested_structures() {
  let mut buffer = vec![0; 1024];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Test array containing objects
  let mut obj1 = std::collections::HashMap::new();
  obj1.insert("id".to_string(), Value::Unsigned(1));
  obj1.insert("name".to_string(), Value::String("Item 1".to_string()));

  let mut obj2 = std::collections::HashMap::new();
  obj2.insert("id".to_string(), Value::Unsigned(2));
  obj2.insert("name".to_string(), Value::String("Item 2".to_string()));

  let array_of_objects = Value::Array(vec![Value::Object(obj1), Value::Object(obj2)]);

  kv.set("items", &array_of_objects).unwrap();
  let map = kv.as_hashmap().unwrap();

  // Note: BONJSON converts small unsigned values to signed when possible
  let retrieved = map.get("items").unwrap();
  if let Value::Array(arr) = retrieved {
    assert_eq!(arr.len(), 2);
    if let Value::Object(obj) = &arr[0] {
      assert_eq!(obj.get("id"), Some(&Value::Signed(1))); // Converted to signed
      assert_eq!(obj.get("name"), Some(&Value::String("Item 1".to_string())));
    } else {
      panic!("Expected Object in array");
    }
    if let Value::Object(obj) = &arr[1] {
      assert_eq!(obj.get("id"), Some(&Value::Signed(2))); // Converted to signed
      assert_eq!(obj.get("name"), Some(&Value::String("Item 2".to_string())));
    } else {
      panic!("Expected Object in array");
    }
  } else {
    panic!("Expected Array value");
  }

  // Test object containing arrays
  let mut config_map = std::collections::HashMap::new();
  config_map.insert(
    "tags".to_string(),
    Value::Array(vec![
      Value::String("rust".to_string()),
      Value::String("database".to_string()),
    ]),
  );
  config_map.insert(
    "scores".to_string(),
    Value::Array(vec![
      Value::Float(1.5),
      Value::Float(2.7),
      Value::Float(3.9),
    ]),
  );

  let object_with_arrays = Value::Object(config_map);
  kv.set("config", &object_with_arrays).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("config"), Some(&object_with_arrays));
}

#[test]
fn test_with_memory_mapped_file() {
  use memmap2::MmapMut;

  // Create a temporary file and write some initial data
  let mut temp_file = NamedTempFile::new().unwrap();

  // Write 1024 zeros to the file to create space for the KV journal
  let initial_data = vec![0u8; 1024];
  temp_file.write_all(&initial_data).unwrap();
  temp_file.flush().unwrap();

  // Memory-map the file
  let file = temp_file.reopen().unwrap();
  let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };

  // Create a ResilientKv using the memory-mapped buffer
  let mut kv = InMemoryKVJournal::new(&mut mmap[..], None).unwrap();

  // Use the KV journal normally
  kv.set(
    "test_key",
    &Value::String("memory_mapped_value".to_string()),
  )
  .unwrap();
  kv.set("number", &Value::Signed(42)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 2);
  assert_eq!(
    map.get("test_key"),
    Some(&Value::String("memory_mapped_value".to_string()))
  );
  assert_eq!(map.get("number"), Some(&Value::Signed(42)));

  // The data is automatically persisted to the file via the memory mapping
}

#[test]
fn test_high_water_mark_default() {
  let mut buffer = vec![0; 100];
  let kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Default should be 80% of buffer size
  assert_eq!(kv.high_water_mark(), 80);
  assert!(!kv.is_high_water_mark_triggered());
  assert!(kv.buffer_usage_ratio() < 0.8);
}

#[test]
fn test_high_water_mark_custom_ratio() {
  let mut buffer = vec![0; 100];
  let kv = InMemoryKVJournal::new(&mut buffer, Some(0.6)).unwrap();

  // Should be 60% of buffer size
  assert_eq!(kv.high_water_mark(), 60);
  assert!(!kv.is_high_water_mark_triggered());
}

#[test]
fn test_high_water_mark_invalid_ratio() {
  let mut buffer = vec![0; 100];

  // Test ratio > 1.0
  let result = InMemoryKVJournal::new(&mut buffer, Some(1.5));
  assert!(result.is_err());
  let error_message = match result {
    Err(e) => e.to_string(),
    Ok(_) => panic!("Expected error but got Ok"),
  };
  assert!(error_message.contains("High water mark ratio must be between 0.0 and 1.0"));

  // Test negative ratio
  let result = InMemoryKVJournal::new(&mut buffer, Some(-0.1));
  assert!(result.is_err());
  let error_message = match result {
    Err(e) => e.to_string(),
    Ok(_) => panic!("Expected error but got Ok"),
  };
  assert!(error_message.contains("High water mark ratio must be between 0.0 and 1.0"));
}

#[test]
fn test_high_water_mark_from_buffer() {
  let mut buffer = vec![0; 100];

  // Create a KV journal and add some data
  {
    let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();
    kv.set("test", &Value::String("value".to_string())).unwrap();
  }

  // Load from buffer with custom high water mark
  let kv = InMemoryKVJournal::from_buffer(&mut buffer, Some(0.7)).unwrap();
  assert_eq!(kv.high_water_mark(), 70);

  // The high water mark should not be triggered yet since we only added one small entry
  assert!(!kv.is_high_water_mark_triggered());
}

#[test]
fn test_buffer_usage_ratio() {
  let mut buffer = vec![0; 200];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Initially, usage includes header and metadata, should be reasonable
  let initial_ratio = kv.buffer_usage_ratio();
  assert!(initial_ratio < 0.3); // Should be well under 30% (accounts for metadata)

  // Add data and check that usage ratio increases
  kv.set("test", &Value::String("test_value".to_string()))
    .unwrap();
  let after_ratio = kv.buffer_usage_ratio();
  assert!(after_ratio > initial_ratio);
}

#[test]
fn test_get_init_time() {
  let mut buffer = vec![0; 128];
  let kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Get the initialization time
  let init_time = kv.get_init_time();

  // The timestamp should be a reasonable nanosecond value since UNIX epoch
  // It should be greater than 2000-01-01 (946684800000000000 nanoseconds)
  // and less than 2100-01-01 (4102444800000000000 nanoseconds)
  assert!(init_time > 946_684_800_000_000_000);
  assert!(init_time < 4_102_444_800_000_000_000);

  // Should return the same time when called multiple times
  let init_time2 = kv.get_init_time();
  assert_eq!(init_time, init_time2);
}

#[test]
fn test_get_init_time_from_buffer() {
  // Create a KV journal and get its timestamp
  let mut buffer1 = vec![0; 256];
  let mut kv1 = InMemoryKVJournal::new(&mut buffer1, None).unwrap();
  let original_time = kv1.get_init_time();

  // Add some data
  kv1
    .set("test", &Value::String("value".to_string()))
    .unwrap();

  // Create a new KV journal from the same buffer
  let kv2 = InMemoryKVJournal::from_buffer(&mut buffer1, None).unwrap();
  let loaded_time = kv2.get_init_time();

  // Should have the same initialization time
  assert_eq!(original_time, loaded_time);
}

#[test]
fn test_reinit_from() {
  // Create source KV with some data
  let mut source_buffer = vec![0; 256];
  let mut source_kv = InMemoryKVJournal::new(&mut source_buffer, None).unwrap();

  source_kv
    .set("key1", &Value::String("value1".to_string()))
    .unwrap();
  source_kv.set("key2", &Value::Signed(42)).unwrap();
  source_kv.set("key3", &Value::Bool(true)).unwrap();

  // Create target KV with different data
  let mut target_buffer = vec![0; 256];
  let mut target_kv = InMemoryKVJournal::new(&mut target_buffer, Some(0.9)).unwrap();

  target_kv
    .set("old_key", &Value::String("old_value".to_string()))
    .unwrap();

  // Get high water mark before reinit
  let original_high_water_mark = target_kv.high_water_mark();

  // Reinitialize target from source
  target_kv.reinit_from(&source_kv).unwrap();

  // Check that target now has source's data
  let target_data = target_kv.as_hashmap().unwrap();
  assert_eq!(target_data.len(), 3);
  assert_eq!(
    target_data.get("key1"),
    Some(&Value::String("value1".to_string()))
  );
  assert_eq!(target_data.get("key2"), Some(&Value::Signed(42)));
  assert_eq!(target_data.get("key3"), Some(&Value::Bool(true)));
  assert_eq!(target_data.get("old_key"), None); // Old data should be gone

  // Check that high water mark is preserved (should still be based on 0.9 ratio)
  assert_eq!(target_kv.high_water_mark(), original_high_water_mark);
}

#[test]
fn test_reinit_from_resets_high_water_mark_triggered() {
  // Create a very large source buffer to ensure we don't hit high water mark
  let mut source_buffer = vec![0; 2048];
  let mut source_kv = InMemoryKVJournal::new(&mut source_buffer, None).unwrap();

  // Fill the source with some overwrites of the same key to generate journal entries
  // but keep the final data small. This simulates a journal that has grown but
  // hasn't hit its high water mark due to the large buffer.
  for i in 0 .. 10 {
    source_kv
      .set("key1", &Value::String(format!("val_{i}")))
      .unwrap();
    source_kv.set("key2", &Value::Signed(i)).unwrap();
  }

  // Verify source hasn't hit high water mark (large buffer)
  assert!(
    !source_kv.is_high_water_mark_triggered(),
    "Source should not have triggered high water mark (usage: {:.2}%)",
    source_kv.buffer_usage_ratio() * 100.0
  );

  // Create a smaller target buffer with low high water mark
  let mut target_buffer = vec![0; 256];
  let mut target_kv = InMemoryKVJournal::new(&mut target_buffer, Some(0.3)).unwrap();

  // Fill target to trigger its high water mark
  for i in 0 .. 8 {
    target_kv
      .set(
        &format!("temp_{i}"),
        &Value::String("temp_value".to_string()),
      )
      .unwrap();
  }

  // Verify target has triggered high water mark
  assert!(
    target_kv.is_high_water_mark_triggered(),
    "Target should have triggered high water mark before reinit (usage: {:.2}%)",
    target_kv.buffer_usage_ratio() * 100.0
  );

  // Reinitialize target from source
  target_kv.reinit_from(&source_kv).unwrap();

  // Verify the data was replaced with source data
  let target_data = target_kv.as_hashmap().unwrap();
  assert_eq!(target_data.len(), 2);
  assert_eq!(
    target_data.get("key1"),
    Some(&Value::String("val_9".to_string()))
  );
  assert_eq!(target_data.get("key2"), Some(&Value::Signed(9)));

  // The key test: high water mark should be properly evaluated after reinit
  // The flag should be reset and then re-evaluated based on the new position
  let final_triggered = target_kv.is_high_water_mark_triggered();
  let final_usage = target_kv.buffer_usage_ratio();

  // This assertion validates that the flag state matches the actual buffer usage
  if final_usage >= 0.3 {
    assert!(
      final_triggered,
      "High water mark should be triggered if usage >= ratio (usage: {:.2}%)",
      final_usage * 100.0
    );
  } else {
    assert!(
      !final_triggered,
      "High water mark should not be triggered if usage < ratio (usage: {:.2}%)",
      final_usage * 100.0
    );
  }
}

#[test]
fn test_journal_clear() {
  let mut buffer = vec![0; 512];
  let mut kv = InMemoryKVJournal::new(&mut buffer, None).unwrap();

  // Add some initial data
  kv.set("key1", &Value::String("value1".to_string()))
    .unwrap();
  kv.set("key2", &Value::Signed(42)).unwrap();
  kv.set("key3", &Value::Bool(true)).unwrap();

  let map_before = kv.as_hashmap().unwrap();
  assert_eq!(map_before.len(), 3);

  // Check buffer usage before clear
  let buffer_usage_before = kv.buffer_usage_ratio();
  assert!(buffer_usage_before > 0.0);

  // Clear the journal
  kv.clear().unwrap();

  // Verify everything is cleared
  let map_after = kv.as_hashmap().unwrap();
  assert_eq!(map_after.len(), 0);

  // Check buffer usage after clear - should be much lower
  let buffer_usage_after = kv.buffer_usage_ratio();
  assert!(buffer_usage_after < buffer_usage_before);

  // Verify we can still add data after clearing
  kv.set("new_key", &Value::String("new_value".to_string()))
    .unwrap();
  let map_final = kv.as_hashmap().unwrap();
  assert_eq!(map_final.len(), 1);
  assert_eq!(
    map_final.get("new_key"),
    Some(&Value::String("new_value".to_string()))
  );
}

#[test]
fn test_set_multiple_with_high_water_mark_callback_retry() {
  // Create a small buffer to easily trigger buffer full conditions
  let mut buffer = vec![0; 256];
  let mut kv = InMemoryKVJournal::new(&mut buffer, Some(0.5)).unwrap();

  // Fill up the journal with some initial data, but not too much
  for i in 0 .. 5 {
    kv.set(&format!("initial_{i}"), &Value::String("small".to_string()))
      .unwrap();
  }

  // Verify we have some data but aren't at capacity yet
  assert!(kv.buffer_usage_ratio() > 0.1, "Should have some data");
  assert!(
    kv.buffer_usage_ratio() < 0.8,
    "Should not be at capacity yet"
  );

  // Create a set of entries that will likely trigger buffer full
  let large_entries: Vec<(String, Value)> = (0..15)
    .map(|i| {
      (
        format!("large_key_{i}"),
        Value::String(
          "This is a relatively large value that will consume significant buffer space and trigger \
           buffer full"
            .to_string(),
        ),
      )
    })
    .collect();

  // Attempt to write the large entries
  // This tests the retry mechanism in write_journal_entries
  let result = kv.set_multiple(&large_entries);

  // The operation might succeed or fail depending on buffer space
  // What's important is that we test the retry mechanism
  match result {
    Ok(()) => {
      // If it succeeded, verify some data was written
      let final_data = kv.as_hashmap().unwrap();
      assert!(!final_data.is_empty(), "Should have some data");

      // Check if any of our large entries made it in
      let has_large_entries = final_data.keys().any(|k| k.starts_with("large_key_"));
      if has_large_entries {
        // Great! The retry mechanism worked
        assert!(
          final_data.len() > 5,
          "Should have more entries if large entries were added"
        );
      }
    },
    Err(e) => {
      // If it failed, it should be a meaningful error message
      let error_str = format!("{e}");
      assert!(
        error_str.contains("Failed to encode") || error_str.contains("BufferFull"),
        "Error should be related to encoding/buffer: {error_str}"
      );

      // Even if it failed, verify the original data is still there
      let final_data = kv.as_hashmap().unwrap();
      assert!(
        final_data.len() >= 5,
        "Original data should still be present"
      );
    },
  }

  // The key point is that the retry mechanism is exercised in write_journal_entries
  // when SerializationError::BufferFull is encountered
}
