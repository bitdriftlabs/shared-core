// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{BasicByteBuffer, ByteBuffer, ResilientKv, ResilientKvError};
use bd_bonjson::Value;

#[test]
fn test_create_resilient_kv() {
  let buffer = BasicByteBuffer::new(vec![0; 32]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();
  assert_eq!(kv.as_hashmap().unwrap().len(), 0);
}

#[test]
fn test_set_and_get_string_value() {
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

  kv.set("number", &Value::Signed(42)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("number"), Some(&Value::Signed(42)));
}

#[test]
fn test_set_and_get_boolean_value() {
  let buffer = BasicByteBuffer::new(vec![0; 32]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

  kv.set("flag", &Value::Bool(true)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("flag"), Some(&Value::Bool(true)));
}

#[test]
fn test_set_multiple_values() {
  let buffer = BasicByteBuffer::new(vec![0; 256]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
fn test_overwrite_existing_key() {
  let buffer = BasicByteBuffer::new(vec![0; 256]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

  kv.set("key", &Value::String("value".to_string())).unwrap();
  kv.delete("key").unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_set_null_value() {
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

  kv.set("null_key", &Value::Null).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_empty_kv_returns_empty_map() {
  let buffer = BasicByteBuffer::new(vec![0; 32]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

  let map = kv.as_hashmap().unwrap();
  assert!(map.is_empty());
}

#[test]
fn test_create_kv_from_existing_store_with_many_entries() {
  // Create an initial KV store with a large buffer to accommodate many entries
  let buffer1 = BasicByteBuffer::new(vec![0; 1024]);
  let mut original_kv = ResilientKv::new(Box::new(buffer1)).unwrap();

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

  // Verify the original store has the expected final state
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

  // Create a new KV store from the existing one
  let buffer2 = BasicByteBuffer::new(vec![0; 1024]);
  let mut new_kv = ResilientKv::from_kv_store(Box::new(buffer2), &mut original_kv).unwrap();

  // Verify the new store has the same state as the original
  let new_map = new_kv.as_hashmap().unwrap();
  assert_eq!(new_map.len(), original_map.len());

  // Verify all entries match
  for (key, value) in &original_map {
    assert_eq!(new_map.get(key), Some(value));
  }

  // Verify the new store is functional by making additional changes
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
  let small_buffer = BasicByteBuffer::new(vec![0; 8]); // Only 8 bytes, need 16
  let result = ResilientKv::from_buffer(Box::new(small_buffer));
  assert!(result.is_err());

  if let Err(e) = result {
    assert!(matches!(e, ResilientKvError::BufferSizeError(_)));
  }
}

#[test]
fn test_from_buffer_with_invalid_version() {
  // Create a buffer with wrong version
  let mut buffer_data = vec![0; 32];
  buffer_data[0] = 99; // Invalid version (should be 1)
  let buffer = BasicByteBuffer::new(buffer_data);
  let result = ResilientKv::from_buffer(Box::new(buffer));
  assert!(result.is_err());

  if let Err(e) = result {
    assert!(matches!(e, ResilientKvError::DecodingError(_)));
  }
}

#[test]
fn test_from_buffer_success() {
  // First create a KV store and populate it
  let buffer1 = BasicByteBuffer::new(vec![0; 128]);
  let mut kv1 = ResilientKv::new(Box::new(buffer1)).unwrap();

  kv1
    .set("key1", &Value::String("value1".to_string()))
    .unwrap();
  kv1.set("key2", &Value::Signed(42)).unwrap();

  // Get the current state to verify
  let original_map = kv1.as_hashmap().unwrap();
  assert_eq!(original_map.len(), 2);

  // Now use from_buffer to load the same data
  let buffer_slice = kv1.buffer.as_slice().to_vec();
  let buffer2 = BasicByteBuffer::new(buffer_slice);
  let mut kv2 = ResilientKv::from_buffer(Box::new(buffer2)).unwrap();

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

  let buffer = BasicByteBuffer::new(buffer_data);
  let result = ResilientKv::from_buffer(Box::new(buffer));
  assert!(result.is_err());

  if let Err(e) = result {
    assert!(matches!(e, ResilientKvError::BufferSizeError(_)));
  }
}

#[test]
fn test_byte_buffer_trait() {
  let data = vec![1, 2, 3, 4, 5];
  let mut buffer = BasicByteBuffer::new(data.clone());

  // Test as_slice
  assert_eq!(buffer.as_slice(), &[1, 2, 3, 4, 5]);

  // Test as_mutable_slice
  {
    let mut_slice = buffer.as_mutable_slice();
    mut_slice[0] = 10;
    mut_slice[4] = 50;
  }

  // Verify the changes
  assert_eq!(buffer.as_slice(), &[10, 2, 3, 4, 50]);
}

#[test]
fn test_float_values() {
  let buffer = BasicByteBuffer::new(vec![0; 256]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

  // Test positive float
  kv.set("pi", &Value::Float(3.14159)).unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("pi"), Some(&Value::Float(3.14159)));

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
  let buffer = BasicByteBuffer::new(vec![0; 256]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
  kv.set("big", &Value::Unsigned(18446744073709551615))
    .unwrap();
  let map = kv.as_hashmap().unwrap();
  assert_eq!(map.get("big"), Some(&Value::Unsigned(18446744073709551615)));
}

#[test]
fn test_array_values() {
  let buffer = BasicByteBuffer::new(vec![0; 512]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
    Value::Float(3.14),
    Value::Unsigned(100),
  ]);
  kv.set("mixed", &mixed_array).unwrap();
  let map = kv.as_hashmap().unwrap();
  // Note: BONJSON converts small unsigned values to signed when possible
  let expected_array = Value::Array(vec![
    Value::String("hello".to_string()),
    Value::Signed(42),
    Value::Bool(true),
    Value::Float(3.14),
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
  let buffer = BasicByteBuffer::new(vec![0; 512]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
  let buffer = BasicByteBuffer::new(vec![0; 1024]);
  let mut kv = ResilientKv::new(Box::new(buffer)).unwrap();

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
