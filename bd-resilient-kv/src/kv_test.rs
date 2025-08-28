// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{BasicByteBuffer, ResilientKv};
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
