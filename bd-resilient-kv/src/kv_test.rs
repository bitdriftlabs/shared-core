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
  let mut kv = ResilientKv::new(Box::new(buffer));
  assert_eq!(kv.as_hashmap().len(), 0);
}

#[test]
fn test_set_and_get_string_value() {
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("test_key", &Value::String("test_value".to_string()));

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 1);
  assert_eq!(
    map.get("test_key"),
    Some(&Value::String("test_value".to_string()))
  );
}

#[test]
fn test_set_and_get_integer_value() {
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("number", &Value::Signed(42));

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("number"), Some(&Value::Signed(42)));
}

#[test]
fn test_set_and_get_boolean_value() {
  let buffer = BasicByteBuffer::new(vec![0; 32]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("flag", &Value::Bool(true));

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("flag"), Some(&Value::Bool(true)));
}

#[test]
fn test_set_multiple_values() {
  let buffer = BasicByteBuffer::new(vec![0; 256]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("key1", &Value::String("value1".to_string()));
  kv.set("key2", &Value::Signed(123));
  kv.set("key3", &Value::Bool(false));

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 3);
  assert_eq!(map.get("key1"), Some(&Value::String("value1".to_string())));
  assert_eq!(map.get("key2"), Some(&Value::Signed(123)));
  assert_eq!(map.get("key3"), Some(&Value::Bool(false)));
}

#[test]
fn test_overwrite_existing_key() {
  let buffer = BasicByteBuffer::new(vec![0; 256]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("key", &Value::String("old_value".to_string()));
  kv.set("key", &Value::String("new_value".to_string()));

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 1);
  assert_eq!(map.get("key"), Some(&Value::String("new_value".to_string())));
}

#[test]
fn test_delete_key() {
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("key", &Value::String("value".to_string()));
  kv.delete("key");

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_set_null_value() {
  let buffer = BasicByteBuffer::new(vec![0; 64]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  kv.set("null_key", &Value::Null);

  let map = kv.as_hashmap();
  assert_eq!(map.len(), 0);
}

#[test]
fn test_empty_kv_returns_empty_map() {
  let buffer = BasicByteBuffer::new(vec![0; 32]);
  let mut kv = ResilientKv::new(Box::new(buffer));

  let map = kv.as_hashmap();
  assert!(map.is_empty());
}
