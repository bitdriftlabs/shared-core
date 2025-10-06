// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::{Storage, Store};

#[test]
fn test_resilient_kv_storage_with_storage_trait() {
  // Create a temporary directory for the test
  let temp_dir = std::env::temp_dir();
  let test_path = temp_dir.join("test_resilient_kv_storage");

  // Clean up any existing files
  let _ = std::fs::remove_file(test_path.with_extension("jrna"));
  let _ = std::fs::remove_file(test_path.with_extension("jrnb"));

  // Create ResilientKvStorage
  let storage = ResilientKvStorage::new(&test_path, 1024 * 1024, None).unwrap();

  // Create Store with our ResilientKvStorage implementation
  let store = Store::new(Box::new(storage));

  // Test basic key-value operations through the Store interface
  let test_key = crate::Key::new("test_string_key");
  let test_value = "Hello, World!".to_string();

  // Set a value
  store.set(&test_key, &test_value);

  // Get the value back
  let retrieved_value = store.get(&test_key);
  assert_eq!(retrieved_value, Some(test_value));

  // Test with a key that doesn't exist
  let non_existent_key: crate::Key<String> = crate::Key::new("non_existent");
  let non_existent_value = store.get(&non_existent_key);
  assert_eq!(non_existent_value, None);

  // Clean up
  drop(store);
  let _ = std::fs::remove_file(test_path.with_extension("jrna"));
  let _ = std::fs::remove_file(test_path.with_extension("jrnb"));
}

#[test]
fn test_resilient_kv_storage_direct_operations() {
  // Create a temporary directory for the test
  let temp_dir = std::env::temp_dir();
  let test_path = temp_dir.join("test_resilient_kv_direct");

  // Clean up any existing files
  let _ = std::fs::remove_file(test_path.with_extension("jrna"));
  let _ = std::fs::remove_file(test_path.with_extension("jrnb"));

  // Create ResilientKvStorage
  let storage = ResilientKvStorage::new(&test_path, 1024 * 1024, None).unwrap();

  // Test direct Storage trait methods
  let key = "direct_test_key";
  let value = "direct_test_value";

  // Set string
  storage.set_string(key, value).unwrap();

  // Get string
  let retrieved = storage.get_string(key).unwrap();
  assert_eq!(retrieved, Some(value.to_string()));

  // Delete
  storage.delete(key).unwrap();

  // Verify deletion
  let after_delete = storage.get_string(key).unwrap();
  assert_eq!(after_delete, None);

  // Clean up
  let _ = std::fs::remove_file(test_path.with_extension("jrna"));
  let _ = std::fs::remove_file(test_path.with_extension("jrnb"));
}
