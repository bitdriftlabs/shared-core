// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::LazyKVStore;
use bd_bonjson::Value;
use std::path::Path;
use tempfile::TempDir;

/// Helper function to verify that backing files don't exist
fn assert_files_not_exist(base_path: &Path) {
  let file_a = base_path.with_extension("jrna");
  let file_b = base_path.with_extension("jrnb");
  assert!(
    !file_a.exists(),
    "Journal file A should not exist before initialization"
  );
  assert!(
    !file_b.exists(),
    "Journal file B should not exist before initialization"
  );
}

/// Helper function to verify that backing files exist
fn assert_files_exist(base_path: &Path) {
  let file_a = base_path.with_extension("jrna");
  let file_b = base_path.with_extension("jrnb");
  assert!(
    file_a.exists(),
    "Journal file A should exist after initialization"
  );
  assert!(
    file_b.exists(),
    "Journal file B should exist after initialization"
  );
}

/// Helper function to test that a mutable operation triggers initialization
fn test_mutable_operation_triggers_initialization<F>(operation: F) -> anyhow::Result<()>
where
  F: FnOnce(&mut LazyKVStore) -> anyhow::Result<()>,
{
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_lazy_store");

  let mut lazy_store = LazyKVStore::new(&base_path, 4096, None);

  // Verify not initialized yet
  assert_files_not_exist(&base_path);

  // Perform the operation - should trigger initialization
  operation(&mut lazy_store)?;

  // Verify it's now initialized
  assert_files_exist(&base_path);

  Ok(())
}

#[test]
fn test_lazy_kv_store_deferred_initialization() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_lazy_store");

  // Create LazyKVStore but don't call any methods yet
  let _lazy_store = LazyKVStore::new(&base_path, 4096, None);

  // Check that backing files don't exist yet
  assert_files_not_exist(&base_path);

  Ok(())
}

#[test]
fn test_lazy_kv_store_initialization_on_read() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_lazy_store");

  let lazy_store = LazyKVStore::new(&base_path, 4096, None);

  // Verify not initialized yet
  assert_files_not_exist(&base_path);

  // Attempt to read - this should trigger initialization
  let value = lazy_store.get("key1")?;
  assert_eq!(value, None);

  // Now backing files should exist
  assert_files_exist(&base_path);

  Ok(())
}

#[test]
fn test_lazy_kv_store_initialization_on_insert() -> anyhow::Result<()> {
  test_mutable_operation_triggers_initialization(|store| {
    store.insert("key1".to_string(), Value::String("value1".to_string()))?;
    // Verify the data was actually stored
    let value = store.get("key1")?;
    assert_eq!(value, Some(&Value::String("value1".to_string())));
    Ok(())
  })
}

#[test]
fn test_lazy_kv_store_explicit_initialization() -> anyhow::Result<()> {
  test_mutable_operation_triggers_initialization(|store| {
    store.initialize()?;
    // Read operations should work now
    let value = store.get("nonexistent")?;
    assert_eq!(value, None);
    Ok(())
  })
}

#[test]
fn test_lazy_kv_store_initialization_on_remove() -> anyhow::Result<()> {
  test_mutable_operation_triggers_initialization(|store| {
    store.remove("key1")?;
    Ok(())
  })
}

#[test]
fn test_lazy_kv_store_initialization_on_clear() -> anyhow::Result<()> {
  test_mutable_operation_triggers_initialization(|store| store.clear())
}

#[test]
fn test_lazy_kv_store_initialization_on_compress() -> anyhow::Result<()> {
  test_mutable_operation_triggers_initialization(|store| store.compress())
}

#[test]
fn test_lazy_kv_store_initialization_on_insert_multiple() -> anyhow::Result<()> {
  test_mutable_operation_triggers_initialization(|store| {
    let entries = vec![
      ("key1".to_string(), Value::String("value1".to_string())),
      ("key2".to_string(), Value::Signed(42)),
    ];
    store.insert_multiple(&entries)
  })
}

#[test]
fn test_lazy_kv_store_double_initialization() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let base_path = temp_dir.path().join("test_lazy_store");

  let mut lazy_store = LazyKVStore::new(&base_path, 4096, None);

  // Initialize once
  lazy_store.initialize()?;
  assert_files_exist(&base_path);

  // Initialize again - should be a no-op
  lazy_store.initialize()?;
  assert_files_exist(&base_path);

  // Insert data
  lazy_store.insert("key1".to_string(), Value::String("value1".to_string()))?;

  // Calling initialize again shouldn't reset the data
  lazy_store.initialize()?;
  let value = lazy_store.get("key1")?;
  assert_eq!(value, Some(&Value::String("value1".to_string())));

  Ok(())
}
