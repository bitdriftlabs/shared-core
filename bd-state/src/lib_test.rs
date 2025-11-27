// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::{InitStrategy, Scope, StateReader, Store};
use bd_resilient_kv::PersistentStoreConfig;
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

struct Setup {
  _dir: TempDir,
  _time_provider: Arc<bd_time::TestTimeProvider>,
  store: Store,
}

impl Setup {
  async fn new() -> Self {
    let temp_dir = tempfile::tempdir().unwrap();
    let time_provider = Arc::new(bd_time::TestTimeProvider::new(
      datetime!(2024-01-01 00:00:00 UTC),
    ));
    let store = Store::persistent(
      temp_dir.path(),
      PersistentStoreConfig::default(),
      time_provider.clone(),
    )
    .await
    .unwrap()
    .store;

    Self {
      _dir: temp_dir,
      _time_provider: time_provider,
      store,
    }
  }
}

#[tokio::test]
async fn clear_scope() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(
      Scope::FeatureFlag,
      "flag1".to_string(),
      "value1".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::FeatureFlag,
      "flag2".to_string(),
      "value2".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::GlobalState,
      "key1".to_string(),
      "global_value".to_string(),
    )
    .await
    .unwrap();

  setup.store.clear(Scope::FeatureFlag).await.unwrap();

  let reader = setup.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
  assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), None);
  assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
}

#[tokio::test]
async fn iter_scope() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(
      Scope::FeatureFlag,
      "flag1".to_string(),
      "value1".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::FeatureFlag,
      "flag2".to_string(),
      "value2".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::GlobalState,
      "key1".to_string(),
      "global_value".to_string(),
    )
    .await
    .unwrap();

  let reader = setup.store.read().await;
  let items: std::collections::HashMap<_, _> = reader
    .iter()
    .filter(|entry| entry.scope == Scope::FeatureFlag)
    .map(|entry| (entry.key.to_string(), entry.value.to_string()))
    .collect();

  assert_eq!(items.len(), 2);
  assert_eq!(items.get("flag1"), Some(&"value1".to_string()));
  assert_eq!(items.get("flag2"), Some(&"value2".to_string()));
  assert_eq!(items.get("key1"), None);
}

#[tokio::test]
async fn iter_empty_scope() {
  let setup = Setup::new().await;

  let reader = setup.store.read().await;
  let count = reader
    .iter()
    .filter(|entry| entry.scope == Scope::FeatureFlag)
    .count();

  assert_eq!(count, 0);
}

#[tokio::test]
async fn large_value() {
  // Use a larger config to accommodate the large value
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));
  let config = PersistentStoreConfig {
    initial_buffer_size: 1024 * 1024,
    max_capacity_bytes: 10 * 1024 * 1024,
    ..Default::default()
  };
  let store = Store::persistent(temp_dir.path(), config, time_provider.clone())
    .await
    .unwrap()
    .store;

  let large_value = "x".repeat(10_000);
  store
    .insert(Scope::FeatureFlag, "large".to_string(), large_value.clone())
    .await
    .unwrap();

  let reader = store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlag, "large"),
    Some(large_value.as_str())
  );
}

#[tokio::test]
async fn ephemeral_scopes_cleared_on_restart() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  // First process: write state and verify snapshot on creation is empty
  {
    let result = Store::persistent(
      temp_dir.path(),
      PersistentStoreConfig::default(),
      time_provider.clone(),
    )
    .await
    .unwrap();
    let store = result.store;
    let prev_snapshot = result.previous_state;

    // First run should have empty snapshot
    assert!(prev_snapshot.is_empty());

    // Insert some values
    store
      .insert(
        Scope::FeatureFlag,
        "flag1".to_string(),
        "value1".to_string(),
      )
      .await
      .unwrap();
    store
      .insert(
        Scope::FeatureFlag,
        "flag2".to_string(),
        "value2".to_string(),
      )
      .await
      .unwrap();
    store
      .insert(
        Scope::GlobalState,
        "key1".to_string(),
        "global_value".to_string(),
      )
      .await
      .unwrap();

    // Verify they're present
    let reader = store.read().await;
    assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some("value1"));
    assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), Some("value2"));
    assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
  }

  // Second process: state should be cleared but snapshot should have previous data
  {
    let result = Store::persistent(
      temp_dir.path(),
      PersistentStoreConfig::default(),
      time_provider.clone(),
    )
    .await
    .unwrap();
    let store = result.store;
    let prev_snapshot = result.previous_state;

    // Snapshot should contain previous process's data
    assert_eq!(
      prev_snapshot
        .iter()
        .filter(|(scope, ..)| *scope == Scope::FeatureFlag)
        .count(),
      2
    );
    assert_eq!(
      prev_snapshot
        .iter()
        .filter(|(scope, ..)| *scope == Scope::GlobalState)
        .count(),
      1
    );
    assert_eq!(
      prev_snapshot
        .get(Scope::FeatureFlag, "flag1")
        .map(|v| v.value.string_value()),
      Some("value1")
    );
    assert_eq!(
      prev_snapshot
        .get(Scope::FeatureFlag, "flag2")
        .map(|v| v.value.string_value()),
      Some("value2")
    );
    assert_eq!(
      prev_snapshot
        .get(Scope::GlobalState, "key1")
        .map(|v| v.value.string_value()),
      Some("global_value")
    );

    // But current store should be empty (ephemeral scopes cleared)
    let reader = store.read().await;
    assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
    assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), None);
    assert_eq!(reader.get(Scope::GlobalState, "key1"), None);
  }
}

#[tokio::test]
async fn fallback_to_in_memory_on_invalid_directory() {
  // Try to create a store with an invalid path (e.g., a file instead of a directory)
  let temp_file = tempfile::NamedTempFile::new().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  let result = Store::persistent_or_fallback(
    temp_file.path(),
    PersistentStoreConfig::default(),
    time_provider,
  )
  .await;
  let store = result.store;

  // Should have fallen back to in-memory
  assert!(result.fallback_occurred);
  assert!(result.data_loss.is_none());
  assert!(result.previous_state.is_empty());

  // Verify in-memory store works correctly
  store
    .insert(
      Scope::FeatureFlag,
      "test_flag".to_string(),
      "test_value".to_string(),
    )
    .await
    .unwrap();

  let reader = store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlag, "test_flag"),
    Some("test_value")
  );
}

#[tokio::test]
async fn from_strategy_in_memory_only() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  let result = Store::from_strategy(
    temp_dir.path(),
    PersistentStoreConfig::default(),
    time_provider,
    InitStrategy::InMemoryOnly,
  )
  .await;

  // Should create an in-memory store
  assert!(!result.fallback_occurred);
  assert!(result.data_loss.is_none());
  assert!(result.previous_state.is_empty());

  // Verify store works
  result
    .store
    .insert(Scope::FeatureFlag, "flag".to_string(), "value".to_string())
    .await
    .unwrap();

  let reader = result.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag"), Some("value"));
}

#[tokio::test]
async fn from_strategy_persistent_with_fallback() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  let result = Store::from_strategy(
    temp_dir.path(),
    PersistentStoreConfig::default(),
    time_provider,
    InitStrategy::PersistentWithFallback,
  )
  .await;

  // Should create a persistent store successfully
  assert!(!result.fallback_occurred);
  assert!(result.data_loss.is_some());
  assert!(result.previous_state.is_empty());

  // Verify store works
  result
    .store
    .insert(Scope::FeatureFlag, "flag".to_string(), "value".to_string())
    .await
    .unwrap();

  let reader = result.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag"), Some("value"));
}

#[tokio::test]
async fn from_strategy_persistent_with_fallback_on_failure() {
  // Use an invalid path to trigger fallback
  let temp_file = tempfile::NamedTempFile::new().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  let result = Store::from_strategy(
    temp_file.path(),
    PersistentStoreConfig::default(),
    time_provider,
    InitStrategy::PersistentWithFallback,
  )
  .await;

  // Should fall back to in-memory
  assert!(result.fallback_occurred);
  assert!(result.data_loss.is_none());
  assert!(result.previous_state.is_empty());

  // Verify in-memory store works
  result
    .store
    .insert(Scope::FeatureFlag, "flag".to_string(), "value".to_string())
    .await
    .unwrap();

  let reader = result.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag"), Some("value"));
}

#[tokio::test]
async fn large_batch_insert_with_rotation_and_restart() {
  // Test that a large batch of entries causing rotation with buffer growth
  // is properly persisted and can be read back after restart
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  // Use a small initial buffer to force rotation with growth
  let config = PersistentStoreConfig {
    initial_buffer_size: 128 * 1024,      // 128KB
    max_capacity_bytes: 10 * 1024 * 1024, // 10MB
    high_water_mark_ratio: 0.7,
  };

  let num_entries = 10_000;
  let expected_keys: Vec<String> = (0 .. num_entries)
    .map(|i| format!("feature_flag_{i}"))
    .collect();

  // First process: insert large batch that will cause rotation
  {
    let store = Store::persistent(temp_dir.path(), config.clone(), time_provider.clone())
      .await
      .unwrap()
      .store;

    // Insert entries as a batch to trigger rotation
    let entries: Vec<(String, String)> = expected_keys
      .iter()
      .map(|key| (key.clone(), format!("value_{key}")))
      .collect();

    store.extend(Scope::FeatureFlag, entries).await.unwrap();

    // Verify all entries are present
    let reader = store.read().await;
    for key in &expected_keys {
      assert_eq!(
        reader.get(Scope::FeatureFlag, key),
        Some(format!("value_{key}").as_str()),
        "Key {key} should be present"
      );
    }

    // Check that buffer grew (should be larger than initial size)
    let current_size = store.inner.read().await.current_buffer_size();
    assert!(
      current_size > config.initial_buffer_size,
      "Buffer should have grown from {} to {}, but it didn't",
      config.initial_buffer_size,
      current_size
    );
  }

  // Second process: restart and verify all data is still there
  {
    let result = Store::persistent(temp_dir.path(), config.clone(), time_provider.clone())
      .await
      .unwrap();
    let store = result.store;

    // Previous state should contain all entries before ephemeral scopes were cleared
    assert_eq!(
      result.previous_state.feature_flags.len(),
      num_entries,
      "Previous state should contain all {num_entries} feature flags",
    );

    // Current store should be empty (ephemeral scopes cleared on restart)
    let reader = store.read().await;
    for key in &expected_keys {
      assert_eq!(
        reader.get(Scope::FeatureFlag, key),
        None,
        "Key {key} should be cleared in current store"
      );
    }

    // Verify buffer size was preserved from the previous run
    let current_size = store.inner.read().await.current_buffer_size();
    assert!(
      current_size > config.initial_buffer_size,
      "Buffer size should be preserved from previous run: {} > {}",
      current_size,
      config.initial_buffer_size
    );
  }
}
