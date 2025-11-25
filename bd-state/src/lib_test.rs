// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::{Scope, StateReader, Store};
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
    let store = Store::persistent(temp_dir.path(), time_provider.clone())
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
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::FeatureFlag, "flag2", "value2".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::GlobalState, "key1", "global_value".to_string())
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
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::FeatureFlag, "flag2", "value2".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::GlobalState, "key1", "global_value".to_string())
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
async fn to_snapshot() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::FeatureFlag, "flag2", "value2".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::GlobalState, "key1", "global_value".to_string())
    .await
    .unwrap();

  let reader = setup.store.read().await;
  let snapshot = reader.to_snapshot();

  assert_eq!(snapshot.feature_flags.len(), 2);
  assert_eq!(snapshot.global_state.len(), 1);
  assert_eq!(
    snapshot.feature_flags.get("flag1").map(|(v, _)| v.as_str()),
    Some("value1")
  );
  assert_eq!(
    snapshot.feature_flags.get("flag2").map(|(v, _)| v.as_str()),
    Some("value2")
  );
  assert_eq!(
    snapshot.global_state.get("key1").map(|(v, _)| v.as_str()),
    Some("global_value")
  );
}

#[tokio::test]
async fn to_scoped_snapshot() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::GlobalState, "key1", "global_value".to_string())
    .await
    .unwrap();

  let reader = setup.store.read().await;
  let snapshot = reader.to_scoped_snapshot(Scope::FeatureFlag);

  assert_eq!(snapshot.len(), 1);
  assert_eq!(
    snapshot.get("flag1").map(|(v, _)| v.as_str()),
    Some("value1")
  );
  assert_eq!(snapshot.get("key1"), None);
}

#[tokio::test]
async fn empty_snapshot() {
  let setup = Setup::new().await;

  let reader = setup.store.read().await;
  let snapshot = reader.to_snapshot();

  assert_eq!(snapshot.feature_flags.len(), 0);
  assert_eq!(snapshot.global_state.len(), 0);
}

#[tokio::test]
async fn large_value() {
  let setup = Setup::new().await;

  let large_value = "x".repeat(10_000);
  setup
    .store
    .insert(Scope::FeatureFlag, "large", large_value.clone())
    .await
    .unwrap();

  let reader = setup.store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlag, "large"),
    Some(large_value.as_str())
  );
}

#[tokio::test]
async fn special_characters_in_keys() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "key:with:colons", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::FeatureFlag, "key/with/slashes", "value2".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::FeatureFlag, "key with spaces", "value3".to_string())
    .await
    .unwrap();

  let reader = setup.store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlag, "key:with:colons"),
    Some("value1")
  );
  assert_eq!(
    reader.get(Scope::FeatureFlag, "key/with/slashes"),
    Some("value2")
  );
  assert_eq!(
    reader.get(Scope::FeatureFlag, "key with spaces"),
    Some("value3")
  );
}

#[tokio::test]
async fn empty_key() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "", "value".to_string())
    .await
    .unwrap();

  let reader = setup.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, ""), Some("value"));
}

#[tokio::test]
async fn empty_value() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", String::new())
    .await
    .unwrap();

  let reader = setup.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some(""));
}

#[tokio::test]
async fn persistence_across_restart() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  {
    let store = Store::persistent(temp_dir.path(), time_provider.clone())
      .await
      .unwrap()
      .store;
    store
      .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
      .await
      .unwrap();
    store
      .insert(Scope::GlobalState, "key1", "global_value".to_string())
      .await
      .unwrap();
  }

  let store = Store::persistent(temp_dir.path(), time_provider.clone())
    .await
    .unwrap()
    .store;

  // After restart, ephemeral scopes are cleared
  let reader = store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
  assert_eq!(reader.get(Scope::GlobalState, "key1"), None);
}

#[tokio::test]
async fn ephemeral_scopes_cleared_on_restart() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  // First process: write state and verify snapshot on creation is empty
  {
    let result = Store::persistent(temp_dir.path(), time_provider.clone())
      .await
      .unwrap();
    let store = result.store;
    let prev_snapshot = result.previous_state;

    // First run should have empty snapshot
    assert!(prev_snapshot.is_empty());

    // Insert some values
    store
      .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
      .await
      .unwrap();
    store
      .insert(Scope::FeatureFlag, "flag2", "value2".to_string())
      .await
      .unwrap();
    store
      .insert(Scope::GlobalState, "key1", "global_value".to_string())
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
    let result = Store::persistent(temp_dir.path(), time_provider.clone())
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

    // Verify snapshot is also empty
    let reader = store.read().await;
    let current_snapshot = reader.to_snapshot();
    assert!(current_snapshot.feature_flags.is_empty());
    assert!(current_snapshot.global_state.is_empty());
  }
}

#[tokio::test]
async fn fallback_to_in_memory_on_invalid_directory() {
  // Try to create a store with an invalid path (e.g., a file instead of a directory)
  let temp_file = tempfile::NamedTempFile::new().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  let result = Store::persistent_or_fallback(temp_file.path(), time_provider).await;
  let store = result.store;

  // Should have fallen back to in-memory
  assert!(result.fallback_occurred);
  assert!(result.data_loss.is_none());
  assert!(result.previous_state.is_empty());

  // Verify in-memory store works correctly
  store
    .insert(Scope::FeatureFlag, "test_flag", "test_value".to_string())
    .await
    .unwrap();

  let reader = store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlag, "test_flag"),
    Some("test_value")
  );
}
