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
    let (store, ..) = Store::new(temp_dir.path(), time_provider.clone())
      .await
      .unwrap();

    Self {
      _dir: temp_dir,
      _time_provider: time_provider,
      store,
    }
  }
}

#[tokio::test]
async fn basic_insert_and_get() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some("value1"));
  assert_eq!(reader.get(Scope::GlobalState, "flag1"), None);
}

#[tokio::test]
async fn insert_multiple_values() {
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

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some("value1"));
  assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), Some("value2"));
  assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
}

#[tokio::test]
async fn update_existing_value() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", "value2".to_string())
    .await
    .unwrap();

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some("value2"));
}

#[tokio::test]
async fn remove_value() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
    .await
    .unwrap();
  setup
    .store
    .remove(Scope::FeatureFlag, "flag1")
    .await
    .unwrap();

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
}

#[tokio::test]
async fn remove_nonexistent_value() {
  let setup = Setup::new().await;

  setup
    .store
    .remove(Scope::FeatureFlag, "nonexistent")
    .await
    .unwrap();

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "nonexistent"), None);
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

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
  assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), None);
  assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
}

#[tokio::test]
async fn scope_isolation() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(Scope::FeatureFlag, "key", "flag_value".to_string())
    .await
    .unwrap();
  setup
    .store
    .insert(Scope::GlobalState, "key", "global_value".to_string())
    .await
    .unwrap();

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "key"), Some("flag_value"));
  assert_eq!(reader.get(Scope::GlobalState, "key"), Some("global_value"));
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

  let items: std::collections::HashMap<_, _> = setup
    .store
    .iter_scope(Scope::FeatureFlag)
    .await
    .into_iter()
    .collect();

  assert_eq!(items.len(), 2);
  assert_eq!(items.get("flag1"), Some(&"value1".to_string()));
  assert_eq!(items.get("flag2"), Some(&"value2".to_string()));
  assert_eq!(items.get("key1"), None);
}

#[tokio::test]
async fn iter_empty_scope() {
  let setup = Setup::new().await;

  let count = setup
    .store
    .iter_scope(Scope::FeatureFlag)
    .await
    .into_iter()
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

  let snapshot = setup.store.to_snapshot().await;

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

  let snapshot = setup.store.to_scoped_snapshot(Scope::FeatureFlag).await;

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

  let snapshot = setup.store.to_snapshot().await;

  assert_eq!(snapshot.feature_flags.len(), 0);
  assert_eq!(snapshot.global_state.len(), 0);
}

#[tokio::test]
async fn persistence_across_restart() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  {
    let (store, ..) = Store::new(temp_dir.path(), time_provider.clone())
      .await
      .unwrap();
    store
      .insert(Scope::FeatureFlag, "flag1", "value1".to_string())
      .await
      .unwrap();
    store
      .insert(Scope::GlobalState, "key1", "global_value".to_string())
      .await
      .unwrap();
  }

  let (store, ..) = Store::new(temp_dir.path(), time_provider.clone())
    .await
    .unwrap();

  // After restart, ephemeral scopes are cleared
  let reader = store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
  assert_eq!(reader.get(Scope::GlobalState, "key1"), None);
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

  let reader = setup.store.lock_for_read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlag, "large"),
    Some(large_value.as_str())
  );
}

#[tokio::test]
async fn many_keys() {
  let setup = Setup::new().await;

  for i in 0 .. 100 {
    setup
      .store
      .insert(Scope::FeatureFlag, &format!("flag{i}"), format!("value{i}"))
      .await
      .unwrap();
  }

  let snapshot = setup.store.to_snapshot().await;
  assert_eq!(snapshot.feature_flags.len(), 100);

  for i in 0 .. 100 {
    assert_eq!(
      snapshot
        .feature_flags
        .get(&format!("flag{i}"))
        .map(|(v, _)| v.as_str()),
      Some(format!("value{i}").as_str())
    );
  }
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

  let reader = setup.store.lock_for_read().await;
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

  let reader = setup.store.lock_for_read().await;
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

  let reader = setup.store.lock_for_read().await;
  assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some(""));
}

#[tokio::test]
async fn ephemeral_scopes_cleared_on_restart() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));

  // First process: write state and verify snapshot on creation is empty
  {
    let (store, _, prev_snapshot) = Store::new(temp_dir.path(), time_provider.clone())
      .await
      .unwrap();

    // First run should have empty snapshot
    assert!(prev_snapshot.feature_flags.is_empty());
    assert!(prev_snapshot.global_state.is_empty());

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
    let reader = store.lock_for_read().await;
    assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), Some("value1"));
    assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), Some("value2"));
    assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
  }

  // Second process: state should be cleared but snapshot should have previous data
  {
    let (store, _, prev_snapshot) = Store::new(temp_dir.path(), time_provider.clone())
      .await
      .unwrap();

    // Snapshot should contain previous process's data
    assert_eq!(prev_snapshot.feature_flags.len(), 2);
    assert_eq!(prev_snapshot.global_state.len(), 1);
    assert_eq!(
      prev_snapshot
        .feature_flags
        .get("flag1")
        .map(|(v, _)| v.as_str()),
      Some("value1")
    );
    assert_eq!(
      prev_snapshot
        .feature_flags
        .get("flag2")
        .map(|(v, _)| v.as_str()),
      Some("value2")
    );
    assert_eq!(
      prev_snapshot
        .global_state
        .get("key1")
        .map(|(v, _)| v.as_str()),
      Some("global_value")
    );

    // But current store should be empty (ephemeral scopes cleared)
    let reader = store.lock_for_read().await;
    assert_eq!(reader.get(Scope::FeatureFlag, "flag1"), None);
    assert_eq!(reader.get(Scope::FeatureFlag, "flag2"), None);
    assert_eq!(reader.get(Scope::GlobalState, "key1"), None);

    // Verify snapshot is also empty
    let current_snapshot = store.to_snapshot().await;
    assert!(current_snapshot.feature_flags.is_empty());
    assert!(current_snapshot.global_state.is_empty());
  }
}
