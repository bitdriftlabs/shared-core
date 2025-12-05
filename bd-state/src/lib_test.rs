// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{InitStrategy, Scope, StateReader, Store};
use bd_client_stats_store::Collector;
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
    let collector = bd_client_stats_store::Collector::default();
    let store = Store::persistent(
      temp_dir.path(),
      PersistentStoreConfig::default(),
      time_provider.clone(),
      &collector.scope("test"),
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
      Scope::FeatureFlagExposure,
      "flag1".to_string(),
      "value1".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
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

  setup.store.clear(Scope::FeatureFlagExposure).await.unwrap();

  let reader = setup.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag1"), None);
  assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag2"), None);
  assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
}

#[tokio::test]
async fn iter_scope() {
  let setup = Setup::new().await;

  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag1".to_string(),
      "value1".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
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
    .filter(|entry| entry.scope == Scope::FeatureFlagExposure)
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
    .filter(|entry| entry.scope == Scope::FeatureFlagExposure)
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
  let store = Store::persistent(
    temp_dir.path(),
    config,
    time_provider.clone(),
    &Collector::default().scope("test"),
  )
  .await
  .unwrap()
  .store;

  let large_value = "x".repeat(10_000);
  store
    .insert(
      Scope::FeatureFlagExposure,
      "large".to_string(),
      large_value.clone(),
    )
    .await
    .unwrap();

  let reader = store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlagExposure, "large"),
    Some(large_value.as_str())
  );
}

#[tokio::test]
async fn ephemeral_scopes_cleared_on_restart() {
  let temp_dir = tempfile::tempdir().unwrap();
  let time_provider = Arc::new(bd_time::TestTimeProvider::new(
    datetime!(2024-01-01 00:00:00 UTC),
  ));
  let scope = Collector::default().scope("test");

  // First process: write state and verify snapshot on creation is empty
  {
    let result = Store::persistent(
      temp_dir.path(),
      PersistentStoreConfig::default(),
      time_provider.clone(),
      &scope,
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
        Scope::FeatureFlagExposure,
        "flag1".to_string(),
        "value1".to_string(),
      )
      .await
      .unwrap();
    store
      .insert(
        Scope::FeatureFlagExposure,
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
    assert_eq!(
      reader.get(Scope::FeatureFlagExposure, "flag1"),
      Some("value1")
    );
    assert_eq!(
      reader.get(Scope::FeatureFlagExposure, "flag2"),
      Some("value2")
    );
    assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
  }

  // Second process: state should be cleared but snapshot should have previous data
  {
    let result = Store::persistent(
      temp_dir.path(),
      PersistentStoreConfig::default(),
      time_provider.clone(),
      &Collector::default().scope("test"),
    )
    .await
    .unwrap();
    let store = result.store;
    let prev_snapshot = result.previous_state;

    // Snapshot should contain previous process's data
    assert_eq!(
      prev_snapshot
        .iter()
        .filter(|(scope, ..)| *scope == Scope::FeatureFlagExposure)
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
        .get(Scope::FeatureFlagExposure, "flag1")
        .map(|v| v.value.string_value()),
      Some("value1")
    );
    assert_eq!(
      prev_snapshot
        .get(Scope::FeatureFlagExposure, "flag2")
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
    assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag1"), None);
    assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag2"), None);
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
    &Collector::default().scope("test"),
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
      Scope::FeatureFlagExposure,
      "test_flag".to_string(),
      "test_value".to_string(),
    )
    .await
    .unwrap();

  let reader = store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlagExposure, "test_flag"),
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
    &Collector::default().scope("test"),
  )
  .await;

  // Should create an in-memory store
  assert!(!result.fallback_occurred);
  assert!(result.data_loss.is_none());
  assert!(result.previous_state.is_empty());

  // Verify store works
  result
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "value".to_string(),
    )
    .await
    .unwrap();

  let reader = result.store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlagExposure, "flag"),
    Some("value")
  );
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
    &Collector::default().scope("test"),
  )
  .await;

  // Should create a persistent store successfully
  assert!(!result.fallback_occurred);
  assert!(result.data_loss.is_some());
  assert!(result.previous_state.is_empty());

  // Verify store works
  result
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "value".to_string(),
    )
    .await
    .unwrap();

  let reader = result.store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlagExposure, "flag"),
    Some("value")
  );
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
    &Collector::default().scope("test"),
  )
  .await;

  // Should fall back to in-memory
  assert!(result.fallback_occurred);
  assert!(result.data_loss.is_none());
  assert!(result.previous_state.is_empty());

  // Verify in-memory store works
  result
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "value".to_string(),
    )
    .await
    .unwrap();

  let reader = result.store.read().await;
  assert_eq!(
    reader.get(Scope::FeatureFlagExposure, "flag"),
    Some("value")
  );
}

#[tokio::test]
async fn insert_returns_inserted_state_change() {
  let setup = Setup::new().await;

  let change = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "new_flag".to_string(),
      "new_value".to_string(),
    )
    .await
    .unwrap();

  assert_eq!(change.scope, Scope::FeatureFlagExposure);
  assert_eq!(change.key, "new_flag");
  assert_eq!(
    change.change_type,
    crate::StateChangeType::Inserted {
      value: "new_value".to_string()
    }
  );
}

#[tokio::test]
async fn insert_returns_updated_state_change() {
  let setup = Setup::new().await;

  // First insert
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "old_value".to_string(),
    )
    .await
    .unwrap();

  // Second insert (update)
  let change = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "new_value".to_string(),
    )
    .await
    .unwrap();

  assert_eq!(change.scope, Scope::FeatureFlagExposure);
  assert_eq!(change.key, "flag");
  assert_eq!(
    change.change_type,
    crate::StateChangeType::Updated {
      old_value: "old_value".to_string(),
      new_value: "new_value".to_string()
    }
  );
}

#[tokio::test]
async fn insert_returns_no_change_when_value_unchanged() {
  let setup = Setup::new().await;

  // First insert
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "same_value".to_string(),
    )
    .await
    .unwrap();

  // Second insert with same value
  let change = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "same_value".to_string(),
    )
    .await
    .unwrap();

  assert_eq!(change.scope, Scope::FeatureFlagExposure);
  assert_eq!(change.key, "flag");
  assert_eq!(change.change_type, crate::StateChangeType::NoChange);
}

#[tokio::test]
async fn remove_returns_removed_state_change() {
  let setup = Setup::new().await;

  // Insert a value first
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag".to_string(),
      "value".to_string(),
    )
    .await
    .unwrap();

  // Remove it
  let change = setup
    .store
    .remove(Scope::FeatureFlagExposure, "flag")
    .await
    .unwrap();

  assert_eq!(change.scope, Scope::FeatureFlagExposure);
  assert_eq!(change.key, "flag");
  assert_eq!(
    change.change_type,
    crate::StateChangeType::Removed {
      old_value: "value".to_string()
    }
  );

  // Verify it's actually removed
  let reader = setup.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag"), None);
}

#[tokio::test]
async fn remove_returns_no_change_for_nonexistent_key() {
  let setup = Setup::new().await;

  let change = setup
    .store
    .remove(Scope::FeatureFlagExposure, "nonexistent")
    .await
    .unwrap();

  assert_eq!(change.scope, Scope::FeatureFlagExposure);
  assert_eq!(change.key, "nonexistent");
  assert_eq!(change.change_type, crate::StateChangeType::NoChange);
}

#[tokio::test]
async fn extend_returns_multiple_state_changes() {
  let setup = Setup::new().await;

  // First insert one value
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "existing".to_string(),
      "old".to_string(),
    )
    .await
    .unwrap();

  // Extend with multiple values, including updating the existing one
  let changes = setup
    .store
    .extend(
      Scope::FeatureFlagExposure,
      vec![
        ("new1".to_string(), "value1".to_string()),
        ("existing".to_string(), "updated".to_string()),
        ("new2".to_string(), "value2".to_string()),
      ],
    )
    .await
    .unwrap();

  assert_eq!(changes.changes.len(), 3);

  // First change should be an insert
  assert_eq!(changes.changes[0].key, "new1");
  assert_eq!(
    changes.changes[0].change_type,
    crate::StateChangeType::Inserted {
      value: "value1".to_string()
    }
  );

  // Second change should be an update
  assert_eq!(changes.changes[1].key, "existing");
  assert_eq!(
    changes.changes[1].change_type,
    crate::StateChangeType::Updated {
      old_value: "old".to_string(),
      new_value: "updated".to_string()
    }
  );

  // Third change should be an insert
  assert_eq!(changes.changes[2].key, "new2");
  assert_eq!(
    changes.changes[2].change_type,
    crate::StateChangeType::Inserted {
      value: "value2".to_string()
    }
  );
}

#[tokio::test]
async fn clear_returns_all_removed_state_changes() {
  let setup = Setup::new().await;

  // Insert multiple values in different scopes
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "flag1".to_string(),
      "value1".to_string(),
    )
    .await
    .unwrap();
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
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

  // Clear FeatureFlag scope
  let changes = setup.store.clear(Scope::FeatureFlagExposure).await.unwrap();

  // Should have 2 removed changes (flag1 and flag2)
  assert_eq!(changes.changes.len(), 2);

  let keys: std::collections::HashSet<_> = changes.changes.iter().map(|c| c.key.as_str()).collect();
  assert!(keys.contains("flag1"));
  assert!(keys.contains("flag2"));

  // All changes should be removals
  for change in &changes.changes {
    assert_eq!(change.scope, Scope::FeatureFlagExposure);
    assert!(matches!(
      change.change_type,
      crate::StateChangeType::Removed { .. }
    ));
  }

  // Verify FeatureFlag scope is cleared but GlobalState remains
  let reader = setup.store.read().await;
  assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag1"), None);
  assert_eq!(reader.get(Scope::FeatureFlagExposure, "flag2"), None);
  assert_eq!(reader.get(Scope::GlobalState, "key1"), Some("global_value"));
}

#[tokio::test]
async fn clear_empty_scope_returns_empty_changes() {
  let setup = Setup::new().await;

  let changes = setup.store.clear(Scope::FeatureFlagExposure).await.unwrap();

  assert_eq!(changes.changes.len(), 0);
}
