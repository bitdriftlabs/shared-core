// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::retention::{RetentionHandle, RetentionRegistry};
use crate::versioned_kv_journal::store::PersistentStoreConfig;
use crate::versioned_kv_journal::{TimestampedValue, make_string_value};
use crate::{DataLoss, Scope, UpdateError, VersionedKVStore};
use bd_proto::protos::state::payload::StateValue;
use bd_time::TestTimeProvider;
use rstest::rstest;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::macros::datetime;

#[derive(Debug, Clone, Copy)]
enum StoreMode {
  Persistent,
  InMemory,
}

// Parameterized setup for tests that work with both modes
struct DualModeSetup {
  temp_dir: Option<TempDir>,
  store: VersionedKVStore,
}

impl DualModeSetup {
  async fn new(mode: StoreMode) -> anyhow::Result<Self> {
    let collector = bd_client_stats_store::Collector::default();
    let stats = collector.scope("test");
    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

    let (temp_dir, store) = match mode {
      StoreMode::Persistent => {
        let temp_dir = TempDir::new()?;
        let registry = Arc::new(RetentionRegistry::new(
          bd_runtime::runtime::IntWatch::new_for_testing(2),
        ));
        let (store, _) = VersionedKVStore::new(
          temp_dir.path(),
          "test",
          PersistentStoreConfig {
            initial_buffer_size: 4096,
            ..Default::default()
          },
          time_provider.clone(),
          registry,
          &stats,
        )
        .await?;
        (Some(temp_dir), store)
      },
      StoreMode::InMemory => {
        let store = VersionedKVStore::new_in_memory(
          time_provider.clone(),
          None,
          &stats,
          bd_runtime::runtime::IntWatch::new_for_testing(2),
        );
        (None, store)
      },
    };

    Ok(Self { temp_dir, store })
  }
}

// Setup for persistent-only tests (snapshots, persistence, etc.)
struct Setup {
  temp_dir: TempDir,
  store: VersionedKVStore,
  time_provider: Arc<TestTimeProvider>,
  _retention_handle: RetentionHandle, // Keep handle alive to retain snapshots
}

impl Setup {
  async fn new() -> anyhow::Result<Self> {
    let collector = bd_client_stats_store::Collector::default();
    let stats = collector.scope("test");
    let temp_dir = TempDir::new()?;
    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
    let registry = Arc::new(RetentionRegistry::new(
      bd_runtime::runtime::IntWatch::new_for_testing(10),
    ));
    let retention_handle = registry.create_handle().await; // Retain all snapshots
    retention_handle.update_retention_micros(0);

    let (store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      PersistentStoreConfig {
        initial_buffer_size: 4096,
        ..Default::default()
      },
      time_provider.clone(),
      registry,
      &stats,
    )
    .await?;

    Ok(Self {
      temp_dir,
      store,
      time_provider,
      _retention_handle: retention_handle,
    })
  }

  async fn make_store_from_snapshot_file(
    &self,
    snapshot_path: &std::path::Path,
  ) -> anyhow::Result<VersionedKVStore> {
    let collector = bd_client_stats_store::Collector::default();
    let stats = collector.scope("test");
    // Decompress the snapshot and journal files into the temp directory
    // so we can open them as a store.
    let data = std::fs::read(snapshot_path)?;
    let decompressed_snapshot = decompress_zlib(&data)?;
    std::fs::write(
      self.temp_dir.path().join("snapshot.jrn.0"),
      decompressed_snapshot,
    )?;

    let registry = Arc::new(RetentionRegistry::new(
      bd_runtime::runtime::IntWatch::new_for_testing(2),
    ));
    let (store, data_loss) = VersionedKVStore::new(
      self.temp_dir.path(),
      "snapshot",
      PersistentStoreConfig {
        initial_buffer_size: 4096,
        ..Default::default()
      },
      self.time_provider.clone(),
      registry,
      &stats,
    )
    .await?;
    assert_eq!(data_loss, DataLoss::None);

    Ok(store)
  }
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn empty_store(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let setup = DualModeSetup::new(mode).await?;

  // Should start empty
  assert!(setup.store.is_empty());
  assert_eq!(setup.store.len(), 0);

  if let Some(ref temp_dir) = setup.temp_dir {
    assert!(temp_dir.path().join("test.jrn.0").exists());
  }

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn basic_crud(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Insert some values
  let (ts1, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  let (ts2, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  assert_eq!(setup.store.len(), 2);
  assert!(ts2 >= ts1);

  // Remove a key
  let ts3 = setup
    .store
    .remove(Scope::FeatureFlagExposure, "key1")
    .await?;
  assert!(ts3.is_some());
  assert!(ts3.unwrap().0 >= ts2);

  assert_eq!(setup.store.len(), 1);
  assert!(!setup.store.contains_key(Scope::FeatureFlagExposure, "key1"));
  assert!(setup.store.contains_key(Scope::FeatureFlagExposure, "key2"));

  // Remove non-existent key
  let removed = setup
    .store
    .remove(Scope::FeatureFlagExposure, "nonexistent")
    .await?;
  assert!(removed.is_none());

  // Read back existing key
  let val = setup.store.get(Scope::FeatureFlagExposure, "key2");
  assert_eq!(val, Some(&make_string_value("value2")));

  // Read non-existent key
  let val = setup.store.get(Scope::FeatureFlagExposure, "key1");
  assert_eq!(val, None);

  Ok(())
}

#[tokio::test]
async fn dedupe_noop_preserves_value() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  let (ts1, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  let (_ts2, old_value) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  assert_eq!(old_value, Some(make_string_value("value1")));
  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
      .unwrap()
      .timestamp,
    ts1
  );

  let _ts3 = setup
    .store
    .extend(vec![
      (
        Scope::FeatureFlagExposure,
        "key1".to_string(),
        make_string_value("value1"),
      ),
      (
        Scope::FeatureFlagExposure,
        "missing".to_string(),
        StateValue::default(),
      ),
    ])
    .await?;

  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
      .unwrap()
      .timestamp,
    ts1
  );
  assert_eq!(setup.store.get(Scope::FeatureFlagExposure, "missing"), None);

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn extend_same_key_ordering(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Test 1: Insert then delete same key - final state should be deleted
  let entries1 = vec![
    (
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value"),
    ),
    (
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      StateValue::default(), // deletion
    ),
  ];

  setup.store.extend(entries1).await?;
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key1"),
    None,
    "key1 should be deleted (last operation was delete)"
  );

  // Test 2: Delete then insert same key - final state should be inserted
  let entries2 = vec![
    (
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      StateValue::default(), // deletion (no-op since doesn't exist)
    ),
    (
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value"),
    ),
  ];

  setup.store.extend(entries2).await?;
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key2"),
    Some(&make_string_value("value")),
    "key2 should exist (last operation was insert)"
  );

  // Test 3: Multiple operations on same key
  let entries3 = vec![
    (
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("v1"),
    ),
    (
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("v2"),
    ),
    (
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("v3"),
    ),
  ];

  setup.store.extend(entries3).await?;
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key3"),
    Some(&make_string_value("v3")),
    "key3 should have the last inserted value"
  );

  Ok(())
}


#[allow(clippy::cast_possible_truncation)]
#[tokio::test]
async fn test_automatic_rotation_on_high_water_mark() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(10),
  ));
  let handle = registry.create_handle().await; // Retain snapshots
  handle.update_retention_micros(0);

  // Create a store with a small buffer and aggressive high water mark to trigger rotation
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 1024,     // Small buffer size
      max_capacity_bytes: 10 * 1024, // 10KB max
      high_water_mark_ratio: 0.3,    // Very low high water mark (30%)
    },
    time_provider.clone(),
    registry,
    &stats,
  )
  .await?;

  // Insert initial data
  store
    .insert(
      Scope::FeatureFlagExposure,
      "initial".to_string(),
      make_string_value("value"),
    )
    .await?;

  // Verify we're on generation 0
  let initial_path = store.journal_path().unwrap();
  assert!(initial_path.to_str().unwrap().contains(".jrn.0"));
  assert!(initial_path.exists());

  // Fill the store with enough data to trigger high water mark
  // With a 1024-byte buffer and 30% high water mark, we need to use about 307 bytes
  // Each insert may trigger a rotation, so we'll see multiple rotations
  for i in 0 .. 20 {
    store
      .insert(
        Scope::FeatureFlagExposure,
        format!("key{}", i),
        make_string_value(&format!("value_with_some_length_{}", i)),
      )
      .await?;
  }

  // After filling, the store should have automatically rotated at least once
  let final_path = store.journal_path().unwrap();
  let final_generation = final_path
    .to_str()
    .unwrap()
    .split(".jrn.")
    .last()
    .unwrap()
    .parse::<u64>()
    .unwrap();

  assert!(
    final_generation > 0,
    "Store should have automatically rotated at least once, but generation is: {}",
    final_generation
  );
  assert!(final_path.exists());

  // Verify the old journals were archived
  let snapshot_dir = temp_dir.path().join("snapshots");
  assert!(snapshot_dir.exists());

  // Count snapshots - should be equal to the number of rotations
  let snapshot_count = std::fs::read_dir(&snapshot_dir)?
    .filter_map(Result::ok)
    .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("zz"))
    .count();

  assert_eq!(
    snapshot_count, final_generation as usize,
    "Should have {} compressed snapshots after {} automatic rotations",
    final_generation, final_generation
  );

  // Verify data integrity after automatic rotation
  assert!(store.contains_key(Scope::FeatureFlagExposure, "initial"));
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "initial"),
    Some(&make_string_value("value"))
  );
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "key0"),
    Some(&make_string_value("value_with_some_length_0"))
  );

  // Verify we can continue writing after automatic rotation
  store
    .insert(
      Scope::FeatureFlagExposure,
      "after_rotation".to_string(),
      make_string_value("works"),
    )
    .await?;
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "after_rotation"),
    Some(&make_string_value("works"))
  );

  Ok(())
}


#[tokio::test]
async fn test_in_memory_size_limit() -> anyhow::Result<()> {
  use bd_client_stats_store::test::StatsHelper;
  use std::collections::BTreeMap;

  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create an in-memory store with a small size limit (1KB)
  let mut store = VersionedKVStore::new_in_memory(
    time_provider,
    Some(1024),
    &stats,
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  );

  // Should be able to insert some small values
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  assert_eq!(store.len(), 2);

  // Try to insert a large value that would exceed the limit
  let large_value = make_string_value(&"x".repeat(2000));
  let result = store
    .insert(Scope::FeatureFlagExposure, "large".to_string(), large_value)
    .await;

  // Should fail with capacity error
  assert!(result.is_err());
  let error = result.unwrap_err();
  assert!(matches!(error, UpdateError::CapacityExceeded));

  // Verify the metric was incremented
  collector.assert_counter_eq(
    1,
    "test:kv:capacity_exceeded_unrecoverable",
    BTreeMap::new(),
  );

  // Original data should still be intact
  assert_eq!(store.len(), 2);
  assert!(store.contains_key(Scope::FeatureFlagExposure, "key1"));
  assert!(store.contains_key(Scope::FeatureFlagExposure, "key2"));

  // Should be able to delete to free up space
  store.remove(Scope::FeatureFlagExposure, "key1").await?;
  assert_eq!(store.len(), 1);

  // Now should be able to insert a smaller value
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("value3"),
    )
    .await?;
  assert_eq!(store.len(), 2);

  Ok(())
}

#[tokio::test]
async fn test_in_memory_no_size_limit() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create an in-memory store with no size limit
  let mut store = VersionedKVStore::new_in_memory(
    time_provider,
    None,
    &stats,
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  );

  // Should be able to insert many large values without limit
  for i in 0 .. 100 {
    let large_value = make_string_value(&"x".repeat(1000));
    store
      .insert(Scope::FeatureFlagExposure, format!("key{}", i), large_value)
      .await?;
  }

  assert_eq!(store.len(), 100);

  Ok(())
}

#[tokio::test]
async fn test_in_memory_size_limit_replacement() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create an in-memory store with a small size limit
  let mut store = VersionedKVStore::new_in_memory(
    time_provider,
    Some(1024),
    &stats,
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  );

  // Insert a value
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("small"),
    )
    .await?;

  // Replace with a similar-sized value (should succeed)
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value"),
    )
    .await?;

  assert_eq!(store.len(), 1);
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "key1"),
    Some(&make_string_value("value"))
  );

  // Try to replace with a much larger value that would exceed capacity
  let large_value = make_string_value(&"x".repeat(2000));
  let result = store
    .insert(Scope::FeatureFlagExposure, "key1".to_string(), large_value)
    .await;

  // Should fail
  assert!(result.is_err());

  // Original value should still be intact
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "key1"),
    Some(&make_string_value("value"))
  );

  Ok(())
}


#[tokio::test]
async fn test_persistence_and_reload() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  // Create store and write some data
  let (ts1, ts2) = {
    let (mut store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      PersistentStoreConfig {
        initial_buffer_size: 4096,
        ..Default::default()
      },
      time_provider.clone(),
      registry.clone(),
      &stats,
    )
    .await?;
    let (ts1, _) = store
      .insert(
        Scope::FeatureFlagExposure,
        "key1".to_string(),
        make_string_value("value1"),
      )
      .await?;
    let (ts2, _) = store
      .insert(
        Scope::FeatureFlagExposure,
        "key2".to_string(),
        make_string_value("foo"),
      )
      .await?;
    store.sync()?;

    (ts1, ts2)
  };

  // Reopen and verify data persisted
  {
    let (store, data_loss) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      PersistentStoreConfig {
        initial_buffer_size: 4096,
        ..Default::default()
      },
      time_provider,
      registry,
      &stats,
    )
    .await?;
    assert_eq!(data_loss, DataLoss::None);
    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get_with_timestamp(Scope::FeatureFlagExposure, "key1"),
      Some(&TimestampedValue {
        value: make_string_value("value1"),
        timestamp: ts1,
      })
    );
    assert_eq!(
      store.get_with_timestamp(Scope::FeatureFlagExposure, "key2"),
      Some(&TimestampedValue {
        value: make_string_value("foo"),
        timestamp: ts2,
      })
    );
  }

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn test_null_value_is_deletion(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Insert a value
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  assert!(setup.store.contains_key(Scope::FeatureFlagExposure, "key1"));

  // Insert empty state to delete
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      StateValue::default(),
    )
    .await?;
  assert!(!setup.store.contains_key(Scope::FeatureFlagExposure, "key1"));
  assert_eq!(setup.store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_manual_rotation() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Insert some data
  let (_ts1, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  let (ts2, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  setup
    .store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Manually trigger rotation
  let rotation = setup.store.rotate_journal().await?;

  // Verify archived file exists (compressed)
  assert!(rotation.snapshot_path.exists());

  // Verify active journal still works
  let (ts3, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("value3"),
    )
    .await?;
  assert!(ts3 >= ts2);
  assert_eq!(setup.store.len(), 3);

  // Verify data is intact
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key1"),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key2"),
    Some(&make_string_value("value2"))
  );
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key3"),
    Some(&make_string_value("value3"))
  );

  // Decompress the archive and load it as a Store to verify that it contains the old state.
  let snapshot_store = setup
    .make_store_from_snapshot_file(&rotation.snapshot_path)
    .await?;
  assert_eq!(
    snapshot_store.get(Scope::FeatureFlagExposure, "key1"),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    snapshot_store.get(Scope::FeatureFlagExposure, "key2"),
    Some(&make_string_value("value2"))
  );
  assert_eq!(snapshot_store.len(), 2);

  Ok(())
}

#[tokio::test]
async fn test_rotation_preserves_state() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  let pre_rotation_state = setup.store.as_hashmap().clone();
  let pre_rotation_ts = setup
    .store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate
  setup.store.rotate_journal().await?;

  // Verify state is preserved exactly
  let post_rotation_state = setup.store.as_hashmap();
  assert_eq!(pre_rotation_state, *post_rotation_state);
  assert_eq!(setup.store.len(), 1);

  // Verify we can continue writing
  let (ts_new, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;
  assert!(ts_new >= pre_rotation_ts);
  assert_eq!(setup.store.len(), 2);

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn test_empty_store_operations(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Operations on empty store
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "nonexistent"),
    None
  );
  assert!(
    !setup
      .store
      .contains_key(Scope::FeatureFlagExposure, "nonexistent")
  );
  assert_eq!(
    setup
      .store
      .remove(Scope::FeatureFlagExposure, "nonexistent")
      .await?,
    None
  );
  assert!(setup.store.is_empty());
  assert_eq!(setup.store.len(), 0);

  Ok(())
}

// Persistent-only tests below this point (snapshots, rotation, etc.)

#[tokio::test]
async fn test_timestamp_preservation_during_rotation() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Insert some keys and capture their timestamps
  let (ts1, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Advance time to ensure different timestamps.
  setup.time_provider.advance(10.milliseconds());

  let (ts2, _) = setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  // Verify timestamps are different
  assert_ne!(ts1, ts2, "Timestamps should be different");
  assert!(ts2 > ts1, "Later writes should have later timestamps");

  // Write enough data to trigger rotation
  for i in 0 .. 50 {
    setup
      .store
      .insert(
        Scope::FeatureFlagExposure,
        format!("fill{i}"),
        make_string_value("foo"),
      )
      .await?;
  }

  // Verify that after rotation, the original timestamps are preserved
  let ts1_after = setup
    .store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  let ts2_after = setup
    .store
    .get_with_timestamp(Scope::FeatureFlagExposure, "key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  assert_eq!(
    ts1, ts1_after,
    "key1 timestamp should be preserved during rotation"
  );
  assert_eq!(
    ts2, ts2_after,
    "key2 timestamp should be preserved during rotation"
  );

  // Verify ordering is still correct
  assert!(
    ts2_after > ts1_after,
    "Timestamp ordering should be preserved"
  );

  Ok(())
}

#[tokio::test]
async fn test_multiple_rotations() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  let mut snapshot_paths = Vec::new();

  // Perform multiple rotations
  for i in 0 .. 3 {
    let key = format!("key{}", i);
    let value = make_string_value(&format!("value{}", i));
    setup
      .store
      .insert(Scope::FeatureFlagExposure, key, value)
      .await?;
    let rotation = setup.store.rotate_journal().await?;
    snapshot_paths.push(rotation.snapshot_path.clone());
  }

  // Verify all compressed archives exist
  for snapshot_path in snapshot_paths {
    assert!(
      snapshot_path.exists(),
      "Compressed archive {} should exist",
      snapshot_path.display()
    );
  }

  Ok(())
}

#[tokio::test]
async fn test_rotation_with_retention_registry() -> anyhow::Result<()> {
  use crate::RetentionRegistry;
  use std::sync::Arc;

  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  // Create store with retention registry
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 4096,
      ..Default::default()
    },
    time_provider.clone(),
    registry.clone(),
    &stats,
  )
  .await?;

  // Insert some data
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Advance time so each rotation has a different timestamp
  time_provider.advance(1.seconds());

  // Rotate WITHOUT any retention handles - snapshot should NOT be created
  let rotation1 = store.rotate_journal().await?;
  let snapshot_path1 = rotation1.snapshot_path;

  // Snapshot file should NOT exist because no handles require it
  assert!(
    !snapshot_path1.exists(),
    "Snapshot should not be created when no retention handles exist"
  );

  // Now create a handle that requires retention - use a timestamp far in the past
  // so that any rotation will be newer than the retention requirement
  let handle = registry.create_handle().await;
  handle.update_retention_micros(0);
  handle.update_retention_micros(0); // Retain all data from epoch

  // Insert more data and rotate WITH retention handle - snapshot SHOULD be created
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  // Advance time so each rotation has a different timestamp
  time_provider.advance(1.seconds());

  let rotation2 = store.rotate_journal().await?;
  let snapshot_path2 = rotation2.snapshot_path;

  // Snapshot file SHOULD exist because handle requires retention
  assert!(
    snapshot_path2.exists(),
    "Snapshot should be created when retention handle exists"
  );

  // Drop the handle and rotate again - snapshot should NOT be created
  drop(handle);

  // Give the registry time to clean up the dropped handle
  tokio::time::sleep(std::time::Duration::from_millis(10)).await;

  store
    .insert(
      Scope::FeatureFlagExposure,
      "key3".to_string(),
      make_string_value("value3"),
    )
    .await?;

  // Advance time so each rotation has a different timestamp
  time_provider.advance(1.seconds());

  let rotation3 = store.rotate_journal().await?;
  let snapshot_path3 = rotation3.snapshot_path;

  // After handle is dropped, snapshot should not be created
  assert!(
    !snapshot_path3.exists(),
    "Snapshot should not be created after handle is dropped"
  );

  Ok(())
}

#[tokio::test]
async fn test_multiple_rotations_with_same_timestamp() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  // Use fixed time so all rotations have the same data timestamp
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));
  let handle = registry.create_handle().await; // Retain all snapshots
  handle.update_retention_micros(0);

  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 4096,
      ..Default::default()
    },
    time_provider,
    registry,
    &stats,
  )
  .await?;

  // Insert data once
  store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Perform first rotation
  let rotation1 = store.rotate_journal().await?;
  assert!(
    rotation1.snapshot_path.exists(),
    "First rotation should create snapshot"
  );

  // Perform second rotation WITHOUT inserting new data
  // This means both rotations will have the same max timestamp
  let rotation2 = store.rotate_journal().await?;
  assert!(
    rotation2.snapshot_path.exists(),
    "Second rotation should create snapshot"
  );

  // Verify both snapshots exist with different filenames (due to different generations)
  assert!(
    rotation1.snapshot_path.exists(),
    "First snapshot should still exist"
  );
  assert!(
    rotation2.snapshot_path.exists(),
    "Second snapshot should exist"
  );

  // Verify the paths are different (different generations prevent collision)
  assert_ne!(
    rotation1.snapshot_path, rotation2.snapshot_path,
    "Snapshots should have different paths despite same timestamp"
  );

  // Verify we can read both snapshots (they should be different files)
  let snapshot1_data = std::fs::read(&rotation1.snapshot_path)?;
  let snapshot2_data = std::fs::read(&rotation2.snapshot_path)?;

  // The files should exist and be valid
  assert!(
    !snapshot1_data.is_empty(),
    "First snapshot should have data"
  );
  assert!(
    !snapshot2_data.is_empty(),
    "Second snapshot should have data"
  );

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn extend_basic(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Extend multiple entries
  let entries = vec![
    (
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("value1"),
    ),
    (
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("value2"),
    ),
    (
      Scope::GlobalState,
      "key3".to_string(),
      make_string_value("value3"),
    ),
  ];

  let ts = setup.store.extend(entries).await?;

  // Verify all entries were inserted with the same timestamp
  assert_eq!(setup.store.len(), 3);
  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
      .unwrap()
      .timestamp,
    ts
  );
  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::FeatureFlagExposure, "key2")
      .unwrap()
      .timestamp,
    ts
  );
  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::GlobalState, "key3")
      .unwrap()
      .timestamp,
    ts
  );

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn extend_with_updates_and_deletions(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Insert initial data
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("old_value"),
    )
    .await?;
  setup
    .store
    .insert(
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      make_string_value("keep_value"),
    )
    .await?;

  assert_eq!(setup.store.len(), 2);

  // Extend with mix of new inserts, updates, and deletions
  let entries = vec![
    // Update existing key
    (
      Scope::FeatureFlagExposure,
      "key1".to_string(),
      make_string_value("new_value"),
    ),
    // Delete existing key (null value)
    (
      Scope::FeatureFlagExposure,
      "key2".to_string(),
      StateValue::default(),
    ),
    // New insert
    (
      Scope::GlobalState,
      "key3".to_string(),
      make_string_value("value3"),
    ),
  ];

  let ts = setup.store.extend(entries).await?;

  // Verify state
  assert_eq!(setup.store.len(), 2); // key1 and key3 remain
  assert_eq!(
    setup.store.get(Scope::FeatureFlagExposure, "key1"),
    Some(&make_string_value("new_value"))
  );
  assert_eq!(setup.store.get(Scope::FeatureFlagExposure, "key2"), None);
  assert_eq!(
    setup.store.get(Scope::GlobalState, "key3"),
    Some(&make_string_value("value3"))
  );

  // All operations should have the same timestamp
  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::FeatureFlagExposure, "key1")
      .unwrap()
      .timestamp,
    ts
  );
  assert_eq!(
    setup
      .store
      .get_with_timestamp(Scope::GlobalState, "key3")
      .unwrap()
      .timestamp,
    ts
  );

  Ok(())
}

#[rstest]
#[case(StoreMode::Persistent)]
#[case(StoreMode::InMemory)]
#[tokio::test]
async fn extend_empty_is_noop(#[case] mode: StoreMode) -> anyhow::Result<()> {
  let mut setup = DualModeSetup::new(mode).await?;

  // Empty extend should succeed and return a timestamp
  let result = setup.store.extend(vec![]).await;
  assert!(result.is_ok());

  // Store should still be empty
  assert_eq!(setup.store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn extend_triggers_rotation_and_succeeds() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  // Create a small buffer that will trigger rotation, with enough max capacity to succeed
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 512, // Small buffer to trigger rotation
      max_capacity_bytes: 8192, // Large enough to fit the batch after rotation
      high_water_mark_ratio: 0.5,
    },
    time_provider,
    registry,
    &stats,
  )
  .await?;

  let initial_buffer_size = store.current_buffer_size();

  // Insert enough data to trigger rotation
  let mut entries = Vec::new();
  for i in 0 .. 50 {
    entries.push((
      Scope::FeatureFlagExposure,
      format!("key_{i}"),
      make_string_value(&format!("value_{i}")),
    ));
  }

  // This should trigger rotation and succeed
  store.extend(entries).await?;

  // Verify the data was written
  assert_eq!(store.len(), 50);
  for i in 0 .. 50 {
    assert!(store.contains_key(Scope::FeatureFlagExposure, &format!("key_{i}")));
  }

  // Buffer should have grown due to rotation
  assert!(store.current_buffer_size() > initial_buffer_size);

  Ok(())
}

#[tokio::test]
async fn extend_triggers_rotation_but_fails_capacity() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  // Create a store with limited max capacity that won't fit the batch even after rotation
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 512,
      max_capacity_bytes: 1024, // Too small to fit 50 entries
      high_water_mark_ratio: 0.5,
    },
    time_provider,
    registry,
    &stats,
  )
  .await?;

  let initial_len = store.len();

  // Try to insert a batch that's too large for max capacity
  let mut entries = Vec::new();
  for i in 0 .. 50 {
    entries.push((
      Scope::FeatureFlagExposure,
      format!("key_{i}"),
      make_string_value(&format!("value_{i}")),
    ));
  }

  // This should fail with CapacityExceeded even after rotation
  let result = store.extend(entries).await;
  assert!(matches!(result, Err(UpdateError::CapacityExceeded)));

  // Verify atomicity - no partial writes
  assert_eq!(store.len(), initial_len);
  for i in 0 .. 50 {
    assert!(!store.contains_key(Scope::FeatureFlagExposure, &format!("key_{i}")));
  }

  Ok(())
}

#[tokio::test]
async fn extend_atomicity_on_capacity_exceeded() -> anyhow::Result<()> {
  use bd_client_stats_store::test::StatsHelper;
  use std::collections::BTreeMap;

  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  // Create a store with limited capacity
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 256, // Very small buffer
      max_capacity_bytes: 512,  // Limited max capacity
      high_water_mark_ratio: 0.8,
    },
    time_provider,
    registry,
    &stats,
  )
  .await?;

  // Fill up the store
  for i in 0 .. 5 {
    let _ = store
      .insert(
        Scope::FeatureFlagExposure,
        format!("existing_{i}"),
        make_string_value(&format!("value_{i}")),
      )
      .await;
  }

  let initial_len = store.len();

  // Try to batch insert entries that would exceed capacity
  let mut entries = Vec::new();
  for i in 0 .. 20 {
    entries.push((
      Scope::FeatureFlagExposure,
      format!("new_key_{i}"),
      make_string_value(&format!("large_value_{}", "x".repeat(100))),
    ));
  }

  let result = store.extend(entries).await;

  // On failure, the journal write is rolled back so no entries are persisted
  if result.is_err() {
    // If it failed, verify no partial inserts occurred
    assert_eq!(store.len(), initial_len, "No partial writes on failure");
    for i in 0 .. 20 {
      assert_eq!(
        store.get(Scope::FeatureFlagExposure, &format!("new_key_{i}")),
        None,
        "No partial inserts should occur on failure"
      );
    }

    // Verify the capacity exceeded metric was incremented
    collector.assert_counter_eq(
      1,
      "test:kv:capacity_exceeded_unrecoverable",
      BTreeMap::new(),
    );
  }

  Ok(())
}

#[tokio::test]
async fn extend_persistence() -> anyhow::Result<()> {
  let collector = bd_client_stats_store::Collector::default();
  let stats = collector.scope("test");
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  {
    let (mut store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      PersistentStoreConfig {
        initial_buffer_size: 4096,
        ..Default::default()
      },
      time_provider.clone(),
      registry.clone(),
      &stats,
    )
    .await?;

    // Extend multiple entries
    let entries = vec![
      (
        Scope::FeatureFlagExposure,
        "key1".to_string(),
        make_string_value("value1"),
      ),
      (
        Scope::FeatureFlagExposure,
        "key2".to_string(),
        make_string_value("value2"),
      ),
      (
        Scope::GlobalState,
        "key3".to_string(),
        make_string_value("value3"),
      ),
    ];

    store.extend(entries).await?;
    store.sync()?;
  }

  // Reopen and verify data persisted
  let (store, data_loss) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    PersistentStoreConfig {
      initial_buffer_size: 4096,
      ..Default::default()
    },
    time_provider,
    registry,
    &stats,
  )
  .await?;

  assert_eq!(data_loss, DataLoss::None);
  assert_eq!(store.len(), 3);
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "key1"),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    store.get(Scope::FeatureFlagExposure, "key2"),
    Some(&make_string_value("value2"))
  );
  assert_eq!(
    store.get(Scope::GlobalState, "key3"),
    Some(&make_string_value("value3"))
  );

  Ok(())
}


#[tokio::test]
async fn test_buffer_size_preserved_across_restart() -> anyhow::Result<()> {
  // Test that buffer size is preserved when reopening a store after dynamic growth
  let temp_dir = tempfile::tempdir()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(2),
  ));

  // Use a small initial buffer to force growth
  let config = PersistentStoreConfig {
    initial_buffer_size: 128 * 1024,      // 128KB
    max_capacity_bytes: 10 * 1024 * 1024, // 10MB
    high_water_mark_ratio: 0.7,
  };

  let num_entries = 10_000;
  let buffer_size_after_growth;

  // First process: insert many entries to trigger buffer growth
  {
    let (mut store, data_loss) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      config.clone(),
      time_provider.clone(),
      registry.clone(),
      &bd_client_stats_store::Collector::default().scope("test"),
    )
    .await?;

    assert_eq!(data_loss, DataLoss::None);

    // Use extend for efficient bulk insert
    let entries: Vec<_> = (0 .. num_entries)
      .map(|i| {
        (
          Scope::FeatureFlagExposure,
          format!("key_{i}"),
          make_string_value(&format!("value_{i}")),
        )
      })
      .collect();

    store.extend(entries).await?;
    store.sync()?;

    // Verify buffer grew beyond initial size
    buffer_size_after_growth = store.current_buffer_size();
    assert!(
      buffer_size_after_growth > config.initial_buffer_size,
      "Buffer should have grown from {} to {}",
      config.initial_buffer_size,
      buffer_size_after_growth
    );
  }

  // Second process: reopen and verify buffer size is preserved
  {
    let (store, data_loss) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      config.clone(),
      time_provider.clone(),
      registry.clone(),
      &bd_client_stats_store::Collector::default().scope("test"),
    )
    .await?;

    assert_eq!(data_loss, DataLoss::None);

    // Verify all data is still present
    assert_eq!(store.len(), num_entries);

    // Verify buffer size was preserved (not reset to initial_buffer_size)
    let current_buffer_size = store.current_buffer_size();
    assert_eq!(
      current_buffer_size, buffer_size_after_growth,
      "Buffer size should be preserved across restart: expected {}, got {}",
      buffer_size_after_growth, current_buffer_size
    );

    // Sample check: verify some entries are present
    for i in (0 .. num_entries).step_by(1000) {
      assert_eq!(
        store.get(Scope::FeatureFlagExposure, &format!("key_{i}")),
        Some(&make_string_value(&format!("value_{i}")))
      );
    }
  }

  Ok(())
}
