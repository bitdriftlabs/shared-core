// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::retention::{RetentionHandle, RetentionRegistry};
use crate::versioned_kv_journal::{TimestampedValue, make_string_value};
use crate::{Scope, UpdateError, VersionedKVStore};
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
    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

    let (temp_dir, store) = match mode {
      StoreMode::Persistent => {
        let temp_dir = TempDir::new()?;
        let registry = Arc::new(RetentionRegistry::new());
        let (store, _) = VersionedKVStore::new(
          temp_dir.path(),
          "test",
          4096,
          None,
          time_provider.clone(),
          registry,
        )
        .await?;
        (Some(temp_dir), store)
      },
      StoreMode::InMemory => {
        let store = VersionedKVStore::new_in_memory(time_provider.clone(), None);
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
    let temp_dir = TempDir::new()?;
    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
    let registry = Arc::new(RetentionRegistry::new());
    let retention_handle = registry.create_handle().await; // Retain all snapshots

    let (store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      4096,
      None,
      time_provider.clone(),
      registry,
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
    // Decompress the snapshot and journal files into the temp directory
    // so we can open them as a store.
    let data = std::fs::read(snapshot_path)?;
    let decompressed_snapshot = decompress_zlib(&data)?;
    std::fs::write(
      self.temp_dir.path().join("snapshot.jrn.0"),
      decompressed_snapshot,
    )?;

    let registry = Arc::new(RetentionRegistry::new());
    let (store, _) = VersionedKVStore::open_existing(
      self.temp_dir.path(),
      "snapshot",
      4096,
      None,
      self.time_provider.clone(),
      registry,
    )
    .await?;

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

#[tokio::test]
async fn test_in_memory_size_limit() -> anyhow::Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create an in-memory store with a small size limit (1KB)
  let mut store = VersionedKVStore::new_in_memory(time_provider, Some(1024));

  // Should be able to insert some small values
  store
    .insert(
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  store
    .insert(
      Scope::FeatureFlag,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  assert_eq!(store.len(), 2);

  // Try to insert a large value that would exceed the limit
  let large_value = make_string_value(&"x".repeat(2000));
  let result = store
    .insert(Scope::FeatureFlag, "large".to_string(), large_value)
    .await;

  // Should fail with capacity error
  assert!(result.is_err());
  let error = result.unwrap_err();
  assert!(matches!(error, UpdateError::CapacityExceeded));

  // Original data should still be intact
  assert_eq!(store.len(), 2);
  assert!(store.contains_key(Scope::FeatureFlag, "key1"));
  assert!(store.contains_key(Scope::FeatureFlag, "key2"));

  // Should be able to delete to free up space
  store.remove(Scope::FeatureFlag, "key1").await?;
  assert_eq!(store.len(), 1);

  // Now should be able to insert a smaller value
  store
    .insert(
      Scope::FeatureFlag,
      "key3".to_string(),
      make_string_value("value3"),
    )
    .await?;
  assert_eq!(store.len(), 2);

  Ok(())
}

#[tokio::test]
async fn test_in_memory_no_size_limit() -> anyhow::Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create an in-memory store with no size limit
  let mut store = VersionedKVStore::new_in_memory(time_provider, None);

  // Should be able to insert many large values without limit
  for i in 0 .. 100 {
    let large_value = make_string_value(&"x".repeat(1000));
    store
      .insert(Scope::FeatureFlag, format!("key{}", i), large_value)
      .await?;
  }

  assert_eq!(store.len(), 100);

  Ok(())
}

#[tokio::test]
async fn test_in_memory_size_limit_replacement() -> anyhow::Result<()> {
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  // Create an in-memory store with a small size limit
  let mut store = VersionedKVStore::new_in_memory(time_provider, Some(1024));

  // Insert a value
  store
    .insert(
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("small"),
    )
    .await?;

  // Replace with a similar-sized value (should succeed)
  store
    .insert(
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("value"),
    )
    .await?;

  assert_eq!(store.len(), 1);
  assert_eq!(
    store.get(Scope::FeatureFlag, "key1"),
    Some(&make_string_value("value"))
  );

  // Try to replace with a much larger value that would exceed capacity
  let large_value = make_string_value(&"x".repeat(2000));
  let result = store
    .insert(Scope::FeatureFlag, "key1".to_string(), large_value)
    .await;

  // Should fail
  assert!(result.is_err());

  // Original value should still be intact
  assert_eq!(
    store.get(Scope::FeatureFlag, "key1"),
    Some(&make_string_value("value"))
  );

  Ok(())
}


#[tokio::test]
async fn test_persistence_and_reload() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new());

  // Create store and write some data
  let (ts1, ts2) = {
    let (mut store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      4096,
      None,
      time_provider.clone(),
      registry.clone(),
    )
    .await?;
    let ts1 = store
      .insert(
        Scope::FeatureFlag,
        "key1".to_string(),
        make_string_value("value1"),
      )
      .await?;
    let ts2 = store
      .insert(
        Scope::FeatureFlag,
        "key2".to_string(),
        make_string_value("foo"),
      )
      .await?;
    store.sync()?;

    (ts1, ts2)
  };

  // Reopen and verify data persisted
  {
    let (store, _) =
      VersionedKVStore::open_existing(temp_dir.path(), "test", 4096, None, time_provider, registry)
        .await?;
    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get_with_timestamp(Scope::FeatureFlag, "key1"),
      Some(&TimestampedValue {
        value: make_string_value("value1"),
        timestamp: ts1,
      })
    );
    assert_eq!(
      store.get_with_timestamp(Scope::FeatureFlag, "key2"),
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
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  assert!(setup.store.contains_key(Scope::FeatureFlag, "key1"));

  // Insert empty state to delete
  setup
    .store
    .insert(
      Scope::FeatureFlag,
      "key1".to_string(),
      StateValue::default(),
    )
    .await?;
  assert!(!setup.store.contains_key(Scope::FeatureFlag, "key1"));
  assert_eq!(setup.store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_manual_rotation() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Insert some data
  let _ts1 = setup
    .store
    .insert(
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;
  let ts2 = setup
    .store
    .insert(
      Scope::FeatureFlag,
      "key2".to_string(),
      make_string_value("value2"),
    )
    .await?;

  setup
    .store
    .get_with_timestamp(Scope::FeatureFlag, "key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Manually trigger rotation
  let rotation = setup.store.rotate_journal().await?;

  // Verify archived file exists (compressed)
  assert!(rotation.snapshot_path.exists());

  // Verify active journal still works
  let ts3 = setup
    .store
    .insert(
      Scope::FeatureFlag,
      "key3".to_string(),
      make_string_value("value3"),
    )
    .await?;
  assert!(ts3 >= ts2);
  assert_eq!(setup.store.len(), 3);

  // Verify data is intact
  assert_eq!(
    setup.store.get(Scope::FeatureFlag, "key1"),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    setup.store.get(Scope::FeatureFlag, "key2"),
    Some(&make_string_value("value2"))
  );
  assert_eq!(
    setup.store.get(Scope::FeatureFlag, "key3"),
    Some(&make_string_value("value3"))
  );

  // Decompress the archive and load it as a Store to verify that it contains the old state.
  let snapshot_store = setup
    .make_store_from_snapshot_file(&rotation.snapshot_path)
    .await?;
  assert_eq!(
    snapshot_store.get(Scope::FeatureFlag, "key1"),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    snapshot_store.get(Scope::FeatureFlag, "key2"),
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
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  let pre_rotation_state = setup.store.as_hashmap().clone();
  let pre_rotation_ts = setup
    .store
    .get_with_timestamp(Scope::FeatureFlag, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Rotate
  setup.store.rotate_journal().await?;

  // Verify state is preserved exactly
  let post_rotation_state = setup.store.as_hashmap();
  assert_eq!(pre_rotation_state, *post_rotation_state);
  assert_eq!(setup.store.len(), 1);

  // Verify we can continue writing
  let ts_new = setup
    .store
    .insert(
      Scope::FeatureFlag,
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
  assert_eq!(setup.store.get(Scope::FeatureFlag, "nonexistent"), None);
  assert!(!setup.store.contains_key(Scope::FeatureFlag, "nonexistent"));
  assert_eq!(
    setup
      .store
      .remove(Scope::FeatureFlag, "nonexistent")
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
  let ts1 = setup
    .store
    .insert(
      Scope::FeatureFlag,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Advance time to ensure different timestamps.
  setup.time_provider.advance(10.milliseconds());

  let ts2 = setup
    .store
    .insert(
      Scope::FeatureFlag,
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
        Scope::FeatureFlag,
        format!("fill{i}"),
        make_string_value("foo"),
      )
      .await?;
  }

  // Verify that after rotation, the original timestamps are preserved
  let ts1_after = setup
    .store
    .get_with_timestamp(Scope::FeatureFlag, "key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  let ts2_after = setup
    .store
    .get_with_timestamp(Scope::FeatureFlag, "key2")
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
    setup.store.insert(Scope::FeatureFlag, key, value).await?;
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

  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new());

  // Create store with retention registry
  let (mut store, _) = VersionedKVStore::new(
    temp_dir.path(),
    "test",
    4096,
    None,
    time_provider.clone(),
    registry.clone(),
  )
  .await?;

  // Insert some data
  store
    .insert(
      Scope::FeatureFlag,
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
  handle.update_retention_micros(0); // Retain all data from epoch

  // Insert more data and rotate WITH retention handle - snapshot SHOULD be created
  store
    .insert(
      Scope::FeatureFlag,
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
      Scope::FeatureFlag,
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
  let temp_dir = TempDir::new()?;
  // Use fixed time so all rotations have the same data timestamp
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new());
  let _handle = registry.create_handle().await; // Retain all snapshots

  let (mut store, _) =
    VersionedKVStore::new(temp_dir.path(), "test", 4096, None, time_provider, registry).await?;

  // Insert data once
  store
    .insert(
      Scope::FeatureFlag,
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
