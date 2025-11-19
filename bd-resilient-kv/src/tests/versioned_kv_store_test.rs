// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::VersionedKVStore;
use crate::tests::decompress_zlib;
use crate::versioned_kv_journal::{TimestampedValue, make_string_value};
use bd_proto::protos::state::payload::StateValue;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::macros::datetime;

struct Setup {
  temp_dir: TempDir,
  store: VersionedKVStore,
  time_provider: Arc<TestTimeProvider>,
}

impl Setup {
  async fn new() -> anyhow::Result<Self> {
    let temp_dir = TempDir::new()?;
    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

    let (store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      4096,
      None,
      time_provider.clone(),
      None,
    )
    .await?;

    Ok(Self {
      temp_dir,
      store,
      time_provider,
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

    let (store, _) = VersionedKVStore::open_existing(
      self.temp_dir.path(),
      "snapshot",
      4096,
      None,
      self.time_provider.clone(),
      None,
    )
    .await?;

    Ok(store)
  }
}

#[tokio::test]
async fn empty_store() -> anyhow::Result<()> {
  let setup = Setup::new().await?;

  // Should start empty
  assert!(setup.store.is_empty());
  assert_eq!(setup.store.len(), 0);

  assert!(setup.temp_dir.path().join("test.jrn.0").exists());

  Ok(())
}

#[tokio::test]
async fn basic_crud() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));


  let (mut store, _) =
    VersionedKVStore::new(temp_dir.path(), "test", 4096, None, time_provider, None).await?;

  // Insert some values
  let ts1 = store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  let ts2 = store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;

  assert_eq!(store.len(), 2);
  assert!(ts2 >= ts1);

  // Remove a key
  let ts3 = store.remove("key1").await?;
  assert!(ts3.is_some());
  assert!(ts3.unwrap() >= ts2);

  assert_eq!(store.len(), 1);
  assert!(!store.contains_key("key1"));
  assert!(store.contains_key("key2"));

  // Remove non-existent key
  let removed = store.remove("nonexistent").await?;
  assert!(removed.is_none());

  // Read back existing key
  let val = store.get("key2");
  assert_eq!(val, Some(&make_string_value("value2")));

  // Read non-existent key
  let val = store.get("key1");
  assert_eq!(val, None);

  Ok(())
}

#[tokio::test]
async fn test_persistence_and_reload() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));


  // Create store and write some data
  let (ts1, ts2) = {
    let (mut store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      4096,
      None,
      time_provider.clone(),
      None,
    )
    .await?;
    let ts1 = store
      .insert("key1".to_string(), make_string_value("value1"))
      .await?;
    let ts2 = store
      .insert("key2".to_string(), make_string_value("foo"))
      .await?;
    store.sync()?;

    (ts1, ts2)
  };

  // Reopen and verify data persisted
  {
    let (store, _) =
      VersionedKVStore::open_existing(temp_dir.path(), "test", 4096, None, time_provider, None)
        .await?;
    assert_eq!(store.len(), 2);
    assert_eq!(
      store.get_with_timestamp("key1"),
      Some(&TimestampedValue {
        value: make_string_value("value1"),
        timestamp: ts1,
      })
    );
    assert_eq!(
      store.get_with_timestamp("key2"),
      Some(&TimestampedValue {
        value: make_string_value("foo"),
        timestamp: ts2,
      })
    );
  }

  Ok(())
}

#[tokio::test]
async fn test_null_value_is_deletion() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Insert a value
  setup
    .store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  assert!(setup.store.contains_key("key1"));

  // Insert empty state to delete
  setup
    .store
    .insert("key1".to_string(), StateValue::default())
    .await?;
  assert!(!setup.store.contains_key("key1"));
  assert_eq!(setup.store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_manual_rotation() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Insert some data
  let _ts1 = setup
    .store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;
  let ts2 = setup
    .store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;

  // Get max timestamp before rotation (this will be used in the archive name)
  let rotation_timestamp = setup
    .store
    .get_with_timestamp("key2")
    .map(|tv| tv.timestamp)
    .unwrap();

  // Manually trigger rotation
  setup.store.rotate_journal().await?;

  // Verify archived file exists (compressed)
  let archived_path = setup
    .temp_dir
    .path()
    .join(format!("test.jrn.t{}.zz", rotation_timestamp));
  assert!(archived_path.exists());

  // Verify active journal still works
  let ts3 = setup
    .store
    .insert("key3".to_string(), make_string_value("value3"))
    .await?;
  assert!(ts3 >= ts2);
  assert_eq!(setup.store.len(), 3);

  // Verify data is intact
  assert_eq!(setup.store.get("key1"), Some(&make_string_value("value1")));
  assert_eq!(setup.store.get("key2"), Some(&make_string_value("value2")));
  assert_eq!(setup.store.get("key3"), Some(&make_string_value("value3")));

  // Decompress the archive and load it as a Store to verify that it contains the old state.
  let snapshot_store = setup.make_store_from_snapshot_file(&archived_path).await?;
  assert_eq!(
    snapshot_store.get("key1"),
    Some(&make_string_value("value1"))
  );
  assert_eq!(
    snapshot_store.get("key2"),
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
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;

  let pre_rotation_state = setup.store.as_hashmap().clone();
  let pre_rotation_ts = setup
    .store
    .get_with_timestamp("key1")
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
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;
  assert!(ts_new >= pre_rotation_ts);
  assert_eq!(setup.store.len(), 2);

  Ok(())
}

#[tokio::test]
async fn test_empty_store_operations() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Operations on empty store
  assert_eq!(setup.store.get("nonexistent"), None);
  assert!(!setup.store.contains_key("nonexistent"));
  assert_eq!(setup.store.remove("nonexistent").await?, None);
  assert!(setup.store.is_empty());
  assert_eq!(setup.store.len(), 0);

  Ok(())
}

#[tokio::test]
async fn test_timestamp_preservation_during_rotation() -> anyhow::Result<()> {
  let mut setup = Setup::new().await?;

  // Insert some keys and capture their timestamps
  let ts1 = setup
    .store
    .insert("key1".to_string(), make_string_value("value1"))
    .await?;

  // Advance time to ensure different timestamps.
  setup.time_provider.advance(10.milliseconds());

  let ts2 = setup
    .store
    .insert("key2".to_string(), make_string_value("value2"))
    .await?;

  // Verify timestamps are different
  assert_ne!(ts1, ts2, "Timestamps should be different");
  assert!(ts2 > ts1, "Later writes should have later timestamps");

  // Write enough data to trigger rotation
  for i in 0 .. 50 {
    setup
      .store
      .insert(format!("fill{i}"), make_string_value("foo"))
      .await?;
  }

  // Verify that after rotation, the original timestamps are preserved
  let ts1_after = setup
    .store
    .get_with_timestamp("key1")
    .map(|tv| tv.timestamp)
    .unwrap();

  let ts2_after = setup
    .store
    .get_with_timestamp("key2")
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

  let mut rotation_timestamps = Vec::new();

  // Perform multiple rotations
  for i in 0 .. 3 {
    let key = format!("key{}", i);
    let value = make_string_value(&format!("value{}", i));
    setup.store.insert(key.clone(), value).await?;
    let timestamp = setup
      .store
      .get_with_timestamp(&key)
      .map(|tv| tv.timestamp)
      .unwrap();
    rotation_timestamps.push(timestamp);
    setup.store.rotate_journal().await?;
  }

  // Verify all compressed archives exist
  for timestamp in rotation_timestamps {
    let archived_path = setup
      .temp_dir
      .path()
      .join(format!("test.jrn.t{}.zz", timestamp));
    assert!(
      archived_path.exists(),
      "Compressed archive for timestamp {} should exist",
      timestamp
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
    Some(registry.clone()),
  )
  .await?;

  // Insert some data
  store
    .insert("key1".to_string(), make_string_value("value1"))
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
  let handle = registry.create_handle("test_subsystem").await;
  handle.update_retention_micros(0); // Retain all data from epoch

  // Insert more data and rotate WITH retention handle - snapshot SHOULD be created
  store
    .insert("key2".to_string(), make_string_value("value2"))
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
    .insert("key3".to_string(), make_string_value("value3"))
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
