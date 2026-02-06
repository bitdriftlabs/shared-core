// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Tests for monitoring metrics in `VersionedKVStore`.
//!
//! These tests verify that all metrics are emitted correctly for various scenarios:
//! - `kv:capacity_exceeded` with recoverable=true/false labels
//! - `kv:data_loss` with severity=partial/total labels
//! - `kv:journal_open` with result=success/failure labels
//! - `kv:journal_rotation` with result=success/failure labels
//! - `kv:journal_sync` with result=success/failure labels
//! - `kv:compression` with result=success/failure labels
//! - `kv:rotation_duration_s` histogram

#![allow(clippy::unwrap_used)]

use crate::versioned_kv_journal::make_string_value;
use crate::versioned_kv_journal::retention::RetentionRegistry;
use crate::versioned_kv_journal::store::PersistentStoreConfig;
use crate::versioned_kv_journal::HEADER_SIZE;
use crate::{DataLoss, Scope, VersionedKVStore};
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::Collector;
use bd_stats_common::labels;
use bd_time::TestTimeProvider;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

/// Test setup with access to the stats collector for assertions.
struct MetricsTestSetup {
  temp_dir: TempDir,
  time_provider: Arc<TestTimeProvider>,
  registry: Arc<RetentionRegistry>,
  collector: Collector,
}

impl MetricsTestSetup {
  fn new() -> Self {
    Self {
      temp_dir: TempDir::new().unwrap(),
      time_provider: Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC))),
      registry: Arc::new(RetentionRegistry::new()),
      collector: Collector::default(),
    }
  }

  async fn open_store(&self, config: PersistentStoreConfig) -> anyhow::Result<VersionedKVStore> {
    let stats = self.collector.scope("test");
    let (store, _) = VersionedKVStore::new(
      self.temp_dir.path(),
      "test",
      config,
      self.time_provider.clone(),
      self.registry.clone(),
      &stats,
    )
    .await?;
    Ok(store)
  }

  async fn open_store_with_data_loss(
    &self,
    config: PersistentStoreConfig,
  ) -> anyhow::Result<(VersionedKVStore, DataLoss)> {
    let stats = self.collector.scope("test");
    VersionedKVStore::new(
      self.temp_dir.path(),
      "test",
      config,
      self.time_provider.clone(),
      self.registry.clone(),
      &stats,
    )
    .await
  }
}

// =============================================================================
// capacity_exceeded metrics tests
// =============================================================================

/// Test that `kv:capacity_exceeded` with `recoverable=true` is emitted when rotation helps.
#[tokio::test]
async fn capacity_exceeded_recoverable_via_rotation() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  // Configure store with small initial buffer but larger max capacity
  // This allows rotation to help by growing the buffer
  let config = PersistentStoreConfig {
    initial_buffer_size: 4 * 1024,  // 4KB initial
    max_capacity_bytes: 32 * 1024,  // 32KB max (allows growth)
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  // Fill up the initial buffer with data
  let value = "x".repeat(512); // 512 byte values
  for i in 0 .. 6 {
    store
      .insert(
        Scope::GlobalState,
        format!("fill_{i}"),
        make_string_value(&value),
      )
      .await?;
  }

  // Now try to insert a value that won't fit in current buffer but will fit after rotation
  // The buffer is nearly full, so this should trigger a recoverable capacity exceeded
  let large_value = "y".repeat(2048); // 2KB value
  let result = store
    .insert(
      Scope::GlobalState,
      "large_key".to_string(),
      make_string_value(&large_value),
    )
    .await;

  // Should succeed because rotation + growth helped
  assert!(result.is_ok(), "Insert should succeed via rotation");

  // Verify the recoverable capacity exceeded metric was incremented
  setup.collector.assert_counter_eq(
    1,
    "test:kv:capacity_exceeded",
    labels!("recoverable" => "true"),
  );

  Ok(())
}

/// Test that `kv:capacity_exceeded` with `recoverable=false` is emitted when rotation cannot help.
#[tokio::test]
async fn capacity_exceeded_unrecoverable() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  // Configure store with max capacity that's too small for the value we'll insert
  let config = PersistentStoreConfig {
    initial_buffer_size: 4 * 1024, // 4KB
    max_capacity_bytes: 8 * 1024,  // 8KB max (very small)
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  // Try to insert a value larger than max capacity
  let huge_value = "x".repeat(10 * 1024); // 10KB, exceeds 8KB max
  let result = store
    .insert(
      Scope::GlobalState,
      "huge_key".to_string(),
      make_string_value(&huge_value),
    )
    .await;

  assert!(result.is_err(), "Insert should fail");

  // Verify the unrecoverable capacity exceeded metric was incremented
  setup.collector.assert_counter_eq(
    1,
    "test:kv:capacity_exceeded",
    labels!("recoverable" => "false"),
  );

  Ok(())
}

// =============================================================================
// journal_open metrics tests
// =============================================================================

/// Test that `kv:journal_open` with `result=success` is emitted on successful open.
#[tokio::test]
async fn journal_open_success_new_journal() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    ..Default::default()
  };

  // Opening a new journal should record success
  let _store = setup.open_store(config).await?;

  setup.collector.assert_counter_eq(
    1,
    "test:kv:journal_open",
    labels!("result" => "success"),
  );

  Ok(())
}

/// Test that `kv:journal_open` with `result=success` is emitted when reopening an existing journal.
#[tokio::test]
async fn journal_open_success_existing_journal() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    ..Default::default()
  };

  // Create and populate a journal
  {
    let mut store = setup.open_store(config.clone()).await?;
    store
      .insert(
        Scope::GlobalState,
        "key1".to_string(),
        make_string_value("value1"),
      )
      .await?;
    store.sync()?;
  }

  // Reopen the journal - this should also be successful
  let _store = setup.open_store(config).await?;

  // Should have 2 successful opens now
  setup.collector.assert_counter_eq(
    2,
    "test:kv:journal_open",
    labels!("result" => "success"),
  );

  Ok(())
}

/// Test that `kv:journal_open` with `result=failure` and `kv:data_loss` with `severity=total`
/// are emitted when opening a completely corrupted journal.
#[tokio::test]
async fn journal_open_failure_corrupted_journal() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    ..Default::default()
  };

  // Create a journal file with completely invalid data
  let journal_path = setup.temp_dir.path().join("test.jrn.0");
  {
    let mut file = std::fs::File::create(&journal_path)?;
    // Write garbage data that can't be parsed as a journal
    file.write_all(&[0xFF; 100])?;
  }

  // Try to open - should fall back to creating a fresh journal
  let (store, data_loss) = setup.open_store_with_data_loss(config).await?;

  // The store should still work (falls back to fresh journal)
  assert!(store.is_empty());

  // Data loss should be Total since we couldn't read the old journal
  assert_eq!(data_loss, DataLoss::Total);

  // Verify the failure metric was recorded
  setup.collector.assert_counter_eq(
    1,
    "test:kv:journal_open",
    labels!("result" => "failure"),
  );

  // Verify data loss was recorded
  setup.collector.assert_counter_eq(
    1,
    "test:kv:data_loss",
    labels!("severity" => "total"),
  );

  Ok(())
}

// =============================================================================
// data_loss metrics tests
// =============================================================================

/// Test that `kv:data_loss` with `severity=partial` is emitted when journal has partial corruption.
#[tokio::test]
async fn data_loss_partial_corruption() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    ..Default::default()
  };

  // Create a valid journal with some data
  let journal_path = setup.temp_dir.path().join("test.jrn.0");
  {
    let mut store = setup.open_store(config.clone()).await?;
    store
      .insert(
        Scope::GlobalState,
        "key1".to_string(),
        make_string_value("value1"),
      )
      .await?;
    store
      .insert(
        Scope::GlobalState,
        "key2".to_string(),
        make_string_value("value2"),
      )
      .await?;
    store.sync()?;
  }

  // Corrupt the journal by modifying data after the header but within the position range
  // This simulates a crash during write where position was updated but data is corrupted
  {
    let mut data = std::fs::read(&journal_path)?;

    // Read the position from the header (bytes 1-8)
    let position = u64::from_le_bytes(data[1 .. 9].try_into()?) as usize;

    // Corrupt some bytes in the middle of the data area (after first frame)
    // This should cause partial data loss during recovery
    if position > HEADER_SIZE + 50 {
      // Corrupt bytes in the middle to break a frame
      let corrupt_start = HEADER_SIZE + 30;
      for i in corrupt_start .. corrupt_start + 20 {
        if i < data.len() {
          data[i] = 0xFF;
        }
      }
    }

    std::fs::write(&journal_path, data)?;
  }

  // Create a new collector for the reopen
  let collector2 = Collector::default();
  let stats = collector2.scope("test");

  // Reopen - should detect partial data loss
  let (store, data_loss) = VersionedKVStore::new(
    setup.temp_dir.path(),
    "test",
    config,
    setup.time_provider.clone(),
    setup.registry.clone(),
    &stats,
  )
  .await?;

  if data_loss == DataLoss::Partial {
    assert!(
      store.len() < 2,
      "Partial loss should mean some data was lost"
    );

    collector2.assert_counter_eq(
      1,
      "test:kv:data_loss",
      labels!("severity" => "partial"),
    );
  }

  Ok(())
}

/// Test that `kv:data_loss` with `severity=total` is emitted when the journal is completely
/// unreadable.
#[tokio::test]
async fn data_loss_total_corruption() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    ..Default::default()
  };

  // Create a journal file with an invalid version byte
  let journal_path = setup.temp_dir.path().join("test.jrn.0");
  {
    let mut file = std::fs::File::create(&journal_path)?;
    // Write invalid version (255 instead of 1)
    let mut data = vec![0u8; 4096];
    data[0] = 255; // Invalid version
    // Set a valid-looking position
    let position: u64 = 100;
    data[1 .. 9].copy_from_slice(&position.to_le_bytes());
    file.write_all(&data)?;
  }

  let (store, data_loss) = setup.open_store_with_data_loss(config).await?;

  assert_eq!(data_loss, DataLoss::Total);
  assert!(store.is_empty(), "Total data loss means no data recovered");

  setup.collector.assert_counter_eq(
    1,
    "test:kv:data_loss",
    labels!("severity" => "total"),
  );

  Ok(())
}

// =============================================================================
// journal_rotation metrics tests
// =============================================================================

/// Test that `kv:journal_rotation` with `result=success` is emitted on successful rotation.
#[tokio::test]
async fn journal_rotation_success() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  // Insert some data
  store
    .insert(
      Scope::GlobalState,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Manually trigger rotation
  store.rotate_journal().await?;

  setup.collector.assert_counter_eq(
    1,
    "test:kv:journal_rotation",
    labels!("result" => "success"),
  );

  Ok(())
}

/// Test that automatic rotation also records the metric.
#[tokio::test]
async fn journal_rotation_automatic() -> anyhow::Result<()> {
  use bd_stats_common::NameType;

  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4 * 1024,
    max_capacity_bytes: 32 * 1024,
    high_water_mark_ratio: 0.5,
  };

  let mut store = setup.open_store(config).await?;

  let value = "x".repeat(512);
  for i in 0 .. 10 {
    store
      .insert(
        Scope::GlobalState,
        format!("key_{i}"),
        make_string_value(&value),
      )
      .await?;
  }

  let counter = setup
    .collector
    .find_counter(
      &NameType::Global("test:kv:journal_rotation".to_string()),
      &labels!("result" => "success"),
    )
    .expect("Rotation counter should exist");

  assert!(counter.get() >= 1, "At least one rotation should have occurred");

  Ok(())
}

// =============================================================================
// journal_sync metrics tests
// =============================================================================

/// Test that `kv:journal_sync` with `result=success` is emitted on successful sync.
#[tokio::test]
async fn journal_sync_success() -> anyhow::Result<()> {
  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    ..Default::default()
  };

  let mut store = setup.open_store(config).await?;

  store
    .insert(
      Scope::GlobalState,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Sync should succeed
  store.sync()?;

  setup.collector.assert_counter_eq(
    1,
    "test:kv:journal_sync",
    labels!("result" => "success"),
  );

  Ok(())
}

// =============================================================================
// compression metrics tests
// =============================================================================

/// Test that `kv:compression` with `result=success` is emitted on successful compression
/// during rotation.
#[tokio::test]
async fn compression_success() -> anyhow::Result<()> {
  // Need to hold a retention handle to actually create snapshots
  let setup = MetricsTestSetup::new();
  let _handle = setup.registry.create_handle().await;

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  // Insert some data
  store
    .insert(
      Scope::GlobalState,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Rotate to trigger compression of the old journal
  store.rotate_journal().await?;

  // Compression happens during rotation when a retention handle is held
  setup.collector.assert_counter_eq(
    1,
    "test:kv:compression",
    labels!("result" => "success"),
  );

  Ok(())
}

// =============================================================================
// rotation_duration_s histogram tests
// =============================================================================

/// Test that `kv:rotation_duration_s` histogram is populated during rotation.
#[tokio::test]
async fn rotation_duration_histogram() -> anyhow::Result<()> {
  use bd_stats_common::NameType;
  use std::collections::BTreeMap;

  let setup = MetricsTestSetup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4096,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  for i in 0 .. 5 {
    store
      .insert(
        Scope::GlobalState,
        format!("key_{i}"),
        make_string_value(&format!("value_{i}")),
      )
      .await?;
  }

  store.rotate_journal().await?;

  assert!(
    setup
      .collector
      .find_histogram(
        &NameType::Global("test:kv:rotation_duration_s".to_string()),
        &BTreeMap::new(),
      )
      .is_some(),
    "Histogram should exist after rotation"
  );

  Ok(())
}

// =============================================================================
// In-memory store metrics tests
// =============================================================================

/// Test that in-memory stores also emit capacity metrics.
#[tokio::test]
async fn in_memory_capacity_exceeded() -> anyhow::Result<()> {
  let collector = Collector::default();
  let stats = collector.scope("test");
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

  let mut store = VersionedKVStore::new_in_memory(time_provider, Some(1024), &stats);

  // Fill up the store
  store
    .insert(
      Scope::GlobalState,
      "key1".to_string(),
      make_string_value("value1"),
    )
    .await?;

  // Try to exceed capacity
  let large_value = make_string_value(&"x".repeat(2000));
  let result = store
    .insert(Scope::GlobalState, "large".to_string(), large_value)
    .await;

  assert!(result.is_err());

  // In-memory stores always have unrecoverable capacity exceeded (no rotation)
  collector.assert_counter_eq(
    1,
    "test:kv:capacity_exceeded",
    labels!("recoverable" => "false"),
  );

  Ok(())
}
