// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::versioned_kv_journal::retention::RetentionRegistry;
use crate::versioned_kv_journal::{PersistentStoreConfig, make_string_value};
use crate::{Scope, VersionedKVStore};
use bd_time::TestTimeProvider;
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

struct Setup {
  temp_dir: TempDir,
  time_provider: Arc<TestTimeProvider>,
  registry: Arc<RetentionRegistry>,
}

impl Setup {
  fn new() -> Self {
    Self {
      temp_dir: TempDir::new().unwrap(),
      time_provider: Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC))),
      registry: Arc::new(RetentionRegistry::new()),
    }
  }

  async fn open_store(&self, config: PersistentStoreConfig) -> anyhow::Result<VersionedKVStore> {
    let (store, _) = VersionedKVStore::new(
      self.temp_dir.path(),
      "test",
      config,
      self.time_provider.clone(),
      self.registry.clone(),
    )
    .await?;
    Ok(store)
  }
}

/// Test that journal grows from 8KB → 16KB → 32KB with power-of-2 pattern.
#[tokio::test]
async fn normal_growth_pattern() -> anyhow::Result<()> {
  let setup = Setup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,   // 8KB
    max_capacity_bytes: 1024 * 1024, // 1MB max
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  // Verify initial size
  assert_eq!(store.initial_buffer_size(), 8 * 1024);
  assert_eq!(store.current_buffer_size(), 8 * 1024);
  assert_eq!(store.max_capacity_bytes(), 1024 * 1024);

  // Fill store to trigger rotation (write large values until high water mark)
  let large_value = "x".repeat(1024); // 1KB value
  for i in 0 .. 10 {
    store
      .insert(
        Scope::GlobalState,
        format!("key_{i}"),
        make_string_value(&large_value),
      )
      .await?;
  }

  // Manually rotate to trigger growth
  store.rotate_journal().await?;

  // Should have grown to 16KB (next power of 2 with headroom)
  assert!(store.current_buffer_size() >= 16 * 1024);
  assert!(store.current_buffer_size() <= 1024 * 1024); // Still under max

  Ok(())
}

/// Test that growth stops when max capacity is reached.
#[tokio::test]
async fn max_capacity_limiting() -> anyhow::Result<()> {
  let setup = Setup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 4 * 1024, // 4KB
    max_capacity_bytes: 16 * 1024, // 16KB max (small for testing)
    high_water_mark_ratio: 0.5,    // Lower threshold for faster rotation
  };

  let mut store = setup.open_store(config).await?;

  assert_eq!(store.current_buffer_size(), 4 * 1024);

  // Fill with data to trigger multiple rotations
  let value = "x".repeat(512);
  for i in 0 .. 20 {
    store
      .insert(
        Scope::GlobalState,
        format!("key_{i}"),
        make_string_value(&value),
      )
      .await?;

    // Try rotating periodically
    if i % 5 == 0 {
      store.rotate_journal().await?;
    }
  }

  // Should have capped at max_capacity_bytes
  assert!(store.current_buffer_size() <= 16 * 1024);

  Ok(())
}

/// Test that changing config on restart is handled correctly.
#[tokio::test]
async fn config_changes_on_restart() -> anyhow::Result<()> {
  let setup = Setup::new();

  // Start with small config
  let config1 = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 64 * 1024,
    high_water_mark_ratio: 0.8,
  };

  {
    let mut store = setup.open_store(config1.clone()).await?;

    // Write some data
    for i in 0 .. 5 {
      store
        .insert(
          Scope::GlobalState,
          format!("key_{i}"),
          make_string_value("value"),
        )
        .await?;
    }

    assert_eq!(store.initial_buffer_size(), 8 * 1024);
  }

  // Restart with larger initial size
  let config2 = PersistentStoreConfig {
    initial_buffer_size: 32 * 1024, // Increased from 8KB
    max_capacity_bytes: 128 * 1024, // Increased cap
    high_water_mark_ratio: 0.8,
  };

  {
    let store = setup.open_store(config2).await?;

    // Should detect existing file and reconcile size
    // File size is still 8KB, so current_buffer_size should be 8KB
    // but initial_buffer_size should be 32KB (from new config)
    assert_eq!(store.initial_buffer_size(), 32 * 1024);
    assert_eq!(store.max_capacity_bytes(), 128 * 1024);

    // Verify data persisted
    assert_eq!(store.len(), 5);
  }

  Ok(())
}

/// Test that config normalization applies defaults to invalid parameters.
#[tokio::test]
async fn config_validation() {
  // Test: initial_buffer_size too small (should default to 8KB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 1024, // Too small, minimum is 4096
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.7,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 8 * 1024); // Falls back to default

  // Test: initial_buffer_size too large (should default to 8KB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 2 * 1024 * 1024 * 1024, // 2GB, way too large
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.7,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 8 * 1024); // Falls back to default

  // Test: initial_buffer_size not power of 2 (should round up)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 9000, // Not power of 2, should round to 16384
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.7,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 16384); // Rounded up from 9000

  // Test: max < initial (should disable max_capacity)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 16 * 1024,
    max_capacity_bytes: 8 * 1024, // Smaller than initial
    high_water_mark_ratio: 0.7,
  };
  config.normalize();
  assert_eq!(config.max_capacity_bytes, 1024 * 1024); // Falls back to 1MB default

  // Test: max too large (should default to 1MB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 2 * 1024 * 1024 * 1024, // 2GB, too large
    high_water_mark_ratio: 0.7,
  };
  config.normalize();
  assert_eq!(config.max_capacity_bytes, 1024 * 1024); // Falls back to 1MB default

  // Test: max_capacity not specified (should default to 1MB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024, // Not specified
    high_water_mark_ratio: 0.7,
  };
  config.normalize();
  assert_eq!(config.max_capacity_bytes, 1024 * 1024); // Falls back to 1MB default

  // Test: high_water_mark_ratio out of range (should default to 0.7)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 1.5, // > 1.0
  };
  config.normalize();
  assert!((config.high_water_mark_ratio - 0.7).abs() < f32::EPSILON); // Falls back to default

  // Test: high_water_mark_ratio NaN (should default to 0.7)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: f32::NAN,
  };
  config.normalize();
  assert!((config.high_water_mark_ratio - 0.7).abs() < f32::EPSILON); // Falls back to default

  // Test: valid config (should remain unchanged)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: 0.8,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 8 * 1024);
  assert_eq!(config.max_capacity_bytes, 1024 * 1024);
  assert!((config.high_water_mark_ratio - 0.8).abs() < f32::EPSILON);
}

/// Test growth with actual compaction size calculation.
#[tokio::test]
async fn growth_with_compaction() -> anyhow::Result<()> {
  let setup = Setup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 256 * 1024,
    high_water_mark_ratio: 0.7,
  };

  let mut store = setup.open_store(config).await?;

  let old_size = store.current_buffer_size();

  // Write enough data to fill ~70% of buffer (triggers high water mark)
  let value = "x".repeat(256);
  #[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
  )]
  let entries_needed = (old_size as f32 * 0.7 / 256.0) as usize;

  for i in 0 .. entries_needed {
    store
      .insert(
        Scope::GlobalState,
        format!("key_{i}"),
        make_string_value(&value),
      )
      .await?;
  }

  // Rotation should have triggered automatically.

  // Should have grown (50% headroom on compacted size)
  let new_size = store.current_buffer_size();
  assert!(new_size > old_size);

  Ok(())
}

/// Test that inserting a value larger than current buffer but smaller than max capacity
/// triggers automatic rotation and retry instead of failing immediately.
#[tokio::test]
async fn insert_triggers_rotation_on_capacity_exceeded() -> anyhow::Result<()> {
  let setup = Setup::new();

  // Start with a very small buffer (4KB) but allow growth up to 64KB
  let config = PersistentStoreConfig {
    initial_buffer_size: 4 * 1024, // 4KB
    max_capacity_bytes: 64 * 1024, // 64KB max
    high_water_mark_ratio: 0.8,    // High threshold to avoid early rotation
  };

  let mut store = setup.open_store(config).await?;

  assert_eq!(store.current_buffer_size(), 4 * 1024);

  // Try to insert a value that's larger than the current buffer (4KB)
  // but smaller than max capacity (64KB).
  // This should trigger automatic rotation with buffer growth, then retry the insert.
  let large_value = "x".repeat(5 * 1024); // 5KB value

  let result = store
    .insert(
      Scope::GlobalState,
      "large_key".to_string(),
      make_string_value(&large_value),
    )
    .await;

  // Should succeed after automatic rotation
  assert!(result.is_ok(), "Insert should succeed after rotation");

  // Buffer should have grown to accommodate the large value
  assert!(
    store.current_buffer_size() > 4 * 1024,
    "Buffer should have grown from initial 4KB"
  );
  assert!(
    store.current_buffer_size() <= 64 * 1024,
    "Buffer should not exceed max capacity"
  );

  // Verify the value was actually inserted
  assert_eq!(store.len(), 1);
  assert!(store.contains_key(Scope::GlobalState, "large_key"));

  Ok(())
}

/// Test that inserting a value larger than max capacity still fails gracefully.
#[tokio::test]
async fn insert_fails_when_exceeding_max_capacity() -> anyhow::Result<()> {
  let setup = Setup::new();

  // Small max capacity for testing
  let config = PersistentStoreConfig {
    initial_buffer_size: 4 * 1024, // 4KB
    max_capacity_bytes: 8 * 1024,  // 8KB max (very small)
    high_water_mark_ratio: 0.8,
  };

  let mut store = setup.open_store(config).await?;

  // Try to insert a value that's larger than max capacity
  let huge_value = "x".repeat(10 * 1024); // 10KB value, exceeds 8KB max

  let result = store
    .insert(
      Scope::GlobalState,
      "huge_key".to_string(),
      make_string_value(&huge_value),
    )
    .await;

  // Should fail with CapacityExceeded even after rotation attempt
  assert!(
    result.is_err(),
    "Insert should fail when value exceeds max capacity"
  );

  // Verify nothing was inserted
  assert_eq!(store.len(), 0);
  assert!(!store.contains_key(Scope::GlobalState, "huge_key"));

  Ok(())
}
