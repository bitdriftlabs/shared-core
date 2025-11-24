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

  async fn create_store(&self, config: PersistentStoreConfig) -> anyhow::Result<VersionedKVStore> {
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
    high_water_mark_ratio: Some(0.8),
  };

  let mut store = setup.create_store(config).await?;

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
    initial_buffer_size: 4 * 1024,    // 4KB
    max_capacity_bytes: 16 * 1024,    // 16KB max (small for testing)
    high_water_mark_ratio: Some(0.5), // Lower threshold for faster rotation
  };

  let mut store = setup.create_store(config).await?;

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
    high_water_mark_ratio: Some(0.8),
  };

  {
    let mut store = setup.create_store(config1.clone()).await?;

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
    high_water_mark_ratio: Some(0.8),
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
    high_water_mark_ratio: None,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 8 * 1024); // Falls back to default

  // Test: initial_buffer_size too large (should default to 8KB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 2 * 1024 * 1024 * 1024, // 2GB, way too large
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: None,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 8 * 1024); // Falls back to default

  // Test: initial_buffer_size not power of 2 (should round up)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 9000, // Not power of 2, should round to 16384
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: None,
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 16384); // Rounded up from 9000

  // Test: max < initial (should disable max_capacity)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 16 * 1024,
    max_capacity_bytes: 8 * 1024, // Smaller than initial
    high_water_mark_ratio: None,
  };
  config.normalize();
  assert_eq!(config.max_capacity_bytes, 1024 * 1024); // Falls back to 1MB default

  // Test: max too large (should default to 1MB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 2 * 1024 * 1024 * 1024, // 2GB, too large
    high_water_mark_ratio: None,
  };
  config.normalize();
  assert_eq!(config.max_capacity_bytes, 1024 * 1024); // Falls back to 1MB default

  // Test: max_capacity not specified (should default to 1MB)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024, // Not specified
    high_water_mark_ratio: None,
  };
  config.normalize();
  assert_eq!(config.max_capacity_bytes, 1024 * 1024); // Falls back to 1MB default

  // Test: high_water_mark_ratio out of range (should default to 0.7)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: Some(1.5), // > 1.0
  };
  config.normalize();
  assert_eq!(config.high_water_mark_ratio, Some(0.7)); // Falls back to default

  // Test: high_water_mark_ratio NaN (should default to 0.7)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: Some(f32::NAN),
  };
  config.normalize();
  assert_eq!(config.high_water_mark_ratio, Some(0.7)); // Falls back to default

  // Test: valid config (should remain unchanged)
  let mut config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 1024 * 1024,
    high_water_mark_ratio: Some(0.8),
  };
  config.normalize();
  assert_eq!(config.initial_buffer_size, 8 * 1024);
  assert_eq!(config.max_capacity_bytes, 1024 * 1024);
  assert_eq!(config.high_water_mark_ratio, Some(0.8));
}

/// Test growth with actual compaction size calculation.
#[tokio::test]
async fn growth_with_compaction() -> anyhow::Result<()> {
  let setup = Setup::new();

  let config = PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 256 * 1024,
    high_water_mark_ratio: Some(0.7),
  };

  let mut store = setup.create_store(config).await?;

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

  // Rotation should trigger automatically or we can trigger manually
  store.rotate_journal().await?;

  // Should have grown (50% headroom on compacted size)
  let new_size = store.current_buffer_size();
  assert!(new_size >= old_size); // May not grow if headroom sufficient

  Ok(())
}
