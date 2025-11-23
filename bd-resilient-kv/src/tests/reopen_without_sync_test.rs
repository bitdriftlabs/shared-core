// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::versioned_kv_journal::make_string_value;
use crate::versioned_kv_journal::retention::RetentionRegistry;
use crate::{DataLoss, Scope, VersionedKVStore};
use bd_time::TestTimeProvider;
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

#[tokio::test]
async fn reopen_without_sync_should_preserve_data() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let registry = Arc::new(RetentionRegistry::new());

  // Create store and insert data WITHOUT syncing
  {
    let (mut store, _) = VersionedKVStore::new(
      temp_dir.path(),
      "test",
      4096,
      None,
      time_provider.clone(),
      registry.clone(),
    )
    .await?;

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

    // NOTE: NOT calling store.sync() here!
    println!("Inserted data without sync, dropping store...");
  }
  // Store is dropped here without sync

  // Reopen
  let (store, data_loss) =
    VersionedKVStore::open_existing(temp_dir.path(), "test", 4096, None, time_provider, registry)
      .await?;

  println!("Reopened store, data_loss: {data_loss:?}");
  println!("Store len: {}", store.len());

  // Verify no data loss
  assert_eq!(
    data_loss,
    DataLoss::None,
    "Expected no data loss on reopen without sync - mmap dirty pages should be preserved"
  );
  assert_eq!(store.len(), 2);
  assert!(store.contains_key(Scope::FeatureFlag, "key1"));
  assert!(store.contains_key(Scope::FeatureFlag, "key2"));

  Ok(())
}
