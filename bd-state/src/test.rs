// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used, clippy::panic)]

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use time::macros::datetime;

//
// TestStore
//

/// A wrapper around `Store` for use in tests. Handles temporary directory
/// creation and cleanup.
pub struct TestStore {
  _dir: tempfile::TempDir,
  inner: crate::Store,
  _time_provider: Arc<bd_time::TestTimeProvider>,
}

impl TestStore {
  #[must_use]
  pub async fn new() -> Self {
    let temp_dir = tempfile::tempdir().unwrap();
    let time_provider = Arc::new(bd_time::TestTimeProvider::new(
      datetime!(2024-01-01 00:00:00 UTC),
    ));
    let store = crate::Store::persistent(
      temp_dir.path(),
      crate::PersistentStoreConfig::default(),
      time_provider.clone(),
      &bd_runtime::runtime::ConfigLoader::new(temp_dir.path()),
      &bd_client_stats_store::Collector::default().scope("test"),
    )
    .await
    .unwrap()
    .store;

    Self {
      _dir: temp_dir,
      inner: store,
      _time_provider: time_provider,
    }
  }

  #[must_use]
  pub fn take_inner(self) -> crate::Store {
    self.inner
  }
}

impl Deref for TestStore {
  type Target = crate::Store;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl DerefMut for TestStore {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}
