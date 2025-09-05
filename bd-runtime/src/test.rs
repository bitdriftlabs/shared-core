// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::expect_used)]

use crate::runtime::ConfigLoader;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

//
// TestConfigLoader
//

/// A wrapper around `ConfigLoader` that can manage a temporary directory used for config caching.
pub struct TestConfigLoader {
  _tempdir: Option<TempDir>,
  inner: Arc<ConfigLoader>,
}

impl Deref for TestConfigLoader {
  type Target = Arc<ConfigLoader>;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl TestConfigLoader {
  pub async fn new() -> Self {
    let tempdir = tempfile::tempdir().expect("Failed to create tempdir");
    let config_loader = ConfigLoader::new(tempdir.path());
    config_loader.try_load_persisted_config().await;

    Self {
      _tempdir: Some(tempdir),
      inner: config_loader,
    }
  }

  pub async fn new_in_directory(directory: &Path) -> Self {
    let config_loader = ConfigLoader::new(directory);
    config_loader.try_load_persisted_config().await;

    Self {
      _tempdir: None,
      inner: config_loader,
    }
  }
}
