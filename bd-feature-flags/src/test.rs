// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used, clippy::panic)]

use std::ops::{Deref, DerefMut};

//
// TestFeatureFlags
//

/// A wrapper around `FeatureFlags` for use in tests. Handles temporary directory
/// creation and cleanup.
pub struct TestFeatureFlags {
  _dir: tempfile::TempDir,
  inner: crate::FeatureFlags,
}

impl TestFeatureFlags {
  #[must_use]
  pub fn new() -> Self {
    let temp_dir = tempfile::tempdir().unwrap();
    let flags = crate::FeatureFlags::new(temp_dir.path(), 1024, None).unwrap();

    Self {
      _dir: temp_dir,
      inner: flags,
    }
  }
}

impl Default for TestFeatureFlags {
  fn default() -> Self {
    Self::new()
  }
}

impl Deref for TestFeatureFlags {
  type Target = crate::FeatureFlags;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl DerefMut for TestFeatureFlags {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}
