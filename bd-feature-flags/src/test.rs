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
