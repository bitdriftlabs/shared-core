// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used, clippy::panic)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use time::macros::datetime;

//
// TestStateReader
//

/// A simple in-memory state reader for tests. This avoids the need for async,
/// temporary directories, or complex setup in tests that only need to read state.
///
/// # Example
/// ```
/// use bd_state::test::TestStateReader;
/// use bd_state::{Scope, StateReader};
///
/// let mut reader = TestStateReader::default();
/// reader.insert(Scope::FeatureFlag, "my_flag", "true");
/// assert_eq!(reader.get(Scope::FeatureFlag, "my_flag"), Some("true"));
/// ```
#[derive(Default)]
pub struct TestStateReader {
  data: HashMap<(crate::Scope, String), String>,
}

impl TestStateReader {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Inserts a value into the test state reader. This is synchronous and doesn't
  /// require async, making it easy to use in tests.
  pub fn insert(&mut self, scope: crate::Scope, key: impl Into<String>, value: impl Into<String>) {
    self.data.insert((scope, key.into()), value.into());
  }
}

impl crate::StateReader for TestStateReader {
  fn get(&self, scope: crate::Scope, key: &str) -> Option<&str> {
    self.data.get(&(scope, key.to_string())).map(String::as_str)
  }

  fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = crate::StateEntry<'a>> + 'a> {
    Box::new(
      self
        .data
        .iter()
        .map(|((scope, key), value)| crate::StateEntry {
          scope: *scope,
          key,
          value,
          timestamp: time::macros::datetime!(2024-01-01 00:00:00 UTC),
        }),
    )
  }
}

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
    let store = crate::Store::persistent(temp_dir.path(), time_provider.clone())
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
