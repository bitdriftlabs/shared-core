// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use bd_bonjson::Value;
use itertools::Itertools as _;
use std::path::Path;

/// A state scope that determines the namespace used for storing state. This avoids key collisions
/// but also allows us to handle the state with different semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
  FeatureFlag,
  GlobalState,
}

//
// StateReader
//

/// A trait for reading state values. This allows for simple in-memory implementations in tests
/// without requiring async or filesystem access.
pub trait StateReader {
  /// Gets a value from the state store.
  fn get(&self, scope: Scope, key: &str) -> Option<&str>;
}

impl Scope {
  #[must_use]
  pub fn as_prefix(&self) -> &'static str {
    // Use a small prefix since we have to pay for the size.
    match self {
      Self::FeatureFlag => "ff",
      Self::GlobalState => "g",
    }
  }
}

//
// StateStore
//

/// Wraps a versioned key-value store for managing application state with namespaced keys.
pub struct Store {
  inner: bd_resilient_kv::VersionedKVStore,
}

impl Store {
  pub fn new(directory: &Path) -> anyhow::Result<Self> {
    Ok(Self {
      inner: bd_resilient_kv::VersionedKVStore::new(directory, "state_current", 1024 * 1024, None)?,
    })
  }

  /// Opens a previous state snapshot for read-only access.
  ///
  /// This is typically used to read state from a previous run, such as for crash reporting.
  /// The previous state is created by calling `backup_previous()` at application startup.
  ///
  /// # Errors
  /// Returns an error if the previous state file does not exist or cannot be opened.
  pub fn open_previous(directory: &Path) -> anyhow::Result<Self> {
    Ok(Self {
      inner: bd_resilient_kv::VersionedKVStore::open_existing(
        directory,
        "state_previous",
        1024 * 1024,
        None,
      )?,
    })
  }

  /// Creates a backup of the current state as "previous" state.
  ///
  /// This should be called once at application startup to preserve the state from the previous
  /// run. This allows crash handlers and other components to access the state that was active
  /// during a previous process that may have crashed.
  ///
  /// # Errors
  /// Returns an error if the file copy operation fails.
  pub fn backup_previous(&self, directory: &Path) -> anyhow::Result<()> {
    let current_path = directory.join("state_current.jrn");
    let previous_path = directory.join("state_previous.jrn");

    // If there's no current state file, nothing to backup
    if !current_path.exists() {
      return Ok(());
    }

    // Copy current to previous, overwriting any existing previous
    std::fs::copy(&current_path, &previous_path)?;
    Ok(())
  }

  pub async fn insert(&mut self, scope: Scope, key: &str, value: String) -> anyhow::Result<()> {
    let namespaced_key = format!("{}:{}", scope.as_prefix(), key);
    self
      .inner
      .insert(namespaced_key, Value::String(value))
      .await
      .map(|_| ())
  }

  pub async fn remove(&mut self, scope: Scope, key: &str) -> anyhow::Result<()> {
    let namespaced_key = format!("{}:{}", scope.as_prefix(), key);
    self.inner.remove(&namespaced_key).await.map(|_| ())
  }

  pub async fn clear(&mut self, scope: Scope) -> anyhow::Result<()> {
    let prefix = scope.as_prefix();
    let keys_to_remove: Vec<String> = self
      .inner
      .as_hashmap_with_timestamps()
      .keys()
      .filter(|key| key.starts_with(prefix))
      .cloned()
      .collect_vec();

    // TODO(snowp): Ideally we should have built in support for batch deletions in the underlying
    // store. This leaves us open for partial deletions if something fails halfway through.
    for key in keys_to_remove {
      self.inner.remove(&key).await?;
    }
    Ok(())
  }

  #[must_use]
  pub fn current_version(&self) -> u64 {
    self.inner.current_version()
  }

  /// Iterates over all key-value pairs in a given scope.
  ///
  /// Returns an iterator that yields (key, value) tuples where the key has been stripped
  /// of its scope prefix.
  pub fn iter_scope(&self, scope: Scope) -> impl Iterator<Item = (&str, &str)> {
    let prefix = format!("{}:", scope.as_prefix());
    let prefix_len = prefix.len();

    self
      .inner
      .as_hashmap_with_timestamps()
      .iter()
      .filter(move |(key, _)| key.starts_with(&prefix))
      .filter_map(move |(key, timestamped_value)| {
        let stripped_key = &key[prefix_len ..];
        if let Value::String(s) = &timestamped_value.value {
          Some((stripped_key, s.as_str()))
        } else {
          None
        }
      })
  }
}

impl StateReader for Store {
  fn get(&self, scope: Scope, key: &str) -> Option<&str> {
    let namespaced_key = format!("{}:{}", scope.as_prefix(), key);
    self.inner.get(&namespaced_key).and_then(|v| {
      if let Value::String(s) = v {
        Some(s.as_str())
      } else {
        None
      }
    })
  }
}

#[path = "./test.rs"]
pub mod test;
