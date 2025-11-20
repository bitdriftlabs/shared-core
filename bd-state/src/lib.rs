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

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub mod test;

use ahash::AHashMap;
pub use bd_resilient_kv::Scope;
use bd_resilient_kv::{DataLoss, StateValue, Value_type};
use bd_time::TimeProvider;
use itertools::Itertools as _;
use std::path::Path;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::RwLock;

/// A timestamped state value, used in snapshots.
pub type TimestampedStateValue = (String, OffsetDateTime);

/// A map of keys to timestamped values for a single scope.
pub type ScopedStateMap = AHashMap<String, TimestampedStateValue>;

//
// StateSnapshot
//

/// A simple snapshot of the state store, used for crash reporting and similar use cases.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
  pub feature_flags: ScopedStateMap,
  pub global_state: ScopedStateMap,
}

//
// StateEntry
//

/// A single entry in the state store, including its scope, key, value, and timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateEntry<'a> {
  pub scope: Scope,
  pub key: &'a str,
  pub value: &'a str,
  pub timestamp: OffsetDateTime,
}

//
// StateReader
//

/// A trait for reading state values. This allows for simple in-memory implementations in tests
/// without requiring async or filesystem access.
///
/// This trait provides a core `iter()` method that returns all entries, along with default
/// implementations for common filtering patterns.
pub trait StateReader {
  /// Gets a value from the state store.
  fn get(&self, scope: Scope, key: &str) -> Option<&str>;

  /// Returns an iterator over all entries in the state store.
  fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = StateEntry<'a>> + 'a>;

  /// Creates an owned snapshot of all entries in a specific scope.
  ///
  /// Returns a map of key -> (value, timestamp) for all entries in the scope.
  fn to_scoped_snapshot(&self, scope: Scope) -> ScopedStateMap {
    self
      .iter()
      .filter(move |entry| entry.scope == scope)
      .map(|entry| {
        (
          entry.key.to_string(),
          (entry.value.to_string(), entry.timestamp),
        )
      })
      .collect()
  }

  /// Creates an owned snapshot of the entire state store, organized by scope. Prefer this over
  /// calling `to_scoped_snapshot` multiple times to avoid multiple iterations.
  fn to_snapshot(&self) -> StateSnapshot {
    let mut feature_flags = AHashMap::new();
    let mut global_state = AHashMap::new();

    for entry in self.iter() {
      let kv = (entry.value.to_string(), entry.timestamp);
      match entry.scope {
        Scope::FeatureFlag => {
          feature_flags.insert(entry.key.to_string(), kv);
        },
        Scope::GlobalState => {
          global_state.insert(entry.key.to_string(), kv);
        },
      }
    }

    StateSnapshot {
      feature_flags,
      global_state,
    }
  }
}

//
// Store
//

/// Wraps a versioned key-value store for managing application state. This adds synchronization,
/// management of ephemeral scopes, and snapshot capabilities.
#[derive(Clone)]
pub struct Store {
  // Use a mutex since we want to be able to share the store across tasks, so we handle locking
  // at this level.
  inner: Arc<RwLock<bd_resilient_kv::VersionedKVStore>>,
}

impl Store {
  /// Creates a new `Store` instance at the given directory with the provided time provider.
  ///
  /// If there is an existing store, it will be loaded and a snapshot of the previous process's
  /// state will be captured before clearing all ephemeral scopes. This allows crash reporting
  /// to access the previous run's feature flags while ensuring the current process starts fresh.
  ///
  /// Both `FeatureFlag` and `GlobalState` scopes are cleared on each process start, requiring
  /// users to re-set these values.
  ///
  /// Returns:
  /// - The store (with ephemeral scopes cleared)
  /// - Data loss information from loading the persisted state
  /// - Snapshot of state from the previous process (before clearing ephemeral scopes)
  pub async fn new(
    directory: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, DataLoss, StateSnapshot)> {
    let (inner, data_loss) =
      bd_resilient_kv::VersionedKVStore::new(directory, "state", 1024 * 1024, None, time_provider)
        .await?;

    let store = Self {
      inner: Arc::new(RwLock::new(inner)),
    };

    // Capture a snapshot of the previous process's state before clearing ephemeral scopes.
    // This snapshot is used for crash reporting to include feature flags from the crashed process.
    let previous_snapshot = store.read().await.to_snapshot();

    // Clear ephemeral scopes so the current process starts with fresh state.
    // Users must re-set feature flags and global state on each process start.
    // TODO(snowp): Consider improving the overhead of clear by adding explicit support for
    // clearing by prefix in the underlying store, rather than iterating and removing individual
    // keys.
    // Ignore errors during clearing - we'll proceed with whatever state we have.
    let _ = store.clear(Scope::FeatureFlag).await;
    let _ = store.clear(Scope::GlobalState).await;

    Ok((store, data_loss, previous_snapshot))
  }

  pub async fn insert(&self, scope: Scope, key: &str, value: String) -> anyhow::Result<()> {
    self
      .inner
      .write()
      .await
      .insert(
        scope,
        key,
        StateValue {
          value_type: Value_type::StringValue(value).into(),
          ..Default::default()
        },
      )
      .await
      .map(|_| ())
  }

  pub async fn remove(&self, scope: Scope, key: &str) -> anyhow::Result<()> {
    self
      .inner
      .write()
      .await
      .remove(scope, key)
      .await
      .map(|_| ())
  }

  pub async fn clear(&self, scope: Scope) -> anyhow::Result<()> {
    let mut store = self.inner.write().await;
    let keys_to_remove: Vec<String> = store
      .as_hashmap()
      .keys()
      .filter(|(s, _)| *s == scope)
      .map(|(_, key)| key.clone())
      .collect_vec();

    // TODO(snowp): Ideally we should have built in support for batch deletions in the underlying
    // store. This leaves us open for partial deletions if something fails halfway through.
    for key in keys_to_remove {
      store.remove(scope, &key).await?;
    }

    Ok(())
  }

  /// Returns a reader for accessing state values.
  ///
  /// The returned reader holds a read lock on the store for its lifetime.
  pub async fn read(&self) -> impl StateReader + '_ {
    ReadLockedStoreGuard {
      guard: self.inner.read().await,
    }
  }
}

struct ReadLockedStoreGuard<'a> {
  guard: tokio::sync::RwLockReadGuard<'a, bd_resilient_kv::VersionedKVStore>,
}

impl StateReader for ReadLockedStoreGuard<'_> {
  fn get(&self, scope: Scope, key: &str) -> Option<&str> {
    self
      .guard
      .get(scope, key)
      .and_then(|v| v.has_string_value().then(|| v.string_value()))
  }

  fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = StateEntry<'a>> + 'a> {
    Box::new(
      self
        .guard
        .as_hashmap()
        .iter()
        .filter_map(|((scope, key), timestamped_value)| {
          let value = timestamped_value
            .value
            .has_string_value()
            .then(|| timestamped_value.value.string_value())?;

          let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(timestamped_value.timestamp) * 1_000,
          )
          .ok()?;

          Some(StateEntry {
            scope: *scope,
            key,
            value,
            timestamp,
          })
        }),
    )
  }
}
