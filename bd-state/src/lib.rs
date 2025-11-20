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
use bd_resilient_kv::{DataLoss, StateValue, TimestampedValue, Value_type};
use bd_time::TimeProvider;
use itertools::Itertools as _;
use std::path::Path;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::RwLock;

//
// StateReader
//

/// A trait for reading state values. This allows for simple in-memory implementations in tests
/// without requiring async or filesystem access.
pub trait StateReader {
  /// Gets a value from the state store.
  fn get(&self, scope: Scope, key: &str) -> Option<&str>;
}

//
// StateSnapshot
//

/// A simple snapshot of the state store, used for crash reporting and similar use cases.
/// Contains (`feature_flags`, `global_state`) as separate hash maps.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
  pub feature_flags: AHashMap<String, (String, OffsetDateTime)>,
  pub global_state: AHashMap<String, (String, OffsetDateTime)>,
}

//
// StateStore
//

/// Wraps a versioned key-value store for managing application state with namespaced keys.
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
    let previous_snapshot = store.to_snapshot().await;

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

  /// Iterates over all key-value pairs in a given scope.
  ///
  /// Returns an iterator that yields (key, value) tuples.
  pub async fn iter_scope(&self, scope: Scope) -> impl IntoIterator<Item = (String, String)> {
    self
      .inner
      .read()
      .await
      .as_hashmap()
      .iter()
      .filter(move |((s, _), _)| *s == scope)
      .filter_map(move |((_, key), timestamped_value)| {
        let value = timestamped_value
          .value
          .has_string_value()
          .then(|| timestamped_value.value.string_value())?;

        // TODO(snowp): With the way we are doing the locking etc we cannot return references
        // whcih seems suboptimal. Consider changing the API to take a closure to avoid allocations.
        Some((key.clone(), value.to_string()))
      })
      .collect_vec()
  }

  pub async fn lock_for_read(&self) -> impl StateReader + '_ {
    ReadLockedStoreGuard {
      guard: self.inner.read().await,
    }
  }

  pub async fn to_scoped_snapshot(
    &self,
    scope: Scope,
  ) -> AHashMap<String, (String, OffsetDateTime)> {
    let mut scoped_snapshot = AHashMap::new();

    for ((s, key), timestamped_value) in self.inner.read().await.as_hashmap() {
      if *s != scope {
        continue;
      }

      let Some((value, timestamp)) = Self::to_string_and_timestamp(timestamped_value) else {
        continue;
      };

      scoped_snapshot.insert(key.clone(), (value.clone(), timestamp));
    }

    scoped_snapshot
  }

  fn to_string_and_timestamp(
    timestamped_value: &TimestampedValue,
  ) -> Option<(String, OffsetDateTime)> {
    // Neither of these should fail but we defensively return None if they do.

    let value = timestamped_value
      .value
      .has_string_value()
      .then(|| timestamped_value.value.string_value())?;

    let Ok(timestamp) =
      OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamped_value.timestamp) * 1_000)
    else {
      return None;
    };

    Some((value.to_string(), timestamp))
  }

  pub async fn to_snapshot(&self) -> StateSnapshot {
    let mut feature_flags = AHashMap::new();
    let mut global_state = AHashMap::new();

    // TODO(snowp): Move the snapshot capture to the ctor so we don't have to lock.
    for ((scope, key), timestamped_value) in self.inner.read().await.as_hashmap() {
      let Some((value, timestamp)) = Self::to_string_and_timestamp(timestamped_value) else {
        continue;
      };

      match scope {
        Scope::FeatureFlag => {
          feature_flags.insert(key.clone(), (value.clone(), timestamp));
        },
        Scope::GlobalState => {
          global_state.insert(key.clone(), (value.clone(), timestamp));
        },
      }
    }

    StateSnapshot {
      feature_flags,
      global_state,
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
}
