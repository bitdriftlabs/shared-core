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

use ahash::AHashMap;
use bd_resilient_kv::{DataLoss, StateValue, TimestampedValue, Value_type};
use bd_time::TimeProvider;
use itertools::Itertools as _;
use std::path::Path;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::RwLock;

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
      Self::FeatureFlag => "ff:",
      Self::GlobalState => "g:",
    }
  }
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
  /// Creates a new `Store` instance at the given directory with the provided time provider. If
  /// there is an existing store, it will be loaded. If there was any data loss during recovery,
  /// it will be indicated in the returned `DataLoss` value.
  ///
  /// Returns the store and any data loss information.
  pub async fn new(
    directory: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, DataLoss)> {
    let (inner, data_loss) =
      bd_resilient_kv::VersionedKVStore::new(directory, "state", 1024 * 1024, None, time_provider)
        .await?;
    Ok((
      Self {
        inner: Arc::new(RwLock::new(inner)),
      },
      data_loss,
    ))
  }

  pub async fn insert(&self, scope: Scope, key: &str, value: String) -> anyhow::Result<()> {
    let namespaced_key = format!("{}{key}", scope.as_prefix());
    self
      .inner
      .write()
      .await
      .insert(
        namespaced_key,
        StateValue {
          value_type: Value_type::StringValue(value).into(),
          ..Default::default()
        },
      )
      .await
      .map(|_| ())
  }

  pub async fn remove(&self, scope: Scope, key: &str) -> anyhow::Result<()> {
    let namespaced_key = format!("{}{key}", scope.as_prefix());
    self
      .inner
      .write()
      .await
      .remove(&namespaced_key)
      .await
      .map(|_| ())
  }

  pub async fn clear(&self, scope: Scope) -> anyhow::Result<()> {
    let prefix = scope.as_prefix();

    let mut store = self.inner.write().await;
    let keys_to_remove: Vec<String> = store
      .as_hashmap()
      .keys()
      .filter(|key| key.starts_with(prefix))
      .cloned()
      .collect_vec();

    // TODO(snowp): Ideally we should have built in support for batch deletions in the underlying
    // store. This leaves us open for partial deletions if something fails halfway through.
    for key in keys_to_remove {
      store.remove(&key).await?;
    }
    Ok(())
  }

  /// Iterates over all key-value pairs in a given scope.
  ///
  /// Returns an iterator that yields (key, value) tuples where the key has been stripped
  /// of its scope prefix.
  pub async fn iter_scope(&self, scope: Scope) -> impl IntoIterator<Item = (String, String)> {
    let prefix = scope.as_prefix();
    let prefix_len = prefix.len();

    self
      .inner
      .read()
      .await
      .as_hashmap()
      .iter()
      .filter(move |(key, _)| key.starts_with(&prefix))
      .filter_map(move |(key, timestamped_value)| {
        let stripped_key = &key[prefix_len ..];

        let value = timestamped_value
          .value
          .has_string_value()
          .then(|| timestamped_value.value.string_value())?;

        // TODO(snowp): With the way we are doing the locking etc we cannot return references
        // whcih seems suboptimal. Consider changing the API to take a closure to avoid allocations.
        Some((stripped_key.to_string(), value.to_string()))
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
    let prefix = scope.as_prefix();

    for (key, timestamped_value) in self.inner.read().await.as_hashmap() {
      let Some((value, timestamp)) = Self::to_string_and_timestamp(timestamped_value) else {
        continue;
      };

      if let Some(stripped_key) = key.strip_prefix(&prefix) {
        scoped_snapshot.insert(stripped_key.to_string(), (value.to_string(), timestamp));
      }
    }

    scoped_snapshot
  }

  fn to_string_and_timestamp(
    timestamped_value: &TimestampedValue,
  ) -> Option<(String, OffsetDateTime)> {
    // Neither of these should fail but we defensively return None if they do.

    let Some(value) = timestamped_value
      .value
      .has_string_value()
      .then(|| timestamped_value.value.string_value())
    else {
      return None;
    };

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
    for (key, timestamped_value) in self.inner.read().await.as_hashmap() {
      let Some((value, timestamp)) = Self::to_string_and_timestamp(timestamped_value) else {
        continue;
      };

      if let Some(stripped_key) = key.strip_prefix(Scope::FeatureFlag.as_prefix()) {
        feature_flags.insert(stripped_key.to_string(), (value.to_string(), timestamp));
      } else if let Some(stripped_key) = key.strip_prefix(Scope::GlobalState.as_prefix()) {
        global_state.insert(stripped_key.to_string(), (value.to_string(), timestamp));
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

impl<'a> StateReader for ReadLockedStoreGuard<'_> {
  fn get(&self, scope: Scope, key: &str) -> Option<&str> {
    let namespaced_key = format!("{}:{}", scope.as_prefix(), key);
    self
      .guard
      .get(&namespaced_key)
      .and_then(|v| v.has_string_value().then(|| v.string_value()))
  }
}

#[path = "./test.rs"]
pub mod test;
