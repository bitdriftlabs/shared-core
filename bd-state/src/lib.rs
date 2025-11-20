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

/// In-memory storage type for the state store.
type InMemoryStateMap = AHashMap<(Scope, String), (String, OffsetDateTime)>;

//
// StoreInitResult
//

/// Result of initializing a Store with persistent storage.
///
/// Contains the initialized store, data loss information from loading persisted state,
/// and a snapshot of the previous process's state (captured before clearing ephemeral scopes).
pub struct StoreInitResult {
  /// The initialized store with ephemeral scopes cleared
  pub store: Store,
  /// Information about any data loss detected when loading the persisted state
  pub data_loss: DataLoss,
  /// Snapshot of state from the previous process, captured before clearing ephemeral scopes
  pub previous_state: StateSnapshot,
}

//
// StoreInitWithFallbackResult
//

/// Result of initializing a Store with automatic fallback to in-memory storage.
///
/// If persistent storage initialization fails, the store automatically falls back to
/// in-memory mode. The `fallback_occurred` flag indicates whether this happened.
pub struct StoreInitWithFallbackResult {
  /// The initialized store (either persistent or in-memory)
  pub store: Store,
  /// Data loss information (None if fallback to in-memory occurred)
  pub data_loss: Option<DataLoss>,
  /// Snapshot of previous state (empty if fallback occurred)
  pub previous_state: StateSnapshot,
  /// Whether fallback to in-memory storage occurred
  pub fallback_occurred: bool,
}

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

/// A trait for reading state values. This is used to abstract over different storage
/// implementations, such as persistent stores and in-memory stores. This pattern allows for
/// non-async access to state values while the underlying store may be async.
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
  inner: StoreInner,
}

#[derive(Clone)]
enum StoreInner {
  Persistent(Arc<RwLock<bd_resilient_kv::VersionedKVStore>>),
  InMemory(Arc<RwLock<InMemoryStateMap>>),
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
  #[allow(clippy::new_ret_no_self)]
  pub async fn new(
    directory: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<StoreInitResult> {
    // TODO(snowp): Ideally we start small and grow as needed rather than pre-allocating 1MB.
    let (inner, data_loss) =
      bd_resilient_kv::VersionedKVStore::new(directory, "state", 1024 * 1024, None, time_provider)
        .await?;

    let store = Self {
      inner: StoreInner::Persistent(Arc::new(RwLock::new(inner))),
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

    Ok(StoreInitResult {
      store,
      data_loss,
      previous_state: previous_snapshot,
    })
  }

  /// Creates a new Store, falling back to an in-memory store if initialization fails.
  ///
  /// This method never fails - if the persistent store cannot be initialized, it will
  /// return an in-memory store instead.
  pub async fn new_or_fallback(
    directory: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> StoreInitWithFallbackResult {
    match Self::new(directory, time_provider.clone()).await {
      Ok(result) => StoreInitWithFallbackResult {
        store: result.store,
        data_loss: Some(result.data_loss),
        previous_state: result.previous_state,
        fallback_occurred: false,
      },
      Err(e) => {
        log::warn!(
          "Failed to initialize persistent state store: {e}, falling back to in-memory store"
        );
        let store = Self::new_in_memory();
        let empty_snapshot = StateSnapshot {
          feature_flags: AHashMap::new(),
          global_state: AHashMap::new(),
        };
        StoreInitWithFallbackResult {
          store,
          data_loss: None,
          previous_state: empty_snapshot,
          fallback_occurred: true,
        }
      },
    }
  }

  /// Creates a new in-memory Store that does not persist to disk.
  ///
  /// This is useful when persistent storage is not needed or when used as a fallback
  /// when the persistent store cannot be initialized.
  #[must_use]
  pub fn new_in_memory() -> Self {
    Self {
      inner: StoreInner::InMemory(Arc::new(RwLock::new(AHashMap::new()))),
    }
  }

  pub async fn insert(&self, scope: Scope, key: &str, value: String) -> anyhow::Result<()> {
    match &self.inner {
      StoreInner::Persistent(store) => store
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
        .map(|_| ()),
      StoreInner::InMemory(map) => {
        map
          .write()
          .await
          .insert((scope, key.to_string()), (value, OffsetDateTime::now_utc()));
        Ok(())
      },
    }
  }

  pub async fn remove(&self, scope: Scope, key: &str) -> anyhow::Result<()> {
    match &self.inner {
      StoreInner::Persistent(store) => store.write().await.remove(scope, key).await.map(|_| ()),
      StoreInner::InMemory(map) => {
        map.write().await.remove(&(scope, key.to_string()));
        Ok(())
      },
    }
  }

  pub async fn clear(&self, scope: Scope) -> anyhow::Result<()> {
    match &self.inner {
      StoreInner::Persistent(store) => {
        let mut locked_store = store.write().await;
        let keys_to_remove: Vec<String> = locked_store
          .as_hashmap()
          .keys()
          .filter(|(s, _)| *s == scope)
          .map(|(_, key)| key.clone())
          .collect_vec();

        // TODO(snowp): Ideally we should have built in support for batch deletions in the
        // underlying store. This leaves us open for partial deletions if something fails halfway
        // through.
        for key in keys_to_remove {
          locked_store.remove(scope, &key).await?;
        }

        Ok(())
      },
      StoreInner::InMemory(map) => {
        let mut locked_map = map.write().await;
        locked_map.retain(|(s, _), _| *s != scope);
        Ok(())
      },
    }
  }

  /// Returns a reader for accessing state values.
  ///
  /// The returned reader holds a read lock on the store for its lifetime.
  pub async fn read(&self) -> impl StateReader + '_ {
    match &self.inner {
      StoreInner::Persistent(store) => ReadLockedStoreGuard::Persistent(store.read().await),
      StoreInner::InMemory(map) => ReadLockedStoreGuard::InMemory(map.read().await),
    }
  }
}

enum ReadLockedStoreGuard<'a> {
  Persistent(tokio::sync::RwLockReadGuard<'a, bd_resilient_kv::VersionedKVStore>),
  InMemory(tokio::sync::RwLockReadGuard<'a, InMemoryStateMap>),
}

impl StateReader for ReadLockedStoreGuard<'_> {
  fn get(&self, scope: Scope, key: &str) -> Option<&str> {
    match self {
      Self::Persistent(guard) => guard
        .get(scope, key)
        .and_then(|v| v.has_string_value().then(|| v.string_value())),
      Self::InMemory(map) => map
        .get(&(scope, key.to_string()))
        .map(|(value, _)| value.as_str()),
    }
  }

  fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = StateEntry<'a>> + 'a> {
    match self {
      Self::Persistent(guard) => Box::new(guard.as_hashmap().iter().filter_map(
        |((scope, key), timestamped_value)| {
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
        },
      )),
      Self::InMemory(map) => {
        Box::new(
          map
            .iter()
            .map(|((scope, key), (value, timestamp))| StateEntry {
              scope: *scope,
              key,
              value,
              timestamp: *timestamp,
            }),
        )
      },
    }
  }
}
