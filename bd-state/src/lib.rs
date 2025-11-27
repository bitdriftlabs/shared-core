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

pub use self::InitStrategy::{InMemoryOnly, PersistentWithFallback};
use ahash::AHashMap;
use bd_resilient_kv::{DataLoss, RetentionRegistry, ScopedMaps, StateValue, Value_type};
pub use bd_resilient_kv::{PersistentStoreConfig, Scope};
use bd_time::TimeProvider;
use itertools::Itertools as _;
use std::path::Path;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{RwLock, watch};

/// A timestamped state value, used in snapshots.
pub type TimestampedStateValue = (String, OffsetDateTime);

/// A map of keys to timestamped values for a single scope.
pub type ScopedStateMap = AHashMap<String, TimestampedStateValue>;

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
  pub previous_state: ScopedMaps,
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
  pub previous_state: ScopedMaps,
  /// Whether fallback to in-memory storage occurred
  pub fallback_occurred: bool,
}

//
// InitStrategy
//

/// Strategy for initializing the state store, based on runtime configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitStrategy {
  /// Use persistent storage with automatic fallback to in-memory if initialization fails
  PersistentWithFallback,
  /// Use in-memory storage only
  InMemoryOnly,
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

/// A trait for reading state values. This pattern allows for non-async access to state values while
/// the underlying store may be async.
pub trait StateReader {
  /// Gets a value from the state store.
  fn get(&self, scope: Scope, key: &str) -> Option<&str>;

  /// Returns an iterator over all entries in the state store.
  fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = StateEntry<'a>> + 'a>;

  /// Returns the underlying scoped maps.
  fn as_scoped_maps(&self) -> &ScopedMaps;
}

//
// Store
//

/// Wraps a versioned key-value store for managing application state. This adds synchronization,
/// management of ephemeral scopes, and snapshot capabilities.
#[derive(Clone)]
pub struct Store {
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
  pub async fn persistent(
    directory: &Path,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<StoreInitResult> {
    let retention_registry = Arc::new(RetentionRegistry::new());
    let (inner, data_loss) = bd_resilient_kv::VersionedKVStore::new(
      directory,
      "state",
      config,
      time_provider,
      retention_registry,
    )
    .await?;

    // Capture a snapshot of the previous process's state before clearing ephemeral scopes.
    // This snapshot is used for crash reporting to include feature flags from the crashed process.
    let previous_snapshot = inner.as_hashmap().clone();
    let store = Self {
      inner: Arc::new(RwLock::new(inner)),
    };

    // Clear ephemeral scopes so the current process starts with fresh state.
    // Users must re-set feature flags and global state on each process start.

    // Ignore errors during clearing - we'll proceed with whatever state we have.
    let _ = store.clear(Scope::FeatureFlag).await;
    let _ = store.clear(Scope::GlobalState).await;

    Ok(StoreInitResult {
      store,
      data_loss,
      previous_state: previous_snapshot,
    })
  }

  /// Creates a new persistent Store, falling back to an in-memory store if initialization fails.
  ///
  /// This method never fails - if the persistent store cannot be initialized, it will
  /// return an in-memory store instead.
  pub async fn persistent_or_fallback(
    directory: &Path,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
  ) -> StoreInitWithFallbackResult {
    match Self::persistent(directory, config, time_provider.clone()).await {
      Ok(result) => StoreInitWithFallbackResult {
        store: result.store,
        data_loss: Some(result.data_loss),
        previous_state: result.previous_state,
        fallback_occurred: false,
      },
      Err(e) => {
        log::debug!(
          "Failed to initialize persistent state store: {e}, falling back to in-memory store"
        );
        let store = Self::in_memory(time_provider, None);
        StoreInitWithFallbackResult {
          store,
          data_loss: None,
          previous_state: ScopedMaps::default(),
          fallback_occurred: true,
        }
      },
    }
  }

  /// Creates a Store based on runtime configuration.
  ///
  /// This method monitors a runtime flag to determine whether to use persistent or in-memory
  /// storage. When persistent storage is enabled, it will automatically fall back to in-memory
  /// storage if initialization fails.
  ///
  /// # Arguments
  ///
  /// * `directory` - Directory for persistent storage
  /// * `config` - Configuration for persistent storage
  /// * `time_provider` - Time provider for timestamps
  /// * `use_persistent_storage` - Watch receiver for the runtime flag
  ///
  /// # Returns
  ///
  /// A `StoreInitWithFallbackResult` containing the initialized store and metadata about
  /// initialization (data loss, previous state snapshot, whether fallback occurred).
  pub async fn from_runtime_config(
    directory: &Path,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    use_persistent_storage: watch::Receiver<bool>,
  ) -> StoreInitWithFallbackResult {
    let strategy = if *use_persistent_storage.borrow() {
      InitStrategy::PersistentWithFallback
    } else {
      InitStrategy::InMemoryOnly
    };

    Self::from_strategy(directory, config, time_provider, strategy).await
  }

  /// Creates a Store based on an explicit initialization strategy.
  ///
  /// This is useful for testing the initialization logic without needing to set up
  /// runtime configuration.
  ///
  /// # Arguments
  ///
  /// * `directory` - Directory for persistent storage
  /// * `config` - Configuration for persistent storage
  /// * `time_provider` - Time provider for timestamps
  /// * `strategy` - The initialization strategy to use
  pub async fn from_strategy(
    directory: &Path,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    strategy: InitStrategy,
  ) -> StoreInitWithFallbackResult {
    match strategy {
      InitStrategy::PersistentWithFallback => {
        Self::persistent_or_fallback(directory, config, time_provider).await
      },
      InitStrategy::InMemoryOnly => StoreInitWithFallbackResult {
        store: Self::in_memory(time_provider, None),
        data_loss: None,
        previous_state: ScopedMaps::default(),
        fallback_occurred: false,
      },
    }
  }

  /// Creates a new in-memory Store that does not persist to disk.
  ///
  /// This is useful when persistent storage is not needed or when used as a fallback
  /// when the persistent store cannot be initialized.
  ///
  /// # Arguments
  ///
  /// * `time_provider` - Time provider for timestamps
  /// * `capacity` - Optional maximum number of entries. If None, no limit is enforced.
  #[must_use]
  pub fn in_memory(time_provider: Arc<dyn TimeProvider>, capacity: Option<usize>) -> Self {
    Self {
      inner: Arc::new(RwLock::new(
        bd_resilient_kv::VersionedKVStore::new_in_memory(time_provider, capacity),
      )),
    }
  }

  pub async fn insert(&self, scope: Scope, key: String, value: String) -> anyhow::Result<()> {
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
      .await?;

    Ok(())
  }

  pub async fn extend(
    &self,
    scope: Scope,
    entries: impl IntoIterator<Item = (String, String)>,
  ) -> anyhow::Result<()> {
    self
      .inner
      .write()
      .await
      .extend(
        entries
          .into_iter()
          .map(|(key, value)| {
            (
              scope,
              key,
              StateValue {
                value_type: Value_type::StringValue(value).into(),
                ..Default::default()
              },
            )
          })
          .collect(),
      )
      .await?;

    Ok(())
  }

  pub async fn remove(&self, scope: Scope, key: &str) -> anyhow::Result<()> {
    self.inner.write().await.remove(scope, key).await?;

    Ok(())
  }

  pub async fn clear(&self, scope: Scope) -> anyhow::Result<()> {
    let mut locked_store = self.inner.write().await;
    let keys_to_remove: Vec<String> = locked_store
      .as_hashmap()
      .iter()
      .filter(|(s, ..)| *s == scope)
      .map(|(_, key, _)| key.clone())
      .collect_vec();

    // TODO(snowp): Ideally we should have built in support for batch deletions in the
    // underlying store. This leaves us open for partial deletions if something fails halfway
    // through.
    for key in keys_to_remove {
      locked_store.remove(scope, &key).await?;
    }

    Ok(())
  }

  /// Returns a reader for accessing state values.
  ///
  /// The returned reader holds a read lock on the store for its lifetime.
  pub async fn read(&self) -> impl StateReader + '_ {
    self.inner.read().await
  }
}

impl StateReader for tokio::sync::RwLockReadGuard<'_, bd_resilient_kv::VersionedKVStore> {
  fn get(&self, scope: Scope, key: &str) -> Option<&str> {
    (**self)
      .get(scope, key)
      .and_then(|v| v.has_string_value().then(|| v.string_value()))
  }

  fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = StateEntry<'a>> + 'a> {
    Box::new(
      self
        .as_hashmap()
        .iter()
        .filter_map(|(scope, key, timestamped_value)| {
          let value = timestamped_value
            .value
            .has_string_value()
            .then(|| timestamped_value.value.string_value())?;

          let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(timestamped_value.timestamp) * 1_000,
          )
          .ok()?;

          Some(StateEntry {
            scope,
            key,
            value,
            timestamp,
          })
        }),
    )
  }

  fn as_scoped_maps(&self) -> &ScopedMaps {
    // TODO(snowp): Consider removing iter() and get() in favor of direct access to the hashmap?
    self.as_hashmap()
  }
}
