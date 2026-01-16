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
use bd_resilient_kv::{DataLoss, ScopedMaps, StateValue};
pub use bd_resilient_kv::{
  PersistentStoreConfig,
  RetentionHandle,
  RetentionRegistry,
  Scope,
  StateValue as Value,
  Value_type,
};
use bd_runtime::runtime::ConfigLoader;
use bd_time::{OffsetDateTimeExt, TimeProvider};
use itertools::Itertools as _;
use std::path::Path;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::RwLock;

/// A listener that gets notified when state changes occur.
///
/// The listener receives the timestamp of when the change occurred.
pub type StateChangeListener = Arc<dyn Fn(OffsetDateTime) + Send + Sync>;

/// Creates a `StateValue` from a string.
#[must_use]
pub fn string_value(s: impl Into<String>) -> Value {
  Value {
    value_type: Value_type::StringValue(s.into()).into(),
    ..Default::default()
  }
}

/// Creates a `StateValue` from an integer.
#[must_use]
pub fn int_value(i: i64) -> Value {
  Value {
    value_type: Value_type::IntValue(i).into(),
    ..Default::default()
  }
}

/// Creates a `StateValue` from a double.
#[must_use]
pub fn double_value(d: f64) -> Value {
  Value {
    value_type: Value_type::DoubleValue(d).into(),
    ..Default::default()
  }
}

/// Creates a `StateValue` from a boolean.
#[must_use]
pub fn bool_value(b: bool) -> Value {
  Value {
    value_type: Value_type::BoolValue(b).into(),
    ..Default::default()
  }
}

/// A timestamped state value, used in snapshots.
pub type TimestampedStateValue = (String, OffsetDateTime);

/// A map of keys to timestamped values for a single scope.
pub type ScopedStateMap = AHashMap<String, TimestampedStateValue>;

//
// StateChangeType
//

/// The type of state change that occurred.
#[derive(Debug, Clone, PartialEq)]
pub enum StateChangeType {
  /// A new key was inserted
  Inserted { value: StateValue },
  /// An existing key was updated
  Updated {
    old_value: StateValue,
    new_value: StateValue,
  },
  /// A key was removed
  Removed { old_value: StateValue },
  /// No change occurred (e.g., setting same value)
  NoChange,
}

//
// StateChange
//

/// Information about a state change operation.
#[derive(Debug, Clone, PartialEq)]
pub struct StateChange {
  pub scope: Scope,
  // TODO(snowp): Ideally we could return &str but avoid copies in this path, but in order to do
  // that we need to extend the lifetime of the write lock such that we can return the &str
  // references safely. For now we copy the strings but we could optimize this later if needed.
  pub key: String,
  pub change_type: StateChangeType,
  pub timestamp: OffsetDateTime,
}

impl StateChange {
  /// Creates a `StateChange` for an inserted value.
  #[must_use]
  pub fn inserted(
    scope: Scope,
    key: impl Into<String>,
    value: StateValue,
    timestamp: OffsetDateTime,
  ) -> Self {
    Self {
      scope,
      key: key.into(),
      change_type: StateChangeType::Inserted { value },
      timestamp,
    }
  }

  /// Creates a `StateChange` for an updated value.
  #[must_use]
  pub fn updated(
    scope: Scope,
    key: impl Into<String>,
    old_value: StateValue,
    new_value: StateValue,
    timestamp: OffsetDateTime,
  ) -> Self {
    Self {
      scope,
      key: key.into(),
      change_type: StateChangeType::Updated {
        old_value,
        new_value,
      },
      timestamp,
    }
  }

  /// Creates a `StateChange` for a removed value.
  #[must_use]
  pub fn removed(
    scope: Scope,
    key: impl Into<String>,
    old_value: StateValue,
    timestamp: OffsetDateTime,
  ) -> Self {
    Self {
      scope,
      key: key.into(),
      change_type: StateChangeType::Removed { old_value },
      timestamp,
    }
  }
}

//
// StateChanges
//

/// Result of operations that may modify multiple keys.
#[derive(Debug, Clone, Default)]
pub struct StateChanges {
  pub changes: Vec<StateChange>,
}

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
  /// Registry for managing snapshot retention across buffers. Each buffer should create a
  /// retention handle from this registry to prevent snapshots from being cleaned up while
  /// logs that reference them are still in the buffer.
  pub retention_registry: Arc<RetentionRegistry>,
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
  /// Registry for managing snapshot retention across buffers. Empty if fallback occurred.
  pub retention_registry: Arc<RetentionRegistry>,
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
#[derive(Debug, Clone, PartialEq)]
pub struct StateEntry {
  pub scope: Scope,
  pub key: String,
  pub value: StateValue,
  pub timestamp: OffsetDateTime,
}

//
// StateReader
//

/// A trait for reading state values. This pattern allows for non-async access to state values while
/// the underlying store may be async.
pub trait StateReader {
  /// Gets a reference to the raw state value from the store.
  fn get(&self, scope: Scope, key: &str) -> Option<&StateValue>;

  /// Returns an iterator over all entries in the state store.
  fn iter(&self) -> Box<dyn Iterator<Item = StateEntry> + '_>;

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
  /// Optional listener that gets notified when state changes occur.
  change_listener: Arc<Mutex<Option<StateChangeListener>>>,
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
  /// The directory will be created if it doesn't exist.
  pub async fn persistent(
    directory: &Path,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    stats: &bd_client_stats_store::Scope,
  ) -> anyhow::Result<StoreInitResult> {
    std::fs::create_dir_all(directory)?;

    let retention_registry = Arc::new(RetentionRegistry::new());
    let (inner, data_loss) = bd_resilient_kv::VersionedKVStore::new(
      directory,
      "state",
      config,
      time_provider,
      retention_registry.clone(),
      stats,
    )
    .await?;

    // Capture a snapshot of the previous process's state before clearing ephemeral scopes.
    // This snapshot is used for crash reporting to include feature flags from the crashed process.
    let previous_snapshot = inner.as_hashmap().clone();
    let store = Self {
      inner: Arc::new(RwLock::new(inner)),
      change_listener: Arc::new(Mutex::new(None)),
    };

    // Clear ephemeral scopes so the current process starts with fresh state.
    // Users must re-set feature flags and global state on each process start.

    // Ignore errors during clearing - we'll proceed with whatever state we have.
    let _ = store.clear(Scope::FeatureFlagExposure).await;
    let _ = store.clear(Scope::GlobalState).await;

    Ok(StoreInitResult {
      store,
      data_loss,
      previous_state: previous_snapshot,
      retention_registry,
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
    stats: &bd_client_stats_store::Scope,
  ) -> StoreInitWithFallbackResult {
    match Self::persistent(directory, config, time_provider.clone(), stats).await {
      Ok(result) => StoreInitWithFallbackResult {
        store: result.store,
        data_loss: Some(result.data_loss),
        previous_state: result.previous_state,
        fallback_occurred: false,
        retention_registry: result.retention_registry,
      },
      Err(e) => {
        log::debug!(
          "Failed to initialize persistent state store: {e}, falling back to in-memory store"
        );
        let store = Self::in_memory(time_provider, None, stats);
        StoreInitWithFallbackResult {
          store,
          data_loss: None,
          previous_state: ScopedMaps::default(),
          fallback_occurred: true,
          // In-memory store doesn't have snapshots to retain, but we still provide a registry
          // so callers don't need to handle the Option case.
          retention_registry: Arc::new(RetentionRegistry::new()),
        }
      },
    }
  }

  /// Creates a Store based on an initialization strategy.
  ///
  /// # Arguments
  ///
  /// * `directory` - Directory for persistent storage
  /// * `config` - Configuration for persistent storage
  /// * `time_provider` - Time provider for timestamps
  /// * `strategy` - The initialization strategy to use
  /// * `stats` - Stats scope for metrics
  pub async fn from_strategy(
    directory: &Path,
    config: PersistentStoreConfig,
    time_provider: Arc<dyn TimeProvider>,
    strategy: InitStrategy,
    stats: &bd_client_stats_store::Scope,
  ) -> StoreInitWithFallbackResult {
    match strategy {
      InitStrategy::PersistentWithFallback => {
        Self::persistent_or_fallback(directory, config, time_provider, stats).await
      },
      InitStrategy::InMemoryOnly => StoreInitWithFallbackResult {
        store: Self::in_memory(time_provider, None, stats),
        data_loss: None,
        previous_state: ScopedMaps::default(),
        fallback_occurred: false,
        retention_registry: Arc::new(RetentionRegistry::new()),
      },
    }
  }

  /// Creates a Store using configuration from the runtime loader.
  ///
  /// This method reads runtime flags to determine store configuration:
  /// - `state.use_persistent_storage`: Whether to use persistent or in-memory storage
  /// - `state.initial_buffer_size_bytes`: Initial buffer size for persistent storage
  /// - `state.max_capacity_bytes`: Max capacity (file size for persistent, entry count for
  ///   in-memory)
  ///
  /// When persistent storage is enabled, it will automatically fall back to in-memory storage
  /// if initialization fails.
  ///
  /// # Arguments
  ///
  /// * `directory` - Directory for persistent storage
  /// * `time_provider` - Time provider for timestamps
  /// * `runtime_loader` - Runtime configuration loader
  /// * `stats` - Stats scope for metrics
  pub async fn from_runtime(
    directory: &Path,
    time_provider: Arc<dyn TimeProvider>,
    runtime_loader: &ConfigLoader,
    stats: &bd_client_stats_store::Scope,
  ) -> StoreInitWithFallbackResult {
    let use_persistent_storage =
      *bd_runtime::runtime::state::UsePersistentStorage::register(runtime_loader)
        .into_inner()
        .borrow();

    let initial_buffer_size =
      *bd_runtime::runtime::state::InitialBufferSize::register(runtime_loader)
        .into_inner()
        .borrow() as usize;

    let max_capacity = *bd_runtime::runtime::state::MaxCapacity::register(runtime_loader)
      .into_inner()
      .borrow() as usize;

    if use_persistent_storage {
      let config = PersistentStoreConfig {
        initial_buffer_size,
        max_capacity_bytes: max_capacity,
        ..Default::default()
      };
      Self::from_strategy(
        directory,
        config,
        time_provider,
        InitStrategy::PersistentWithFallback,
        stats,
      )
      .await
    } else {
      // For in-memory, use max_capacity as the entry count limit
      StoreInitWithFallbackResult {
        store: Self::in_memory(time_provider, Some(max_capacity), stats),
        data_loss: None,
        previous_state: ScopedMaps::default(),
        fallback_occurred: false,
        retention_registry: Arc::new(RetentionRegistry::new()),
      }
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
  /// * `stats` - Stats scope for metrics
  #[must_use]
  pub fn in_memory(
    time_provider: Arc<dyn TimeProvider>,
    capacity: Option<usize>,
    stats: &bd_client_stats_store::Scope,
  ) -> Self {
    Self {
      inner: Arc::new(RwLock::new(
        bd_resilient_kv::VersionedKVStore::new_in_memory(time_provider, capacity, stats),
      )),
      change_listener: Arc::new(Mutex::new(None)),
    }
  }

  /// Sets a listener that will be notified whenever state changes.
  ///
  /// The listener receives the timestamp of the change. This is used for coordinating state
  /// snapshot uploads with log uploads - the logger needs to know when state has changed so it can
  /// upload the relevant snapshots before uploading logs.
  ///
  /// Only one listener can be active at a time; setting a new listener replaces any previous one.
  pub fn set_change_listener(&self, listener: StateChangeListener) {
    if let Ok(mut guard) = self.change_listener.lock() {
      *guard = Some(listener);
    }
  }

  /// Notifies the change listener if one is set.
  fn notify_change(&self, timestamp: OffsetDateTime) {
    if let Ok(guard) = self.change_listener.lock()
      && let Some(ref listener) = *guard
    {
      listener(timestamp);
    }
  }

  pub async fn insert(
    &self,
    scope: Scope,
    key: String,
    value: StateValue,
  ) -> anyhow::Result<StateChange> {
    let mut locked = self.inner.write().await;

    // Perform the insert and get both timestamp and old value in one operation
    let (timestamp_u64, old_state_value) = locked.insert(scope, key.clone(), value.clone()).await?;

    // Convert timestamp
    let timestamp =
      OffsetDateTime::from_unix_timestamp_micros(timestamp_u64.try_into().unwrap_or_default())
        .unwrap_or_else(|_| OffsetDateTime::now_utc());

    // Determine change type
    let change_type = match old_state_value {
      Some(old) if old == value => StateChangeType::NoChange,
      Some(old) => StateChangeType::Updated {
        old_value: old,
        new_value: value,
      },
      None => StateChangeType::Inserted { value },
    };

    // Notify the listener if state actually changed
    if !matches!(change_type, StateChangeType::NoChange) {
      self.notify_change(timestamp);
    }

    Ok(StateChange {
      scope,
      key,
      change_type,
      timestamp,
    })
  }

  // TODO(snowp): Extend should return StateChanges to track what was inserted/updated, but this
  // requires support from the underlying store to return old values for batch operations.
  pub async fn extend(
    &self,
    scope: Scope,
    entries: impl IntoIterator<Item = (String, StateValue)>,
  ) -> anyhow::Result<()> {
    let entries_vec: Vec<_> = entries
      .into_iter()
      .map(|(key, value)| (scope, key, value))
      .collect();

    let has_entries = !entries_vec.is_empty();

    self.inner.write().await.extend(entries_vec).await?;

    // Notify the listener if any entries were inserted.
    // Note: We can't tell if entries were actually new vs updates without tracking old values,
    // so we conservatively notify on any non-empty extend.
    if has_entries {
      self.notify_change(OffsetDateTime::now_utc());
    }

    Ok(())
  }

  pub async fn remove(&self, scope: Scope, key: &str) -> anyhow::Result<StateChange> {
    let mut locked = self.inner.write().await;

    let result = locked.remove(scope, key).await?;

    let (change_type, timestamp) = match result {
      Some((timestamp_u64, old_state_value)) => {
        let timestamp =
          OffsetDateTime::from_unix_timestamp_micros(timestamp_u64.try_into().unwrap_or_default())
            .unwrap_or_else(|_| OffsetDateTime::now_utc());

        let change_type = if old_state_value.value_type.is_some() {
          StateChangeType::Removed {
            old_value: old_state_value,
          }
        } else {
          StateChangeType::NoChange
        };

        (change_type, timestamp)
      },
      None => (StateChangeType::NoChange, OffsetDateTime::now_utc()),
    };

    // Notify the listener if state actually changed
    if !matches!(change_type, StateChangeType::NoChange) {
      self.notify_change(timestamp);
    }

    Ok(StateChange {
      scope,
      key: key.to_string(),
      change_type,
      timestamp,
    })
  }

  pub async fn clear(&self, scope: Scope) -> anyhow::Result<StateChanges> {
    let mut locked_store = self.inner.write().await;

    // Collect all keys in this scope
    let keys_to_remove: Vec<String> = locked_store
      .as_hashmap()
      .iter()
      .filter(|(s, ..)| *s == scope)
      .map(|(_, key, _)| key.clone())
      .collect_vec();

    let mut changes = Vec::new();
    let mut latest_timestamp = None;

    // TODO(snowp): Ideally we should have built in support for batch deletions in the
    // underlying store. This leaves us open for partial deletions if something fails halfway
    // through.
    for key in keys_to_remove {
      if let Some((timestamp_u64, old_state_value)) = locked_store.remove(scope, &key).await? {
        let timestamp =
          OffsetDateTime::from_unix_timestamp_micros(timestamp_u64.try_into().unwrap_or_default())
            .unwrap_or_else(|_| OffsetDateTime::now_utc());

        if old_state_value.value_type.is_some() {
          // Track the latest timestamp among all changes
          if latest_timestamp.is_none() || timestamp > latest_timestamp.unwrap_or(timestamp) {
            latest_timestamp = Some(timestamp);
          }
          changes.push(StateChange {
            scope,
            key,
            change_type: StateChangeType::Removed {
              old_value: old_state_value,
            },
            timestamp,
          });
        }
      }
    }

    // Notify the listener once with the latest timestamp if any changes occurred
    if let Some(ts) = latest_timestamp {
      self.notify_change(ts);
    }

    Ok(StateChanges { changes })
  }

  /// Returns a reader for accessing state values.
  ///
  /// The returned reader holds a read lock on the store for its lifetime.
  pub async fn read(&self) -> impl StateReader + '_ {
    self.inner.read().await
  }
}

impl StateReader for tokio::sync::RwLockReadGuard<'_, bd_resilient_kv::VersionedKVStore> {
  fn get(&self, scope: Scope, key: &str) -> Option<&StateValue> {
    (**self).get(scope, key)
  }

  fn iter(&self) -> Box<dyn Iterator<Item = StateEntry> + '_> {
    Box::new(
      self
        .as_hashmap()
        .iter()
        .filter_map(|(scope, key, timestamped_value)| {
          // Only include entries that have a value
          timestamped_value.value.value_type.as_ref()?;

          let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(timestamped_value.timestamp) * 1_000,
          )
          .ok()?;

          Some(StateEntry {
            scope,
            key: key.clone(),
            value: timestamped_value.value.clone(),
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
