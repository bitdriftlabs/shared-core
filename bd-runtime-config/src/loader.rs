// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_server_stats::stats::Scope;
use notify::event::{ModifyKind, RenameMode};
use notify::{recommended_watcher, Event, EventHandler, EventKind, RecommendedWatcher, Watcher};
use parking_lot::Mutex;
use prometheus::IntCounter;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// Type wrapper for a loaded configuration which may not exist.
pub type ConfigPtr<ConfigType> = Option<Arc<ConfigType>>;

//
// LoaderEvent
//

/// Loader events.
pub enum LoaderEvent {
  /// The loader's snapshot has been updated.
  SnapshotUpdated,
}

//
// LoaderError
//

/// Generic result wrapper.
#[derive(thiserror::Error, Debug)]
pub enum LoaderError {
  /// Error from the notify library.
  #[error("notify library error")]
  Notify(#[from] notify::Error),
}

//
// Loader
//

/// Provides asynchronous loading of an immutable configuration snapshot.
#[async_trait::async_trait]
pub trait Loader<ConfigType: ?Sized>: Send + Sync {
  /// Return a watch to the current immutable configuration snapshot, if any. Note that
  /// some implementations may guarantee that a configuration is always
  /// available. See implementations for more information.
  fn snapshot_watch(&self) -> watch::Receiver<ConfigPtr<ConfigType>>;

  /// Shutdown the loader and synchronously cleanup any resources including
  /// joining threads, etc. Multiple calls to this function are a NOP.
  async fn shutdown(&self);
}

//
// WatchedFileLoaderState
//

struct WatchedFileLoaderState {
  watcher: Option<RecommendedWatcher>,
  join_handle: Option<JoinHandle<()>>,
  shutdown: bool,
}

//
// Stats
//

/// Loader stats.
#[derive(Clone)]
pub struct Stats {
  /// Number of successful deserializations.
  pub deserialize_success: IntCounter,
  /// Number of failed deserializations.
  pub deserialize_failure: IntCounter,
  /// Number of failed file loads.
  pub io_failure: IntCounter,
}

impl Stats {
  /// Create a new stats object given a scope.
  #[must_use]
  pub fn new(scope: &Scope) -> Self {
    let deserialize_success = scope.counter("deserialize_success");
    let deserialize_failure = scope.counter("deserialize_failure");
    let io_failure = scope.counter("io_failure");
    Self {
      deserialize_success,
      deserialize_failure,
      io_failure,
    }
  }
}

impl StatsCallbacks for Stats {
  fn on_deserialize_success(&self) {
    self.deserialize_success.inc();
  }

  fn on_deserialize_failure(&self) {
    self.deserialize_failure.inc();
  }

  fn on_io_failure(&self) {
    self.io_failure.inc();
  }
}

//
// StatsCallbacks
//

/// Trait for loader stats callbacks.
pub trait StatsCallbacks: Send + 'static {
  /// Called on deserialize success.
  fn on_deserialize_success(&self);

  /// Called on deserialize failure.
  fn on_deserialize_failure(&self);

  /// Called on IO failure.
  fn on_io_failure(&self);
}

//
// AsyncEventEventHandler
//

// Send events from notify to tokio.
struct AsyncEventEventHandler {
  tx: mpsc::Sender<notify::Result<Event>>,
}

impl EventHandler for AsyncEventEventHandler {
  fn handle_event(&mut self, event: notify::Result<Event>) {
    let _ = self.tx.blocking_send(event);
  }
}

//
// WatchedFileLoader
//

/// See [`Self::new_loader`] for more information.
pub struct WatchedFileLoader<ConfigType: ?Sized> {
  snapshot_sender: watch::Sender<ConfigPtr<ConfigType>>,
  snapshot_receiver: watch::Receiver<ConfigPtr<ConfigType>>,
  state: Mutex<WatchedFileLoaderState>,
}

#[async_trait::async_trait]
impl<ConfigType: ?Sized + Send + Sync> Loader<ConfigType> for WatchedFileLoader<ConfigType> {
  fn snapshot_watch(&self) -> watch::Receiver<ConfigPtr<ConfigType>> {
    self.snapshot_receiver.clone()
  }

  async fn shutdown(&self) {
    // Cause watcher to drop which will also release the channel and cause the
    // thread to exit.
    log::info!("shutting down WatchedFileLoader thread");

    if let Some(join_handle) = {
      let mut guard = self.state.lock();
      if guard.shutdown {
        None
      } else {
        guard.watcher = None;
        guard.shutdown = true;
        Some(guard.join_handle.take().unwrap())
      }
    } {
      join_handle.await.unwrap();
    }

    log::info!("WatchedFileLoader thread shutdown complete");
  }
}

impl<ConfigType: ?Sized> Drop for WatchedFileLoader<ConfigType> {
  fn drop(&mut self) {
    if !self.state.lock().shutdown {
      log::warn!("dropping WatchedFileLoader without calling shutdown()");
    }
  }
}

impl<ConfigType: Send + Sync + 'static + ?Sized> WatchedFileLoader<ConfigType> {
  // The default fsevents backend only provides "any" and can't merge rename events. This makes it
  // difficult to avoid spurious reloads. This works on linux though with inotify.
  #[cfg(not(target_os = "linux"))]
  const fn rename_mode() -> RenameMode {
    RenameMode::Any
  }

  #[cfg(target_os = "linux")]
  const fn rename_mode() -> RenameMode {
    RenameMode::Both
  }

  /// Create a generic filesystem based loader that can deserialize YAML into a
  /// supplied type, and optionally run a conversion function to convert it to
  /// a different type. See
  /// [`crate::feature_flags::new_memory_feature_flags_loader`] for a concrete
  /// use of this method.
  ///
  /// Note that `directory_to_watch` watches for *renames* in this directory
  /// only without recursion. This limits watched changes and is required for
  /// Kubernetes `ConfigMap` deployments.
  ///
  /// A Kubernetes `ConfigMap` deployment might work as follows:
  /// 1. Mount `ConfigMap` to `/config_map/foo`.
  /// 2. Set `directory_to_watch` to `/config_map/foo`.
  /// 3. Set `file_to_load` to `/config_map/foo/foo.yaml`.
  ///
  /// Internally Kubernetes will create the following structure:
  /// 1. `/config_map/foo/real_data/foo.yaml`.
  /// 2. `/config_map/foo/..data` -> `/config_map/foo/real_data`.
  /// 3. `/config_map/foo/foo.yaml` -> `/config_map/foo/..data/foo.yaml`.
  ///
  /// Further data swaps will only rename the `..data` symlink.
  pub fn new_loader<
    SerializedType: DeserializeOwned,
    ConversionFunc: Fn(Option<SerializedType>) -> ConfigPtr<ConfigType> + Send + 'static,
  >(
    directory_to_watch: impl AsRef<std::path::Path>,
    file_to_load: impl AsRef<std::path::Path>,
    conversion_function: ConversionFunc,
    stats: impl StatsCallbacks,
  ) -> Result<Arc<dyn Loader<ConfigType>>, LoaderError> {
    let directory_to_watch = directory_to_watch.as_ref();
    let file_to_load = file_to_load.as_ref();

    let (tx, mut rx) = mpsc::channel(16);
    let mut watcher = recommended_watcher(AsyncEventEventHandler { tx })?;
    watcher.watch(directory_to_watch, notify::RecursiveMode::NonRecursive)?;

    let (snapshot_sender, snapshot_receiver) = watch::channel(Self::load_config(
      file_to_load,
      &conversion_function,
      &stats,
    ));

    let new_loader = Arc::new(Self {
      snapshot_sender,
      snapshot_receiver,
      state: Mutex::new(WatchedFileLoaderState {
        watcher: Some(watcher),
        join_handle: None,
        shutdown: false,
      }),
    });

    let cloned_loader = new_loader.clone();
    let cloned_file_to_load = std::path::PathBuf::from(file_to_load);
    // TODO(mattklein123): The way this is currently written will require 2 threads
    // for every watched file: 1 notify thread and 1 thread here to receive
    // channel events. If we want to watch a large number of files in a single
    // process we should allow individual loaders to watch multiple files.
    new_loader
      .state
      .lock()
      .join_handle
      .replace(tokio::spawn(async move {
        while let Some(Ok(event)) = rx.recv().await {
          log::debug!("got event: {event:?}");
          match event.kind {
            EventKind::Modify(ModifyKind::Name(mode)) if mode == Self::rename_mode() => {},
            _ => continue,
          }

          cloned_loader
            .snapshot_sender
            .send(Self::load_config(
              &cloned_file_to_load,
              &conversion_function,
              &stats,
            ))
            .unwrap();
        }

        log::debug!("loader notify task shutting down");
      }));

    Ok(new_loader)
  }

  fn load_config<
    SerializedType: DeserializeOwned,
    ConversionFunc: Fn(Option<SerializedType>) -> ConfigPtr<ConfigType>,
  >(
    file_to_load: &std::path::Path,
    conversion_function: &ConversionFunc,
    stats: &impl StatsCallbacks,
  ) -> ConfigPtr<ConfigType> {
    log::info!("attempting to reload: {file_to_load:?}");
    conversion_function(match std::fs::read_to_string(file_to_load) {
      Ok(file_data) => match serde_yaml::from_str::<SerializedType>(&file_data) {
        Ok(config) => {
          stats.on_deserialize_success();
          Some(config)
        },
        Err(e) => {
          log::warn!("unable to deserialize config: {e:?}");
          stats.on_deserialize_failure();
          None
        },
      },
      Err(e) => {
        log::warn!("unable to read config file: {e:?}");
        stats.on_io_failure();
        None
      },
    })
  }
}
