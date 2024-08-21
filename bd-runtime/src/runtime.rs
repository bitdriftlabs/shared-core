// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./runtime_test.rs"]
mod runtime_test;

use anyhow::anyhow;
use bd_client_common::error::handle_unexpected;
use bd_proto::protos::client::api::RuntimeUpdate;
use bd_proto::protos::client::runtime::runtime::Value;
use bd_proto::protos::client::runtime::Runtime;
use protobuf::Message;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

//
// RuntimeManager
//

#[allow(clippy::module_name_repetitions)]
pub struct RuntimeManager {
  loader: Arc<ConfigLoader>,
}

impl RuntimeManager {
  /// Creates a new `RuntimeManager` which is responsible for applying configuration updates and a
  /// handle to the config loader which can be used to read runtime values.
  pub fn new(loader: Arc<ConfigLoader>) -> Self {
    Self { loader }
  }

  #[must_use]
  pub fn version_nonce(&self) -> Option<String> {
    self.loader.snapshot().nonce.clone()
  }

  // Processes a new runtime update, validaing the update and updating the cached version
  // if the update is valid. Returns the Nack responses to respond to the server with.
  pub fn process_update(&self, update: &RuntimeUpdate) {
    self.loader.update_snapshot(update);
  }

  pub fn server_is_available(&self) {
    self.loader.mark_safe();
  }

  pub fn handle_cached_config(&self) {
    self.loader.handle_cached_config();
  }
}

//
// Snapshot
//

#[derive(Debug, Default)]
pub struct Snapshot {
  runtime: Runtime,
  nonce: Option<String>,
}

impl Snapshot {
  pub const fn new(runtime: Runtime, nonce: Option<String>) -> Self {
    Self { runtime, nonce }
  }

  pub fn get_bool(&self, name: &str, default: bool) -> bool {
    self
      .runtime
      .values
      .get(name)
      .map_or(default, Value::bool_value)
  }

  pub fn get_integer(&self, name: &str, default: u32) -> u32 {
    self
      .runtime
      .values
      .get(name)
      .map_or(default, Value::uint_value)
  }

  pub fn get_string<'a>(&'a self, name: &'static str, default: &'static str) -> &'a str {
    self
      .runtime
      .values
      .get(name)
      .map_or(default, Value::string_value)
  }
}

/// Internal state used by the runtime loader.
struct LoaderState {
  /// The current snapshot, containing the most up to date set of runtime values.
  snapshot: Arc<Snapshot>,

  /// Tracks watches for each runtime key.
  watches: HashMap<String, InternalWatchKind>,
}

impl LoaderState {
  fn new(runtime: Runtime, version_nonce: Option<String>) -> Self {
    Self {
      snapshot: Arc::new(Snapshot::new(runtime, version_nonce)),
      watches: HashMap::new(),
    }
  }
}

// A simple config loader which works by resetting the entire runtime whenever we receive a
// configuration update from the backend.
pub struct ConfigLoader {
  state: Mutex<LoaderState>,

  /// The parent directory containing the two cached files.
  runtime_directory: PathBuf,
  /// The file containing the cache load retry counter, as a string integer (e.g. "0").
  retry_count_file: PathBuf,
  /// The file containing the cached runtime update protobuf.
  protobuf_file: PathBuf,

  /// The file that previously contained the runtime protobuf. We attempt to clean this up to
  /// remove stale data from the SDK directory.
  legacy_protobuf_file: PathBuf,

  /// Flag used to guard resetting the retry count file more than necessary.
  cached_config_validated: AtomicBool,
}

impl ConfigLoader {
  #[must_use]
  pub fn new(sdk_directory: &Path) -> Arc<Self> {
    let runtime_directory = sdk_directory.join("runtime");

    // Create the directory if it doesn't exist.
    let _ignored = std::fs::create_dir(&runtime_directory);

    Arc::new(Self {
      state: Mutex::new(LoaderState::new(Runtime::default(), None)),
      retry_count_file: runtime_directory.join("retry_count"),
      protobuf_file: runtime_directory.join("update.pb"),
      legacy_protobuf_file: runtime_directory.join("protobuf.pb"),
      runtime_directory,
      cached_config_validated: AtomicBool::new(false),
    })
  }

  /// Called to mark the cached config as "safe", meaning that we feel comfortable about letting
  /// the app continue to read this from disk. This should be called when we receive a handshake
  /// response from the server, as this implies that the runtime configuration is "good enough"
  /// for us to be able to receive an updated set of runtime config. Due to the way runtime values
  /// are read from the snapshot at any time, we're forced to rely on this kind of heuristic to
  /// attempt to validate that the runtime config doesn't cause a crash loop.
  pub fn mark_safe(&self) {
    // We load the config from cache only at startup, so we only need to update the file once.
    if !self.cached_config_validated.swap(true, Ordering::SeqCst) {
      // If this fails worst case we'll use a stale retry count and eventually disable caching.
      let _ignored = self.persist_cache_load_retry_count(0);
    }
  }

  fn persist_cache_load_retry_count(&self, retry_count: u32) -> anyhow::Result<()> {
    // This could fail, but by being defensive when we read this we should ideally worst case just
    // fall back to not reading from cache.
    std::fs::write(&self.retry_count_file, format!("{retry_count}").as_bytes())
      .map_err(|e| anyhow!("an io error occurred: {e}"))
  }

  pub fn handle_cached_config(&self) {
    // Attempt to load the cached config from disk. Should we run into any unexpected issues,
    // eagerly wipe out all the disk state by recreating the runtime directory. This should help
    // us clean up any dirty state we see on disk and avoid issues persisting between process
    // restarts. As the errors bubble up through the error handler we may find we can handle some
    // of these failures. We also wipe the cache on a few expected cases, like hitting the retry
    // limit or a partial cache state.
    if match self.try_load_cached_config() {
      Ok(reset_cache) => reset_cache,
      Err(e) => {
        handle_unexpected::<(), anyhow::Error>(Err(e), "runtime cache load");
        true
      },
    } {
      // Recreate the directory instead of deleting individual files, making sure we really clean
      // up any bad state.
      let _ignored = std::fs::remove_dir_all(&self.runtime_directory);
      let _ignored = std::fs::create_dir(&self.runtime_directory);
    }
  }

  // Attempts to apply cached configuration. Returns true if the underlying cache state should be
  // reset.
  fn try_load_cached_config(&self) -> anyhow::Result<bool> {
    // We expect at most two files in this directory: a protobuf.pb which contains the cached
    // runtime protobuf and a retry_count file which contains the number of times this cached
    // runtime has been attempted applied during startup. The idea behind the retry count is
    // allow a client that received bad configuration to eventually recover, avoiding an infinte
    // crash loop.

    // If either of the files don't exist, we're not going to try to load the config and we'll wipe
    // out the other file if it's there. This could handle naturally if the system shuts down in the
    // middle of caching config.
    if !self.retry_count_file.exists() || !self.protobuf_file.exists() {
      return Ok(true);
    }

    // If the retry count file contains invalid data we defensively bail on reading the cached
    // value. If we were to treat an empty file as count=0 we could theoretically find ourselves in
    // a loop where the file is not properly updated.
    let Ok(retry_count) = Self::parse_retry_count(
      &std::fs::read(&self.retry_count_file).map_err(|e| anyhow!("an io error ocurred: {e}"))?,
    ) else {
      return Ok(true);
    };

    log::debug!("loaded runtime file at retry count {}", retry_count);

    // Update the retry count before we apply the runtime. If this fails, we bail out (which will
    // attempt to clear the cache directory) and disable caching. We do this because being unable
    // to update the retry count may result in us getting stuck processing what we think is retry
    // 0 over and over again.
    self.persist_cache_load_retry_count(retry_count + 1)?;

    // TODO(snowp): Should we read this from runtime as well? It would make it possible for a bad
    // runtime config to accidentally set this really high, but right now this is not
    // configurable at all.
    if retry_count > 5 {
      return Ok(true);
    }

    // TODO(snowp): Add stats for how often the read fails beyond ENOENT.
    // TODO(snowp): When we add a callback, only trigger it if we successfully loaded the config.
    let runtime = RuntimeUpdate::parse_from_bytes(&std::fs::read(&self.protobuf_file)?)
      .map_err(|e| anyhow!("A protobuf error occurred: {e}"))?;

    self.update_snapshot_inner(&runtime);

    Ok(false)
  }

  fn parse_retry_count(data: &[u8]) -> anyhow::Result<u32> {
    Ok(std::str::from_utf8(data)?.parse()?)
  }

  pub fn snapshot(&self) -> Arc<Snapshot> {
    self.state.lock().unwrap().snapshot.clone()
  }

  /// Registers a watch for the runtime flag given by the provided type.
  pub fn register_watch<T, C: FeatureFlag<T>>(&self) -> anyhow::Result<Watch<T, C>>
  where
    Snapshot: ReadValue<T>,
    InternalWatchKind: TypedWatch<T> + From<(T, tokio::sync::watch::Sender<T>)>,
  {
    let mut l = self.state.lock().unwrap();

    // If there is already a watch for this path, just return it directly.
    // TODO(snowp): If there are two flags that specify the same path we might be in trouble
    // since we just key off the path. Fix this.
    if let Some(existing_watch) = l.watches.get(C::path()) {
      return Ok(Watch {
        watch: existing_watch.typed_watch()?,
        _type: PhantomData::<C> {},
      });
    }

    // Initialize the watch with the current (or default if absent) value.
    let (watch_tx, watch_rx) =
      tokio::sync::watch::channel(l.snapshot.read_value(C::path(), C::default()));

    l.watches
      .insert(C::path().to_string(), (C::default(), watch_tx).into());
    drop(l);

    Ok(Watch {
      watch: watch_rx,
      _type: PhantomData::<C> {},
    })
  }

  fn send_if_modified<T: PartialEq + Display + Copy>(
    key: &str,
    snapshot: &Snapshot,
    internal_watch: &InternalWatch<T>,
  ) where
    Snapshot: ReadValue<T>,
  {
    let updated_value = snapshot.read_value(key, internal_watch.default);

    // Only send a new value if it differs from the old one. This ensures that consumers only get
    // updates when the value has actually changed.
    let updated = internal_watch.watch.send_if_modified(|state| {
      if *state == updated_value {
        false
      } else {
        *state = updated_value;
        true
      }
    });

    if updated {
      log::debug!("updated value of {} to {}", key, updated_value);
    }
  }

  pub fn update_snapshot(&self, runtime_update: &RuntimeUpdate) {
    self.update_snapshot_inner(runtime_update);
    // Failing here is fine, worst case we'll use an old retry count or leave is missing, which
    // will eventually disable caching.
    let _ignored = self.persist_cache_load_retry_count(0);
  }

  /// Updates the current runtime snapshot, updating all registered watchers as appropriate.
  fn update_snapshot_inner(&self, runtime_update: &RuntimeUpdate) {
    {
      let mut l = self.state.lock().unwrap();

      let snapshot = Arc::new(Snapshot::new(
        runtime_update.runtime.clone().unwrap_or_default(),
        runtime_update.version_nonce.clone().into(),
      ));

      // Update the value for each active watch if the data changed.
      for (k, mut watch) in &mut l.watches {
        match &mut watch {
          InternalWatchKind::Int(watch) => Self::send_if_modified(k.as_str(), &snapshot, watch),
          InternalWatchKind::Bool(watch) => Self::send_if_modified(k.as_str(), &snapshot, watch),
        }
      }

      l.snapshot = snapshot;
    }

    // Drop the lock before we write to disk to reduce lock contention on the runtime snapshot.
    // We're operating within the single threaded executor, so there is no race to worry about
    // between multiple updates.
    self.cache_update(runtime_update);
  }

  fn cache_update(&self, runtime: &RuntimeUpdate) {
    // Remove the stale legacy file that we're no longer using.
    // TODO(snowp): Remove this once this has been live for a while.
    let _ignored = std::fs::remove_file(&self.legacy_protobuf_file);
    let _ignored = std::fs::write(&self.protobuf_file, runtime.write_to_bytes().unwrap());
  }
}

// TODO(snowp): Consider moving feature flags to their own crate and/or file.

// Feature flags
//
// A feature flag allows code to refer to a typed handle which wraps a
// `tokio::sync::watch::Receiver`, allowing the consumer to read the current value of said flag or
// check for changes without having to be concerned with underlying runtime path.

/// A runtime feature flag watcher. This can be used to read the current value, wait for changes or
/// to determine if there has been a recent change.
/// By embedding a `impl FeatureFlag<T>` type, we are able to embed a specific feature flag type
/// (defined by the `feature_flag!` macro), which helps two feature flags that read the same
/// type (e.g. u64) have different type signatures.
#[derive(Clone, Debug)]
pub struct Watch<T, P: FeatureFlag<T>> {
  watch: tokio::sync::watch::Receiver<T>,
  _type: PhantomData<P>,
}

// TODO(snowp): We'll want a different implementation for String
impl<T: Copy, P: FeatureFlag<T>> Watch<T, P> {
  /// Reads the latest record, and marks the watch as having seen the update. This can be used in
  /// conjunction with `changed()` to let the caller check if the flag has changed since the last
  /// time `read_mark_update()` has been called.
  pub fn read_mark_update(&mut self) -> T {
    // We use borrow_and_update to record the read. This enables us to use changed() to determine
    // whether there has been any new updates since read() was called.
    *self.watch.borrow_and_update()
  }

  /// Performs a read without updating the watch to indicate that the value has been read. This
  /// won't effect `changed()`, but doesn't require `&mut self`.
  #[must_use]
  pub fn read(&self) -> T {
    *self.watch.borrow()
  }

  /// Returns true if the underlying value has changed since the last time `read_mark_update` has
  /// been called.
  #[must_use]
  pub fn has_changed(&self) -> bool {
    // Returns Err if the sender has closed, which we just treat as no change since we're likely
    // shutting down at that point.
    self.watch.has_changed().unwrap_or(false)
  }

  pub async fn changed(&mut self) -> anyhow::Result<()> {
    self
      .watch
      .changed()
      .await
      .map_err(|_| anyhow!("runtime watch"))
  }

  /// Returns the inner watch, for use in code that can't depend on this crate.
  #[must_use]
  pub fn into_inner(self) -> tokio::sync::watch::Receiver<T> {
    self.watch
  }
}

// Helper around a Watch<u32> that converts a millisecond integer flag into a Duration.
#[derive(Clone, Debug)]
pub struct DurationWatch<P: FeatureFlag<u32>>(Watch<u32, P>);

impl<P: FeatureFlag<u32>> DurationWatch<P> {
  #[must_use]
  pub const fn wrap(watch: Watch<u32, P>) -> Self {
    Self(watch)
  }

  #[must_use]
  pub fn duration(&self) -> Duration {
    Duration::from_millis(self.0.read().into())
  }
}

//
// FeatureFlag
//

/// Defines the runtime path and default for a feature flag.
pub trait FeatureFlag<T> {
  /// The runtime path to use when reading this feature flag.
  fn path() -> &'static str;

  /// The default value to use if this feature flag is not used.
  fn default() -> T;
}

//
// InternalWatch
//

/// An internal representation of a registered watch, allowing for subscriptions to be made
/// agaisnt the watch channel.
pub struct InternalWatch<T> {
  default: T,
  watch: tokio::sync::watch::Sender<T>,
}

//
// InternalWatchKind
//

pub enum InternalWatchKind {
  Int(InternalWatch<u32>),
  Bool(InternalWatch<bool>),
}

//
// ReadValue
//

/// Reads a typed value. This is used to provide a type-generic function that can be used to read
/// from the snapshot.
pub trait ReadValue<T> {
  fn read_value(&self, path: &str, default: T) -> T;
}

//
// TypedWatch
//

/// Constructs a typed watch subscriber. This is used to provide a type-generic function that be
/// used to extract the correct watch type from a `InternalWatchKind`.
pub trait TypedWatch<T> {
  /// Returns an appropriately typed watch if the underlying object matches the requested type,
  /// otherwise an error is returned.
  fn typed_watch(&self) -> anyhow::Result<tokio::sync::watch::Receiver<T>>;
}

// Defines the necessary traits allowing for the feature_flag! macro to work for the different
// primitive types. Since the generic code assumes Copy this cannot currently be used for String
// flags.
macro_rules! define_primitive_flag_type {
  ($primitive:ty, $kind_type:tt, $getter:tt) => {
    impl From<($primitive, tokio::sync::watch::Sender<$primitive>)> for InternalWatchKind {
      fn from((default, watch): ($primitive, tokio::sync::watch::Sender<$primitive>)) -> Self {
        Self::$kind_type(InternalWatch { default, watch })
      }
    }

    impl ReadValue<$primitive> for Snapshot {
      fn read_value(&self, path: &str, default: $primitive) -> $primitive {
        self.$getter(path, default)
      }
    }

    impl TypedWatch<$primitive> for InternalWatchKind {
      fn typed_watch(&self) -> anyhow::Result<tokio::sync::watch::Receiver<$primitive>> {
        match self {
          Self::$kind_type(w) => Ok(w.watch.subscribe()),
          _ => Err(anyhow::anyhow!("Incompatible runtime subscription")),
        }
      }
    }
  };
}

define_primitive_flag_type!(u32, Int, get_integer);
define_primitive_flag_type!(bool, Bool, get_bool);

/// Defines a statically typed feature flag that reads runtime from a specific path, returning a
/// default value if the path is not set.
///
/// The first argument is the name of the generated type, which can be used to provide stronger
/// typing to feature flags and to ensure consistent default handling.
// TODO(snowp): Support conversion functions + validation on config load.
// TODO(snowp): Support the different integral types.
#[macro_export]
macro_rules! feature_flag {
  ($name:tt, $flag_type:ty, $path:literal, $default:expr) => {
    #[derive(Clone, Debug)]
    pub struct $name;

    // Define the FeatureFlag trait, allowing us to embed the path and the default value into the
    // type.
    impl $crate::runtime::FeatureFlag<$flag_type> for $name {
      fn path() -> &'static str {
        $path
      }

      fn default() -> $flag_type {
        $default
      }
    }
  };
}

/// Defines a statically typed integer feature flag with the specified default, reading the
/// runtime value from the provided path.
#[macro_export]
macro_rules! int_feature_flag {
  ($name:tt, $path:literal, $default:expr) => {
    $crate::feature_flag!($name, u32, $path, $default);
  };
}

/// Defines a statically typed boolean feature flag with the specified default, reading the
/// runtime value from the provided path.
#[macro_export]
macro_rules! bool_feature_flag {
  ($name:tt, $path:literal, $default:expr) => {
    $crate::feature_flag!($name, bool, $path, $default);
  };
}

// Below we keep track of all the feature flags used by the SDK. While we could declare them where
// they are used, this makes it easier to skim them at a glance and requiers less acrobatics in test
// to refer to these constants.
// TODO(snowp): Consider a dedicated file for this.

pub mod debugging {
  // Controls whether internal logging is enabled. When set to true, logging is enabled and logs are
  // attempted written to the buffer. This serves as an additional protection against the internal
  // logs flooding the system on top of buffer selection. We default this to false as runtime is not
  // cached, and this ensures clients won't perform any internal logging on startup before it
  // receives the runtime update.
  bool_feature_flag!(InternalLoggingFlag, "internal_logging.enabled", false);

  // Controls whether periodic internal logging is enabled. When set to `true` the SDK reports
  // periodic logs with general information about the number and types of emitted logs.
  bool_feature_flag!(
    PeriodicInternalLoggingFlag,
    "internal_logging.periodic_logs.enabled",
    false
  );
}

pub mod log_upload {
  // This controls the limit for how many logs to include in each log upload. This has slightly
  // different implications for the two buffer types:
  // - For continuous buffers, when logs are consumed, but we are below the batch size for the
  //   upload currently being prepared, we delay the upload until either there are more logs or the
  //   upload deadline (configured below) is hit.
  // - For trigger buffers, the batch size dictates how we chunk up the buffer that is to be
  //   uploaded. Because a trigger upload is conceptually a oneoff upload of the current state of
  //   the buffer, we never wait for more logs or delay uploads. When the buffer has no more logs,
  //   we upload the current batch regardless of size.
  int_feature_flag!(BatchSizeFlag, "log_uploader.batch_size", 1000);

  // This controls the limit for the total number of bytes to include in each log upload. See
  // BatchSizeFlag for how this limit affects the different buffer implementations.
  int_feature_flag!(
    BatchSizeBytesFlag,
    "log_uploader.batch_size_bytes",
    1024 * 1024 // 1 MiB
  );

  // Continuous logs are uploaded in batchs either when the batch size is hit or when the deadline
  // has been hit. This controls how long the client will wait before triggering a flush for a
  // batch that has not yet reached the batch limit.
  int_feature_flag!(
    BatchDeadlineFlag,
    "log_uploader.batch_deadline_ms",
    30 * 1000
  ); // 30s

  // This controls how often we poll continuous buffers to see if there are logs ready to be
  // consumed and added to the current batch.
  // TODO(snowp): This goes away once we start using a future to determine respond to new data
  // instead of polling.
  int_feature_flag!(
    ContinuousBufferPollIntervalFlag,
    "log_uploader.continuous_buffer_poll_interval_ms",
    1000
  ); // 1s

  // This controls how many times we'll retry an upload before giving up. Note that this tracks the
  // number of times the log upload is dispatched to the internal API mux, not direct attempts at
  // sending it over the wire. This has the effect that a retry attempt is only processed while
  // there is an active API stream, and so retry attempts will not be expended during periods where
  // the SDK is not connected to the backend. A consequence of this is that if the logger is
  // unable to connect to the backend for some time, the upload will remained queued for that entire
  // duration, which will eventually lead to newer logs being dropped.
  int_feature_flag!(RetryCountFlag, "log_uploader.retry_count", 10);

  // This controls the number of bytes we permit to be uploaded per time period (defined below).
  // Uploads from all buffers share the same ratelimit quota, so setting this limit too low could
  // result in uploads contending for what little quota is left, delaying uploads and causing an
  // increase in dropped logs (especially when trigger uploads are happening).
  //
  // Note that the ratelimit implementation is not super strict on expending the quotas, and will
  // allow a single request to go through if request_size > quota before disbling uploads until the
  // quota has been refreshed. This is done to prevent potentially blocking a large upload, and does
  // result in the ratelimit being somewhat inaccurate.
  //
  // TODO(snowp): Consider giving trigger uploads precedence compared to todays fair algorithm, as
  // starving the trigger upload is likely worse than the continuous one due to us locking the
  // trigger buffer.
  int_feature_flag!(
    RatelimitByteCountPerPeriodFlag,
    "upload_ratelimit.bytes_count_per_period",
    2 << 21 // 2 MiB
  );

  // This controls the interval at which the above quota is refreshed.
  //
  // Note that the quota is only refreshed when log uploads are being attempted, so if uploads are
  // done infrequently the refresh might happen less often than it should, resulting in more
  // aggressive limits than what the configured value would imply.
  int_feature_flag!(RatelimitPeriodFlag, "upload_ratelimit.period_ms", 6 * 1000); // 60s

  // Controls the initial backoff interval used by the uploader when attempting to retry an upload.
  //
  // Note that the backoff is implemented as jittered backoff, meaning the each attempt is executed
  // in [0, current_backoff].
  int_feature_flag!(
    RetryBackoffInitialFlag,
    "log_uploader.initial_retry_backoff_ms",
    30 * 1000
  ); // 30s

  // Controls the maximum backoff interval used by the uploader when attempting to retry an upload.
  int_feature_flag!(
    RetryBackoffMaxFlag,
    "log_uploader.max_retry_backoff_ms",
    30 * 60 * 1000
  ); // 30 min
}

pub mod resource_utilization {
  bool_feature_flag!(
    ResourceUtilizationEnabledFlag,
    "resource_utilization.enabled",
    false
  );

  int_feature_flag!(
    ResourceUtilizationReportingIntervalFlag,
    "resource_utilization.reporting_interval_ms",
    6_000
  ); // 6s
}

#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
pub mod api {
  use time::ext::NumericalDuration;

  // This controls the maximum backoff used when connecting to the API backend. The backoff delay
  // will never exceed the value configured here, though note that if the client has not yet
  // received a runtime configuration the default will always apply.
  int_feature_flag!(
    MaxBackoffInterval,
    "api.max_backoff_interval_ms",
    // This cast is safe since 5 minutes in ms fits within u32.
    5.minutes().whole_milliseconds() as u32
  );

  // This controls the initial backoff used when connecting to the API backend after a sucessful
  // handshake or initial attempt. A handshake resets the exponential back off, starting at this
  // value. Note that this is not the exact value it starts at, but the upper limit to the
  // randomized interval used initially.
  int_feature_flag!(
    InitialBackoffInterval,
    "api.initial_backoff_interval_ms",
    500
  );

  // Controls whether clients should compress uploaded payloads and advertise support for
  // compression to the API. To be removed once we prove that compression works fine.
  bool_feature_flag!(CompressionEnabled, "api.requests_compression_enabled", true);
}

pub mod stats {
  // This controls how often we flush periodic stats to the local aggregate file. Stats will be
  // aggregated to this file, pending an upload event controlled by the below flag.
  int_feature_flag!(
    DirectStatFlushIntervalFlag,
    "stats.disk_flush_interval_ms",
    60 * 1000
  );

  // This controls how often we attempt to read from the aggregrated file in order to prepare and
  // send a stats upload request. Note that this only comes into play whenever we are not actively
  // trying to upload a stats request (which can take longer if we are retrying or there is no
  // active API stream)
  int_feature_flag!(
    UploadStatFlushIntervalFlag,
    "stats.upload_flush_interval_ms",
    60 * 1000
  );

  // This controls how many unique counters we allow before rejecting new metrics. This limit
  // prevents unbounded growth of metrics, which could result in the system running out of memory.
  int_feature_flag!(MaxDynamicCountersFlag, "stats.max_dynamic_stats", 500);
}

pub mod buffers {
  // Controls the size of the streaming buffer that is created on-demand whenever the client
  // is told to stream logs.
  int_feature_flag!(
    StreamBufferSizeBytes,
    "buffers.stream_buffer_size_bytes",
    1024 * 1024 // 1 MiB
  );
}

pub mod workflows {
  // Controls whether workflows are enabled. This feature flag should never be enabled to any
  // real users. Its key is will be changed once workflows are ready to be shipped and only
  // then we can start rolling out this variable to real users.
  bool_feature_flag!(WorkflowsEnabledFlag, "workflows.enabled", false);

  // Controls whether workflows insights are collected for triggering workflow actions.
  bool_feature_flag!(
    WorkflowsInsightsEnabledFlag,
    "workflows.insights_enabled",
    false
  );

  // This controls how often we attempt to persist the complete state of the workflows to disk.
  // Note that this is not used as a consistent interval but instead sets a minimum amount of time
  // that must have elapsed between writing attempts.
  int_feature_flag!(
    PersistenceWriteIntervalFlag,
    "workflows.persistence_write_interval_ms",
    1000
  ); // 1s

  // The maximum number of workflow traversals that may be active.
  int_feature_flag!(
    TraversalsCountLimitFlag,
    "workflows.traversals_global_count_limit",
    200
  );

  // The interval at which workflows state persistence attempts to disk are made.
  int_feature_flag!(
    StatePeriodicWriteIntervalFlag,
    "workflows.state_periodic_write_interval_ms",
    5000
  ); // 5s
}

pub mod platform_events {
  // Controls whether the platform events listener is enabled or not. In practice, it determines
  // whether the platform layer emits events in response to various host platform-provided events
  // such as memory warnings or application lifecycle changes.
  bool_feature_flag!(ListenerEnabledFlag, "platform_events.enabled", false);
}
