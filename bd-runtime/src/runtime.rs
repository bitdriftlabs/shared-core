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
use bd_client_common::payload_conversion::{FromResponse, IntoRequest, RuntimeConfigurationUpdate};
use bd_client_common::safe_file_cache::SafeFileCache;
use bd_client_common::{ConfigurationUpdate, HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE};
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ConfigurationUpdateAck,
  HandshakeRequest,
  RuntimeUpdate,
};
use bd_proto::protos::client::runtime::runtime::Value;
use bd_proto::protos::client::runtime::Runtime;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch::Ref;

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

  pub fn get_duration(&self, name: &str, default: time::Duration) -> time::Duration {
    self
      .runtime
      .values
      .get(name)
      .map(Value::uint_value)
      .map_or(default, |v| time::Duration::milliseconds(v.into()))
  }

  pub fn get_str<'a>(&'a self, name: &'static str, default: &'static str) -> &'a str {
    self
      .runtime
      .values
      .get(name)
      .map_or(default, Value::string_value)
  }

  pub fn get_string(&self, name: &str, default: String) -> String {
    self
      .runtime
      .values
      .get(name)
      .map_or(default, |v| v.string_value().to_string())
  }
}

/// Internal state used by the runtime loader.
struct LoaderState {
  /// The current snapshot, containing the most up to date set of runtime values.
  snapshot: Arc<Snapshot>,

  /// Tracks watches for each runtime key.
  watches: HashMap<&'static str, InternalWatchKind>,

  initialized: bool,
}

impl LoaderState {
  fn new(runtime: Runtime, version_nonce: Option<String>) -> Self {
    Self {
      snapshot: Arc::new(Snapshot::new(runtime, version_nonce)),
      watches: HashMap::new(),
      initialized: false,
    }
  }
}

// A simple config loader which works by resetting the entire runtime whenever we receive a
// configuration update from the backend.
pub struct ConfigLoader {
  state: Mutex<LoaderState>,
  file_cache: SafeFileCache<RuntimeUpdate>,
}

#[async_trait::async_trait]
impl ConfigurationUpdate for ConfigLoader {
  async fn try_apply_config(&self, response: &ApiResponse) -> Option<ApiRequest> {
    let update = RuntimeUpdate::from_response(response)?;
    log::debug!("applying runtime update: {}", update);
    self.update_snapshot(update).await;

    Some(
      RuntimeConfigurationUpdate(ConfigurationUpdateAck {
        last_applied_version_nonce: self.snapshot().nonce.clone().unwrap_or_default(),
        nack: None.into(),
        ..Default::default()
      })
      .into_request(),
    )
  }

  async fn try_load_persisted_config(&self) {
    self.handle_cached_config().await;
  }

  fn fill_handshake(&self, handshake: &mut HandshakeRequest) {
    handshake.runtime_version_nonce = self.snapshot().nonce.clone().unwrap_or_default();
  }

  async fn on_handshake_complete(&self, configuration_update_status: u32) {
    if configuration_update_status & HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE != 0 {
      self.file_cache.mark_safe().await;
    }
  }

  async fn mark_safe(&self) {
    self.file_cache.mark_safe().await;
  }
}

impl ConfigLoader {
  #[must_use]
  pub fn new(sdk_directory: &Path) -> Arc<Self> {
    Arc::new(Self {
      state: Mutex::new(LoaderState::new(Runtime::default(), None)),
      file_cache: SafeFileCache::new("runtime", sdk_directory),
    })
  }

  pub fn snapshot(&self) -> Arc<Snapshot> {
    self.state.lock().snapshot.clone()
  }

  /// Registers a watch for the runtime flag given by the provided type.
  pub fn register_watch<T, C: FeatureFlag<T>>(&self) -> anyhow::Result<Watch<T, C>>
  where
    Snapshot: ReadValue<T>,
    InternalWatchKind: TypedWatch<T> + From<(T, tokio::sync::watch::Sender<T>)>,
  {
    let mut l = self.state.lock();

    // If there is already a watch for this path, just return it directly.
    // TODO(snowp): If there are two flags that specify the same path we might be in trouble
    // since we just key off the path. Fix this.
    if let Some(existing_watch) = l.watches.get(C::path()) {
      return Ok(Watch {
        watch: existing_watch.typed_watch()?,
        _type: PhantomData,
      });
    }

    // Initialize the watch with the current (or default if absent) value.
    let (watch_tx, watch_rx) =
      tokio::sync::watch::channel(l.snapshot.read_value(C::path(), C::default()));

    l.watches.insert(C::path(), (C::default(), watch_tx).into());
    drop(l);

    Ok(Watch {
      watch: watch_rx,
      _type: PhantomData,
    })
  }

  fn send_if_modified<T: PartialEq + Display + Clone>(
    key: &str,
    snapshot: &Snapshot,
    internal_watch: &InternalWatch<T>,
  ) where
    Snapshot: ReadValue<T>,
  {
    let updated_value = snapshot.read_value(key, internal_watch.default.clone());

    // Only send a new value if it differs from the old one. This ensures that consumers only get
    // updates when the value has actually changed.
    let updated = internal_watch.watch.send_if_modified(|state| {
      if *state == updated_value {
        false
      } else {
        *state = updated_value.clone();
        true
      }
    });

    if updated {
      log::debug!("updated value of {key} to {updated_value}");
    }
  }

  async fn handle_cached_config(&self) {
    if let Some(runtime) = self.file_cache.handle_cached_config().await {
      self.update_snapshot_inner(&runtime);
    } else {
      self.state.lock().initialized = true;
    }
  }

  pub fn expect_initialized(&self) {
    debug_assert!(self.state.lock().initialized);
  }

  pub async fn update_snapshot(&self, runtime_update: &RuntimeUpdate) {
    self.update_snapshot_inner(runtime_update);
    self.file_cache.cache_update(runtime_update).await;
  }

  /// Updates the current runtime snapshot, updating all registered watchers as appropriate.
  fn update_snapshot_inner(&self, runtime_update: &RuntimeUpdate) {
    let mut l = self.state.lock();
    l.initialized = true;

    let snapshot = Arc::new(Snapshot::new(
      runtime_update.runtime.clone().unwrap_or_default(),
      runtime_update.version_nonce.clone().into(),
    ));

    // Update the value for each active watch if the data changed.
    for (k, mut watch) in &mut l.watches {
      match &mut watch {
        InternalWatchKind::Int(watch) => Self::send_if_modified(k, &snapshot, watch),
        InternalWatchKind::Bool(watch) => Self::send_if_modified(k, &snapshot, watch),
        InternalWatchKind::Duration(watch) => {
          Self::send_if_modified(k, &snapshot, watch);
        },
        InternalWatchKind::String(watch) => {
          Self::send_if_modified(k, &snapshot, watch);
        },
      }
    }

    l.snapshot = snapshot;
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

impl<T, P: FeatureFlag<T>> Watch<T, P> {
  /// Reads the latest record, and marks the watch as having seen the update. This can be used in
  /// conjunction with `changed()` to let the caller check if the flag has changed since the last
  /// time `read_mark_update()` has been called.
  pub fn read_mark_update(&mut self) -> Ref<'_, T> {
    // We use borrow_and_update to record the read. This enables us to use changed() to determine
    // whether there has been any new updates since read() was called.
    self.watch.borrow_and_update()
  }

  /// Performs a read without updating the watch to indicate that the value has been read. This
  /// won't effect `changed()`, but doesn't require `&mut self`.
  #[must_use]
  pub fn read(&self) -> Ref<'_, T> {
    self.watch.borrow()
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

pub type BoolWatch<P> = Watch<bool, P>;
pub type IntWatch<P> = Watch<u32, P>;
pub type DurationWatch<P> = Watch<time::Duration, P>;
pub type StringWatch<P> = Watch<String, P>;

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
/// against the watch channel.
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
  Duration(InternalWatch<time::Duration>),
  String(InternalWatch<String>),
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
define_primitive_flag_type!(time::Duration, Duration, get_duration);
define_primitive_flag_type!(String, String, get_string);

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

    impl $name {
      #[allow(unused)]
      pub fn register(
        loader: &$crate::runtime::ConfigLoader,
      ) -> anyhow::Result<$crate::runtime::Watch<$flag_type, Self>> {
        loader.register_watch()
      }
    }

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

/// Defines a statically typed boolean feature flag with the specified default, reading the
/// runtime value from the provided path.
#[macro_export]
macro_rules! string_feature_flag {
  ($name:tt, $path:literal, $default:expr) => {
    $crate::feature_flag!($name, String, $path, $default);
  };
}

#[macro_export]
macro_rules! duration_feature_flag {
  ($name:tt, $path:literal, $default:expr) => {
    $crate::feature_flag!($name, time::Duration, $path, $default);
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

pub mod crash_handling {
  // Controls the list of directories that the platform layer should monitor for crash reports.
  // This is a :-separated list of directories of platforms that may be of interest to the platform
  // layer.
  string_feature_flag!(
    CrashDirectories,
    "crash_handling.directories",
    String::new()
  );

  // Controls the list of paths that should be searched in order to attempt to determine the crash
  // reason.
  string_feature_flag!(
    CrashReasonPaths,
    "crash_handling.crash_reason_paths",
    String::new()
  );

  // Controls the list of paths that should be searched in order to attempt to determine the crash
  // details.
  string_feature_flag!(
    CrashDetailsPaths,
    "crash_handling.crash_details_paths",
    String::new()
  );
}

pub mod log_upload {
  use time::ext::NumericalDuration as _;

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

  // Continuous logs are uploaded in batches either when the batch size is hit or when the deadline
  // has been hit. This controls how long the client will wait before triggering a flush for a
  // batch that has not yet reached the batch limit.
  int_feature_flag!(
    BatchDeadlineFlag,
    "log_uploader.batch_deadline_ms",
    30 * 1000
  ); // 30s

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
  duration_feature_flag!(
    RatelimitPeriodFlag,
    "upload_ratelimit.period_ms",
    1.minutes()
  );

  // Controls the initial backoff interval used by the uploader when attempting to retry an upload.
  //
  // Note that the backoff is implemented as jittered backoff, meaning the each attempt is executed
  // in [0, current_backoff].
  duration_feature_flag!(
    RetryBackoffInitialFlag,
    "log_uploader.initial_retry_backoff_ms",
    30.seconds()
  );

  // Controls the maximum backoff interval used by the uploader when attempting to retry an upload.
  duration_feature_flag!(
    RetryBackoffMaxFlag,
    "log_uploader.max_retry_backoff_ms",
    30.minutes()
  );

  // Normally when logs are flushed from the trigger buffer we upload all logs in the buffer. This
  // flag allows adding a maximum lookback period, which has us drop all logs older than the
  // specified time. This is useful for limiting the amount of logs uploaded per trigger upload.
  duration_feature_flag!(
    FlushBufferLookbackWindow,
    "workflows.flush_buffer_lookback_ms",
    time::Duration::ZERO
  );
}

pub mod client_kill {
  use time::ext::NumericalDuration as _;

  // This flag is used to target specific sets of clients for kill/shutdown. The default is 0ms
  // which is a no-op. This flag can be used to shut down specific SDK versions, or anything else
  // the matching system supports.
  duration_feature_flag!(
    GenericKillDuration,
    "client_kill.generic_kill_duration_ms",
    time::Duration::ZERO
  );

  // This flag is specifically used when the client fails to authenticate with an unauthenticated
  // error. Generally this case will not be recoverable, but to account for the case in which
  // the SaaS sends unauthenticated by accident, we will kill the client for a certain amount of
  // time and then allow it to try again.
  duration_feature_flag!(
    UnauthenticatedKillDuration,
    "client_kill.unauthenticated_kill_duration_ms",
    1.days()
  );
}

pub mod resource_utilization {
  use time::ext::NumericalDuration as _;

  bool_feature_flag!(
    ResourceUtilizationEnabledFlag,
    "resource_utilization.enabled",
    false
  );

  duration_feature_flag!(
    ResourceUtilizationReportingIntervalFlag,
    "resource_utilization.reporting_interval_ms",
    6.seconds()
  );
}

pub mod session_replay {
  use time::ext::NumericalDuration as _;

  bool_feature_flag!(
    PeriodicScreensEnabledFlag,
    "session_replay.screens.enabled",
    false
  );

  duration_feature_flag!(
    ReportingIntervalFlag,
    "session_replay.screens.interval_ms",
    3.seconds()
  );

  bool_feature_flag!(
    ScreenshotsEnabledFlag,
    "session_replay.screenshots.enabled",
    false
  );
}

#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
pub mod api {
  use time::ext::NumericalDuration;

  // This controls the maximum backoff used when connecting to the API backend. The backoff delay
  // will never exceed the value configured here, though note that if the client has not yet
  // received a runtime configuration the default will always apply.
  duration_feature_flag!(
    MaxBackoffInterval,
    "api.max_backoff_interval_ms",
    20.minutes()
  );

  // This controls the initial backoff used when connecting to the API backend after a sucessful
  // handshake or initial attempt. A handshake resets the exponential back off, starting at this
  // value. Note that this is not the exact value it starts at, but the upper limit to the
  // randomized interval used initially.
  duration_feature_flag!(
    InitialBackoffInterval,
    "api.initial_backoff_interval_ms",
    500.milliseconds()
  );
}

pub mod stats {
  use time::ext::NumericalDuration as _;
  // This controls how often we flush periodic stats to the local aggregate file. Stats will be
  // aggregated to this file, pending an upload event controlled by the below flag.
  duration_feature_flag!(
    DirectStatFlushIntervalFlag,
    "stats.disk_flush_interval_ms",
    60.seconds()
  );

  // This controls how often we attempt to read from the aggregated file in order to prepare and
  // send a stats upload request. Note that this only comes into play whenever we are not actively
  // trying to upload a stats request (which can take longer if we are retrying or there is no
  // active API stream)
  duration_feature_flag!(
    UploadStatFlushIntervalFlag,
    "stats.upload_flush_interval_ms",
    60.seconds()
  );

  // The maximum number of pending stat upload files to keep on disk.
  int_feature_flag!(MaxAggregatedFilesFlag, "stats.max_aggregated_files", 10);

  // The maximum aggregation window of each pending file, in minutes.
  duration_feature_flag!(
    MaxAggregationWindowPerFileFlag,
    "stats.max_aggregation_window_per_file_ms",
    5.minutes()
  );

  // This controls how much tag cardinality we allow before rejecting new metrics *per workflow*.
  // This limit prevents unbounded growth of metrics, which could result in the system running out
  // of memory.
  int_feature_flag!(MaxDynamicCountersFlag, "stats.max_dynamic_stats", 500);
}

pub mod sleep_mode {
  use time::ext::NumericalDuration as _;

  // This is an override for "stats.upload_flush_interval_ms" when operating in sleep mode.
  duration_feature_flag!(
    UploadStatFlushIntervalFlag,
    "sleep_mode.stats_upload_flush_interval_ms",
    15.minutes()
  );
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
  use time::ext::NumericalDuration as _;

  // This controls how often we attempt to persist the complete state of the workflows to disk.
  // Note that this is not used as a consistent interval but instead sets a minimum amount of time
  // that must have elapsed between writing attempts.
  duration_feature_flag!(
    PersistenceWriteIntervalFlag,
    "workflows.persistence_write_interval_ms",
    1.seconds()
  );

  // The maximum number of workflow traversals that may be active.
  int_feature_flag!(
    TraversalsCountLimitFlag,
    "workflows.traversals_global_count_limit",
    200
  );

  // The interval at which workflows state persistence attempts to disk are made.
  duration_feature_flag!(
    StatePeriodicWriteIntervalFlag,
    "workflows.state_periodic_write_interval_ms",
    5.seconds()
  );
}

pub mod platform_events {
  // Controls whether the platform events listener is enabled or not. In practice, it determines
  // whether the platform layer emits events in response to various host platform-provided events
  // such as memory warnings or application lifecycle changes.
  bool_feature_flag!(ListenerEnabledFlag, "platform_events.enabled", false);
}

pub mod artifact_upload {
  bool_feature_flag!(Enabled, "artifact_upload.enabled", false);

  int_feature_flag!(MaxPendingEntries, "artifact_upload.max_pending_entries", 10);

  int_feature_flag!(BufferCountLimit, "artifact_upload.buffer_count_limit", 100);

  static ONE_MEGABYTE: u32 = 1024 * 1024; // 1 MiB in bytes

  int_feature_flag!(
    BufferByteLimit,
    "artifact_upload.buffer_byte_limit",
    ONE_MEGABYTE
  );
}

pub mod session_capture {
  // How many logs should be streamed as part of an explicit session capture request.
  //
  // Typically session capture is driven by remote workflow config that allows server side
  // control of the number of logs to stream. In the case of an explicit session capture being
  // requested by the client, this flag controls how many logs to stream.
  int_feature_flag!(
    StreamingLogCount,
    "session_capture.streaming_log_count",
    100_000
  );
}
