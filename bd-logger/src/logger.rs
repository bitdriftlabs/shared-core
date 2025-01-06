// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "logger_test.rs"]
mod logger_test;

use crate::app_version::{AppVersion, AppVersionExtra, Repository};
use crate::async_log_buffer::{
  AsyncLogBuffer,
  AsyncLogBufferMessage,
  LogAttributesOverridesPreviousRunSessionID,
};
use crate::log_replay::LoggerReplay;
use crate::memory_bound::{self, Sender as MemoryBoundSender};
use crate::{app_version, MetadataProvider};
use bd_api::{Metadata, Platform};
use bd_client_stats_store::Scope;
use bd_log::warn_every;
use bd_log_metadata::AnnotatedLogFields;
use bd_log_primitives::{log_level, AnnotatedLogField, LogField, LogLevel, LogMessage};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_runtime::runtime::Snapshot;
use bd_session_replay::SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE;
use bd_shutdown::ComponentShutdownTrigger;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use time::ext::NumericalDuration;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct Stats {
  pub(crate) log_emission_counters: memory_bound::SendCounters,
  pub(crate) field_addition_counters: memory_bound::SendCounters,
  pub(crate) field_removal_counters: memory_bound::SendCounters,
  pub(crate) state_flushing_counters: memory_bound::SendCounters,
  pub(crate) session_replay_duration_histogram: bd_client_stats_store::Histogram,
}

impl Stats {
  fn new(stats: &Scope) -> Self {
    // replay session stats use separate scope for legacy reasons
    let replay_scope = stats.scope("replay");
    let stats_scope = stats.scope("logger");
    let async_log_buffer_scope = stats_scope.scope("async_log_buffer");

    Self {
      log_emission_counters: memory_bound::SendCounters::new(
        &async_log_buffer_scope,
        "log_enqueueing",
      ),
      field_addition_counters: memory_bound::SendCounters::new(
        &async_log_buffer_scope,
        "field_additions",
      ),
      field_removal_counters: memory_bound::SendCounters::new(
        &async_log_buffer_scope,
        "field_removals",
      ),
      state_flushing_counters: memory_bound::SendCounters::new(
        &async_log_buffer_scope,
        "state_flushing",
      ),
      session_replay_duration_histogram: replay_scope.histogram("capture_time_s"),
    }
  }
}

// This is a thread local guard that is used to prevent re-entrancy into the logger. By borrowing
// this guard during log processing, we can ensure that any code that is executed as part of
// platform callbacks will not contribute additional logs to the system. This helps avoid
// throughput starvation that can happen due to a feedback loop of logs being generated as part of
// processing logs.
// TODO(snowp): If we ever want to support async work as part of the callbacks we'll need to rework
// how this work, as we'll need to propagate the guard through the async work.
thread_local! {
  static LOGGER_GUARD: RefCell<()> = const { RefCell::new(()) };
}

/// Executes a closure with the logger guard borrowed. This is used to prevent re-entrancy into the
/// logger.
///
/// # Panics
/// This will panic if the logger guard has an outstanding borrowk, e.g. either if this function is
/// called again within the provided closure or some other code starts holding a borrow while
/// calling this.
pub fn with_thread_local_logger_guard<R>(f: impl FnOnce() -> R) -> R {
  LOGGER_GUARD.with(|cell| {
    let _guard = cell.borrow_mut();
    f()
  })
}

/// A handle to the logger that can be used to log messages. This is the primary interface for
/// submitting logs into the system.
pub struct LoggerHandle {
  tx: MemoryBoundSender<AsyncLogBufferMessage>,

  session_strategy: Arc<bd_session::Strategy>,
  device: Arc<bd_device::Device>,
  sdk_version: String,

  app_version_repo: app_version::Repository,

  stats: Stats,
}

impl LoggerHandle {
  /// Log a message with the given log level, log type, message, and fields. This will enqueue the
  /// log onto a bounded queue for further processing.
  pub fn log(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    attributes_overrides: Option<LogAttributesOverridesPreviousRunSessionID>,
    blocking: bool,
  ) {
    LOGGER_GUARD.with(|cell| {
      // We just need to see if we can borrow - no need to hold it for any longer than that, as
      // this indicates that nothing is holding a mut borrow. This also guards for a hypothetical
      // situation in which code in `enqueue_log` would try to mut borrow the guard.
      if cell.try_borrow().is_ok() {
        let result = AsyncLogBuffer::<LoggerReplay>::enqueue_log(
          &self.tx,
          log_level,
          log_type,
          message,
          fields,
          matching_fields,
          attributes_overrides,
          blocking,
        );

        self.stats.log_emission_counters.record(&result);

        if let Err(e) = result {
          warn_every!(15.seconds(), "dropping log: {:?}", e);
        };
      } else {
        warn_every!(
          15.seconds(),
          "dropping log: message {:?}: emitting logs from within a field provider is not allowed",
          message
        );
      }
    });
  }

  pub fn log_resource_utilization(&self, mut fields: AnnotatedLogFields, duration: time::Duration) {
    fields.push(AnnotatedLogField::new_ootb(
      "_duration_ms".into(),
      (duration.as_seconds_f64() * 1_000f64).to_string().into(),
    ));

    self.log(
      log_level::DEBUG,
      LogType::Resource,
      "".into(),
      fields,
      vec![],
      None,
      false,
    );
  }

  pub fn log_session_replay_screen(&self, fields: AnnotatedLogFields, duration: time::Duration) {
    self.log_session_replay("Screen captured", fields, duration);
  }

  pub fn log_session_replay_screenshot(
    &self,
    fields: AnnotatedLogFields,
    duration: time::Duration,
  ) {
    self.log_session_replay(SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE, fields, duration);
  }

  fn log_session_replay(
    &self,
    message: &str,
    mut fields: AnnotatedLogFields,
    duration: time::Duration,
  ) {
    fields.push(AnnotatedLogField::new_ootb(
      "_duration_ms".to_string(),
      (duration.as_seconds_f64() * 1_000f64).to_string().into(),
    ));

    self.log(
      log_level::INFO,
      LogType::Replay,
      message.into(),
      fields,
      vec![],
      None,
      false,
    );

    self
      .stats
      .session_replay_duration_histogram
      .observe(duration.as_seconds_f64());
  }

  pub fn log_sdk_start(&self, mut fields: AnnotatedLogFields, duration: time::Duration) {
    fields.extend(
      [
        AnnotatedLogField::new_ootb(
          "_duration_ms".into(),
          (duration.as_seconds_f64() * 1_000f64).to_string().into(),
        ),
        AnnotatedLogField::new_ootb("_sdk_version".into(), self.sdk_version.to_string().into()),
        AnnotatedLogField::new_ootb(
          "_session_strategy".into(),
          self.session_strategy.type_name().into(),
        ),
      ]
      .into_iter(),
    );

    self.log(
      log_level::INFO,
      LogType::Lifecycle,
      "SDKConfigured".into(),
      fields,
      vec![],
      None,
      false,
    );
  }

  #[must_use]
  pub fn should_log_app_update(
    &self,
    app_version: String,
    app_version_extra: AppVersionExtra,
  ) -> bool {
    let version = AppVersion {
      app_version,
      app_version_extra,
    };

    self.app_version_repo.has_changed(&version)
  }

  pub fn log_app_update(
    &self,
    app_version: String,
    app_version_extra: AppVersionExtra,
    app_install_size_bytes: Option<u64>,
    mut fields: AnnotatedLogFields,
    duration: time::Duration,
  ) {
    let version = AppVersion {
      app_version,
      app_version_extra,
    };

    let Some(previous_app_version) = self.app_version_repo.set(&version) else {
      return;
    };

    log::debug!("emitting app update event: {:?}", version);

    fields.push(AnnotatedLogField::new_ootb(
      "_duration_ms".into(),
      (duration.as_seconds_f64() * 1_000f64).to_string().into(),
    ));
    if let Some(app_install_size_bytes) = app_install_size_bytes {
      fields.push(AnnotatedLogField::new_ootb(
        "_app_install_size_bytes".into(),
        app_install_size_bytes.to_string().into(),
      ));
    }
    fields.push(AnnotatedLogField::new_ootb(
      "_previous_app_version".into(),
      previous_app_version.app_version.into(),
    ));
    fields.push(AnnotatedLogField::new_ootb(
      format!(
        "_previous_{}",
        previous_app_version.app_version_extra.name()
      ),
      previous_app_version.app_version_extra.string_value().into(),
    ));

    self.log(
      log_level::INFO,
      LogType::Lifecycle,
      "AppUpdated".into(),
      fields,
      vec![],
      None,
      false,
    );
  }

  pub fn add_log_field(&self, field: LogField) {
    LOGGER_GUARD.with(|cell| {
      if cell.try_borrow().is_ok() {
        let field_name = field.key.clone();
        let result = AsyncLogBuffer::<LoggerReplay>::add_log_field(&self.tx, field);

        self.stats.field_addition_counters.record(&result);

        if let Err(e) = result {
          log::warn!("failed to add {:?} log field: {e:?}", field_name);
        }
      } else {
        warn_every!(
          15.seconds(),
          "failed to add {:?} log field, adding log fields from within a field provider is not \
           allowed",
          field.key
        );
      }
    });
  }

  pub fn remove_log_field(&self, field_name: &str) {
    LOGGER_GUARD.with(|cell| {
      if cell.try_borrow().is_ok() {
        let result = AsyncLogBuffer::<LoggerReplay>::remove_log_field(&self.tx, field_name);

        self.stats.field_removal_counters.record(&result);

        if let Err(e) = result {
          log::warn!("failed to remove {:?} log field: {e:?}", field_name);
        }
      } else {
        warn_every!(
          15.seconds(),
          "failed to remove {:?} log field, adding log fields from within a field provider is not \
           allowed",
          field_name
        );
      }
    });
  }

  pub fn flush_state(&self, blocking: bool) {
    log::debug!("state flushing initiated");
    let result = AsyncLogBuffer::<LoggerReplay>::flush_state(&self.tx, blocking);
    self.stats.state_flushing_counters.record(&result);
  }

  #[must_use]
  pub fn session_id(&self) -> String {
    self.session_strategy.session_id()
  }

  pub fn start_new_session(&self) {
    LOGGER_GUARD.with(|cell| {
      if cell.try_borrow().is_ok() {
        self.session_strategy.start_new_session();
      } else {
        log::warn!(
          "failed to start a new session, the operation is not allowed from within a field \
           provider"
        );
      }
    });
  }

  #[must_use]
  pub fn device_id(&self) -> String {
    self.device.id()
  }
}

/// Initialization parameters that are required to start up the logger.
pub struct InitParams {
  pub sdk_directory: PathBuf,
  pub api_key: String,
  pub session_strategy: Arc<bd_session::Strategy>,

  pub store: Arc<bd_key_value::Store>,

  pub metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
  pub resource_utilization_target: Box<dyn bd_resource_utilization::Target + Send + Sync>,
  pub session_replay_target: Box<dyn bd_session_replay::Target + Send + Sync>,
  pub events_listener_target: Box<dyn bd_events::ListenerTarget + Send + Sync>,

  pub device: Arc<bd_device::Device>,

  /// The platform network implementation to use. This provides the implementation of the transport
  /// used to talk to the backend.
  pub network: Box<dyn bd_api::PlatformNetworkManager<bd_runtime::runtime::ConfigLoader>>,
  pub platform: Platform,
  // Static metadata used to identify the client when communicating with the backend.
  pub static_metadata: Arc<dyn Metadata + Send + Sync>,
}

/// A single logger instance. This manages the lifetime of the logger and can be used to access
/// other components of the logger. Logging itself happens via the thread local logger, see
/// `write_log`.
pub struct Logger {
  // This is used to facilitate shutdown, the caller needs to be able to take over both the sender
  // and option. Since Logger is generally used as an Arc, it is not possible to get a mut
  // reference to it, so we use a Mutex to perform this internal mutation on shutdown.
  // This will be None if the logger is used in managed mode, as we expect something else to own
  // the `ComponentShutdownTrigger` that is used to initiate shutdown.
  shutdown_state: Mutex<Option<ComponentShutdownTrigger>>,

  runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,

  async_log_buffer_tx: MemoryBoundSender<AsyncLogBufferMessage>,

  session_strategy: Arc<bd_session::Strategy>,
  device: Arc<bd_device::Device>,
  sdk_version: String,

  store: Arc<bd_key_value::Store>,

  pub(crate) stats: Stats,

  stats_scope: Scope,
}

impl Logger {
  pub fn new(
    shutdown_state: Option<ComponentShutdownTrigger>,
    runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,
    stats: Scope,
    async_log_buffer_tx: MemoryBoundSender<AsyncLogBufferMessage>,
    session_strategy: Arc<bd_session::Strategy>,
    device: Arc<bd_device::Device>,
    sdk_version: &str,
    store: Arc<bd_key_value::Store>,
  ) -> Self {
    Self {
      shutdown_state: Mutex::new(shutdown_state),
      session_strategy,
      device,
      sdk_version: sdk_version.to_string(),
      runtime_loader,
      async_log_buffer_tx,
      stats: Stats::new(&stats),
      stats_scope: stats,
      store,
    }
  }

  /// Create the SDK and corresponding buffer directory if it doesn't already exist.
  pub(crate) fn initialize_buffer_directory(directory: &Path) -> anyhow::Result<PathBuf> {
    let buffer_directory = directory.join("buffers");

    if let Err(e) = std::fs::create_dir_all(&buffer_directory) {
      anyhow::bail!("failed to create sdk buffer(s) directory: {:?}", e);
    }

    Ok(buffer_directory)
  }

  pub fn shutdown(&self, blocking: bool) {
    let shutdown_trigger = self.shutdown_state.lock().unwrap().take();

    if let Some(shutdown_trigger) = shutdown_trigger {
      if blocking {
        shutdown_trigger.shutdown_blocking();
      }
    }
  }

  pub fn runtime_snapshot(&self) -> Arc<Snapshot> {
    self.runtime_loader.snapshot()
  }

  pub fn stats(&self) -> Scope {
    self.stats_scope.clone()
  }

  pub fn new_logger_handle(&self) -> LoggerHandle {
    LoggerHandle {
      tx: self.async_log_buffer_tx.clone(),
      session_strategy: self.session_strategy.clone(),
      device: self.device.clone(),
      sdk_version: self.sdk_version.clone(),
      app_version_repo: Repository::new(self.store.clone()),
      stats: self.stats.clone(),
    }
  }
}

/// Helper wrapper for passing both ends of a mpsc channel around.
pub struct ChannelPair<T> {
  pub tx: Sender<T>,
  pub rx: Receiver<T>,
}

impl<T> From<(Sender<T>, Receiver<T>)> for ChannelPair<T> {
  fn from(value: (Sender<T>, Receiver<T>)) -> Self {
    Self {
      tx: value.0,
      rx: value.1,
    }
  }
}
