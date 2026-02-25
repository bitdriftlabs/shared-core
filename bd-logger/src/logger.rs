// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "logger_test.rs"]
mod logger_test;

use crate::app_version::{AppVersion, Repository};
use crate::async_log_buffer::{self, AsyncLogBuffer, LogAttributesOverrides};
use crate::log_replay::LoggerReplay;
use crate::{MetadataProvider, app_version};
use bd_api::Metadata;
use bd_bounded_buffer::{self};
use bd_client_stats_store::{Counter, Scope};
use bd_log::warn_every;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  LogFieldValue,
  LogLevel,
  LogMessage,
  log_level,
};
use bd_proto::protos::client::key_value::app_version::Extra as AppVersionExtra;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::Snapshot;
use bd_session_replay::SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE;
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use time::ext::NumericalDuration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct Stats {
  pub(crate) log_emission_counters: bd_bounded_buffer::SendCounters,
  pub(crate) state_flushing_counters: bd_bounded_buffer::SendCounters,
  pub(crate) session_replay_duration_histogram: bd_client_stats_store::Histogram,
  sleep_enabled: Counter,
  sleep_disabled: Counter,

  app_open: Counter,
}

pub enum ReportProcessingSession {
  Current,
  PreviousRun,
}

impl Stats {
  fn new(stats: &Scope) -> Self {
    // replay session stats use separate scope for legacy reasons
    let replay_scope = stats.scope("replay");
    let stats_scope = stats.scope("logger");
    let async_log_buffer_scope = stats_scope.scope("async_log_buffer");
    let sleep_scope = stats.scope("sleep");

    Self {
      log_emission_counters: bd_bounded_buffer::SendCounters::new(
        &async_log_buffer_scope,
        "log_enqueueing",
      ),
      state_flushing_counters: bd_bounded_buffer::SendCounters::new(
        &async_log_buffer_scope,
        "state_flushing",
      ),
      session_replay_duration_histogram: replay_scope.histogram("capture_time_s"),
      sleep_enabled: sleep_scope.counter_with_labels(
        "transitions",
        labels!(
          "state" => "enabled",
        ),
      ),
      sleep_disabled: sleep_scope.counter_with_labels(
        "transitions",
        labels!(
          "state" => "disabled",
        ),
      ),
      app_open: stats_scope.counter("app_open"),
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
/// This will panic if the logger guard has an outstanding borrow, e.g. either if this function is
/// called again within the provided closure or some other code starts holding a borrow while
/// calling this.
pub fn with_thread_local_logger_guard<R>(f: impl FnOnce() -> R) -> R {
  LOGGER_GUARD.with(|cell| {
    let _guard = cell.borrow_mut();
    f()
  })
}

/// Macro to execute a block of code with the reentrancy guard, logging a warning if the guard is
/// already held.
macro_rules! with_reentrancy_guard {
  ($f:expr, $msg:expr, $($args:expr),*) => {
    // We just need to see if we can borrow - no need to hold it for any longer than that, as
    // this indicates that nothing is holding a mut borrow.
    LOGGER_GUARD.with(|cell| {
      if cell.try_borrow().is_ok() {
        $f
      } else {
        warn_every!(30.seconds(), $msg, $($args),*);
      }
    });
  };
}

#[derive(Clone, Copy)]
pub enum Block {
  Yes(Duration),
  No,
}

impl From<Block> for bool {
  fn from(block: Block) -> Self {
    match block {
      Block::Yes(_) => true,
      Block::No => false,
    }
  }
}

//
// CaptureSession
//

#[derive(Default)]
pub struct CaptureSession(Option<&'static str>);

impl CaptureSession {
  #[must_use]
  pub fn capture_with_id(id: &'static str) -> Self {
    Self(Some(id))
  }
}

/// A handle to the logger that can be used to log messages. This is the primary interface for
/// submitting logs into the system.
pub struct LoggerHandle {
  tx: async_log_buffer::Sender,

  session_strategy: Arc<bd_session::Strategy>,
  device: Arc<bd_device::Device>,
  sdk_version: String,

  app_version_repo: app_version::Repository,

  stats: Stats,

  sleep_mode_active: watch::Sender<bool>,

  state_storage_fallback: Arc<AtomicBool>,
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
    attributes_overrides: Option<LogAttributesOverrides>,
    block: Block,
    capture_session: &CaptureSession,
  ) {
    with_reentrancy_guard!(
      {
        let result = AsyncLogBuffer::<LoggerReplay>::enqueue_log(
          &self.tx,
          log_level,
          log_type,
          message,
          fields,
          matching_fields,
          attributes_overrides,
          block,
          capture_session.0,
        );

        self.stats.log_emission_counters.record(&result);

        if let Err(e) = result {
          warn_every!(15.seconds(), "dropping log: {:?}", e);
        }
      },
      "failed to log {:?}, emitting logs from within a field provider is not allowed",
      message
    );
  }

  pub fn log_resource_utilization(&self, mut fields: AnnotatedLogFields, duration: time::Duration) {
    fields.insert(
      "_duration_ms".into(),
      AnnotatedLogField::new_ootb((duration.as_seconds_f64() * 1_000f64).to_string()),
    );

    self.log(
      log_level::DEBUG,
      LogType::RESOURCE,
      "".into(),
      fields,
      [].into(),
      None,
      Block::No,
      &CaptureSession::default(),
    );
  }

  pub fn transition_sleep_mode(&self, enable: bool) {
    self.sleep_mode_active.send_if_modified(|enabled| {
      if *enabled == enable {
        false
      } else {
        log::debug!("transitioning sleep mode to {enable}");
        *enabled = enable;
        if enable {
          self.stats.sleep_enabled.inc();
        } else {
          self.stats.sleep_disabled.inc();
        }
        true
      }
    });
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
    fields.insert(
      "_duration_ms".into(),
      AnnotatedLogField::new_ootb((duration.as_seconds_f64() * 1_000f64).to_string()),
    );

    self.log(
      log_level::INFO,
      LogType::REPLAY,
      message.into(),
      fields,
      [].into(),
      None,
      Block::No,
      &CaptureSession::default(),
    );

    self
      .stats
      .session_replay_duration_histogram
      .observe(duration.as_seconds_f64());
  }

  pub fn log_sdk_start(&self, mut fields: AnnotatedLogFields, duration: time::Duration) {
    fields.extend([
      (
        "_duration_ms".into(),
        AnnotatedLogField::new_ootb((duration.as_seconds_f64() * 1_000f64).to_string()),
      ),
      (
        "_sdk_version".into(),
        AnnotatedLogField::new_ootb(self.sdk_version.clone()),
      ),
      (
        "_session_strategy".into(),
        AnnotatedLogField::new_ootb(self.session_strategy.type_name()),
      ),
      (
        "_state_storage_fallback".into(),
        AnnotatedLogField::new_ootb(
          self
            .state_storage_fallback
            .load(Ordering::Relaxed)
            .to_string(),
        ),
      ),
    ]);

    self.log(
      log_level::INFO,
      LogType::LIFECYCLE,
      "SDKConfigured".into(),
      fields,
      [].into(),
      None,
      Block::No,
      &CaptureSession::default(),
    );
  }

  pub fn set_state_storage_fallback(&self, occurred: bool) {
    self
      .state_storage_fallback
      .store(occurred, Ordering::Relaxed);
  }

  #[must_use]
  pub fn should_log_app_update(
    &self,
    app_version: String,
    app_version_extra: AppVersionExtra,
  ) -> bool {
    self
      .app_version_repo
      .has_changed(&AppVersion::new(app_version, app_version_extra))
  }

  pub fn log_app_update(
    &self,
    app_version: String,
    app_version_extra: AppVersionExtra,
    app_install_size_bytes: Option<u64>,
    mut fields: AnnotatedLogFields,
    duration: time::Duration,
  ) {
    let version = AppVersion::new(app_version, app_version_extra);

    let Some(previous_app_version) = self.app_version_repo.set(&version) else {
      return;
    };

    log::debug!("emitting app update event: {version:?}");

    fields.insert(
      "_duration_ms".into(),
      AnnotatedLogField::new_ootb((duration.as_seconds_f64() * 1_000f64).to_string()),
    );
    if let Some(app_install_size_bytes) = app_install_size_bytes {
      fields.insert(
        "_app_install_size_bytes".into(),
        AnnotatedLogField::new_ootb(app_install_size_bytes.to_string()),
      );
    }
    fields.insert(
      "_previous_app_version".into(),
      AnnotatedLogField::new_ootb(previous_app_version.version.clone()),
    );
    if let Some(extra) = &previous_app_version.extra {
      match extra {
        AppVersionExtra::AppVersionCode(code) => {
          fields.insert(
            "_previous_app_version_code".into(),
            AnnotatedLogField::new_ootb(code.to_string()),
          );
        },
        AppVersionExtra::BuildNumber(build) => {
          fields.insert(
            "_previous_build_number".into(),
            AnnotatedLogField::new_ootb(build.clone()),
          );
        },
      }
    }

    self.log(
      log_level::INFO,
      LogType::LIFECYCLE,
      "AppUpdated".into(),
      fields,
      [].into(),
      None,
      Block::No,
      &CaptureSession::default(),
    );
  }

  pub fn add_log_field(&self, key: String, value: LogFieldValue) {
    with_reentrancy_guard!(
      {
        let field_name = key.clone();
        let result =
          self
            .tx
            .try_send_state_update(async_log_buffer::StateUpdateMessage::AddLogField(
              key, value,
            ));

        if let Err(e) = result {
          log::warn!("failed to add {field_name:?} log field: {e:?}");
        }
      },
      "failed to add {:?} log field, adding log fields from within a field provider is not allowed",
      key
    );
  }

  pub fn remove_log_field(&self, field_name: &str) {
    with_reentrancy_guard!(
      {
        let result =
          self
            .tx
            .try_send_state_update(async_log_buffer::StateUpdateMessage::RemoveLogField(
              field_name.to_string(),
            ));

        if let Err(e) = result {
          log::warn!("failed to remove {field_name:?} log field: {e:?}");
        }
      },
      "failed to remove {:?} log field, adding log fields from within a callback is not permitted",
      field_name
    );
  }

  pub fn set_feature_flag_exposure(&self, flag: String, variant: Option<String>) {
    with_reentrancy_guard!(
      {
        let result = self.tx.try_send_state_update(
          async_log_buffer::StateUpdateMessage::SetFeatureFlagExposure(flag, variant),
        );
        if let Err(e) = result {
          log::warn!("failed to set feature flag: {e:?}");
        }
      },
      "failed to set {:?} feature flag, setting flags from within a callback is not permitted",
      flag
    );
  }

  pub fn flush_state(&self, block: Block) {
    log::debug!("state flushing initiated");
    let result = self.tx.flush_state(block);
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

  // Static metadata used to identify the client when communicating with the backend.
  pub static_metadata: Arc<dyn Metadata + Send + Sync>,

  // Whether the logger should start in sleep mode. It can then be transitioned using the provided
  // transition APIs.
  pub start_in_sleep_mode: bool,
}

pub struct ReportProcessingRequest {
  /// Session to use in reports
  pub session: ReportProcessingSession,
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

  async_log_buffer_tx: async_log_buffer::Sender,
  report_processor_tx: Sender<ReportProcessingRequest>,

  session_strategy: Arc<bd_session::Strategy>,
  device: Arc<bd_device::Device>,
  sdk_version: String,

  store: Arc<bd_key_value::Store>,

  pub(crate) stats: Stats,

  stats_scope: Scope,

  sleep_mode_active: watch::Sender<bool>,

  state_storage_fallback: Arc<AtomicBool>,
}

impl Logger {
  pub fn new(
    shutdown_state: Option<ComponentShutdownTrigger>,
    runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,
    stats_scope: Scope,
    async_log_buffer_tx: async_log_buffer::Sender,
    report_processor_tx: Sender<ReportProcessingRequest>,
    session_strategy: Arc<bd_session::Strategy>,
    device: Arc<bd_device::Device>,
    sdk_version: &str,
    store: Arc<bd_key_value::Store>,
    sleep_mode_active: watch::Sender<bool>,
    state_storage_fallback: Arc<AtomicBool>,
  ) -> Self {
    let stats = Stats::new(&stats_scope);

    // record initial app open for this launch's logger
    stats.app_open.inc();

    Self {
      shutdown_state: Mutex::new(shutdown_state),
      session_strategy,
      device,
      sdk_version: sdk_version.to_string(),
      runtime_loader,
      async_log_buffer_tx,
      report_processor_tx,
      stats,
      stats_scope,
      store,
      sleep_mode_active,
      state_storage_fallback,
    }
  }

  /// Create the SDK and corresponding buffer directory if it doesn't already exist.
  pub(crate) fn initialize_buffer_directory(directory: &Path) -> anyhow::Result<PathBuf> {
    let buffer_directory = directory.join("buffers");

    if let Err(e) = std::fs::create_dir_all(&buffer_directory) {
      anyhow::bail!("failed to create sdk buffer(s) directory: {e:?}");
    }

    Ok(buffer_directory)
  }

  /// Handler for platform-level code to indicate that platform-specific
  /// processing is done and the artifacts are ready for dispatch and logging.
  /// The `session` parameter is used to determine which session ID is used for
  /// the logs, depending on whether the crash occurred in the current or prior
  /// session.
  pub fn process_crash_reports(&mut self, session: ReportProcessingSession) -> anyhow::Result<()> {
    Ok(
      self
        .report_processor_tx
        .try_send(ReportProcessingRequest { session })?,
    )
  }

  pub fn shutdown(&self, blocking: bool) {
    let shutdown_trigger = self.shutdown_state.lock().take();

    if let Some(shutdown_trigger) = shutdown_trigger
      && blocking
    {
      shutdown_trigger.shutdown_blocking();
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
      sleep_mode_active: self.sleep_mode_active.clone(),
      state_storage_fallback: self.state_storage_fallback.clone(),
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
