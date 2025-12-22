// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::logger::{Block, CaptureSession};
use crate::{
  AnnotatedLogFields,
  InitParams,
  LogAttributesOverrides,
  LogLevel,
  LogMessage,
  Logger,
  ReportProcessingSession,
};
use bd_client_stats::FlushTrigger;
use bd_device::Store;
use bd_noop_network::NoopNetwork;
use bd_proto::protos::client::api::ConfigurationUpdate;
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::client::api::configuration_update_ack::Nack;
use bd_proto::protos::config::v1::config::{BufferConfigList, buffer_config};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::FeatureFlag as _;
use bd_session::Strategy;
use bd_session::fixed::{self, UUIDCallbacks};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_test_helpers::config_helper::{
  configuration_update,
  default_buffer_config,
  make_buffer_matcher_matching_everything,
};
use bd_test_helpers::metadata::EmptyMetadata;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_test_helpers::session::{DiskStorage, in_memory_store};
use bd_test_helpers::test_api_server::{ExpectedStreamEvent, StreamAction, StreamHandle};
use bd_time::TimeProvider;
use bd_time::test::TestTicker;
use bd_workflows::engine::WORKFLOWS_STATE_FILE_NAME;
use std::sync::{Arc, atomic::AtomicUsize};
use tempfile::TempDir;
// removed unused import
use tokio::sync::mpsc;

/// Wait for a condition to be true, or panic after 5 seconds.
#[macro_export]
macro_rules! wait_for {
  ($condition:expr) => {
    let start = std::time::Instant::now();
    while !$condition {
      if start.elapsed() > std::time::Duration::from_secs(5) {
        panic!("Timeout waiting for condition");
      }
      std::thread::sleep(std::time::Duration::from_millis(10));
    }
  };
}

#[derive(Default)]
struct MockSessionReplayTarget {
  capture_screen_count: Arc<AtomicUsize>,
  capture_screenshot_count: Arc<AtomicUsize>,
}

impl bd_session_replay::Target for MockSessionReplayTarget {
  fn capture_screen(&self) {
    self
      .capture_screen_count
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  }

  fn capture_screenshot(&self) {
    self
      .capture_screenshot_count
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  }
}

//
// SetupOptions
//

pub struct SetupOptions {
  pub sdk_directory: Arc<TempDir>,
  pub metadata_provider: Arc<LogMetadata>,
  pub disk_storage: bool,
  pub start_in_sleep_mode: bool,
  pub time_provider: Option<Arc<dyn TimeProvider>>,
  pub extra_runtime_values: Vec<(&'static str, ValueKind)>,
}

impl Default for SetupOptions {
  fn default() -> Self {
    Self {
      sdk_directory: Arc::new(TempDir::with_prefix("sdk").unwrap()),
      metadata_provider: Arc::default(),
      disk_storage: false,
      start_in_sleep_mode: false,
      time_provider: None,
      extra_runtime_values: vec![],
    }
  }
}

//
// Setup
//

pub struct Setup {
  pub logger: Logger,
  pub logger_handle: crate::LoggerHandle,
  pub sdk_directory: Arc<TempDir>,
  pub server: Box<bd_test_helpers::test_api_server::ServerHandle>,
  pub current_api_stream: Option<StreamHandle>,
  pub store: Arc<Store>,

  pub capture_screen_count: Arc<AtomicUsize>,
  pub capture_screenshot_count: Arc<AtomicUsize>,

  _shutdown: ComponentShutdownTrigger,

  pub _stats_flush_tx: mpsc::Sender<()>,
  pub stats_upload_tx: mpsc::Sender<()>,
  pub stats_flush_trigger: FlushTrigger,
}

impl Setup {
  pub fn new() -> Self {
    Self::new_with_options(SetupOptions::default())
  }

  pub fn new_with_metadata(metadata_provider: Arc<LogMetadata>) -> Self {
    Self::new_with_options(SetupOptions {
      metadata_provider,
      ..Default::default()
    })
  }

  pub fn new_with_options(options: SetupOptions) -> Self {
    let mut server = bd_test_helpers::test_api_server::start_server(false, None);
    let shutdown = ComponentShutdownTrigger::default();

    let storage: Box<dyn bd_key_value::Storage> = if options.disk_storage {
      Box::new(DiskStorage::default())
    } else {
      Box::new(bd_test_helpers::session::InMemoryStorage::default())
    };

    let store = Arc::new(Store::new(storage));
    let device = Arc::new(bd_device::Device::new(store.clone()));

    let session_replay_target = Box::new(MockSessionReplayTarget::default());
    let capture_screen_count = session_replay_target.capture_screen_count.clone();
    let capture_screenshot_count = session_replay_target.capture_screenshot_count.clone();

    let (flush_tick_tx, flush_ticker) = TestTicker::new();
    let (upload_tick_tx, upload_ticker) = TestTicker::new();

    let (logger, _, flush_trigger) = crate::LoggerBuilder::new(InitParams {
      sdk_directory: options.sdk_directory.path().into(),
      api_key: "foo-api-key".to_string(),
      session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
        store.clone(),
        Arc::new(UUIDCallbacks),
      ))),
      metadata_provider: options.metadata_provider,
      resource_utilization_target: Box::new(EmptyTarget),
      session_replay_target,
      events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
      device,
      store: store.clone(),
      network: Box::new(Self::run_network(server.port, shutdown.make_shutdown())),
      static_metadata: Arc::new(EmptyMetadata),
      start_in_sleep_mode: options.start_in_sleep_mode,
    })
    .with_client_stats_tickers(Box::new(flush_ticker), Box::new(upload_ticker))
    .with_internal_logger(true)
    .with_time_provider(options.time_provider)
    .build_dedicated_thread()
    .unwrap();

    let logger_handle = logger.new_logger_handle();
    let current_api_stream = Self::do_stream_setup(
      &mut server,
      options.start_in_sleep_mode,
      options.extra_runtime_values,
    );
    Self {
      logger,
      logger_handle,
      sdk_directory: options.sdk_directory,
      server,
      current_api_stream: Some(current_api_stream),
      store,
      capture_screen_count,
      capture_screenshot_count,
      _shutdown: shutdown,
      _stats_flush_tx: flush_tick_tx,
      stats_upload_tx: upload_tick_tx,
      stats_flush_trigger: flush_trigger,
    }
  }

  pub fn run_network(port: u16, shutdown: ComponentShutdown) -> bd_hyper_network::Handle {
    bd_hyper_network::HyperNetwork::run_on_thread(&format!("http://localhost:{port}"), shutdown)
  }

  pub fn restart_stream(&mut self, expect_sleep_mode: bool) {
    self
      .current_api_stream()
      .blocking_stream_action(StreamAction::CloseStream);
    self.current_api_stream = Some(Self::do_stream_setup(
      &mut self.server,
      expect_sleep_mode,
      vec![],
    ));
  }

  pub fn do_stream_setup(
    server: &mut bd_test_helpers::test_api_server::ServerHandle,
    expect_sleep_mode: bool,
    extra_runtime_values: Vec<(&'static str, ValueKind)>,
  ) -> StreamHandle {
    let stream = server.blocking_next_stream().unwrap();
    assert!(stream.await_event_with_timeout(
      ExpectedStreamEvent::Handshake {
        matcher: None,
        sleep_mode: expect_sleep_mode
       },
      time::Duration::seconds(2),
    ));

    stream.blocking_stream_action(StreamAction::SendRuntime(make_update(
      Self::get_default_runtime_values()
        .into_iter()
        .chain(extra_runtime_values)
        .collect(),
      "base".to_string(),
    )));

    let (_, response) = server.blocking_next_runtime_ack();
    assert!(response.nack.is_none());

    stream
  }

  pub fn get_default_runtime_values() -> Vec<(&'static str, ValueKind)> {
    vec![
      (
        bd_runtime::runtime::platform_events::ListenerEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
        bd_test_helpers::runtime::ValueKind::Int(10),
      ),
      (
        "upload_ratelimit.bytes_count",
        bd_test_helpers::runtime::ValueKind::Int(100_000),
      ),
      (
        bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
        ValueKind::Bool(false),
      ),
      (
        bd_runtime::runtime::session_replay::ScreenshotsEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::session_replay::PeriodicScreensEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::sleep_mode::MinReconnectInterval::path(),
        ValueKind::Int(time::Duration::seconds(1).whole_milliseconds().try_into().unwrap()),
      ),
    ]
  }

  pub fn flush_and_upload_stats(&self) {
    let (sender, receiver) = bd_completion::Sender::new();
    self
      .stats_flush_trigger
      .blocking_flush_for_test(Some(sender))
      .unwrap();
    receiver.blocking_recv().unwrap();
    self.stats_upload_tx.blocking_send(()).unwrap();
  }

  pub fn log(
    &self,
    level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    attributes_overrides: Option<LogAttributesOverrides>,
  ) {
    self.logger_handle.log(
      level,
      log_type,
      message,
      fields,
      matching_fields,
      attributes_overrides,
      Block::No,
      &CaptureSession::default(),
    );
  }

  pub fn blocking_log(
    &self,
    level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
  ) {
    self.logger_handle.log(
      level,
      log_type,
      message,
      fields,
      matching_fields,
      None,
      Block::Yes(std::time::Duration::from_secs(15)),
      &CaptureSession::default(),
    );
  }

  pub fn log_with_session_capture(
    &self,
    level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
  ) {
    self.logger_handle.log(
      level,
      log_type,
      message,
      fields,
      matching_fields,
      None,
      Block::No,
      &CaptureSession::capture_with_id("test"),
    );
  }

  pub fn configure_stream_all_logs(&mut self) {
    self.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![default_buffer_config(
            buffer_config::Type::CONTINUOUS,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
  }

  pub fn current_api_stream(&self) -> &StreamHandle {
    self.current_api_stream.as_ref().unwrap()
  }

  pub fn upload_individual_logs(&mut self) {
    self
      .current_api_stream()
      .blocking_stream_action(StreamAction::SendRuntime(make_update(
        vec![(
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          bd_test_helpers::runtime::ValueKind::Int(1),
        )],
        "base".to_string(),
      )));

    assert_eq!(
      self.server.blocking_next_runtime_ack().0,
      self.current_api_stream().id()
    );
  }

  pub fn upload_crash_reports(&mut self, session: ReportProcessingSession) {
    if let Err(e) = self.logger.process_crash_reports(session) {
      panic!("failed to process reports: {e}");
    }
  }

  pub fn send_configuration_update(&mut self, config: ConfigurationUpdate) -> Option<Nack> {
    self
      .current_api_stream()
      .blocking_stream_action(StreamAction::SendConfiguration(config));
    let (stream_id, mut ack) = self.server.blocking_next_configuration_ack();
    assert_eq!(stream_id, self.current_api_stream().id());

    ack.nack.take()
  }

  pub fn send_runtime_update(&self) {
    let values = Self::get_default_runtime_values();

    self
      .current_api_stream()
      .blocking_stream_action(StreamAction::SendRuntime(make_update(
        values,
        "version".to_string(),
      )));
  }

  pub fn pending_aggregation_index_file_path(&self) -> std::path::PathBuf {
    self
      .sdk_directory
      .path()
      .join("stats_uploads/pending_aggregation_index.pb")
  }

  pub fn workflows_state_file_path(&self) -> std::path::PathBuf {
    self.sdk_directory.path().join(WORKFLOWS_STATE_FILE_NAME)
  }
}

impl Drop for Setup {
  fn drop(&mut self) {
    // Perform blocking shutdown to ensure that the ring buffers are released by the time Drop
    // completes.
    self.logger.shutdown(true);
  }
}

/// Creates minimal `InitParams` for testing without a server connection.
/// Useful for testing infrastructure-level concerns like directory locking.
pub fn create_minimal_init_params(
  sdk_directory: &std::path::Path,
) -> InitParams {
  let session_store = in_memory_store();
  let device_store = in_memory_store();

  InitParams {
    sdk_directory: sdk_directory.into(),
    api_key: "test-api-key".to_string(),
    session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
      session_store,
      Arc::new(UUIDCallbacks),
    ))),
    metadata_provider: Arc::new(LogMetadata::default()),
    resource_utilization_target: Box::new(EmptyTarget),
    session_replay_target: Box::new(bd_test_helpers::session_replay::NoOpTarget),
    events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
    device: Arc::new(bd_device::Device::new(device_store.clone())),
    store: device_store,
    network: Box::new(NoopNetwork),
    static_metadata: Arc::new(EmptyMetadata),
    start_in_sleep_mode: false,
  }
}
