// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{
  AnnotatedLogFields,
  InitParams,
  LogAttributesOverrides,
  LogLevel,
  LogMessage,
  LogType,
  Logger,
};
use bd_client_stats::test::TestTicker;
use bd_client_stats::FlushTrigger;
use bd_device::Store;
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::client::api::configuration_update_ack::Nack;
use bd_proto::protos::client::api::ConfigurationUpdate;
use bd_proto::protos::config::v1::config::{buffer_config, BufferConfigList};
use bd_runtime::runtime::FeatureFlag as _;
use bd_session::fixed::{self, UUIDCallbacks};
use bd_session::Strategy;
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_test_helpers::config_helper::{
  configuration_update,
  default_buffer_config,
  make_buffer_matcher_matching_everything,
};
use bd_test_helpers::metadata::EmptyMetadata;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::runtime::{make_update, ValueKind};
use bd_test_helpers::session::{DiskStorage, InMemoryStorage};
use bd_test_helpers::test_api_server::{ExpectedStreamEvent, StreamAction, StreamHandle};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use tokio::sync::mpsc;

/// Wait for a condition to be true, or panic after 5 seconds.
#[macro_export]
macro_rules! wait_for {
  ($condition:expr) => {
    let start = std::time::Instant::now();
    while !$condition {
      if start.elapsed() > 5.seconds() {
        panic!("Timeout waiting for condition");
      }
      std::thread::sleep(10.std_milliseconds());
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
}

impl Default for SetupOptions {
  fn default() -> Self {
    Self {
      sdk_directory: Arc::new(TempDir::with_prefix("sdk").unwrap()),
      metadata_provider: Arc::default(),
      disk_storage: false,
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
  pub current_api_stream: StreamHandle,
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

    let store = if options.disk_storage {
      Arc::new(Store::new(Box::new(DiskStorage::new(
        options.sdk_directory.path().join("store"),
      ))))
    } else {
      Arc::new(Store::new(Box::<InMemoryStorage>::default()))
    };
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
    })
    .with_client_stats_tickers(Box::new(flush_ticker), Box::new(upload_ticker))
    .with_internal_logger(true)
    .build_dedicated_thread()
    .unwrap();

    let logger_handle = logger.new_logger_handle();
    let current_api_stream = Self::do_stream_setup(&mut server);
    Self {
      logger,
      logger_handle,
      sdk_directory: options.sdk_directory,
      server,
      current_api_stream,
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

  pub fn do_stream_setup(
    server: &mut bd_test_helpers::test_api_server::ServerHandle,
  ) -> StreamHandle {
    let stream = server.blocking_next_stream().unwrap();
    assert!(stream.await_event_with_timeout(ExpectedStreamEvent::Handshake(None), 2.seconds(),));

    stream.blocking_stream_action(StreamAction::SendRuntime(make_update(
      Self::get_default_runtime_values(),
      "base".to_string(),
    )));

    let (_, response) = server.blocking_next_runtime_ack();
    assert!(response.nack.is_none());

    stream
  }

  pub fn get_default_runtime_values() -> Vec<(&'static str, ValueKind)> {
    vec![
      (
        bd_runtime::runtime::filters::FilterChainEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
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
    ]
  }

  pub fn flush_and_upload_stats(&self) {
    let (sender, receiver) = bd_completion::Sender::new();
    self
      .stats_flush_trigger
      .blocking_flush_for_test(Some(sender));
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
      false,
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
      true,
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

  pub fn upload_individual_logs(&mut self) {
    self
      .current_api_stream
      .blocking_stream_action(StreamAction::SendRuntime(make_update(
        vec![(
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          bd_test_helpers::runtime::ValueKind::Int(1),
        )],
        "base".to_string(),
      )));

    assert_eq!(
      self.server.blocking_next_runtime_ack().0,
      self.current_api_stream.id()
    );
  }

  pub fn send_configuration_update(&mut self, config: ConfigurationUpdate) -> Option<Nack> {
    self
      .current_api_stream
      .blocking_stream_action(StreamAction::SendConfiguration(config));
    let (stream_id, mut ack) = self.server.blocking_next_configuration_ack();
    assert_eq!(stream_id, self.current_api_stream.id());

    ack.nack.take()
  }

  pub fn send_runtime_update(&self) {
    let values = Self::get_default_runtime_values();

    self
      .current_api_stream
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
    self
      .sdk_directory
      .path()
      .join("workflows_state_snapshot.7.bin")
  }
}

impl Drop for Setup {
  fn drop(&mut self) {
    // Perform blocking shutdown to ensure that the ring buffers are released by the time Drop
    // completes.
    self.logger.shutdown(true);
  }
}
