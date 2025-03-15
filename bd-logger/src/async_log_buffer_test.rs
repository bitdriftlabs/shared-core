// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::AsyncLogBufferMessage;
use crate::async_log_buffer::{AsyncLogBuffer, LogLine, LogReplay};
use crate::bounded_buffer::{self, MemorySized};
use crate::client_config::TailConfigurations;
use crate::log_replay::{LoggerReplay, ProcessingPipeline};
use crate::logging_state::{BufferProducers, ConfigUpdate, UninitializedLoggingContext};
use bd_api::api::SimpleNetworkQualityProvider;
use bd_client_stats::{DynamicStats, FlushTrigger};
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::Collector;
use bd_key_value::Store;
use bd_log_filter::FilterChain;
use bd_log_metadata::{AnnotatedLogFields, LogFieldKind};
use bd_log_primitives::{log_level, AnnotatedLogField, Log, LogFields, StringOrBytes};
use bd_matcher::buffer_selector::BufferSelector;
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::filter::filter::FiltersConfiguration;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::fixed::UUIDCallbacks;
use bd_session::{fixed, Strategy};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::events::NoOpListenerTarget;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::runtime::ValueKind;
use bd_test_helpers::session::InMemoryStorage;
use bd_test_helpers::{state, workflow_proto};
use bd_time::TimeDurationExt;
use bd_workflows::config::{Config, WorkflowsConfiguration};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use time::ext::NumericalDuration;
use time::macros::datetime;
use tokio::sync::oneshot::Sender;
use tokio_test::assert_ok;

struct Setup {
  buffer_manager: Arc<bd_buffer::Manager>,
  runtime: Arc<ConfigLoader>,
  stats: Collector,
  dynamic_counter_stats: Arc<DynamicStats>,
  tmp_dir: Arc<tempfile::TempDir>,

  replayer_log_count: Arc<AtomicUsize>,
  replayer_logs: Arc<parking_lot::Mutex<Vec<String>>>,
  replayer_fields: Arc<parking_lot::Mutex<Vec<LogFields>>>,
  shutdown: Option<ComponentShutdownTrigger>,
}

impl Setup {
  fn new() -> Self {
    let tmp_dir = Arc::new(tempfile::TempDir::with_prefix("root-").unwrap());
    let runtime = &Self::make_runtime(&tmp_dir);
    let helper = Collector::default();
    let dynamic_counter_stats = Arc::new(DynamicStats::new(&helper.scope(""), runtime));

    Self {
      buffer_manager: bd_buffer::Manager::new(
        tmp_dir.path().join("buffer"),
        &helper.scope(""),
        runtime,
      )
      .0,
      runtime: Self::make_runtime(&tmp_dir),
      stats: helper,
      dynamic_counter_stats,
      tmp_dir,
      replayer_log_count: Arc::default(),
      replayer_logs: Arc::default(),
      replayer_fields: Arc::default(),
      shutdown: Some(ComponentShutdownTrigger::default()),
    }
  }

  fn shutdown_in(&mut self, duration: time::Duration) {
    let shutdown = self.shutdown.take().unwrap();
    tokio::spawn(async move {
      duration.sleep().await;
      shutdown.shutdown().await;
    });
  }


  fn make_test_async_log_buffer(
    &mut self,
    config_update_rx: tokio::sync::mpsc::Receiver<ConfigUpdate>,
  ) -> (
    AsyncLogBuffer<TestReplay>,
    bounded_buffer::Sender<AsyncLogBufferMessage>,
  ) {
    let replayer = TestReplay::new();
    self.replayer_log_count = replayer.logs_count.clone();
    self.replayer_logs = replayer.logs.clone();
    self.replayer_fields = replayer.fields.clone();

    AsyncLogBuffer::new(
      self.make_logging_context(),
      replayer,
      Arc::new(Strategy::Fixed(fixed::Strategy::new(
        Arc::new(Store::new(Box::<InMemoryStorage>::default())),
        Arc::new(UUIDCallbacks),
      ))),
      Arc::new(LogMetadata::default()),
      Box::new(EmptyTarget),
      Box::new(bd_test_helpers::session_replay::NoOpTarget),
      Box::new(NoOpListenerTarget),
      config_update_rx,
      self.shutdown.as_ref().unwrap().make_handle(),
      &self.runtime,
      Arc::new(SimpleNetworkQualityProvider::default()),
      String::new(),
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
    )
  }

  fn make_real_async_log_buffer(
    &self,
    config_update_rx: tokio::sync::mpsc::Receiver<ConfigUpdate>,
  ) -> (
    AsyncLogBuffer<LoggerReplay>,
    bounded_buffer::Sender<AsyncLogBufferMessage>,
  ) {
    AsyncLogBuffer::new(
      self.make_logging_context(),
      LoggerReplay {},
      Arc::new(Strategy::Fixed(fixed::Strategy::new(
        Arc::new(Store::new(Box::<InMemoryStorage>::default())),
        Arc::new(UUIDCallbacks),
      ))),
      Arc::new(LogMetadata::default()),
      Box::new(EmptyTarget),
      Box::new(bd_test_helpers::session_replay::NoOpTarget),
      Box::new(NoOpListenerTarget),
      config_update_rx,
      self.shutdown.as_ref().unwrap().make_handle(),
      &self.runtime,
      Arc::new(SimpleNetworkQualityProvider::default()),
      String::new(),
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
    )
  }

  fn make_logging_context(&self) -> UninitializedLoggingContext<Log> {
    let (trigger_upload_tx, _) = tokio::sync::mpsc::channel(1);
    let (data_upload_tx, _) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, _) = tokio::sync::mpsc::channel(1);
    let (flush_stats_trigger, _) = FlushTrigger::new();

    UninitializedLoggingContext::new(
      self.tmp_dir.path(),
      &self.runtime,
      self.stats.scope(""),
      self.dynamic_counter_stats.clone(),
      trigger_upload_tx,
      data_upload_tx,
      flush_buffers_tx,
      Some(flush_stats_trigger),
      5_000,
      1_000_000,
    )
  }

  fn make_config_update(&self, workflows_configuration: WorkflowsConfiguration) -> ConfigUpdate {
    ConfigUpdate {
      buffer_producers: BufferProducers::new(&self.buffer_manager).unwrap(),
      buffer_selector: BufferSelector::new(&BufferConfigList::default()).unwrap(),
      workflows_configuration,
      tail_configs: TailConfigurations::default(),
      filter_chain: FilterChain::new(FiltersConfiguration::default()).0,
    }
  }

  fn make_runtime(tmp_dir: &Arc<tempfile::TempDir>) -> std::sync::Arc<ConfigLoader> {
    ConfigLoader::new(tmp_dir.path())
  }
}

struct TestReplay {
  logs_count: Arc<AtomicUsize>,
  logs: Arc<parking_lot::Mutex<Vec<std::string::String>>>,
  fields: Arc<parking_lot::Mutex<Vec<LogFields>>>,
}

impl TestReplay {
  fn new() -> Self {
    Self {
      logs_count: Arc::new(AtomicUsize::new(0)),
      logs: Arc::new(parking_lot::Mutex::new(vec![])),
      fields: Arc::new(parking_lot::Mutex::new(vec![])),
    }
  }
}

#[async_trait::async_trait]
impl LogReplay for TestReplay {
  async fn replay_log(
    &mut self,
    log: Log,
    _log_processing_completed_tx: Option<Sender<()>>,
    _processing_pipeline: &mut ProcessingPipeline,
  ) -> anyhow::Result<Vec<Log>> {
    self.logs_count.fetch_add(1, Ordering::SeqCst);
    if let StringOrBytes::String(message) = &log.message {
      self.logs.lock().push(message.clone());
    }

    self.fields.lock().push(log.fields);

    Ok(vec![])
  }
}

#[test]
fn log_line_size_is_computed_correctly() {
  fn create_baseline_log() -> LogLine {
    LogLine {
      log_level: 0,
      log_type: LogType::Normal,
      message: "foo".into(),
      fields: [("foo".into(), AnnotatedLogField::new_ootb("bar".into()))].into(),
      matching_fields: [].into(),
      attributes_overrides: None,
      log_processing_completed_tx: None,
    }
  }

  let baseline_log_expected_size = 466;
  let baseline_log = create_baseline_log();
  assert_eq!(baseline_log_expected_size, baseline_log.size());

  // Add one extra character to the `message` and verify that reported size increases by 1 byte
  let mut baseline_log_with_longer_message = create_baseline_log();
  baseline_log_with_longer_message.message =
    StringOrBytes::String(baseline_log.message.as_str().unwrap().to_owned() + "1");
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_message.size()
  );

  // Add one extra character to one of the fields' keys and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_key = create_baseline_log();
  baseline_log_with_longer_field_key.fields = [(
    "foo1".to_string(),
    AnnotatedLogField::new_ootb("bar".into()),
  )]
  .into();

  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields = [(
    "foo".to_string(),
    AnnotatedLogField {
      kind: LogFieldKind::Ootb,
      value: StringOrBytes::String("bar1".to_string()),
    },
  )]
  .into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_value.size()
  );
}

#[test]
fn annotated_log_line_size_is_computed_correctly() {
  fn create_baseline_log() -> Log {
    Log {
      log_level: 0,
      log_type: LogType::Normal,
      message: "foo".into(),
      fields: [("foo".to_string(), StringOrBytes::String("bar".to_string()))].into(),
      matching_fields: [].into(),
      session_id: "foo".into(),
      occurred_at: time::OffsetDateTime::now_utc(),
    }
  }

  let baseline_log_expected_size = 484;
  let baseline_log = create_baseline_log();
  assert_eq!(baseline_log_expected_size, baseline_log.size());

  // Add one extra character to the `message` and verify that reported size increases by 1 bytes
  let mut baseline_log_with_longer_message = create_baseline_log();
  baseline_log_with_longer_message.message =
    StringOrBytes::String(baseline_log.message.as_str().unwrap().to_owned() + "1");
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_message.size()
  );

  // Add one extra character to the `group` and verify that reported size increases by 1 bytes
  let mut baseline_log_with_longer_group = create_baseline_log();
  baseline_log_with_longer_group.session_id += "1";
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_group.size()
  );

  // Add one extra character to one of the fields' keys and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_key = create_baseline_log();
  baseline_log_with_longer_field_key.fields =
    [("foo1".to_string(), StringOrBytes::String("bar".to_string()))].into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields =
    [("foo".to_string(), StringOrBytes::String("bar1".to_string()))].into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_value.size()
  );
}

#[tokio::test]
async fn logs_are_replayed_in_order() {
  let mut setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, buffer_tx) = setup.make_test_async_log_buffer(config_update_rx);

  let written_logs = Arc::new(Mutex::new(vec![]));
  let shutdown = Arc::new(AtomicBool::new(false));
  let cloned_shutdown = shutdown.clone();

  let written_logs_clone = written_logs.clone();
  // The test sometimes produces zero logs on the background threads when left unchecked, so use
  // a second channel to ensure that we get a certain number of logs processed.
  let (counting_logs_tx, mut counting_logs_rx) = tokio::sync::mpsc::channel(5000);

  let logging_task = std::thread::spawn(move || {
    let mut counter = 0;
    while !cloned_shutdown.load(Ordering::SeqCst) {
      let current_log_message = format!("{counter}");
      written_logs_clone
        .lock()
        .unwrap()
        .push(current_log_message.clone());

      counter += 1;
      let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
        &buffer_tx,
        0,
        LogType::Normal,
        current_log_message.as_str().into(),
        [].into(),
        [].into(),
        None,
        false,
      );

      assert_ok!(result);

      // It's possible that we fill up this channel and we don't want that to prevent the threads
      // from being able to shut down on cancel.
      let _ignored = counting_logs_tx.blocking_send(());
    }
  });

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let config_update_task = std::thread::spawn(move || {
    // Send an initial workflows config update to allow
    // the async log buffer to start replaying buffered logs.
    assert_ok!(config_update_tx.blocking_send(config_update));
    drop(config_update_tx);
  });

  // Wait until we've seen significant activity from the logging threads before we try to replay
  // the logs.
  let mut counted_logs = 0;
  while counted_logs < 100 {
    counting_logs_rx.recv().await.unwrap();
    counted_logs += 1;
  }

  setup.shutdown_in(1.seconds());

  let run_buffer_task = tokio::task::spawn(async move {
    _ = buffer.run(vec![]).await;
  });

  shutdown.store(true, Ordering::SeqCst);

  assert_ok!(logging_task.join());
  assert_ok!(config_update_task.join());

  _ = run_buffer_task.await;

  let written_logs = written_logs.lock().unwrap();

  assert!(written_logs.len() > 0);
  assert_eq!(
    written_logs.len(),
    setup.replayer_log_count.load(Ordering::SeqCst),
  );
  for index in 0 .. written_logs.len() {
    assert_eq!(
      written_logs[index],
      setup.replayer_logs.lock()[index].as_str()
    );
  }
}

#[test]
fn enqueuing_log_does_not_block() {
  let setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (mut _buffer, buffer_tx) = setup.make_real_async_log_buffer(config_update_rx);

  let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
    &buffer_tx,
    0,
    LogType::Normal,
    "test".into(),
    [].into(),
    [].into(),
    None,
    false,
  );

  assert_ok!(result);
}

#[test]
fn enqueuing_log_blocks() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, buffer_tx) = setup.make_real_async_log_buffer(config_update_rx);

  let rt = tokio::runtime::Runtime::new().unwrap();
  rt.spawn(async move {
    assert_ok!(
      config_update_tx
        .send(ConfigUpdate {
          buffer_producers: BufferProducers::new(&setup.buffer_manager).unwrap(),
          buffer_selector: BufferSelector::new(&BufferConfigList::default()).unwrap(),
          workflows_configuration: WorkflowsConfiguration::default(),
          tail_configs: TailConfigurations::default(),
          filter_chain: FilterChain::new(FiltersConfiguration::default()).0,
        })
        .await
    );

    let shutdown_trigger = ComponentShutdownTrigger::default();
    buffer
      .run_with_shutdown(shutdown_trigger.make_shutdown(), vec![])
      .await;
    shutdown_trigger.shutdown().await;
  });

  let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
    &buffer_tx,
    0,
    LogType::Normal,
    "test".into(),
    [].into(),
    [].into(),
    None,
    true,
  );

  assert_ok!(result);
}

#[tokio::test]
async fn creates_workflows_engine_in_response_to_config_update() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (mut buffer, _buffer_tx) = setup.make_real_async_log_buffer(config_update_rx);

  // Simulate config update.
  assert_ok!(
    config_update_tx
      .send(setup.make_config_update(WorkflowsConfiguration::default()))
      .await
  );

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(shutdown_trigger.make_shutdown(), vec![]));
  1.seconds().sleep().await;
  shutdown_trigger.shutdown().await;
  buffer = handle.await.unwrap();

  assert!(buffer.logging_state.workflows_engine().is_some());
}

#[tokio::test]
async fn initial_logs_are_processed_first() {
  let mut setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, buffer_tx) = setup.make_test_async_log_buffer(config_update_rx);

  let t0 = datetime!(2021-01-01 00:00:00 UTC);
  let shutdown = setup.shutdown.as_ref().unwrap().make_shutdown();

  let handle = tokio::spawn(async move {
    buffer
      .run_with_shutdown(
        shutdown,
        vec![Log {
          log_level: log_level::ERROR,
          log_type: LogType::Normal,
          message: "first".into(),
          fields: [].into(),
          matching_fields: [].into(),
          session_id: "first session".into(),
          occurred_at: t0,
        }],
      )
      .await;
  });

  // One log is sent over the channel before the config update.

  buffer_tx
    .try_send(AsyncLogBufferMessage::EmitLog(LogLine {
      log_level: log_level::INFO,
      log_type: LogType::Normal,
      message: "second".into(),
      fields: [].into(),
      matching_fields: [].into(),
      attributes_overrides: None,
      log_processing_completed_tx: None,
    }))
    .unwrap();

  // Simulate config update.
  assert_ok!(
    config_update_tx
      .send(setup.make_config_update(WorkflowsConfiguration::default()))
      .await
  );

  setup.shutdown_in(1.seconds());

  handle.await.unwrap();

  let logs = setup.replayer_logs.lock().clone();
  assert_eq!(2, logs.len());
  assert_eq!("first", logs[0].as_str());
  assert_eq!("second", logs[1].as_str());
}

#[tokio::test]
async fn updates_workflow_engine_in_response_to_config_update() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let config_update_tx_clone = config_update_tx.clone();

  let config_update1 = setup.make_config_update(WorkflowsConfiguration::default());
  let config_update2 = setup.make_config_update(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![Config::new(
      workflow_proto!(exclusive with state!("a")),
    )
    .unwrap()]),
  );
  let task = std::thread::spawn(move || {
    // Simulate config update with no workflows.
    assert_ok!(config_update_tx_clone.blocking_send(config_update1));
    // Simulate config update with one workflow.
    assert_ok!(config_update_tx_clone.blocking_send(config_update2));
  });

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(shutdown_trigger.make_shutdown(), vec![]));
  1.seconds().sleep().await;
  shutdown_trigger.shutdown().await;
  buffer = handle.await.unwrap();

  task.join().unwrap();

  setup.stats.assert_counter_eq(
    1,
    "workflows:workflows_total",
    labels! { "operation" => "start" },
  );

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(shutdown_trigger.make_shutdown(), vec![]));
  1.seconds().sleep().await;
  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  task.join().unwrap();

  setup.stats.assert_counter_eq(
    1,
    "workflows:workflows_total",
    labels! {"operation" => "stop"},
  );
}

#[tokio::test]
async fn logs_resource_utilization_log() {
  let mut setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  setup
    .runtime
    .update_snapshot(&bd_test_helpers::runtime::make_simple_update(vec![
      (
        bd_runtime::runtime::debugging::PeriodicInternalLoggingFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::platform_events::ListenerEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::resource_utilization::ResourceUtilizationReportingIntervalFlag::path(),
        ValueKind::Int(250),
      ),
    ]));

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let message = AsyncLogBufferMessage::EmitLog(LogLine {
    log_level: log_level::DEBUG,
    log_type: LogType::Resource,
    message: StringOrBytes::String(String::new()),
    fields: AnnotatedLogFields::new(),
    matching_fields: AnnotatedLogFields::new(),
    attributes_overrides: None,
    log_processing_completed_tx: None,
  });

  sender.try_send(message).unwrap();

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(shutdown_trigger.make_shutdown(), vec![]));
  500.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  let _buffer = handle.await.unwrap();

  assert_ok!(task.join());

  // There should be at least one periodic internal log reported by using >= to avoid flakes as
  // there are many time dependant things happening in this test.
  assert!(setup.replayer_log_count.load(Ordering::SeqCst) >= 1);
  assert_eq!("", setup.replayer_logs.lock()[0]);

  // Confirm that internal fields are added if enabled.
  assert!(setup.replayer_fields.lock().len() > 0);
  assert!(setup.replayer_fields.lock()[0].contains_key("_logs_count"));
}
