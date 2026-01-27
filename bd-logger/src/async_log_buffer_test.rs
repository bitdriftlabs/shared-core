// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Block;
use crate::async_log_buffer::{AsyncLogBuffer, LogLine, LogReplay, PreConfigItem, Sender};
use crate::buffer_selector::BufferSelector;
use crate::client_config::TailConfigurations;
use crate::log_replay::{LogReplayResult, LoggerReplay, ProcessingPipeline};
use crate::logging_state::{BufferProducers, ConfigUpdate, UninitializedLoggingContext};
use bd_api::{DataUpload, SimpleNetworkQualityProvider};
use bd_client_common::init_lifecycle::InitLifecycleState;
use bd_client_stats::{FlushTrigger, Stats};
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_log_filter::FilterChain;
use bd_log_matcher::builder::message_equals;
use bd_log_primitives::size::MemorySized;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  Log,
  LogFields,
  StringOrBytes,
  log_level,
};
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::filter::filter::FiltersConfiguration;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::fixed::UUIDCallbacks;
use bd_session::{Strategy, fixed};
use bd_shutdown::ComponentShutdownTrigger;
use bd_state::test::TestStore;
use bd_state::{Scope, StateReader};
use bd_stats_common::labels;
use bd_test_helpers::events::NoOpListenerTarget;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::rule;
use bd_test_helpers::runtime::ValueKind;
use bd_test_helpers::session::in_memory_store;
use bd_test_helpers::workflow::{WorkflowBuilder, state};
use bd_time::{SystemTimeProvider, TimeDurationExt};
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::test::MakeConfig;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use time::ext::{NumericalDuration, NumericalStdDuration};
use tokio::sync::mpsc;
use tokio_test::assert_ok;

struct Setup {
  buffer_manager: Arc<bd_buffer::Manager>,
  runtime: Arc<ConfigLoader>,
  collector: Collector,
  stats: Arc<Stats>,
  tmp_dir: Arc<tempfile::TempDir>,
  _data_upload_rx: mpsc::Receiver<DataUpload>,
  data_upload_tx: mpsc::Sender<DataUpload>,

  replayer_log_count: Arc<AtomicUsize>,
  replayer_logs: Arc<parking_lot::Mutex<Vec<String>>>,
  replayer_fields: Arc<parking_lot::Mutex<Vec<LogFields>>>,
  shutdown: Option<ComponentShutdownTrigger>,
  store: Arc<bd_device::Store>,
  session_strategy: Arc<Strategy>,
}

impl Setup {
  fn new() -> Self {
    let tmp_dir = Arc::new(tempfile::TempDir::with_prefix("root-").unwrap());
    let runtime = &Self::make_runtime(&tmp_dir);
    let collector = Collector::default();
    let stats = Stats::new(collector.clone());
    let (data_upload_tx, data_upload_rx) = mpsc::channel(1);

    let session_strategy = Arc::new(Strategy::Fixed(fixed::Strategy::new(
      in_memory_store(),
      Arc::new(UUIDCallbacks),
    )));

    Self {
      buffer_manager: bd_buffer::Manager::new(
        tmp_dir.path().join("buffer"),
        &collector.scope(""),
        runtime,
      )
      .0,
      runtime: Self::make_runtime(&tmp_dir),
      collector,
      stats,
      tmp_dir,
      replayer_log_count: Arc::default(),
      replayer_logs: Arc::default(),
      replayer_fields: Arc::default(),
      shutdown: Some(ComponentShutdownTrigger::default()),
      _data_upload_rx: data_upload_rx,
      data_upload_tx,
      store: in_memory_store(),
      session_strategy,
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
  ) -> (AsyncLogBuffer<TestReplay>, Sender) {
    let replayer = TestReplay::new();
    self.replayer_log_count = replayer.logs_count.clone();
    self.replayer_logs = replayer.logs.clone();
    self.replayer_fields = replayer.fields.clone();

    let (_, report_rx) = tokio::sync::mpsc::channel(1);

    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());

    AsyncLogBuffer::new(
      self.make_logging_context(),
      replayer,
      self.session_strategy.clone(),
      Arc::new(LogMetadata::default()),
      Box::new(EmptyTarget),
      Box::new(bd_test_helpers::session_replay::NoOpTarget),
      Box::new(NoOpListenerTarget),
      config_update_rx,
      report_rx,
      self.shutdown.as_ref().unwrap().make_handle(),
      &self.runtime,
      network_quality_provider.clone(),
      network_quality_provider,
      String::new(),
      &self.store,
      Arc::new(SystemTimeProvider),
      InitLifecycleState::new(),
      self.data_upload_tx.clone(),
    )
  }

  fn make_real_async_log_buffer(
    &self,
    config_update_rx: tokio::sync::mpsc::Receiver<ConfigUpdate>,
  ) -> (AsyncLogBuffer<LoggerReplay>, Sender) {
    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
    let (_, report_rx) = tokio::sync::mpsc::channel(1);
    AsyncLogBuffer::new(
      self.make_logging_context(),
      LoggerReplay {},
      self.session_strategy.clone(),
      Arc::new(LogMetadata::default()),
      Box::new(EmptyTarget),
      Box::new(bd_test_helpers::session_replay::NoOpTarget),
      Box::new(NoOpListenerTarget),
      config_update_rx,
      report_rx,
      self.shutdown.as_ref().unwrap().make_handle(),
      &self.runtime,
      network_quality_provider.clone(),
      network_quality_provider,
      String::new(),
      &self.store,
      Arc::new(SystemTimeProvider),
      InitLifecycleState::new(),
      self.data_upload_tx.clone(),
    )
  }

  fn make_logging_context(&self) -> UninitializedLoggingContext<PreConfigItem> {
    let (trigger_upload_tx, _) = tokio::sync::mpsc::channel(1);
    let (data_upload_tx, _) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, _) = tokio::sync::mpsc::channel(1);
    let (flush_stats_trigger, _) = FlushTrigger::new();

    UninitializedLoggingContext::new(
      self.tmp_dir.path(),
      &self.runtime,
      self.collector.scope(""),
      self.stats.clone(),
      trigger_upload_tx,
      data_upload_tx,
      flush_buffers_tx,
      flush_stats_trigger,
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
      from_cache: false,
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
    _block: bool,
    _processing_pipeline: &mut ProcessingPipeline,
    _state: &bd_state::Store,
    _now: OffsetDateTime,
  ) -> anyhow::Result<LogReplayResult> {
    self.logs_count.fetch_add(1, Ordering::SeqCst);
    if let Some(message) = log.message.as_str() {
      self.logs.lock().push(message.to_string());
    }

    self.fields.lock().push(log.fields);

    Ok(LogReplayResult::default())
  }

  async fn replay_state_change(
    &mut self,
    _state_change: bd_state::StateChange,
    _pipeline: &mut ProcessingPipeline,
    _state: &bd_state::Store,
    _now: OffsetDateTime,
    _session_id: &str,
    _fields: &bd_log_primitives::LogFields,
    _matching_fields: &bd_log_primitives::LogFields,
  ) -> LogReplayResult {
    // Test implementation does nothing with state changes
    LogReplayResult::default()
  }
}

#[test]
fn log_line_size_is_computed_correctly() {
  fn create_baseline_log() -> LogLine {
    LogLine {
      log_level: 0,
      log_type: LogType::NORMAL,
      message: "foo".into(),
      fields: [("foo".into(), AnnotatedLogField::new_ootb("bar"))].into(),
      matching_fields: [].into(),
      attributes_overrides: None,
      capture_session: None,
    }
  }

  let baseline_log_expected_size = 488;
  let baseline_log = create_baseline_log();
  assert_eq!(baseline_log_expected_size, baseline_log.size());

  // Add one extra character to the `message` and verify that reported size increases by 1 byte
  let mut baseline_log_with_longer_message = create_baseline_log();
  baseline_log_with_longer_message.message =
    StringOrBytes::from(baseline_log.message.as_str().unwrap().to_owned() + "1");
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_message.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_key = create_baseline_log();
  baseline_log_with_longer_field_key.fields =
    [("foo".into(), AnnotatedLogField::new_ootb("bar1"))].into();

  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields =
    [("foo".into(), AnnotatedLogField::new_ootb("bar1"))].into();
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
      log_type: LogType::NORMAL,
      message: "foo".into(),
      fields: [("foo".into(), "bar".into())].into(),
      matching_fields: [].into(),
      session_id: "foo".into(),
      occurred_at: time::OffsetDateTime::now_utc(),
      capture_session: None,
    }
  }

  let baseline_log_expected_size = 561;
  let baseline_log = create_baseline_log();
  assert_eq!(baseline_log_expected_size, baseline_log.size());

  // Add one extra character to the `message` and verify that reported size increases by 1 bytes
  let mut baseline_log_with_longer_message = create_baseline_log();
  baseline_log_with_longer_message.message =
    StringOrBytes::from(baseline_log.message.as_str().unwrap().to_owned() + "1");
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
    [("foo".into(), StringOrBytes::String("bar1".to_string()))].into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields =
    [("foo".into(), StringOrBytes::String("bar1".to_string()))].into();
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
  let (counting_logs_tx, mut counting_logs_rx) = tokio::sync::mpsc::unbounded_channel();

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
        LogType::NORMAL,
        current_log_message.as_str().into(),
        [].into(),
        [].into(),
        None,
        Block::No,
        None,
      );

      if result.is_err() {
        break;
      }

      // It's possible that we fill up this channel and we don't want that to prevent the threads
      // from being able to shut down on cancel.
      let _ignored = counting_logs_tx.send(());
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

  let test_store = TestStore::new().await;
  let state_store = (*test_store).clone();
  let run_buffer_task = tokio::task::spawn(async move {
    _ = buffer.run(state_store, ()).await;
  });

  shutdown.store(true, Ordering::SeqCst);

  assert_ok!(logging_task.join());
  assert_ok!(config_update_task.join());

  _ = run_buffer_task.await;
  drop(test_store);

  let written_logs = written_logs.lock().unwrap();

  assert!(!written_logs.is_empty());
  let replayed_logs = setup.replayer_logs.lock();
  assert!(!replayed_logs.is_empty());
  let prefix_len = written_logs.len().min(replayed_logs.len());
  for index in 0 .. prefix_len {
    assert_eq!(written_logs[index], replayed_logs[index].as_str());
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
    LogType::NORMAL,
    "test".into(),
    [].into(),
    [].into(),
    None,
    Block::No,
    None,
  );

  assert_ok!(result);
}

#[test]
fn enqueuing_log_blocks() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, buffer_sender) = setup.make_real_async_log_buffer(config_update_rx);

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
          from_cache: false,
        })
        .await
    );

    let shutdown_trigger = ComponentShutdownTrigger::default();
    let test_store = TestStore::new().await;
    let state_store = (*test_store).clone();
    buffer
      .run_with_shutdown(state_store, (), shutdown_trigger.make_shutdown())
      .await;
    shutdown_trigger.shutdown().await;
    drop(test_store);
  });

  let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
    &buffer_sender,
    0,
    LogType::NORMAL,
    "test".into(),
    [].into(),
    [].into(),
    None,
    Block::Yes(15.std_seconds()),
    None,
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

  let test_store = TestStore::new().await;
  let state_store = (*test_store).clone();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(state_store, (), shutdown_trigger.make_shutdown()));
  1.seconds().sleep().await;
  shutdown_trigger.shutdown().await;
  buffer = handle.await.unwrap();
  drop(test_store);

  assert!(buffer.logging_state.workflows_engine().is_some());
}

#[tokio::test]
async fn updates_workflow_engine_in_response_to_config_update() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let config_update_tx_clone = config_update_tx.clone();

  let config_update1 = setup.make_config_update(WorkflowsConfiguration::default());
  let mut a = state("A");
  let b = state("B");
  a = a.declare_transition(&b, rule!(message_equals("foo")));

  let config_update2 = setup.make_config_update(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      WorkflowBuilder::new("1", &[&a, &b]).make_config(),
    ]),
  );
  let task = std::thread::spawn(move || {
    // Simulate config update with no workflows.
    assert_ok!(config_update_tx_clone.blocking_send(config_update1));
    // Simulate config update with one workflow.
    assert_ok!(config_update_tx_clone.blocking_send(config_update2));
  });

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let test_store = TestStore::new().await;
  let state_store = (*test_store).clone();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(state_store, (), shutdown_trigger.make_shutdown()));
  1.seconds().sleep().await;
  shutdown_trigger.shutdown().await;
  buffer = handle.await.unwrap();
  drop(test_store);

  task.join().unwrap();

  setup.collector.assert_counter_eq(
    1,
    "workflows:workflows_total",
    labels! { "operation" => "start" },
  );

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let state_store = TestStore::new().await;

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  1.seconds().sleep().await;
  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  task.join().unwrap();

  setup.collector.assert_counter_eq(
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
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![
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
    ]))
    .await
    .unwrap();

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let log = LogLine {
    log_level: log_level::DEBUG,
    log_type: LogType::RESOURCE,
    message: StringOrBytes::String(String::new()),
    fields: AnnotatedLogFields::new(),
    matching_fields: AnnotatedLogFields::new(),
    attributes_overrides: None,
    capture_session: None,
  };

  sender.try_send_log(log.into()).unwrap();

  let state_store = TestStore::new().await;

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  500.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  let _buffer = handle.await.unwrap();

  assert_ok!(task.join());

  // There should be at least one periodic internal log reported by using >= to avoid flakes as
  // there are many time dependant things happening in this test.
  assert!(setup.replayer_log_count.load(Ordering::SeqCst) >= 1);
  assert_eq!("", setup.replayer_logs.lock()[0]);

  // Confirm that internal fields are added if enabled.
  assert!(!setup.replayer_fields.lock().is_empty());
  assert!(setup.replayer_fields.lock()[0].contains_key("_logs_count"));
}

#[tokio::test]
async fn updates_system_session_id_for_new_sessions() {
  let mut setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let test_store = TestStore::new().await;
  let state_store = (*test_store).clone();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(state_store, (), shutdown_trigger.make_shutdown()));

  let first_session_id = setup.session_strategy.session_id();
  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "first".into(),
    [].into(),
    [].into(),
    None,
    Block::No,
    None,
  ));

  setup.session_strategy.start_new_session();
  let second_session_id = setup.session_strategy.session_id();
  assert_ne!(first_session_id, second_session_id);

  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "second".into(),
    [].into(),
    [].into(),
    None,
    Block::No,
    None,
  ));

  200.milliseconds().sleep().await;
  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  {
    let reader = test_store.read().await;
    let value = reader.get(Scope::System, "session_id");
    assert!(value.is_some_and(|stored| {
      stored.has_string_value() && stored.string_value() == second_session_id
    }));
  }

  drop(test_store);
  task.join().unwrap();
}

#[tokio::test]
async fn previous_run_log_does_not_override_system_session_id() {
  let mut setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let test_store = TestStore::new().await;
  let state_store = (*test_store).clone();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(state_store, (), shutdown_trigger.make_shutdown()));

  let current_session_id = setup.session_strategy.session_id();
  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "current".into(),
    [].into(),
    [].into(),
    None,
    Block::No,
    None,
  ));

  setup.session_strategy.start_new_session();
  let next_session_id = setup.session_strategy.session_id();
  assert_ne!(current_session_id, next_session_id);

  let log = LogLine {
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    message: "previous".into(),
    fields: AnnotatedLogFields::new(),
    matching_fields: AnnotatedLogFields::new(),
    attributes_overrides: Some(
      crate::async_log_buffer::LogAttributesOverrides::PreviousRunSessionID(
        time::OffsetDateTime::now_utc(),
      ),
    ),
    capture_session: None,
  };
  sender.try_send_log(log.into()).unwrap();

  200.milliseconds().sleep().await;
  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  {
    let reader = test_store.read().await;
    let value = reader.get(Scope::System, "session_id");
    assert!(value.is_some_and(|stored| {
      stored.has_string_value() && stored.string_value() == next_session_id
    }));
  }

  drop(test_store);
  task.join().unwrap();
}

#[tokio::test]
async fn processes_log_with_global_state_in_attributes_overrides() {
  let mut setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  // Use the SAME store that Setup created, so buffer and test share it!
  // In previous attempts we created a NEW store here, but the buffer was using its own
  // store created inside make_test_async_log_buffer (which was also creating a new
  // in_memory_store). Now we've patched Setup to hold the store, so we can access it if needed,
  // but importantly make_test_async_log_buffer uses that same store.

  // We need to pass a store to run_with_shutdown for state_store (session state),
  // but the global state store is passed in AsyncLogBuffer::new inside make_test_async_log_buffer.

  // The store passed to run_with_shutdown is for session state (workflows etc).
  // The global state tracker uses the store passed to AsyncLogBuffer::new.

  // Since we updated Setup to use a shared store, global state should persist correctly in that
  // store.

  let state_store = TestStore::new().await;

  let shutdown_trigger = ComponentShutdownTrigger::default();
  // Spawn buffer first
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));

  // 1. Add global state field via state update
  sender
    .try_send_state_update(crate::async_log_buffer::StateUpdateMessage::AddLogField(
      "global_key".to_string(),
      StringOrBytes::String("global_value".to_string()),
    ))
    .unwrap();

  // 2. Send a NORMAL log. This will cause the buffer to call:
  //    normalized_metadata_with_extra_fields(...) which in turn calls
  //    global_state_tracker.maybe_update_global_state(...) updating the global state in memory.
  AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    log_level::DEBUG,
    LogType::NORMAL,
    "prime".into(),
    [].into(),
    [].into(),
    None,
    Block::No,
    None,
  )
  .unwrap();

  // 3. Flush state.
  sender.flush_state(Block::Yes(5.std_seconds())).unwrap();

  // Wait a bit for file I/O to be sure
  500.milliseconds().sleep().await;

  // 4. Send log with PreviousRunSessionID. This triggers
  //    metadata_from_fields_with_previous_global_state(...) which reads from global_state_reader.
  let log = LogLine {
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    message: "test".into(),
    fields: AnnotatedLogFields::new(),
    matching_fields: AnnotatedLogFields::new(),
    attributes_overrides: Some(
      crate::async_log_buffer::LogAttributesOverrides::PreviousRunSessionID(
        time::OffsetDateTime::now_utc(),
      ),
    ),
    capture_session: None,
  };

  // The reader is initialized in make_test_async_log_buffer with Reader::new(store).
  // Reader::new reads the initial state from the store and CACHES it in self.prevous_global_state.
  // This cached value is used by previous_global_state_fields().

  // So, if we want the reader to see the updated state as "previous" state, we need to
  // re-initialize the reader or ensure it reads fresh data?

  // Looking at Reader code:
  // pub fn new(store: Arc<Store>) -> Self {
  //   let prevous_global_state = Arc::new(store.get(&KEY).map(|s| s.0));
  //   ...
  // }
  // pub fn previous_global_state_fields(&self) -> Option<&LogFields> {
  //   (*self.prevous_global_state).as_ref()
  // }

  // The Reader captures the state at the time of its creation!
  // It is intended to read the state from the *previous process run*.
  // In this test, we are simulating a single process run where we update state and then try to use
  // it as "previous" state? No, we want to simulate:
  // 1. App starts (empty state)
  // 2. App runs, updates state (persisted to disk)
  // 3. App crashes/restarts (simulated here by reading the now-persisted state as "previous")

  // BUT the AsyncLogBuffer is long-lived in this test. It holds a Reader created at start.
  // That Reader has the empty initial state cached.
  // When we process the log with PreviousRunSessionID, it uses that Reader with stale (empty)
  // state.

  // To test this properly, we should:
  // 1. Run buffer, write state, stop buffer.
  // 2. Start NEW buffer with same store. This new buffer will create a new Reader, which will read
  //    the persisted state from the store.
  // 3. Send the PreviousRunSessionID log to the NEW buffer.

  // Let's restructure the test to do this restart simulation.

  // Stop the first buffer
  shutdown_trigger.shutdown().await;
  let _buffer = handle.await.unwrap();
  assert_ok!(task.join());

  // Wait for shutdown

  // Start NEW buffer with SAME store
  let (config_update_tx_2, config_update_rx_2) = tokio::sync::mpsc::channel(1);
  // We need to use make_test_async_log_buffer again but ensure it uses the SAME store.
  // We modified Setup to hold the store, so calling make_test_async_log_buffer uses self.store.

  let (buffer_2, sender_2) = setup.make_test_async_log_buffer(config_update_rx_2);

  let config_update_2 = setup.make_config_update(WorkflowsConfiguration::default());
  let task_2 = std::thread::spawn(move || {
    assert_ok!(config_update_tx_2.blocking_send(config_update_2));
  });

  let shutdown_trigger_2 = ComponentShutdownTrigger::default();
  // Create a new TestStore for the second buffer run.
  let state_store_2 = TestStore::new().await;
  let handle_2 = tokio::task::spawn(buffer_2.run_with_shutdown(
    state_store_2.take_inner(),
    (),
    shutdown_trigger_2.make_shutdown(),
  ));

  // Now send the log to the new buffer
  sender_2.try_send_log(log.into()).unwrap();

  // Wait for processing
  500.milliseconds().sleep().await;

  shutdown_trigger_2.shutdown().await;
  let _buffer_2 = handle_2.await.unwrap();
  assert_ok!(task_2.join());

  // Verify
  // make_test_async_log_buffer resets setup.replayer_* refs to the NEW replayer.
  // The first buffer's logs are in the OLD replayer, which we lost access to via setup.
  // The second buffer's logs are in the NEW replayer, accessible via setup.
  // So setup.replayer_log_count should be 1 (for the "test" log).
  assert_eq!(1, setup.replayer_log_count.load(Ordering::SeqCst));

  let logs = setup.replayer_logs.lock();
  let fields = setup.replayer_fields.lock();

  // Find the "test" log
  // With only 1 log, it should be at index 0.
  assert_eq!("test", logs[0]);

  // Debug print keys if assertion fails
  if !fields[0].contains_key("global_key") {
    println!("Available keys in 'test' log: {:?}", fields[0].keys());
  }

  assert!(fields[0].contains_key("global_key"));
  // Verify value matches
  let val = &fields[0]["global_key"];
  match val {
    bd_log_primitives::LogFieldValue::String(s) => assert_eq!("global_value", s),
    _ => panic!("Unexpected value type"),
  }
}
