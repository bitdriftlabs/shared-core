// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::AsyncLogBufferMessage;
use crate::async_log_buffer::{AsyncLogBuffer, LogLine, LogReplay};
use crate::bounded_buffer::MemorySized;
use crate::client_config::TailConfigurations;
use crate::log_replay::{LoggerReplay, ProcessingPipeline};
use crate::logging_state::{
  BufferProducers,
  ConfigUpdate,
  LoggingState,
  UninitializedLoggingContext,
};
use assert_matches::assert_matches;
use bd_api::api::SimpleNetworkQualityProvider;
use bd_client_stats::{DynamicStats, FlushTrigger};
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::Collector;
use bd_key_value::Store;
use bd_log_filter::FilterChain;
use bd_log_metadata::{AnnotatedLogFields, LogFieldKind};
use bd_log_primitives::{log_level, AnnotatedLogField, Log, LogField, LogFields, StringOrBytes};
use bd_matcher::buffer_selector::BufferSelector;
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::filter::filter::FiltersConfiguration;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::fixed::UUIDCallbacks;
use bd_session::{fixed, Strategy};
use bd_shutdown::{ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_stats_common::labels;
use bd_test_helpers::events::NoOpListenerTarget;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::runtime::{make_simple_update, ValueKind};
use bd_test_helpers::session::InMemoryStorage;
use bd_test_helpers::{state, workflow_proto};
use bd_time::TimeDurationExt;
use bd_workflows::config::{Config, WorkflowsConfiguration};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use time::ext::NumericalDuration;
use time::Duration;
use tokio::sync::oneshot::Sender;
use tokio_test::assert_ok;

#[derive(Clone)]
struct Setup {
  buffer_manager: Arc<bd_buffer::Manager>,
  runtime: Arc<ConfigLoader>,
  stats: Collector,
  dynamic_counter_stats: Arc<DynamicStats>,
  tmp_dir: Arc<tempfile::TempDir>,
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
    }
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

  fn enable_workflows(&self, enable_workflows: bool) {
    self.runtime.update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::workflows::WorkflowsEnabledFlag::path(),
      ValueKind::Bool(enable_workflows),
    )]));
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
  ) -> anyhow::Result<()> {
    self.logs_count.fetch_add(1, Ordering::SeqCst);
    if let StringOrBytes::String(message) = &log.message {
      self.logs.lock().push(message.clone());
    }

    self.fields.lock().push(log.fields);

    Ok(())
  }
}

fn make_shutdown(timeout: Duration) -> ComponentShutdownTriggerHandle {
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown_trigger_handle = shutdown_trigger.make_handle();
  std::thread::spawn(move || {
    std::thread::sleep(timeout.unsigned_abs());
    shutdown_trigger.shutdown_blocking();
  });

  shutdown_trigger_handle
}

#[test]
fn log_line_size_is_computed_correctly() {
  fn create_baseline_log() -> LogLine {
    LogLine {
      log_level: 0,
      log_type: LogType::Normal,
      message: "foo".into(),
      fields: vec![AnnotatedLogField::new_ootb("foo".to_string(), "bar".into())],
      matching_fields: vec![],
      attributes_overrides: None,
      log_processing_completed_tx: None,
    }
  }

  let baseline_log_expected_size = 362;
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
  baseline_log_with_longer_field_key.fields = vec![AnnotatedLogField::new_ootb(
    "foo1".to_string(),
    "bar".into(),
  )];

  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields = vec![AnnotatedLogField {
    field: LogField {
      key: "foo".to_string(),
      value: StringOrBytes::String("bar1".to_string()),
    },
    kind: LogFieldKind::Ootb,
  }];
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
      fields: vec![LogField {
        key: "foo".to_string(),
        value: StringOrBytes::String("bar".to_string()),
      }],
      matching_fields: vec![],
      session_id: "foo".into(),
      occurred_at: time::OffsetDateTime::now_utc(),
    }
  }

  let baseline_log_expected_size = 308;
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
  baseline_log_with_longer_field_key.fields = vec![LogField {
    key: "foo1".to_string(),
    value: StringOrBytes::String("bar".to_string()),
  }];
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.size()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields = vec![LogField {
    key: "foo".to_string(),
    value: StringOrBytes::String("bar1".to_string()),
  }];
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_value.size()
  );
}

#[tokio::test]
async fn no_logs_are_lost() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let replayer = TestReplay::new();
  let replayer_logs_count = replayer.logs_count.clone();

  let (buffer, buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    replayer,
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    make_shutdown(1.seconds()),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

  let written_logs_count = Arc::new(AtomicUsize::new(0));
  let shutdown = Arc::new(AtomicBool::new(false));

  // The test sometimes produces zero logs on the background threads when left unchecked, so use
  // a second channel to ensure that we get a certain number of logs processed.
  let (counting_logs_tx, mut counting_logs_rx) = tokio::sync::mpsc::channel(5000);

  let mut tasks = Vec::new();
  for _ in 0 .. 4 {
    let shutdown = shutdown.clone();
    let buffer_tx = buffer_tx.clone();
    let written_logs_count: Arc<AtomicUsize> = written_logs_count.clone();
    let counting_logs_tx = counting_logs_tx.clone();

    tasks.push(std::thread::spawn(move || {
      while !shutdown.load(Ordering::SeqCst) {
        written_logs_count.fetch_add(1, Ordering::SeqCst);

        let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
          &buffer_tx,
          0,
          LogType::Normal,
          "test".into(),
          vec![],
          vec![],
          None,
          false,
        );

        assert_ok!(result);

        // It's possible that we fill up this channel and we don't want that to prevent the
        // threads from being able to shut down on cancel.
        let _ignored = counting_logs_tx.blocking_send(());
      }
    }));
  }

  tasks.push(std::thread::spawn(move || {
    let setup = Setup::new();
    // Send an initial config update to allow the buffer to start
    // replaying buffered logs.
    _ = config_update_tx.try_send(setup.make_config_update(WorkflowsConfiguration::default()));
    drop(config_update_tx);
  }));

  // Make sure to drop the original tx - otherwise it will hold up the replay.
  drop(buffer_tx);

  // Wait until we've seen significant activity from the logging threads before we try to replay
  // the logs.
  let mut counted_logs = 0;
  while counted_logs < 100 {
    counting_logs_rx.recv().await.unwrap();
    counted_logs += 1;
  }

  let run_buffer_task = tokio::task::spawn(async move {
    _ = buffer.run(vec![]).await;
  });

  shutdown.store(true, Ordering::SeqCst);

  for t in tasks {
    t.join().unwrap();
  }

  assert_ok!(run_buffer_task.await);

  assert!(written_logs_count.load(Ordering::SeqCst) > 0);
  assert_eq!(
    written_logs_count.load(Ordering::SeqCst),
    replayer_logs_count.load(Ordering::SeqCst)
  );
}

#[tokio::test]
async fn logs_are_replayed_in_order() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let replayer = TestReplay::new();
  let replayer_logs = replayer.logs.clone();
  let replayer_logs_count = replayer.logs_count.clone();

  let (buffer, buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    replayer,
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    make_shutdown(5.seconds()),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

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
        vec![],
        vec![],
        None,
        false,
      );

      assert_ok!(result);

      // It's possible that we fill up this channel and we don't want that to prevent the threads
      // from being able to shut down on cancel.
      let _ignored = counting_logs_tx.blocking_send(());
    }
  });

  let setup_clone = setup.clone();
  let config_update_task = std::thread::spawn(move || {
    // Send an initial workflows config update to allow
    // the async log buffer to start replaying buffered logs.
    assert_ok!(config_update_tx
      .blocking_send(setup_clone.make_config_update(WorkflowsConfiguration::default())));
    drop(config_update_tx);
  });

  // Wait until we've seen significant activity from the logging threads before we try to replay
  // the logs.
  let mut counted_logs = 0;
  while counted_logs < 100 {
    counting_logs_rx.recv().await.unwrap();
    counted_logs += 1;
  }

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
    replayer_logs_count.load(Ordering::SeqCst),
  );
  for index in 0 .. written_logs.len() {
    assert_eq!(written_logs[index], replayer_logs.lock()[index].as_str());
  }
}

#[test]
fn enqueuing_log_does_not_block() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (mut _buffer, buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    TestReplay::new(),
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    shutdown_trigger.make_handle(),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

  let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
    &buffer_tx,
    0,
    LogType::Normal,
    "test".into(),
    vec![],
    vec![],
    None,
    false,
  );

  assert_ok!(result);
}

#[test]
fn take_screenshot_action() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (mut _buffer, buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    TestReplay::new(),
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    shutdown_trigger.make_handle(),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

  let result = AsyncLogBuffer::<TestReplay>::enqueue_log(
    &buffer_tx,
    0,
    LogType::Normal,
    "test".into(),
    vec![],
    vec![],
    None,
    false,
  );

  assert_ok!(result);
}

#[test]
fn enqueuing_log_blocks() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (buffer, buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    LoggerReplay {},
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    shutdown_trigger.make_handle(),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

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
    vec![],
    vec![],
    None,
    true,
  );

  assert_ok!(result);
}

#[tokio::test]
async fn creates_workflows_engine_in_response_to_config_update() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (mut buffer, _buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    LoggerReplay {},
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    make_shutdown(1.seconds()),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

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

  assert_matches!(
    buffer.logging_state,
    LoggingState::Initialized(initialized_logging_state)
      if initialized_logging_state.workflows_engine().is_some()
  );
}

#[tokio::test]
async fn does_not_create_workflows_engine_in_response_to_config_update_if_workflows_are_disabled() {
  let setup = Setup::new();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (mut buffer, _buffer_tx) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    LoggerReplay {},
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    make_shutdown(1.seconds()),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

  // Simulate config update.
  assert_ok!(
    config_update_tx
      .send(setup.make_config_update(WorkflowsConfiguration::default()))
      .await
  );

  buffer = buffer.run(vec![]).await;

  assert_matches!(
    buffer.logging_state,
    LoggingState::Initialized(initialized_logging_state)
      if initialized_logging_state.processing_pipeline.workflows_engine.is_none()
  );
}

#[tokio::test]
async fn updates_workflow_engine_in_response_to_config_update() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (mut buffer, _) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    LoggerReplay {},
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    shutdown_trigger.make_handle(),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

  let setup_clone = setup.clone();
  let config_update_tx_clone = config_update_tx.clone();

  let task = std::thread::spawn(move || {
    // Simulate config update with no workflows.
    assert_ok!(config_update_tx_clone
      .blocking_send(setup_clone.make_config_update(WorkflowsConfiguration::default())));
    // Simulate config update with one workflow.
    assert_ok!(
      config_update_tx_clone.blocking_send(setup_clone.make_config_update(
        WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
          Config::new(&workflow_proto!(exclusive with state!("a"))).unwrap()
        ])
      ))
    );
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

  assert_matches!(
    &buffer.logging_state,
    LoggingState::Initialized(initialized_logging_state)
      if initialized_logging_state.processing_pipeline.workflows_engine.is_some()
  );

  setup.stats.assert_counter_eq(
    1,
    "workflows:workflows_total",
    labels! { "operation" => "start" },
  );

  let setup_clone = setup.clone();
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx
      .blocking_send(setup_clone.make_config_update(WorkflowsConfiguration::default())));
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

  assert_matches!(
    &buffer.logging_state,
    LoggingState::Initialized(state)
      if state.processing_pipeline.workflows_engine.is_some()
  );

  setup.stats.assert_counter_eq(
    1,
    "workflows:workflows_total",
    labels! {"operation" => "stop"},
  );
}

#[tokio::test]
async fn stops_workflows_engine_if_workflows_runtime_flag_is_disabled() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (buffer, _) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    LoggerReplay {},
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    shutdown_trigger.make_handle(),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

  let setup_clone = setup.clone();
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx
      .blocking_send(setup_clone.make_config_update(WorkflowsConfiguration::default())));
  });

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(shutdown_trigger.make_shutdown(), vec![]));
  500.milliseconds().sleep().await;
  shutdown_trigger.shutdown().await;
  let buffer = handle.await.unwrap();

  assert_ok!(task.join());

  assert_matches!(
    &buffer.logging_state,
    LoggingState::Initialized(state)
      if state.workflows_engine().is_some()
  );

  setup.enable_workflows(false);

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle =
    tokio::task::spawn(buffer.run_with_shutdown(shutdown_trigger.make_shutdown(), vec![]));
  500.milliseconds().sleep().await;
  shutdown_trigger.shutdown().await;
  let buffer = handle.await.unwrap();

  assert_matches!(
    &buffer.logging_state,
    LoggingState::Initialized(state)
      if state.workflows_engine().is_none()
  );
}

#[tokio::test]
async fn logs_resource_utilization_log() {
  let setup = Setup::new();
  setup.enable_workflows(true);

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let replayer = TestReplay::new();
  let replayer_logs_count = replayer.logs_count.clone();
  let replayer_logs = replayer.logs.clone();
  let replayer_fields = replayer.fields.clone();

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let (buffer, sender) = AsyncLogBuffer::new(
    setup.make_logging_context(),
    replayer,
    Arc::new(Strategy::Fixed(fixed::Strategy::new(
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(UUIDCallbacks),
    ))),
    Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![],
    }),
    Box::new(EmptyTarget),
    Box::new(bd_test_helpers::session_replay::NoOpTarget),
    Box::new(NoOpListenerTarget),
    config_update_rx,
    shutdown_trigger.make_handle(),
    &setup.runtime,
    Arc::new(SimpleNetworkQualityProvider::default()),
    String::new(),
  );

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

  let setup_clone = setup.clone();
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx
      .blocking_send(setup_clone.make_config_update(WorkflowsConfiguration::default())));
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
  assert!(replayer_logs_count.load(Ordering::SeqCst) >= 1);
  assert_eq!("", replayer_logs.lock()[0]);

  // Confirm that internal fields are added if enabled.
  assert!(replayer_fields.lock().len() > 0);
  assert!(replayer_fields.lock()[0]
    .iter()
    .any(|f| f.key == "_logs_count"));
}
