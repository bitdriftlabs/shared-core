// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::async_log_buffer::{
  AdmissionError,
  AsyncLogBuffer,
  EventBufferLimitWatches,
  LogAttributesOverrides,
  LogLine,
  LogReplay,
  LoggerControl,
  ReportProcessor,
  Sender,
  admission_context,
  current_process_admission_context,
  workflow_generated_log,
};
use crate::buffer_selector::BufferSelector;
use crate::client_config::TailConfigurations;
use crate::log_replay::{LogReplayResult, LoggerReplay, ProcessingPipeline};
use crate::logging_state::{BufferProducers, ConfigUpdate, UninitializedLoggingContext};
use crate::{Block, InitializationState, StartupReplayEligibility};
use bd_api::{DataUpload, SimpleNetworkQualityProvider};
use bd_client_common::init_lifecycle::{InitLifecycle, InitLifecycleState};
use bd_client_stats::{FlushTrigger, Stats};
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_event_buffer::{
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventContext,
  LoggerIngressEvent,
  LoggerIngressPayload,
};
use bd_log_filter::FilterChain;
use bd_log_matcher::builder::message_equals;
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  Log,
  LogFields,
  log_level,
};
use bd_macros::ApproximateSize;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::filter::filter::FiltersConfiguration;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::Strategy;
use bd_session::test::no_timeout;
use bd_shutdown::ComponentShutdownTrigger;
use bd_state::test::TestStore;
use bd_state::{
  InMemoryStateReader,
  MEMORY_PRESSURE_LEVEL_KEY,
  PersistentStoreConfig,
  SYSTEM_SESSION_ID_KEY,
  Scope,
  StateReader,
};
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
use bd_workflows::engine::ProcessLocalPendingFlushState;
use bd_workflows::test::MakeConfig;
use futures_util::poll;
use std::future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use time::ext::{NumericalDuration, NumericalStdDuration};
use tokio::sync::{Notify, mpsc};
use tokio_test::assert_ok;

//
// StartupGateReady
//

#[derive(Default)]
struct StartupGateReady {
  ready: AtomicBool,
  notify: Notify,
}

impl StartupGateReady {
  fn reset(&self) {
    self.ready.store(false, Ordering::SeqCst);
  }

  fn mark_ready(&self) {
    self.ready.store(true, Ordering::SeqCst);
    self.notify.notify_waiters();
  }

  async fn wait(&self) {
    loop {
      let notified = self.notify.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if self.ready.load(Ordering::SeqCst) {
        return;
      }
      notified.await;
    }
  }
}

//
// AsyncLogBufferTestHooks
//

struct AsyncLogBufferTestHooks {
  startup_gate_ready: Arc<StartupGateReady>,
  startup_gate_opened: Arc<StartupGateReady>,
  delegate: Option<Arc<dyn crate::TestHooks>>,
}

impl crate::TestHooks for AsyncLogBufferTestHooks {
  fn remote_streaming_action_processed(&self) {
    if let Some(delegate) = &self.delegate {
      delegate.remote_streaming_action_processed();
    }
  }

  fn remote_streaming_trigger_upload_completed(&self) {
    if let Some(delegate) = &self.delegate {
      delegate.remote_streaming_trigger_upload_completed();
    }
  }

  fn workflow_event_processed(&self) {
    if let Some(delegate) = &self.delegate {
      delegate.workflow_event_processed();
    }
  }

  fn startup_gate_ready(&self) {
    self.startup_gate_ready.mark_ready();
    if let Some(delegate) = &self.delegate {
      delegate.startup_gate_ready();
    }
  }

  fn startup_replay_gate_opened(&self) {
    self.startup_gate_opened.mark_ready();
    if let Some(delegate) = &self.delegate {
      delegate.startup_replay_gate_opened();
    }
  }

  fn startup_replay_eligibility_initialized(&self, eligibility: StartupReplayEligibility) {
    if let Some(delegate) = &self.delegate {
      delegate.startup_replay_eligibility_initialized(eligibility);
    }
  }
}

struct Setup {
  buffer_manager: Arc<bd_buffer::Manager>,
  runtime: Arc<ConfigLoader>,
  collector: Collector,
  stats: Arc<Stats>,
  tmp_dir: Arc<tempfile::TempDir>,
  _data_upload_rx: mpsc::Receiver<DataUpload>,
  data_upload_tx: mpsc::Sender<DataUpload>,

  replayer_log_count: Arc<AtomicUsize>,
  replayer_log_notify: Arc<Notify>,
  replayer_logs: Arc<parking_lot::Mutex<Vec<String>>>,
  replayer_fields: Arc<parking_lot::Mutex<Vec<LogFields>>>,
  replayer_feature_flags: Arc<parking_lot::Mutex<Vec<Option<String>>>>,
  shutdown: Option<ComponentShutdownTrigger>,
  store: Arc<bd_device::Store>,
  session_strategy: Arc<Strategy>,
  startup_gate_ready: Arc<StartupGateReady>,
  startup_gate_opened: Arc<StartupGateReady>,
  lifecycle_state: InitLifecycleState,
  sdk_status_tracker: bd_client_common::sdk_status::SdkStatusTracker,
  test_hooks: Option<Arc<dyn crate::TestHooks>>,
}

impl Setup {
  fn new() -> Self {
    let tmp_dir = Arc::new(tempfile::TempDir::with_prefix("root-").unwrap());
    let runtime = &Self::make_runtime(&tmp_dir);
    let collector = Collector::default();
    let stats = Stats::new(collector.clone());
    let (data_upload_tx, data_upload_rx) = mpsc::channel(1);
    let session_strategy = no_timeout(tmp_dir.path()).strategy();

    Self {
      buffer_manager: bd_buffer::Manager::new(
        tmp_dir.path().join("buffer"),
        &collector.scope(""),
        runtime,
        Arc::new(bd_versioned_kv::RetentionRegistry::new(
          bd_runtime::runtime::IntWatch::new_for_testing(0),
        )),
      )
      .0,
      runtime: Self::make_runtime(&tmp_dir),
      collector,
      stats,
      tmp_dir,
      replayer_log_count: Arc::default(),
      replayer_log_notify: Arc::new(Notify::new()),
      replayer_logs: Arc::default(),
      replayer_fields: Arc::default(),
      replayer_feature_flags: Arc::default(),
      shutdown: Some(ComponentShutdownTrigger::default()),
      _data_upload_rx: data_upload_rx,
      data_upload_tx,
      store: in_memory_store(),
      session_strategy,
      startup_gate_ready: Arc::default(),
      startup_gate_opened: Arc::default(),
      lifecycle_state: InitLifecycleState::new(),
      sdk_status_tracker: bd_client_common::sdk_status::SdkStatusTracker::new(),
      test_hooks: None,
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
    self.make_test_async_log_buffer_with_startup_replay_eligibility(
      config_update_rx,
      StartupReplayEligibility::Unknown,
    )
  }

  fn make_test_async_log_buffer_with_startup_replay_eligibility(
    &mut self,
    config_update_rx: tokio::sync::mpsc::Receiver<ConfigUpdate>,
    startup_replay_eligibility: StartupReplayEligibility,
  ) -> (AsyncLogBuffer<TestReplay>, Sender) {
    let replayer = TestReplay::new();
    self.replayer_log_count = replayer.logs_count.clone();
    self.replayer_log_notify = replayer.logs_notify.clone();
    self.replayer_logs = replayer.logs.clone();
    self.replayer_fields = replayer.fields.clone();
    self.replayer_feature_flags = replayer.feature_flags.clone();

    let (_, report_rx) = tokio::sync::mpsc::channel(1);

    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());

    AsyncLogBuffer::new(
      self.make_logging_context(),
      replayer,
      self.session_strategy.clone(),
      Arc::new(LogMetadata::default()),
      [].into(),
      [].into(),
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
      self.lifecycle_state.clone(),
      self.sdk_status_tracker.clone(),
      self.data_upload_tx.clone(),
      startup_replay_eligibility,
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
      [].into(),
      [].into(),
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
      self.lifecycle_state.clone(),
      self.sdk_status_tracker.clone(),
      self.data_upload_tx.clone(),
      StartupReplayEligibility::Unknown,
    )
  }

  fn make_logging_context(&self) -> UninitializedLoggingContext {
    self.startup_gate_ready.reset();
    self.startup_gate_opened.reset();
    let (trigger_upload_tx, _) = tokio::sync::mpsc::channel(1);
    let (_remote_flush_streaming_tx, remote_flush_streaming_rx) = tokio::sync::mpsc::channel(1);
    let (data_upload_tx, _) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, _) = tokio::sync::mpsc::channel(1);
    let (flush_stats_trigger, _) = FlushTrigger::new();

    UninitializedLoggingContext::new(
      self.tmp_dir.path(),
      &self.runtime,
      self.collector.scope(""),
      self.stats.clone(),
      trigger_upload_tx,
      remote_flush_streaming_rx,
      data_upload_tx,
      flush_buffers_tx,
      flush_stats_trigger,
      1_000_000,
      Arc::new(AtomicBool::new(false)),
      Arc::new(ProcessLocalPendingFlushState::default()),
      Some(Arc::new(AsyncLogBufferTestHooks {
        startup_gate_ready: self.startup_gate_ready.clone(),
        startup_gate_opened: self.startup_gate_opened.clone(),
        delegate: self.test_hooks.clone(),
      })),
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

#[tokio::test]
async fn event_buffer_limit_watches_register_runtime_budget_updates() {
  let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let runtime = ConfigLoader::new(sdk_directory.path());
  let mut watches = EventBufferLimitWatches::new(&runtime);

  let limits = watches.read_mark_update();
  assert_eq!(
    bd_runtime::runtime::event_buffer::LogLimitBytesFlag::default() as usize,
    limits.log_limit_bytes
  );
  assert_eq!(
    bd_runtime::runtime::event_buffer::TotalLimitBytesFlag::default() as usize,
    limits.total_limit_bytes
  );

  runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![
      (
        bd_runtime::runtime::event_buffer::LogLimitBytesFlag::path(),
        ValueKind::Int(123),
      ),
      (
        bd_runtime::runtime::event_buffer::TotalLimitBytesFlag::path(),
        ValueKind::Int(456),
      ),
    ]))
    .await
    .unwrap();

  let limits = watches.read_mark_update();
  assert_eq!(123, limits.log_limit_bytes);
  assert_eq!(456, limits.total_limit_bytes);
}

#[tokio::test]
async fn runtime_budget_updates_apply_on_the_next_event_buffer_admission() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![
      (
        bd_runtime::runtime::event_buffer::LogLimitBytesFlag::path(),
        ValueKind::Int(0),
      ),
      (
        bd_runtime::runtime::event_buffer::TotalLimitBytesFlag::path(),
        ValueKind::Int(0),
      ),
    ]))
    .await
    .unwrap();

  buffer.refresh_event_buffer_limits();

  assert!(sender.try_send_log(normal_log("rejected")).is_err());
}

#[tokio::test(start_paused = true)]
async fn startup_gate_holds_preconfiguration_logs_until_the_replay_timer() {
  let mut setup = Setup::new();
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
      ValueKind::Int(250),
    )]))
    .await
    .unwrap();
  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  sender.try_send_log(normal_log("held")).unwrap();

  config_update_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();

  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));

  wait_for_startup_gate_ready(&setup).await;
  tokio::time::advance(100.std_milliseconds()).await;
  assert_eq!(0, setup.replayer_log_count.load(Ordering::SeqCst));

  tokio::time::advance(150.std_milliseconds()).await;
  wait_for_startup_gate_opened(&setup).await;
  wait_for_replayed_logs(&setup, 1).await;
  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_gate_opened",
    labels!("reason" => "timer", "eligibility" => "unknown"),
  );

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
  assert_eq!(vec!["held"], *setup.replayer_logs.lock());
}

#[tokio::test]
async fn empty_startup_gate_opening_marks_log_processing_running() {
  let mut setup = Setup::new();
  let (config_update_tx, config_update_rx) = mpsc::channel(1);
  let (buffer, _) = setup.make_test_async_log_buffer_with_startup_replay_eligibility(
    config_update_rx,
    StartupReplayEligibility::NoPriorCrash,
  );
  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));

  config_update_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();
  wait_for_startup_gate_ready(&setup).await;
  // EventBuffer returns the opening even without entries, so lifecycle state cannot depend on the
  // first log arriving after configuration.
  wait_for_startup_gate_opened(&setup).await;

  assert_eq!(
    InitLifecycle::LogProcessingStarted,
    setup.lifecycle_state.get()
  );
  assert_eq!(
    InitializationState::Running,
    setup.sdk_status_tracker.get().initialization_state
  );
  assert_eq!(0, setup.replayer_log_count.load(Ordering::SeqCst));
  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_gate_opened",
    labels!("reason" => "no_prior_crash", "eligibility" => "no_prior_crash"),
  );

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
}

#[tokio::test]
async fn shutdown_before_configuration_does_not_open_the_startup_gate() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = mpsc::channel(1);
  let (buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let event_buffer = buffer.event_buffer.clone();
  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  assert!(!event_buffer.is_startup_gate_open());
  assert!(!setup.startup_gate_opened.ready.load(Ordering::SeqCst));
  assert_eq!(InitLifecycle::NotStarted, setup.lifecycle_state.get());
  assert_eq!(
    InitializationState::Loaded,
    setup.sdk_status_tracker.get().initialization_state
  );
}

#[tokio::test(start_paused = true)]
async fn runtime_startup_replay_delay_extension_rearms_the_running_gate() {
  for (eligibility, default_ms, flag) in [
    (
      StartupReplayEligibility::Unknown,
      50,
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
    ),
    (
      StartupReplayEligibility::MayHavePriorCrash,
      1_000,
      bd_runtime::runtime::event_buffer::StartupReplayCrashDelayFlag::path(),
    ),
  ] {
    let mut setup = Setup::new();
    let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
    let (buffer, sender) = setup
      .make_test_async_log_buffer_with_startup_replay_eligibility(config_update_rx, eligibility);
    sender.try_send_log(normal_log("held")).unwrap();

    config_update_tx
      .send(setup.make_config_update(WorkflowsConfiguration::default()))
      .await
      .unwrap();

    let state_store = TestStore::new().await;
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let handle = tokio::task::spawn(buffer.run_with_shutdown(
      state_store.take_inner(),
      (),
      shutdown_trigger.make_shutdown(),
    ));

    wait_for_startup_gate_ready(&setup).await;
    setup
      .runtime
      .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
        flag,
        ValueKind::Int(2_000),
      )]))
      .await
      .unwrap();
    // EventBuffer reads the updated runtime watch before considering the old deadline.

    tokio::time::advance(default_ms.std_milliseconds()).await;
    tokio::task::yield_now().await;
    assert_eq!(0, setup.replayer_log_count.load(Ordering::SeqCst));

    tokio::time::advance((2_000 - default_ms).std_milliseconds()).await;
    wait_for_startup_gate_opened(&setup).await;
    wait_for_replayed_logs(&setup, 1).await;

    shutdown_trigger.shutdown().await;
    handle.await.unwrap();
  }
}

#[tokio::test(start_paused = true)]
async fn report_processing_does_not_change_the_selected_startup_delay() {
  for (eligibility, delay_ms) in [
    (StartupReplayEligibility::NoPriorCrash, 0),
    (StartupReplayEligibility::Unknown, 50),
    (StartupReplayEligibility::MayHavePriorCrash, 1_000),
  ] {
    let mut setup = Setup::new();
    let (config_tx, config_rx) = mpsc::channel(1);
    let (mut buffer, sender) =
      setup.make_test_async_log_buffer_with_startup_replay_eligibility(config_rx, eligibility);
    let (report_tx, report_rx) = mpsc::channel(1);
    buffer.report_processor_rx = report_rx;
    report_tx
      .send(crate::logger::ReportProcessingRequest {
        session: crate::ReportProcessingSession::PreviousRun,
      })
      .await
      .unwrap();
    sender.try_send_log(normal_log("held")).unwrap();
    config_tx
      .send(setup.make_config_update(WorkflowsConfiguration::default()))
      .await
      .unwrap();
    let event_buffer = buffer.event_buffer.clone();
    let processed = Arc::new(Notify::new());
    let state_store = TestStore::new().await;
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let handle = tokio::spawn(buffer.run_with_shutdown(
      state_store.take_inner(),
      ReportProcessingSignal(processed.clone()),
      shutdown_trigger.make_shutdown(),
    ));
    processed.notified().await;
    wait_for_startup_gate_ready(&setup).await;
    if delay_ms > 0 {
      tokio::time::advance((delay_ms - 1).std_milliseconds()).await;
      assert!(!event_buffer.is_startup_gate_open());
      tokio::time::advance(1.std_milliseconds()).await;
    }
    wait_for_startup_gate_opened(&setup).await;
    assert!(event_buffer.is_startup_gate_open());
    shutdown_trigger.shutdown().await;
    let buffer = handle.await.unwrap();
    assert_eq!(eligibility, buffer.startup_replay_eligibility);
    assert_eq!(vec!["held"], *setup.replayer_logs.lock());
  }
}

struct ReportProcessingSignal(Arc<Notify>);

struct PausedReportProcessor {
  entered: Arc<Notify>,
  resume: Arc<Notify>,
}

impl ReportProcessor for PausedReportProcessor {
  async fn process_all_pending_reports(&self) -> Vec<bd_crash_handler::CrashLog> {
    self.entered.notify_one();
    self.resume.notified().await;
    vec![crash_log("previous", OffsetDateTime::UNIX_EPOCH)]
  }
}

#[tokio::test]
async fn flush_during_report_discovery_preserves_previous_process_replay_priority() {
  let mut setup = Setup::new();
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
      ValueKind::Int(60_000),
    )]))
    .await
    .unwrap();
  let (config_tx, config_rx) = mpsc::channel(1);
  let (mut buffer, sender) = setup.make_test_async_log_buffer(config_rx);
  let (report_tx, report_rx) = mpsc::channel(1);
  buffer.report_processor_rx = report_rx;
  let event_buffer = buffer.event_buffer.clone();
  let entered = Arc::new(Notify::new());
  let resume = Arc::new(Notify::new());
  let state_store = TestStore::new().await;
  let shutdown = ComponentShutdownTrigger::default();
  let handle = tokio::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    PausedReportProcessor {
      entered: entered.clone(),
      resume: resume.clone(),
    },
    shutdown.make_shutdown(),
  ));
  config_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();
  wait_for_startup_gate_ready(&setup).await;
  sender.try_send_log(normal_log("current")).unwrap();
  report_tx
    .send(crate::logger::ReportProcessingRequest {
      session: crate::ReportProcessingSession::PreviousRun,
    })
    .await
    .unwrap();
  entered.notified().await;
  let (completion, receiver) = bd_completion::Sender::new();
  assert_eq!(
    bd_event_buffer::FlushAdmissionOutcome::Admission(bd_event_buffer::AdmissionOutcome::Admitted),
    event_buffer.admit_flush(Some(completion))
  );
  assert!(!event_buffer.is_startup_gate_open());
  resume.notify_one();
  receiver.recv().await.unwrap();
  assert_eq!(vec!["previous", "current"], *setup.replayer_logs.lock());
  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_gate_opened",
    labels!("reason" => "barrier", "eligibility" => "unknown"),
  );
  shutdown.shutdown().await;
  handle.await.unwrap();
}

impl ReportProcessor for ReportProcessingSignal {
  fn process_all_pending_reports(
    &self,
  ) -> impl future::Future<Output = Vec<bd_crash_handler::CrashLog>> {
    self.0.notify_one();
    future::ready(Vec::new())
  }
}

#[tokio::test]
async fn before_startup_gate_ready_blocking_flush_completes_without_event_buffer_admission() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  assert_ok!(sender.flush_state(Block::Yes {
    timeout: 1.std_seconds(),
    poll_callback: None,
  }));

  buffer.event_buffer.mark_startup_gate_ready();
  assert_ok!(sender.try_send_log(normal_log("after early flush")));
  let entries = buffer.event_buffer.next_batch(2).await.entries;
  assert!(matches!(
    entries.as_slice(),
    [EventBufferEntry::Ingress(LoggerIngressEvent {
      payload: LoggerIngressPayload::Log(log),
      ..
    })] if log.message.as_str() == Some("after early flush")
  ));
}

#[tokio::test(start_paused = true)]
async fn startup_gate_ready_blocking_flush_releases_after_older_work() {
  let mut setup = Setup::new();
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
      ValueKind::Int(5_000),
    )]))
    .await
    .unwrap();
  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  config_update_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();

  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  wait_for_startup_gate_ready(&setup).await;

  assert_ok!(sender.try_send_log(normal_log("before barrier")));
  let blocking_sender = sender.clone();
  assert_ok!(
    tokio::task::spawn_blocking(move || {
      blocking_sender.flush_state(Block::Yes {
        timeout: 1.std_seconds(),
        poll_callback: None,
      })
    })
    .await
    .expect("blocking flush task must complete")
  );
  assert_eq!(vec!["before barrier"], *setup.replayer_logs.lock());
  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_gate_opened",
    labels!("reason" => "barrier", "eligibility" => "unknown"),
  );

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn startup_gate_ready_nonblocking_flush_does_not_release() {
  let mut setup = Setup::new();
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
      ValueKind::Int(5_000),
    )]))
    .await
    .unwrap();
  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  config_update_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();

  let event_buffer = buffer.event_buffer.clone();
  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  wait_for_startup_gate_ready(&setup).await;

  assert_ok!(sender.try_send_log(normal_log("behind nonblocking flush")));
  assert_ok!(sender.flush_state(Block::No));
  assert!(!event_buffer.is_startup_gate_open());
  assert_eq!(0, setup.replayer_log_count.load(Ordering::SeqCst));

  tokio::time::advance(5.std_seconds()).await;
  wait_for_startup_gate_opened(&setup).await;
  wait_for_replayed_logs(&setup, 1).await;
  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_gate_opened",
    labels!("reason" => "timer", "eligibility" => "unknown"),
  );
  setup.collector.assert_histogram_observed(
    5.0,
    "logger:event_buffer:startup_replay_gate_hold_duration_s",
    labels!("reason" => "timer", "eligibility" => "unknown"),
  );

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn startup_gate_releases_when_loaded_runtime_limits_expose_existing_pressure() {
  let mut setup = Setup::new();
  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  let mut retained_log = normal_log("pressure release");
  retained_log.log_type = LogType::LIFECYCLE;
  sender.try_send_log(retained_log).unwrap();
  for _ in 0 .. 500 {
    let _ = sender.try_send_control(LoggerControl::SetMemoryPressureLevel {
      level: MemoryPressureLevel::Warning,
    });
  }
  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));

  // The entries were admitted under the bootstrap budget. Loading a lower runtime budget must
  // still recognize their protected-pressure high watermark without waiting for another admission.
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![
      (
        bd_runtime::runtime::event_buffer::TotalLimitBytesFlag::path(),
        ValueKind::Int(2_048),
      ),
      (
        bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
        ValueKind::Int(5_000),
      ),
    ]))
    .await
    .unwrap();
  config_update_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();

  wait_for_startup_gate_ready(&setup).await;
  wait_for_startup_gate_opened(&setup).await;
  wait_for_replayed_logs(&setup, 1).await;
  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_gate_opened",
    labels!("reason" => "high_watermark", "eligibility" => "unknown"),
  );

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
  assert_eq!(vec!["pressure release"], *setup.replayer_logs.lock());
}

#[tokio::test(start_paused = true)]
async fn startup_replay_classification_selects_independent_configured_delays() {
  for (eligibility, expected_ms) in [
    (StartupReplayEligibility::NoPriorCrash, 0),
    (StartupReplayEligibility::MayHavePriorCrash, 1_000),
    (StartupReplayEligibility::Unknown, 250),
  ] {
    let mut setup = Setup::new();
    setup
      .runtime
      .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![
        (
          bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
          ValueKind::Int(250),
        ),
        (
          bd_runtime::runtime::event_buffer::StartupReplayCrashDelayFlag::path(),
          ValueKind::Int(1_000),
        ),
      ]))
      .await
      .unwrap();
    let (_tx, rx) = mpsc::channel(1);
    let (mut buffer, _) =
      setup.make_test_async_log_buffer_with_startup_replay_eligibility(rx, eligibility);
    buffer
      .event_buffer
      .start_startup_gate(buffer.startup_replay_delay.take());
    assert!(poll!(std::pin::pin!(buffer.event_buffer.next_batch(1))).is_pending());
    buffer.event_buffer.mark_startup_gate_ready();
    if expected_ms > 0 {
      assert!(poll!(std::pin::pin!(buffer.event_buffer.next_batch(1))).is_pending());
      tokio::time::advance((expected_ms - 1).std_milliseconds()).await;
      assert!(poll!(std::pin::pin!(buffer.event_buffer.next_batch(1))).is_pending());
      tokio::time::advance(1.std_milliseconds()).await;
    }
    let batch = buffer.event_buffer.next_batch(1).await;
    assert!(batch.entries.is_empty());
    let opening = batch.startup_gate_opened.unwrap();
    assert_eq!(expected_ms.std_milliseconds(), opening.hold_duration);
    buffer.record_startup_gate_opening(opening);
    setup.collector.assert_counter_eq(1, "logger:event_buffer:startup_replay_gate_opened",
      labels!("reason" => if expected_ms == 0 { "no_prior_crash" } else { "timer" }, "eligibility" => eligibility.label()));
  }
}

#[tokio::test(start_paused = true)]
async fn startup_replay_ignores_updates_to_the_unselected_delay() {
  for (eligibility, expected_ms, other) in [
    (
      StartupReplayEligibility::Unknown,
      50,
      bd_runtime::runtime::event_buffer::StartupReplayCrashDelayFlag::path(),
    ),
    (
      StartupReplayEligibility::MayHavePriorCrash,
      1_000,
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
    ),
    (
      StartupReplayEligibility::NoPriorCrash,
      0,
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
    ),
  ] {
    let mut setup = Setup::new();
    let (_tx, rx) = mpsc::channel(1);
    let (mut buffer, _) =
      setup.make_test_async_log_buffer_with_startup_replay_eligibility(rx, eligibility);
    buffer
      .event_buffer
      .start_startup_gate(buffer.startup_replay_delay.take());
    assert!(poll!(std::pin::pin!(buffer.event_buffer.next_batch(1))).is_pending());
    setup
      .runtime
      .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
        other,
        ValueKind::Int(5_000),
      )]))
      .await
      .unwrap();
    buffer.event_buffer.mark_startup_gate_ready();
    if expected_ms > 0 {
      assert!(poll!(std::pin::pin!(buffer.event_buffer.next_batch(1))).is_pending());
      tokio::time::advance(expected_ms.std_milliseconds()).await;
    }
    assert_eq!(
      expected_ms.std_milliseconds(),
      buffer
        .event_buffer
        .next_batch(1)
        .await
        .startup_gate_opened
        .unwrap()
        .hold_duration
    );
  }
}

#[tokio::test(start_paused = true)]
async fn startup_gate_replays_previous_process_entries_before_current_entries() {
  let mut setup = Setup::new();
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
      ValueKind::Int(0),
    )]))
    .await
    .unwrap();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  sender.try_send_log(normal_log("current")).unwrap();
  assert_eq!(
    bd_event_buffer::AdmissionOutcome::Admitted,
    buffer
      .event_buffer
      .admit(EventBufferEntry::ingress(LoggerIngressEvent::log(
        normal_log("previous"),
        EventContext::PreviousProcess {
          logged_at: OffsetDateTime::UNIX_EPOCH,
        },
        None,
      )))
  );

  config_update_tx
    .send(setup.make_config_update(WorkflowsConfiguration::default()))
    .await
    .unwrap();

  let state_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));

  wait_for_replayed_logs(&setup, 2).await;

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
  assert_eq!(vec!["previous", "current"], *setup.replayer_logs.lock());
}

struct TestReplay {
  logs_count: Arc<AtomicUsize>,
  logs_notify: Arc<Notify>,
  logs: Arc<parking_lot::Mutex<Vec<std::string::String>>>,
  fields: Arc<parking_lot::Mutex<Vec<LogFields>>>,
  feature_flags: Arc<parking_lot::Mutex<Vec<Option<String>>>>,
}

struct StaticReportProcessor(parking_lot::Mutex<Vec<bd_crash_handler::CrashLog>>);

struct FailingMetadataProvider;

impl MetadataProvider for FailingMetadataProvider {
  fn timestamp(&self) -> anyhow::Result<OffsetDateTime> {
    Err(anyhow::anyhow!("metadata provider failed"))
  }

  fn fields(&self) -> anyhow::Result<(LogFields, LogFields)> {
    Err(anyhow::anyhow!("metadata provider failed"))
  }
}

struct FieldsFailingMetadataProvider {
  timestamp: OffsetDateTime,
}

impl MetadataProvider for FieldsFailingMetadataProvider {
  fn timestamp(&self) -> anyhow::Result<OffsetDateTime> {
    Ok(self.timestamp)
  }

  fn fields(&self) -> anyhow::Result<(LogFields, LogFields)> {
    Err(anyhow::anyhow!("metadata provider fields failed"))
  }
}

impl StaticReportProcessor {
  fn new(reports: Vec<bd_crash_handler::CrashLog>) -> Self {
    Self(parking_lot::Mutex::new(reports))
  }
}

impl ReportProcessor for StaticReportProcessor {
  fn process_all_pending_reports(
    &self,
  ) -> impl future::Future<Output = Vec<bd_crash_handler::CrashLog>> {
    future::ready(std::mem::take(&mut *self.0.lock()))
  }
}

fn crash_log(message: &str, timestamp: OffsetDateTime) -> bd_crash_handler::CrashLog {
  bd_crash_handler::CrashLog {
    log_level: log_level::ERROR,
    fields: [].into(),
    timestamp,
    message: message.into(),
  }
}

fn normal_log(message: &str) -> LogLine {
  LogLine {
    log_level: log_level::INFO,
    log_type: LogType::NORMAL,
    message: message.into(),
    fields: [].into(),
    matching_fields: [].into(),
    attributes_overrides: None,
    capture_session: None,
  }
}

async fn wait_for_startup_gate_ready(setup: &Setup) {
  setup.startup_gate_ready.wait().await;
}

async fn wait_for_startup_gate_opened(setup: &Setup) {
  setup.startup_gate_opened.wait().await;
}

async fn wait_for_replayed_logs(setup: &Setup, expected_count: usize) {
  loop {
    let notified = setup.replayer_log_notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if setup.replayer_log_count.load(Ordering::SeqCst) >= expected_count {
      return;
    }
    notified.await;
  }
}

impl TestReplay {
  fn new() -> Self {
    Self {
      logs_count: Arc::new(AtomicUsize::new(0)),
      logs_notify: Arc::new(Notify::new()),
      logs: Arc::new(parking_lot::Mutex::new(vec![])),
      fields: Arc::new(parking_lot::Mutex::new(vec![])),
      feature_flags: Arc::new(parking_lot::Mutex::new(vec![])),
    }
  }
}

#[async_trait::async_trait]
impl LogReplay for TestReplay {
  async fn replay_log(
    &mut self,
    log: Log,
    _processing_pipeline: &mut ProcessingPipeline,
    _state: &dyn StateReader,
    _now: OffsetDateTime,
  ) -> anyhow::Result<LogReplayResult> {
    if let Some(message) = log.message.as_str() {
      self.logs.lock().push(message.to_string());
    }

    self.feature_flags.lock().push(
      _state
        .get(Scope::FeatureFlagExposure, "flag")
        .filter(|value| value.has_string_value())
        .map(|value| value.string_value().to_string()),
    );
    self.fields.lock().push(log.fields);
    self.logs_count.fetch_add(1, Ordering::SeqCst);
    self.logs_notify.notify_waiters();

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

#[tokio::test]
async fn current_crash_reports_are_admitted_in_report_order() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let session_id = setup.session_strategy.session_id().unwrap();
  let first_timestamp = OffsetDateTime::UNIX_EPOCH + 1.seconds();
  let second_timestamp = OffsetDateTime::UNIX_EPOCH + 2.seconds();
  let report_processor = StaticReportProcessor::new(vec![
    crash_log("first", first_timestamp),
    crash_log("second", second_timestamp),
  ]);

  buffer.admit_crash_reports(
    report_processor.process_all_pending_reports().await,
    &crate::ReportProcessingSession::Current,
  );
  buffer.event_buffer.mark_startup_gate_ready();

  let entries = buffer.event_buffer.next_batch(2).await.entries;
  assert_eq!(2, entries.len());
  for (entry, (expected_message, expected_timestamp)) in entries
    .into_iter()
    .zip([("first", first_timestamp), ("second", second_timestamp)])
  {
    let EventBufferEntry::Ingress(event) = entry else {
      panic!("crash report must be EventBuffer ingress");
    };
    assert!(matches!(
      event.context,
      EventContext::CurrentProcess(context) if context.session_id == session_id
    ));
    let LoggerIngressPayload::Log(log) = event.payload else {
      panic!("crash report ingress must carry a log");
    };
    assert_eq!(Some(expected_message), log.message.as_str());
    assert!(matches!(
      log.attributes_overrides,
      Some(LogAttributesOverrides::OccurredAt(timestamp)) if timestamp == expected_timestamp
    ));
    assert_eq!(Some("crash_handler"), log.capture_session);
  }
}

#[tokio::test]
async fn late_previous_process_crash_work_is_recorded_once_per_startup() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer_with_startup_replay_eligibility(
    config_update_rx,
    StartupReplayEligibility::MayHavePriorCrash,
  );
  buffer.event_buffer.mark_startup_gate_ready();
  assert!(
    buffer
      .event_buffer
      .next_batch(1)
      .await
      .startup_gate_opened
      .is_some()
  );

  buffer.admit_crash_reports(
    vec![
      crash_log("first", OffsetDateTime::UNIX_EPOCH),
      crash_log("second", OffsetDateTime::UNIX_EPOCH),
    ],
    &crate::ReportProcessingSession::PreviousRun,
  );
  buffer.admit_crash_reports(
    vec![crash_log("third", OffsetDateTime::UNIX_EPOCH)],
    &crate::ReportProcessingSession::PreviousRun,
  );

  setup.collector.assert_counter_eq(
    1,
    "logger:event_buffer:startup_replay_late_previous_process_work",
    labels!("eligibility" => "may_have_prior_crash"),
  );
}

#[tokio::test]
async fn crash_report_batch_stays_at_its_event_buffer_admission_boundary() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  let report_processor = StaticReportProcessor::new(vec![
    crash_log("first", OffsetDateTime::UNIX_EPOCH),
    crash_log("second", OffsetDateTime::UNIX_EPOCH),
  ]);

  sender.try_send_log(normal_log("before")).unwrap();
  buffer.admit_crash_reports(
    report_processor.process_all_pending_reports().await,
    &crate::ReportProcessingSession::Current,
  );
  sender.try_send_log(normal_log("after")).unwrap();
  buffer.event_buffer.mark_startup_gate_ready();

  let messages = buffer
    .event_buffer
    .next_batch(4)
    .await
    .entries
    .into_iter()
    .map(|entry| {
      let EventBufferEntry::Ingress(event) = entry else {
        panic!("expected log ingress");
      };
      let LoggerIngressPayload::Log(log) = event.payload else {
        panic!("expected log payload");
      };
      log.message.as_str().unwrap().to_string()
    })
    .collect::<Vec<_>>();

  assert_eq!(vec!["before", "first", "second", "after"], messages);
}

#[tokio::test]
async fn previous_run_crash_reports_use_previous_process_context() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let first_timestamp = OffsetDateTime::UNIX_EPOCH + 1.seconds();
  let second_timestamp = OffsetDateTime::UNIX_EPOCH + 2.seconds();
  let report_processor = StaticReportProcessor::new(vec![
    crash_log("first", first_timestamp),
    crash_log("second", second_timestamp),
  ]);

  buffer.admit_crash_reports(
    report_processor.process_all_pending_reports().await,
    &crate::ReportProcessingSession::PreviousRun,
  );
  buffer.event_buffer.mark_startup_gate_ready();

  let entries = buffer.event_buffer.next_batch(2).await.entries;
  assert_eq!(2, entries.len());
  for (entry, (expected_message, expected_timestamp)) in entries
    .into_iter()
    .zip([("first", first_timestamp), ("second", second_timestamp)])
  {
    let EventBufferEntry::Ingress(event) = entry else {
      panic!("crash report must be EventBuffer ingress");
    };
    assert!(matches!(
      event.context,
      EventContext::PreviousProcess { logged_at } if logged_at != OffsetDateTime::UNIX_EPOCH
    ));
    let LoggerIngressPayload::Log(log) = event.payload else {
      panic!("crash report ingress must carry a log");
    };
    assert_eq!(Some(expected_message), log.message.as_str());
    assert!(matches!(
      log.attributes_overrides,
      Some(LogAttributesOverrides::PreviousRunSessionID(timestamp))
        if timestamp == expected_timestamp
    ));
    assert_eq!(Some("crash_handler"), log.capture_session);
  }
}

#[test]
fn workflow_generated_logs_keep_the_parent_context_and_overrides() {
  let report_timestamp = OffsetDateTime::UNIX_EPOCH + 1.seconds();
  let admitted_at = OffsetDateTime::UNIX_EPOCH + 2.seconds();
  let (log, context) = workflow_generated_log(
    Log {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "generated".into(),
      fields: [].into(),
      matching_fields: [].into(),
      session_id: "ignored-by-generated-log".into(),
      occurred_at: OffsetDateTime::UNIX_EPOCH,
      capture_session: Some("generated"),
    },
    Some(EventContext::PreviousProcess {
      logged_at: admitted_at,
    }),
    Some(LogAttributesOverrides::PreviousRunSessionID(
      report_timestamp,
    )),
  );

  assert_eq!(Some("generated"), log.message.as_str());
  assert!(matches!(
    log.attributes_overrides,
    Some(LogAttributesOverrides::PreviousRunSessionID(timestamp)) if timestamp == report_timestamp
  ));
  assert_eq!(Some("generated"), log.capture_session);
  assert!(matches!(
    context,
    Some(EventContext::PreviousProcess { logged_at }) if logged_at == admitted_at
  ));
}

#[tokio::test]
async fn feature_flag_exposure_captures_its_admission_session() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  let admitted_session_id = setup.session_strategy.session_id().unwrap();

  sender
    .try_send_feature_flag_exposure("flag".to_string(), Some("variant".to_string()))
    .unwrap();
  setup.session_strategy.start_new_session(None).unwrap();
  buffer.event_buffer.mark_startup_gate_ready();

  let entry = buffer
    .event_buffer
    .next_batch(1)
    .await
    .entries
    .pop()
    .unwrap();
  let EventBufferEntry::Ingress(event) = entry else {
    panic!("feature flag exposure must be EventBuffer ingress");
  };
  assert!(matches!(
    event.context,
    EventContext::CurrentProcess(context) if context.session_id == admitted_session_id
  ));
  assert!(matches!(
    event.payload,
    LoggerIngressPayload::FeatureFlagExposure { flag, variant }
      if flag == "flag" && variant.as_deref() == Some("variant")
  ));
}

#[test]
fn sender_reports_context_capture_failures_separately_from_capacity() {
  let setup = Setup::new();
  let sender = Sender::new(
    EventBuffer::new(EventBufferLimits {
      log_limit_bytes: 1_000_000,
      total_limit_bytes: 10_000_000,
    }),
    Arc::new(FailingMetadataProvider),
    setup.session_strategy,
  );

  assert!(matches!(
    sender.try_send_log(normal_log("unadmitted")),
    Err(AdmissionError::ContextCaptureFailed)
  ));
}

#[test]
fn current_process_admission_context_captures_provider_snapshot() {
  let setup = Setup::new();
  let timestamp = OffsetDateTime::UNIX_EPOCH + 3.seconds();
  let metadata_provider: Arc<dyn bd_log_metadata::MetadataProvider + Send + Sync> =
    Arc::new(LogMetadata {
      timestamp: parking_lot::Mutex::new(timestamp),
      custom_fields: [("custom".into(), "custom-value".into())].into(),
      ootb_fields: [("ootb".into(), "ootb-value".into())].into(),
    });

  let context =
    current_process_admission_context(&metadata_provider, &setup.session_strategy).unwrap();

  assert_eq!(timestamp, context.admitted_at);
  assert_eq!(timestamp, context.provider.timestamp);
  assert_eq!(1, context.provider.custom_fields.len());
  assert_eq!(1, context.provider.ootb_fields.len());
  assert_eq!(
    setup.session_strategy.session_id().unwrap(),
    context.session_id
  );
}

#[test]
fn previous_process_admission_does_not_capture_provider_fields() {
  let setup = Setup::new();
  let logged_at = OffsetDateTime::UNIX_EPOCH + 3.seconds();
  let metadata_provider: Arc<dyn MetadataProvider + Send + Sync> =
    Arc::new(FieldsFailingMetadataProvider {
      timestamp: logged_at,
    });

  let context = admission_context(
    Some(&LogAttributesOverrides::PreviousRunSessionID(
      OffsetDateTime::UNIX_EPOCH,
    )),
    &metadata_provider,
    &setup.session_strategy,
  )
  .unwrap();

  assert!(matches!(
    context,
    EventContext::PreviousProcess { logged_at: actual } if actual == logged_at
  ));
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

  let baseline_log_expected_size = 566;
  let baseline_log = create_baseline_log();
  assert_eq!(
    baseline_log_expected_size,
    baseline_log.approximate_size_bytes()
  );

  // The approximate accounting reserves string capacity. Appending to the three-byte message grows
  // its allocation from three bytes to eight bytes.
  let mut baseline_log_with_longer_message = create_baseline_log();
  baseline_log_with_longer_message.message =
    DataValue::from(baseline_log.message.as_str().unwrap().to_owned() + "1");
  assert_eq!(
    baseline_log_expected_size + 5,
    baseline_log_with_longer_message.approximate_size_bytes()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_key = create_baseline_log();
  baseline_log_with_longer_field_key.fields =
    [("foo".into(), AnnotatedLogField::new_ootb("bar1"))].into();

  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.approximate_size_bytes()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields =
    [("foo".into(), AnnotatedLogField::new_ootb("bar1"))].into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_value.approximate_size_bytes()
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

  let baseline_log_expected_size = 550;
  let baseline_log = create_baseline_log();
  assert_eq!(
    baseline_log_expected_size,
    baseline_log.approximate_size_bytes()
  );

  // The approximate accounting reserves string capacity. Appending to the three-byte message grows
  // its allocation from three bytes to eight bytes.
  let mut baseline_log_with_longer_message = create_baseline_log();
  baseline_log_with_longer_message.message =
    DataValue::from(baseline_log.message.as_str().unwrap().to_owned() + "1");
  assert_eq!(
    baseline_log_expected_size + 5,
    baseline_log_with_longer_message.approximate_size_bytes()
  );

  // Session IDs are shared, so their allocation is not charged to each log.
  let mut baseline_log_with_longer_group = create_baseline_log();
  baseline_log_with_longer_group.session_id =
    format!("{}1", baseline_log_with_longer_group.session_id).into();
  assert_eq!(
    baseline_log_expected_size,
    baseline_log_with_longer_group.approximate_size_bytes()
  );

  // Add one extra character to one of the fields' keys and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_key = create_baseline_log();
  baseline_log_with_longer_field_key.fields =
    [("foo".into(), DataValue::String("bar1".to_string()))].into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_key.approximate_size_bytes()
  );

  // Add one extra character to one of the fields' values and verify that reported size increases
  // by 1 byte
  let mut baseline_log_with_longer_field_value = baseline_log;
  baseline_log_with_longer_field_value.fields =
    [("foo".into(), DataValue::String("bar1".to_string()))].into();
  assert_eq!(
    baseline_log_expected_size + 1,
    baseline_log_with_longer_field_value.approximate_size_bytes()
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
    _ = Box::pin(buffer.run(state_store, ())).await;
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

  let config_update2 =
    setup.make_config_update(WorkflowsConfiguration::new_with_workflow_configurations(
      vec![WorkflowBuilder::new("1", &[&a, &b]).make_config()],
    ));
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
    message: DataValue::String(String::new()),
    fields: AnnotatedLogFields::new(),
    matching_fields: AnnotatedLogFields::new(),
    attributes_overrides: None,
    capture_session: None,
  };

  sender.try_send_log(log).unwrap();

  let state_store = TestStore::new().await;

  // Timeout as otherwise buffer's workflows engine continues to try
  // to periodically flush its state to disk which hold us stuck here.
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  wait_for_replayed_logs(&setup, 1).await;

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
  wait_for_startup_gate_ready(&setup).await;

  let first_session_id = setup.session_strategy.session_id().unwrap();
  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "first".into(),
    [].into(),
    [].into(),
    None,
    None,
  ));

  setup.session_strategy.start_new_session(None).unwrap();
  let second_session_id = setup.session_strategy.session_id().unwrap();
  assert_ne!(first_session_id, second_session_id);

  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "second".into(),
    [].into(),
    [].into(),
    None,
    None,
  ));

  wait_for_replayed_logs(&setup, 2).await;
  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  {
    let reader = test_store.read().await;
    let value = reader.get(Scope::System, SYSTEM_SESSION_ID_KEY);
    assert!(value.is_some_and(|stored| {
      stored.has_string_value() && stored.string_value() == second_session_id.as_ref()
    }));
  }

  drop(test_store);
  task.join().unwrap();
}

#[tokio::test]
async fn set_memory_pressure_level_writes_to_system_scope() {
  let mut setup = Setup::new();
  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let test_store = TestStore::new().await;
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    (*test_store).clone(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  wait_for_startup_gate_ready(&setup).await;

  sender
    .try_send_control(LoggerControl::SetMemoryPressureLevel {
      level: MemoryPressureLevel::Warning,
    })
    .unwrap();

  let flush_sender = sender.clone();
  tokio::task::spawn_blocking(move || {
    assert_ok!(flush_sender.flush_state(Block::Yes {
      timeout: 5.std_seconds(),
      poll_callback: None,
    }));
  })
  .await
  .unwrap();

  {
    let reader = test_store.read().await;
    assert!(
      reader
        .get(Scope::System, MEMORY_PRESSURE_LEVEL_KEY)
        .is_some_and(|v| v.has_string_value() && v.string_value() == "Warning")
    );
  }

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
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
  wait_for_startup_gate_ready(&setup).await;

  let current_session_id = setup.session_strategy.session_id().unwrap();
  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "current".into(),
    [].into(),
    [].into(),
    None,
    None,
  ));

  setup.session_strategy.start_new_session(None).unwrap();
  let next_session_id = setup.session_strategy.session_id().unwrap();
  assert_ne!(current_session_id, next_session_id);

  assert_ok!(AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    0,
    LogType::NORMAL,
    "next".into(),
    [].into(),
    [].into(),
    None,
    None,
  ));

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
  sender.try_send_log(log).unwrap();

  // The flush control follows both current-process logs and the previous-process log in the
  // EventBuffer's admission order, so completion proves all three were processed.
  let flush_sender = sender.clone();
  tokio::task::spawn_blocking(move || {
    assert_ok!(flush_sender.flush_state(Block::Yes {
      timeout: 5.std_seconds(),
      poll_callback: None,
    }));
  })
  .await
  .unwrap();

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();

  {
    let reader = test_store.read().await;
    let value = reader.get(Scope::System, SYSTEM_SESSION_ID_KEY);
    assert!(value.is_some_and(|stored| {
      stored.has_string_value() && stored.string_value() == next_session_id.as_ref()
    }));
  }

  drop(test_store);
  task.join().unwrap();
}

#[test]
fn initial_field_state_updates_skip_unchanged_values() {
  let initial_custom_fields: LogFields = [("field".into(), "value".into())].into();
  let mut state = InMemoryStateReader::new();
  state.insert(
    Scope::CustomFields,
    "field",
    super::persistent_field_value(DataValue::String("value".to_string())),
  );

  assert!(
    super::initial_field_state_updates(LogFields::default(), initial_custom_fields, &state,)
      .is_empty()
  );
}

#[tokio::test]
async fn ootb_ownership_prevents_custom_state_changes() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let state_store = TestStore::new().await;
  let ootb_value = super::persistent_field_value(DataValue::String("ootb".to_string()));
  let custom_value = super::persistent_field_value(DataValue::String("custom".to_string()));

  assert_ok!(
    state_store
      .insert(Scope::OotbFields, "shared".to_string(), ootb_value.clone())
      .await
  );

  buffer
    .process_control(
      LoggerControl::AddLogField(
        "shared".to_string(),
        DataValue::String("custom".to_string()),
      ),
      &state_store,
    )
    .await;

  let state = state_store.read().await;
  assert_eq!(state.get(Scope::OotbFields, "shared"), Some(&ootb_value));
  assert!(state.get(Scope::CustomFields, "shared").is_none());
  drop(state);
  assert!(!buffer.metadata_collector.is_ootb_field("shared"));

  // Preserve a legacy custom value while its OOTB counterpart owns the virtual field. This can
  // occur after upgrading from a version that allowed both state entries to coexist.
  assert_ok!(
    state_store
      .insert(
        Scope::CustomFields,
        "shared".to_string(),
        custom_value.clone()
      )
      .await
  );
  buffer
    .process_control(
      LoggerControl::RemoveLogField("shared".to_string()),
      &state_store,
    )
    .await;

  assert_eq!(
    state_store.read().await.get(Scope::CustomFields, "shared"),
    Some(&custom_value)
  );
}

#[tokio::test]
async fn metadata_ootb_ownership_prevents_custom_state_changes() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let state_store = TestStore::new().await;

  buffer
    .metadata_collector
    .update_ootb_field("metadata_only".into(), "ootb".into());
  buffer
    .process_control(
      LoggerControl::AddLogField(
        "metadata_only".to_string(),
        DataValue::String("custom".to_string()),
      ),
      &state_store,
    )
    .await;

  assert!(
    state_store
      .read()
      .await
      .get(Scope::CustomFields, "metadata_only")
      .is_none()
  );

  let custom_value = super::persistent_field_value(DataValue::String("custom".to_string()));
  assert_ok!(
    state_store
      .insert(
        Scope::CustomFields,
        "metadata_only".to_string(),
        custom_value.clone(),
      )
      .await
  );
  buffer
    .process_control(
      LoggerControl::RemoveLogField("metadata_only".to_string()),
      &state_store,
    )
    .await;

  assert_eq!(
    state_store
      .read()
      .await
      .get(Scope::CustomFields, "metadata_only"),
    Some(&custom_value)
  );
}

#[tokio::test]
async fn capacity_rejected_field_updates_preserve_state_and_metadata() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let state_store = TestStore::new_with_config(PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 8 * 1024,
    high_water_mark_ratio: 0.8,
  })
  .await;
  let custom_initial = DataValue::String("custom_initial".to_string());
  let ootb_initial = DataValue::String("ootb_initial".to_string());

  buffer
    .process_control(
      LoggerControl::AddLogField("custom".to_string(), custom_initial.clone()),
      &state_store,
    )
    .await;
  buffer
    .process_control(
      LoggerControl::UpdateOotbLogField("ootb".to_string(), ootb_initial.clone()),
      &state_store,
    )
    .await;
  assert_ok!(
    state_store
      .insert(
        Scope::System,
        "unrelated".to_string(),
        bd_state::string_value("x".repeat(6_200)),
      )
      .await
  );

  // These writes cannot fit alongside the surviving system state after compaction. Rejection
  // must leave both the virtual state and the metadata source of emitted fields unchanged.
  let rejected = DataValue::String("rejected".repeat(585));
  buffer
    .process_control(
      LoggerControl::AddLogField("custom".to_string(), rejected.clone()),
      &state_store,
    )
    .await;
  buffer
    .process_control(
      LoggerControl::UpdateOotbLogField("ootb".to_string(), rejected),
      &state_store,
    )
    .await;

  let state = state_store.read().await;
  assert_eq!(
    state.get(Scope::CustomFields, "custom"),
    Some(&super::persistent_field_value(custom_initial.clone()))
  );
  assert_eq!(
    state.get(Scope::OotbFields, "ootb"),
    Some(&super::persistent_field_value(ootb_initial.clone()))
  );
  drop(state);

  let (ootb_fields, custom_fields) = buffer.metadata_collector.initial_persistent_fields();
  assert_eq!(custom_fields.get("custom"), Some(&custom_initial));
  assert_eq!(ootb_fields.get("ootb"), Some(&ootb_initial));
}

#[tokio::test]
async fn capacity_rejected_initial_fields_are_dropped_from_metadata() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let state_store = TestStore::new_with_config(PersistentStoreConfig {
    initial_buffer_size: 8 * 1024,
    max_capacity_bytes: 8 * 1024,
    high_water_mark_ratio: 0.8,
  })
  .await;
  assert_ok!(
    state_store
      .insert(
        Scope::System,
        "unrelated".to_string(),
        bd_state::string_value("x".repeat(6_200)),
      )
      .await
  );
  let rejected = DataValue::String("rejected".repeat(585));
  assert_ok!(
    buffer
      .metadata_collector
      .add_field("initial_custom".into(), rejected.clone())
  );
  buffer
    .metadata_collector
    .update_ootb_field("initial_ootb".into(), rejected.clone());

  // Startup has no previous inline value to retain, so an unpersistable field is dropped rather
  // than emitted without a matching virtual state value.
  buffer
    .persist_initial_log_fields(
      [("initial_ootb".into(), rejected.clone())].into(),
      [("initial_custom".into(), rejected)].into(),
      &state_store,
    )
    .await;

  let state = state_store.read().await;
  assert!(state.get(Scope::CustomFields, "initial_custom").is_none());
  assert!(state.get(Scope::OotbFields, "initial_ootb").is_none());
  drop(state);

  let (ootb_fields, custom_fields) = buffer.metadata_collector.initial_persistent_fields();
  assert!(!custom_fields.contains_key("initial_custom"));
  assert!(!ootb_fields.contains_key("initial_ootb"));
}

#[tokio::test]
async fn previous_process_logs_use_snapshot_state() {
  let mut setup = Setup::new();
  let (config_update_tx, config_update_rx) = mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  let state_store = TestStore::new().await;
  assert_ok!(
    state_store
      .insert(
        Scope::FeatureFlagExposure,
        "flag".to_string(),
        bd_state::string_value("current"),
      )
      .await
  );

  let mut previous_run_state = bd_versioned_kv::ScopedMaps::default();
  previous_run_state.insert(
    Scope::FeatureFlagExposure,
    "flag".to_string(),
    bd_versioned_kv::TimestampedValue {
      timestamp: 0,
      value: bd_state::string_value("previous"),
    },
  );

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    assert_ok!(config_update_tx.blocking_send(config_update));
  });
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown_and_previous_state(
    state_store.take_inner(),
    (),
    Arc::new(previous_run_state),
    shutdown_trigger.make_shutdown(),
  ));
  wait_for_startup_gate_ready(&setup).await;

  sender
    .try_send_log(LogLine {
      log_level: log_level::DEBUG,
      log_type: LogType::NORMAL,
      message: "previous".into(),
      fields: AnnotatedLogFields::new(),
      matching_fields: AnnotatedLogFields::new(),
      attributes_overrides: Some(LogAttributesOverrides::PreviousRunSessionID(
        OffsetDateTime::now_utc(),
      )),
      capture_session: None,
    })
    .unwrap();
  wait_for_replayed_logs(&setup, 1).await;

  shutdown_trigger.shutdown().await;
  handle.await.unwrap();
  task.join().unwrap();

  assert_eq!(
    &[Some("previous".to_string())],
    setup.replayer_feature_flags.lock().as_slice()
  );
}

#[tokio::test]
async fn processes_log_with_global_state_in_attributes_overrides() {
  let mut setup = Setup::new();

  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
      bd_runtime::runtime::event_buffer::StartupReplayDelayFlag::path(),
      ValueKind::Int(0),
    )]))
    .await
    .unwrap();

  let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

  let (buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);

  let config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let task = std::thread::spawn(move || {
    // Config push disables workflow engine by pushing an empty workflow config.
    assert_ok!(config_update_tx.blocking_send(config_update));
  });

  let state_store = TestStore::new().await;

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let handle = tokio::task::spawn(buffer.run_with_shutdown(
    state_store.take_inner(),
    (),
    shutdown_trigger.make_shutdown(),
  ));
  wait_for_startup_gate_ready(&setup).await;

  sender
    .try_send_control(LoggerControl::AddLogField(
      "global_key".to_string(),
      DataValue::String("global_value".to_string()),
    ))
    .unwrap();

  // A normal log persists the current global state before the following ordered flush completes.
  AsyncLogBuffer::<TestReplay>::enqueue_log(
    &sender,
    log_level::DEBUG,
    LogType::NORMAL,
    "prime".into(),
    [].into(),
    [].into(),
    None,
    None,
  )
  .unwrap();

  // The flush follows the state update and log in EventBuffer admission order.
  let flush_sender = sender.clone();
  tokio::task::spawn_blocking(move || {
    assert_ok!(flush_sender.flush_state(Block::Yes {
      timeout: 5.std_seconds(),
      poll_callback: None,
    }));
  })
  .await
  .unwrap();

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

  // A new buffer snapshots the persisted values as previous-process state.
  shutdown_trigger.shutdown().await;
  let _buffer = handle.await.unwrap();
  assert_ok!(task.join());

  let (config_update_tx_2, config_update_rx_2) = tokio::sync::mpsc::channel(1);
  let (buffer_2, sender_2) = setup.make_test_async_log_buffer(config_update_rx_2);

  let config_update_2 = setup.make_config_update(WorkflowsConfiguration::default());
  let task_2 = std::thread::spawn(move || {
    assert_ok!(config_update_tx_2.blocking_send(config_update_2));
  });

  let shutdown_trigger_2 = ComponentShutdownTrigger::default();
  let state_store_2 = TestStore::new().await;
  let handle_2 = tokio::task::spawn(buffer_2.run_with_shutdown(
    state_store_2.take_inner(),
    (),
    shutdown_trigger_2.make_shutdown(),
  ));
  wait_for_startup_gate_ready(&setup).await;

  sender_2.try_send_log(log).unwrap();
  wait_for_replayed_logs(&setup, 1).await;

  shutdown_trigger_2.shutdown().await;
  let _buffer_2 = handle_2.await.unwrap();
  assert_ok!(task_2.join());

  assert_eq!(1, setup.replayer_log_count.load(Ordering::SeqCst));

  let logs = setup.replayer_logs.lock();
  let fields = setup.replayer_fields.lock();

  assert_eq!("test", logs[0]);

  assert!(fields[0].contains_key("global_key"));
  let val = &fields[0]["global_key"];
  match val {
    bd_log_primitives::LogFieldValue::String(s) => assert_eq!("global_value", s),
    _ => panic!("Unexpected value type"),
  }
}
