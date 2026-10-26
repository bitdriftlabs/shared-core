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
  FlushError,
  LogAdmission,
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
use crate::device_command::{
  CommandAttachment,
  CommandExecutionPolicy,
  CommandInvocation,
  CommandResult,
  DeviceCommandDispatcher,
  DeviceCommandRegistry,
  RegisteredCommandDispatcher,
  RegisteredCommandHandler,
  WorkflowCommandCompletion,
  WorkflowCommandDispatcher,
};
use crate::log_replay::{LogReplayResult, LoggerReplay, ProcessingPipeline};
use crate::logging_state::{BufferProducers, ConfigUpdate, UninitializedLoggingContext};
use crate::metadata::MetadataCollector;
use crate::{Block, InitializationState, StartupReplayEligibility};
use bd_api::upload::DeviceCommandState;
use bd_api::{DataUpload, SimpleNetworkQualityProvider};
use bd_artifact_upload::{MockClient, UploadSource};
use bd_client_common::file::read_checksummed_data;
use bd_client_common::init_lifecycle::{InitLifecycle, InitLifecycleState};
use bd_client_stats::{FlushTrigger, Stats};
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_completion::RecvWithTimeoutError;
use bd_event_buffer::{
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventContext,
  LoggerIngressEvent,
  LoggerIngressPayload,
  ProviderSnapshot,
};
use bd_log_filter::FilterChain;
use bd_log_matcher::builder::message_equals;
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::command_attachment::{AttachmentSource, LogAttachment};
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  COMMAND_OUTCOME_MESSAGE,
  COMMAND_START_MESSAGE,
  DataValue,
  LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON,
  LOG_FIELD_NAME_DURATION_MS,
  LOG_FIELD_NAME_RESULT,
  LOG_FIELD_NAME_SPAN_ID,
  Log,
  LogFields,
  SPAN_TYPE_END,
  SPAN_TYPE_START,
  log_level,
};
use bd_macros::ApproximateSize;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
use bd_proto::protos::client::api::device_command_update;
use bd_proto::protos::client::artifact::WorkflowAttachmentState;
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::filter::filter::FiltersConfiguration;
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::state::scope::StateScope;
use bd_proto::protos::workflow::workflow::workflow::rule::Rule_type;
use bd_proto::protos::workflow::workflow::workflow::{MatchRunCommand, Rule};
use bd_proto::protos::workflow::workflow_command::{
  WorkflowCommandSelector,
  workflow_command_selector,
};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::Strategy;
use bd_session::test::no_timeout;
use bd_shutdown::ComponentShutdownTrigger;
use bd_state::test::TestStore;
use bd_state::{MEMORY_PRESSURE_LEVEL_KEY, SYSTEM_SESSION_ID_KEY, Scope, StateReader};
use bd_stats_common::labels;
use bd_test_helpers::config_helper::{BufferType, default_buffer_config, match_message};
use bd_test_helpers::events::NoOpListenerTarget;
use bd_test_helpers::metadata_provider::LogMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::rule;
use bd_test_helpers::runtime::ValueKind;
use bd_test_helpers::session::in_memory_store;
use bd_test_helpers::workflow::{
  WorkflowBuilder,
  make_emit_counter_action,
  make_flush_buffers_action,
  make_generate_log_action_proto,
  make_on_new_session_rule,
  make_state_change_rule,
  metric_value,
  state,
};
use bd_time::{
  OffsetDateTimeExt,
  SystemTimeProvider,
  TestTimeProvider,
  TimeDurationExt,
  TimeProvider,
};
use bd_workflows::config::{Config, WorkflowsConfiguration};
use bd_workflows::engine::ProcessLocalPendingFlushState;
use bd_workflows::test::MakeConfig;
use bd_workflows::workflow::{
  WorkflowCommandCompletionToken,
  WorkflowCommandEvent,
  WorkflowCommandOutcome,
};
use futures_util::{FutureExt, poll};
use protobuf::Message;
use std::collections::VecDeque;
use std::future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use time::ext::{NumericalDuration, NumericalStdDuration};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_test::assert_ok;
use uuid::Uuid;

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

  fn trigger_buffer_upload_completed(&self, buffer_id: &str) {
    if let Some(delegate) = &self.delegate {
      delegate.trigger_buffer_upload_completed(buffer_id);
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
  buffer_event_rx: Option<tokio::sync::mpsc::Receiver<bd_buffer::BufferEventWithResponse>>,
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
    let (buffer_manager, buffer_event_rx) = bd_buffer::Manager::new(
      tmp_dir.path().join("buffer"),
      &collector.scope(""),
      runtime,
      Arc::new(bd_versioned_kv::RetentionRegistry::new(
        bd_runtime::runtime::IntWatch::new_for_testing(0),
      )),
    );

    Self {
      buffer_manager,
      buffer_event_rx: Some(buffer_event_rx),
      runtime: Self::make_runtime(&tmp_dir),
      collector,
      stats,
      tmp_dir,
      replayer_log_count: Arc::default(),
      replayer_log_notify: Arc::new(Notify::new()),
      replayer_logs: Arc::default(),
      replayer_fields: Arc::default(),
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

  fn drain_buffer_events(&mut self) {
    let mut buffer_event_rx = self.buffer_event_rx.take().unwrap();
    tokio::spawn(async move { while buffer_event_rx.recv().await.is_some() {} });
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

    let (_, report_rx) = tokio::sync::mpsc::channel(1);

    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());

    let (buffer, sender) = AsyncLogBuffer::new(
      self.make_logging_context(),
      replayer,
      self.session_strategy.clone(),
      Arc::new(LogMetadata::default()),
      [].into(),
      [].into(),
      Box::new(EmptyTarget),
      Box::new(bd_test_helpers::session_replay::NoOpTarget),
      Box::new(NoOpListenerTarget),
      RegisteredCommandDispatcher::default(),
      CommandExecutionPolicy::new(&self.runtime),
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
    );
    (buffer, sender)
  }

  fn make_real_async_log_buffer(
    &self,
    config_update_rx: tokio::sync::mpsc::Receiver<ConfigUpdate>,
  ) -> (AsyncLogBuffer<LoggerReplay>, Sender) {
    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
    let (_, report_rx) = tokio::sync::mpsc::channel(1);
    let (buffer, sender) = AsyncLogBuffer::new(
      self.make_logging_context(),
      LoggerReplay {},
      self.session_strategy.clone(),
      Arc::new(LogMetadata::default()),
      [].into(),
      [].into(),
      Box::new(EmptyTarget),
      Box::new(bd_test_helpers::session_replay::NoOpTarget),
      Box::new(NoOpListenerTarget),
      RegisteredCommandDispatcher::default(),
      CommandExecutionPolicy::new(&self.runtime),
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
    );
    (buffer, sender)
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

  assert!(
    sender
      .try_send_log(normal_log("rejected"), LogAdmission::Current)
      .is_err()
  );
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
  sender
    .try_send_log(normal_log("held"), LogAdmission::Current)
    .unwrap();

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
    sender
      .try_send_log(normal_log("held"), LogAdmission::Current)
      .unwrap();

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
    sender
      .try_send_log(normal_log("held"), LogAdmission::Current)
      .unwrap();
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
  sender
    .try_send_log(normal_log("current"), LogAdmission::Current)
    .unwrap();
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
  assert_ok!(sender.try_send_log(normal_log("after early flush"), LogAdmission::Current));
  let entries = buffer.event_buffer.next_batch(2).await.entries;
  assert!(matches!(
    entries.as_slice(),
    [EventBufferEntry::Ingress(LoggerIngressEvent {
      payload: LoggerIngressPayload::Log(log),
      ..
    })] if log.message.as_str() == Some("after early flush")
  ));
}

#[tokio::test]
async fn checked_flush_reports_timeout_and_closed_admission() {
  let mut setup = Setup::new();
  let (_tx, rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(rx);
  // Early flush is an intentional successful no-op, including for checked callers.
  assert_ok!(sender.flush_state_with_result(Block::Yes {
    timeout: 1.std_seconds(),
    poll_callback: None,
  }));
  buffer.event_buffer.mark_startup_gate_ready();
  // No consumer runs, so the admitted barrier cannot complete.
  assert!(matches!(
    sender.flush_state_with_result(Block::Yes {
      timeout: 0.std_seconds(),
      poll_callback: None,
    }),
    Err(FlushError::Completion(RecvWithTimeoutError::Timeout))
  ));
  buffer.event_buffer.close();
  assert!(matches!(
    sender.flush_state_with_result(Block::No),
    Err(FlushError::Admission(AdmissionError::Closed))
  ));
}

#[tokio::test]
async fn checked_flush_reports_lost_completion() {
  let mut setup = Setup::new();
  let (_tx, rx) = tokio::sync::mpsc::channel(1);
  let (buffer, sender) = setup.make_test_async_log_buffer(rx);
  buffer.event_buffer.mark_startup_gate_ready();
  let event_buffer = buffer.event_buffer;
  assert!(matches!(
    sender.flush_state_with_result(Block::Yes {
      timeout: 1.std_seconds(),
      // Admission precedes this callback: drop the queued barrier without completing it.
      poll_callback: Some(Box::new(move || {
        drop(event_buffer.next_batch(1).now_or_never().unwrap());
      })),
    }),
    Err(FlushError::Completion(RecvWithTimeoutError::ChannelClosed))
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

  assert_ok!(sender.try_send_log(normal_log("before barrier"), LogAdmission::Current));
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

  assert_ok!(sender.try_send_log(
    normal_log("behind nonblocking flush"),
    LogAdmission::Current
  ));
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
  sender
    .try_send_log(retained_log, LogAdmission::Current)
    .unwrap();
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
  sender
    .try_send_log(normal_log("current"), LogAdmission::Current)
    .unwrap();
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
  timestamps: Vec<OffsetDateTime>,
  session_ids: Vec<String>,
  log_levels: Vec<bd_log_primitives::LogLevel>,
  completion_tokens: Arc<parking_lot::Mutex<Vec<Option<WorkflowCommandCompletionToken>>>>,
  results: Arc<parking_lot::Mutex<VecDeque<anyhow::Result<LogReplayResult>>>>,
  forward_logs_to_pipeline: bool,
}

//
// GatedAttachmentHandler
//

struct GatedAttachmentHandler {
  started_tx: mpsc::UnboundedSender<()>,
  result_rx: Mutex<Option<oneshot::Receiver<CommandResult>>>,
  returned_tx: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for GatedAttachmentHandler {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
    assert_eq!(invocation.command_id, None);
    let result_rx = self.result_rx.lock().unwrap().take().unwrap();
    log::debug!("workflow span timing handler started");
    self.started_tx.send(()).unwrap();
    let result = result_rx.await.unwrap();
    log::debug!("workflow span timing handler returning its attachment");
    self
      .returned_tx
      .lock()
      .unwrap()
      .take()
      .unwrap()
      .send(())
      .unwrap();
    result
  }
}

struct StaticReportProcessor(parking_lot::Mutex<Vec<bd_crash_handler::CrashLog>>);

struct GatedDirectCommandHandler {
  started_tx: Mutex<Option<oneshot::Sender<CommandInvocation>>>,
  result_rx: Mutex<Option<oneshot::Receiver<CommandResult>>>,
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for GatedDirectCommandHandler {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
    let result_rx = self.result_rx.lock().unwrap().take().unwrap();
    self
      .started_tx
      .lock()
      .unwrap()
      .take()
      .unwrap()
      .send(invocation)
      .unwrap();
    result_rx.await.unwrap()
  }
}

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
    command_attachment: None,
    capture_session: None,
  }
}

fn command_rule() -> Rule {
  Rule {
    rule_type: Some(Rule_type::MatchRunCommand(MatchRunCommand {
      command_selector: Some(WorkflowCommandSelector {
        command_selector: Some(
          workflow_command_selector::Command_selector::RegisteredCommand(
            workflow_command_selector::RegisteredCommand {
              registered_command_id: "handler".to_string(),
              ..Default::default()
            },
          ),
        ),
        ..Default::default()
      })
      .into(),
      minimum_execution_interval: Some(protobuf::well_known_types::duration::Duration {
        seconds: 60,
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })),
    ..Default::default()
  }
}

fn command_workflow() -> Config {
  let terminal = state("terminal");
  let command = state("command").declare_transition(&terminal, command_rule());
  let start = state("start").declare_transition(&command, rule!(message_equals("start")));
  WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config()
}

fn command_workflows() -> WorkflowsConfiguration {
  WorkflowsConfiguration::new_with_workflow_configurations(vec![command_workflow()])
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
      timestamps: Vec::new(),
      session_ids: Vec::new(),
      log_levels: Vec::new(),
      completion_tokens: Arc::new(parking_lot::Mutex::new(vec![])),
      results: Arc::new(parking_lot::Mutex::new(VecDeque::new())),
      forward_logs_to_pipeline: false,
    }
  }
}

#[async_trait::async_trait]
impl LogReplay for TestReplay {
  async fn replay_log(
    &mut self,
    log: Log,
    command_event: Option<WorkflowCommandEvent>,
    processing_pipeline: &mut ProcessingPipeline,
    state: &bd_state::Store,
    now: OffsetDateTime,
  ) -> anyhow::Result<LogReplayResult> {
    if let Some(message) = log.message.as_str() {
      self.logs.lock().push(message.to_string());
    }

    self.fields.lock().push(log.fields.clone());
    self.timestamps.push(log.occurred_at);
    self.session_ids.push(log.session_id.to_string());
    self.log_levels.push(log.log_level);
    self.completion_tokens.lock().push(match &command_event {
      Some(WorkflowCommandEvent::Completion(token)) => Some(token.clone()),
      Some(
        WorkflowCommandEvent::Start | WorkflowCommandEvent::Cancelled | WorkflowCommandEvent::Log,
      )
      | None => None,
    });
    self.logs_count.fetch_add(1, Ordering::SeqCst);
    self.logs_notify.notify_waiters();

    if self.forward_logs_to_pipeline {
      return LoggerReplay {}
        .replay_log(log, command_event, processing_pipeline, state, now)
        .await;
    }

    self
      .results
      .lock()
      .pop_front()
      .unwrap_or_else(|| Ok(LogReplayResult::default()))
  }

  async fn replay_state_change(
    &mut self,
    state_change: bd_state::StateChange,
    pipeline: &mut ProcessingPipeline,
    state: &bd_state::Store,
    now: OffsetDateTime,
    session_id: &str,
    fields: &LogFields,
    matching_fields: &LogFields,
  ) -> LogReplayResult {
    if self.forward_logs_to_pipeline {
      return LoggerReplay {}
        .replay_state_change(
          state_change,
          pipeline,
          state,
          now,
          session_id,
          fields,
          matching_fields,
        )
        .await;
    }
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

  sender
    .try_send_log(normal_log("before"), LogAdmission::Current)
    .unwrap();
  buffer.admit_crash_reports(
    report_processor.process_all_pending_reports().await,
    &crate::ReportProcessingSession::Current,
  );
  sender
    .try_send_log(normal_log("after"), LogAdmission::Current)
    .unwrap();
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
      command_attachment: None,
      capture_session: Some("generated"),
    },
    Some(EventContext::PreviousProcess {
      logged_at: admitted_at,
    }),
    Some(LogAttributesOverrides::PreviousRunSessionID(
      report_timestamp,
    )),
    false,
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

#[tokio::test]
async fn command_span_admission_retains_its_invocation_session_after_rollover() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, sender) = setup.make_test_async_log_buffer(config_update_rx);
  let session_id = setup.session_strategy.session_id().unwrap();
  let occurred_at = OffsetDateTime::UNIX_EPOCH;
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  for phase in [SPAN_TYPE_START, SPAN_TYPE_END] {
    let mut log = normal_log(phase);
    log.log_type = LogType::SPAN;
    log.attributes_overrides = Some(LogAttributesOverrides::OccurredAt(occurred_at));
    sender
      .try_send_log(
        log,
        LogAdmission::Command {
          session_id: session_id.clone(),
          cancelled: phase == SPAN_TYPE_END,
        },
      )
      .unwrap();
    setup.session_strategy.start_new_session(None).unwrap();
  }
  buffer.event_buffer.mark_startup_gate_ready();
  let batch = buffer.event_buffer.next_batch(2).await;
  assert_eq!(batch.entries.len(), 2);
  for entry in batch.entries {
    let EventBufferEntry::Ingress(event) = entry else {
      panic!("expected command span ingress");
    };
    let LoggerIngressPayload::Log(log) = event.payload else {
      panic!("expected command span log");
    };
    assert!(matches!(
      &event.context,
      EventContext::Command { session_id: owner, cancelled, .. }
        if owner == &session_id && *cancelled == (log.message.as_str() == Some(SPAN_TYPE_END))
    ));
    let normalized = buffer
      .normalize_log(log, &state_store, Some(event.context))
      .await
      .unwrap();
    assert_eq!(normalized.session_id, session_id);
    assert_eq!(normalized.occurred_at, occurred_at);
  }
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
    DeviceCommandRegistry::default(),
  );

  for admission in [
    LogAdmission::Current,
    LogAdmission::Command {
      session_id: "owner".into(),
      cancelled: true,
    },
  ] {
    assert!(matches!(
      sender.try_send_log(normal_log("unadmitted"), admission),
      Err(AdmissionError::ContextCaptureFailed)
    ));
  }
}

#[tokio::test]
async fn supplied_provider_snapshot_is_retained_without_recapture() {
  for (previous_process, use_fixture) in
    [(false, false), (true, false), (false, true), (true, true)]
  {
    let setup = Setup::new();
    let session_id = setup.session_strategy.session_id().unwrap();
    let logs = EventBuffer::new(EventBufferLimits {
      log_limit_bytes: 100_000,
      total_limit_bytes: 100_000,
    });
    let sender = if use_fixture {
      Sender::from_event_buffer(logs.clone(), setup.session_strategy.clone())
    } else {
      Sender::new(
        logs.clone(),
        Arc::new(FailingMetadataProvider),
        setup.session_strategy.clone(),
        DeviceCommandRegistry::default(),
      )
    };
    let provider = ProviderSnapshot {
      timestamp: OffsetDateTime::UNIX_EPOCH + 3.seconds(),
      ootb_fields: [("ootb".into(), "captured-ootb".into())].into(),
      custom_fields: [("custom".into(), "captured-custom".into())].into(),
    };
    let mut log = normal_log("snapshot");
    if previous_process {
      log.attributes_overrides = Some(LogAttributesOverrides::PreviousRunSessionID(
        OffsetDateTime::UNIX_EPOCH,
      ));
    }
    sender
      .try_send_log(log, LogAdmission::WithProvider(provider.clone()))
      .unwrap();
    setup.session_strategy.start_new_session(None).unwrap();
    logs.mark_startup_gate_ready();
    logs.start_startup_gate(None);
    let EventBufferEntry::Ingress(event) = logs.next_batch(1).await.entries.pop().unwrap() else {
      panic!("expected log ingress");
    };
    if previous_process {
      assert!(matches!(
        event.context,
        EventContext::PreviousProcess { logged_at } if logged_at == provider.timestamp
      ));
    } else {
      let EventContext::CurrentProcess(context) = event.context else {
        panic!("expected current-process snapshot");
      };
      assert_eq!(context.session_id, session_id);
      assert_eq!(context.admitted_at, provider.timestamp);
      assert_eq!(context.provider.timestamp, provider.timestamp);
      assert_eq!(context.provider.ootb_fields, provider.ootb_fields);
      assert_eq!(context.provider.custom_fields, provider.custom_fields);
    }
  }
}

#[tokio::test]
async fn sender_fixture_captures_real_session_for_logs_and_feature_flags() {
  let setup = Setup::new();
  let session_id = setup.session_strategy.session_id().unwrap();
  let logs = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 100_000,
    total_limit_bytes: 100_000,
  });
  let sender = Sender::from_event_buffer(logs.clone(), setup.session_strategy.clone());
  sender
    .try_send_log(normal_log("current"), LogAdmission::Current)
    .unwrap();
  sender
    .try_send_feature_flag_exposure("flag".into(), Some("variant".into()))
    .unwrap();
  setup.session_strategy.start_new_session(None).unwrap();
  logs.mark_startup_gate_ready();
  logs.start_startup_gate(None);
  let entries = logs.next_batch(2).await.entries;
  assert_eq!(entries.len(), 2);
  for entry in entries {
    let EventBufferEntry::Ingress(event) = entry else {
      panic!("expected log or feature-flag ingress");
    };
    let EventContext::CurrentProcess(context) = event.context else {
      panic!("expected current-process context");
    };
    assert_eq!(context.session_id, session_id);
    assert_eq!(context.admitted_at, OffsetDateTime::UNIX_EPOCH);
    assert_eq!(context.provider.timestamp, OffsetDateTime::UNIX_EPOCH);
    assert!(context.provider.ootb_fields.is_empty());
    assert!(context.provider.custom_fields.is_empty());
  }
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

#[tokio::test]
async fn previous_process_admission_does_not_capture_provider_fields() {
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
  let logs = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 100_000,
    total_limit_bytes: 100_000,
  });
  let sender = Sender::new(
    logs.clone(),
    metadata_provider,
    setup.session_strategy,
    DeviceCommandRegistry::default(),
  );
  let mut log = normal_log("previous process");
  log.attributes_overrides = Some(LogAttributesOverrides::PreviousRunSessionID(
    OffsetDateTime::UNIX_EPOCH,
  ));
  sender.try_send_log(log, LogAdmission::Current).unwrap();
  logs.mark_startup_gate_ready();
  logs.start_startup_gate(None);
  let EventBufferEntry::Ingress(event) = logs.next_batch(1).await.entries.pop().unwrap() else {
    panic!("expected previous-process ingress");
  };
  assert!(matches!(
    event.context,
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
      command_attachment: None,
      capture_session: None,
    }
  }

  let baseline_log_expected_size = 574;
  let baseline_log = create_baseline_log();
  assert_eq!(
    baseline_log_expected_size,
    baseline_log.approximate_size_bytes()
  );

  let attachment = LogAttachment {
    artifact_id: "artifact".to_string(),
    content_type: "image/jpeg".to_string(),
    size_bytes: 7,
    source: AttachmentSource::WORKFLOW,
  };
  let attachment_size = attachment.approximate_size_bytes();
  let mut log_with_attachment = create_baseline_log();
  log_with_attachment.command_attachment = Some(Box::new(attachment));
  assert_eq!(
    baseline_log_expected_size + attachment_size,
    log_with_attachment.approximate_size_bytes()
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
      command_attachment: None,
      capture_session: None,
    }
  }

  let baseline_log_expected_size = 558;
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
async fn workflow_command_processed_session_boundary_and_attribution() {
  for scenario in [
    "session",
    "state_session",
    "workflow_reset",
    "queued_completion",
    "queued_attachment",
    "late_direct",
    "strategy_reset_before_completion",
  ] {
    let mut setup = Setup::new();
    let (_config_update_tx, config_update_rx) = mpsc::channel(1);
    let (buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
    let state_store = TestStore::new().await;
    let state_store = (*state_store).clone();
    let mut buffer = buffer
      .update(setup.make_config_update(command_workflows()), &state_store)
      .await;
    let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
    buffer.time_provider = clock.clone();
    buffer.replayer.forward_logs_to_pipeline = true;
    let owner = setup.session_strategy.session_id().unwrap();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = oneshot::channel();
    let (returned_tx, returned_rx) = oneshot::channel();
    let registry = RegisteredCommandDispatcher::default();
    registry.register(
      "handler".into(),
      Arc::new(GatedAttachmentHandler {
        started_tx,
        result_rx: Mutex::new(Some(result_rx)),
        returned_tx: Mutex::new(Some(returned_tx)),
      }),
    );
    let (completion_tx, completion_rx) = mpsc::channel(1);
    buffer.workflow_command_dispatcher = WorkflowCommandDispatcher::new(
      registry,
      CommandExecutionPolicy::new(&setup.runtime),
      completion_tx,
      buffer.workflow_attachment_store(),
    );
    buffer.workflow_command_completion_rx = completion_rx;
    buffer
      .process_all_logs(normal_log("start"), &state_store, None)
      .await
      .unwrap();
    buffer
      .process_all_logs(normal_log("execute"), &state_store, None)
      .await
      .unwrap();
    tokio::time::timeout(5.std_seconds(), started_rx.recv())
      .await
      .expect("command handler did not start")
      .unwrap();
    clock.advance(125.milliseconds());

    let completion = if matches!(
      scenario,
      "queued_completion"
        | "queued_attachment"
        | "late_direct"
        | "strategy_reset_before_completion"
    ) {
      if scenario == "late_direct" {
        let mut log = normal_log("old direct span end");
        log.log_type = LogType::SPAN;
        buffer
          .process_all_logs_with_completion(
            log,
            &state_store,
            Some(EventContext::Command {
              session_id: "previous-session".into(),
              provider: None,
              cancelled: false,
            }),
            Some(WorkflowCommandEvent::Log),
          )
          .await
          .map_err(|error| error.error)
          .unwrap();
        assert_eq!(
          state_store
            .read()
            .await
            .get(Scope::System, SYSTEM_SESSION_ID_KEY)
            .unwrap()
            .string_value(),
          owner.as_ref(),
        );
      }
      result_tx
        .send(CommandResult::Completed {
          fields: LogFields::default(),
          attachment: (scenario == "queued_attachment").then(|| CommandAttachment {
            source: UploadSource::Bytes(b"cancelled attachment".to_vec()),
            content_type: None,
          }),
        })
        .unwrap();
      returned_rx.await.unwrap();
      let completion = tokio::time::timeout(
        5.std_seconds(),
        buffer.workflow_command_completion_rx.recv(),
      )
      .await
      .expect("completed handler did not produce a completion")
      .unwrap();
      if scenario != "late_direct" {
        setup.session_strategy.start_new_session(None).unwrap();
        if scenario != "strategy_reset_before_completion" {
          buffer
            .process_all_logs(normal_log("new session boundary"), &state_store, None)
            .await
            .unwrap();
        }
      }
      completion
    } else {
      if matches!(scenario, "session" | "state_session") {
        setup.session_strategy.start_new_session(None).unwrap();
        if scenario == "state_session" {
          let context =
            current_process_admission_context(&buffer.metadata_provider, &setup.session_strategy)
              .unwrap();
          buffer
            .process_feature_flag_exposure("flag".into(), Some("on".into()), context, &state_store)
            .await;
        } else {
          buffer
            .process_all_logs(normal_log("new session boundary"), &state_store, None)
            .await
            .unwrap();
        }
      } else {
        buffer = buffer
          .update(
            setup.make_config_update(WorkflowsConfiguration::default()),
            &state_store,
          )
          .await;
      }
      // Workflow configuration reset is immediate; session reset waits for the new-session log.
      let completion = tokio::time::timeout(
        5.std_seconds(),
        buffer.workflow_command_completion_rx.recv(),
      )
      .await
      .expect("cancelled handler did not produce a completion")
      .unwrap();
      assert!(
        result_tx
          .send(CommandResult::Completed {
            fields: LogFields::default(),
            attachment: None,
          })
          .is_err()
      );
      drop(returned_rx);
      completion
    };
    let duplicate = WorkflowCommandCompletion {
      token: completion.token.clone(),
      outcome: completion.outcome.clone(),
    };
    let artifact_id = match &completion.outcome {
      WorkflowCommandOutcome::SucceededWithAttachment { artifact_id, .. } => Some(*artifact_id),
      _ => None,
    };
    buffer
      .process_workflow_command_completion(completion, &state_store)
      .await;
    buffer
      .process_workflow_command_completion(duplicate, &state_store)
      .await;
    if scenario == "queued_attachment" {
      let id = artifact_id.expect("expected an admitted attachment before cancellation");
      let store = buffer.workflow_attachment_store().get().await.unwrap();
      assert!(store.metadata(id).await.unwrap().is_none());
      assert!(
        !tokio::fs::try_exists(
          setup
            .tmp_dir
            .path()
            .join("workflow-attachments")
            .join(format!("{id}.payload"))
        )
        .await
        .unwrap()
      );
    }

    let logs = buffer.replayer.logs.lock().clone();
    let starts = logs
      .iter()
      .enumerate()
      .filter_map(|(index, message)| (message == COMMAND_START_MESSAGE).then_some(index))
      .collect::<Vec<_>>();
    let ends = logs
      .iter()
      .enumerate()
      .filter_map(|(index, message)| (message == COMMAND_OUTCOME_MESSAGE).then_some(index))
      .collect::<Vec<_>>();
    assert_eq!(starts.len(), 1, "{scenario}");
    assert_eq!(ends.len(), 1, "{scenario}");
    let start = starts[0];
    let end = ends[0];
    assert_eq!(buffer.replayer.session_ids[start], owner.as_ref());
    assert_eq!(buffer.replayer.session_ids[end], owner.as_ref());
    let fields = buffer.replayer.fields.lock().clone();
    assert_eq!(
      fields[start].get(LOG_FIELD_NAME_SPAN_ID),
      fields[end].get(LOG_FIELD_NAME_SPAN_ID)
    );
    assert!(!fields[start].contains_key(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON));
    assert_eq!(
      fields[end]
        .get(LOG_FIELD_NAME_DURATION_MS)
        .unwrap()
        .as_str(),
      Some("125")
    );
    if matches!(scenario, "late_direct" | "strategy_reset_before_completion") {
      assert!(!fields[end].contains_key(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON));
      assert_eq!(
        fields[end].get(LOG_FIELD_NAME_RESULT).unwrap().as_str(),
        Some("success")
      );
    } else {
      assert_eq!(buffer.replayer.log_levels[end], log_level::ERROR);
      assert_eq!(
        fields[end].get(LOG_FIELD_NAME_RESULT).unwrap().as_str(),
        Some("canceled")
      );
      assert_eq!(
        fields[end]
          .get(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON)
          .unwrap()
          .as_str(),
        Some(
          if scenario == "workflow_reset" {
            "workflow_reset"
          } else {
            "session_changed"
          }
        ),
      );
      assert_eq!(buffer.replayer.completion_tokens.lock()[end], None);
    }
    if matches!(
      scenario,
      "session" | "state_session" | "queued_completion" | "queued_attachment"
    ) {
      let current = setup.session_strategy.session_id().unwrap();
      assert_eq!(
        state_store
          .read()
          .await
          .get(Scope::System, SYSTEM_SESSION_ID_KEY)
          .unwrap()
          .string_value(),
        current.as_ref(),
      );
      buffer
        .process_all_logs(normal_log("still new session"), &state_store, None)
        .await
        .unwrap();
      setup
        .collector
        .assert_counter_eq(2, "workflows:sessions_total", labels! {});
      setup
        .collector
        .assert_counter_eq(0, "workflows:session_thrashes_total", labels! {});
    }
  }
}

#[tokio::test]
async fn workflow_command_generated_descendants_do_not_rewind_session_or_complete_commands() {
  for command_event in [WorkflowCommandEvent::Cancelled, WorkflowCommandEvent::Log] {
    let mut setup = Setup::new();
    let terminal = state("terminal");
    let second = state("second").declare_transition_with_actions(
      &terminal,
      rule!(message_equals("second descendant")),
      &[make_emit_counter_action(
        "descendants_observed",
        metric_value(1),
        vec![],
      )],
    );
    let first = state("first").declare_transition_with_actions(
      &second,
      rule!(message_equals("first descendant")),
      &[make_generate_log_action_proto(
        "second descendant",
        &[],
        "second",
        LogType::NORMAL,
      )],
    );
    let observer = state("observer").declare_transition_with_actions(
      &first,
      rule!(message_equals(COMMAND_OUTCOME_MESSAGE)),
      &[make_generate_log_action_proto(
        "first descendant",
        &[],
        "first",
        LogType::NORMAL,
      )],
    );
    let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
      command_workflow(),
      WorkflowBuilder::new("observer", &[&observer, &first, &second, &terminal]).make_config(),
    ]);
    let (_config_tx, config_rx) = mpsc::channel(1);
    let (buffer, _) = setup.make_test_async_log_buffer(config_rx);
    let state_store = TestStore::new().await;
    let state_store = (*state_store).clone();
    let mut buffer = buffer
      .update(setup.make_config_update(workflows), &state_store)
      .await;
    buffer.replayer.forward_logs_to_pipeline = true;
    let previous = setup.session_strategy.session_id().unwrap();
    buffer
      .process_all_logs(normal_log("start"), &state_store, None)
      .await
      .unwrap();
    setup.session_strategy.start_new_session(None).unwrap();
    let current = setup.session_strategy.session_id().unwrap();
    buffer
      .process_all_logs(normal_log("start"), &state_store, None)
      .await
      .unwrap();
    // Park a current-session command without dispatching a worker. Only engine acceptance may
    // complete it, regardless of the public span fields on the historical event or descendants.
    let request = buffer
      .process_log(normal_log("execute"), &state_store, None, None)
      .await
      .unwrap()
      .workflow_commands_to_start
      .pop()
      .unwrap();
    let mut log = normal_log(COMMAND_OUTCOME_MESSAGE);
    log.log_type = LogType::SPAN;
    log.fields = request
      .start_log()
      .fields
      .into_iter()
      .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
      .collect();
    let historical_index = buffer.replayer.timestamps.len();
    buffer
      .process_all_logs_with_completion(
        log,
        &state_store,
        Some(EventContext::Command {
          session_id: previous.clone(),
          provider: None,
          cancelled: command_event == WorkflowCommandEvent::Cancelled,
        }),
        Some(command_event),
      )
      .await
      .map_err(|error| error.error)
      .unwrap();

    assert_eq!(
      &buffer.replayer.logs.lock()[historical_index ..],
      [
        COMMAND_OUTCOME_MESSAGE,
        "first descendant",
        "second descendant",
      ]
    );
    assert!(
      buffer.replayer.session_ids[historical_index ..]
        .iter()
        .all(|session| session == previous.as_ref())
    );
    assert!(
      buffer.replayer.completion_tokens.lock()[historical_index ..]
        .iter()
        .all(Option::is_none)
    );
    setup
      .collector
      .assert_workflow_counter_eq(1, "descendants_observed", labels! {});
    setup
      .collector
      .assert_counter_eq(2, "workflows:sessions_total", labels! {});
    setup
      .collector
      .assert_counter_eq(0, "workflows:session_thrashes_total", labels! {});
    assert_eq!(
      state_store
        .read()
        .await
        .get(Scope::System, SYSTEM_SESSION_ID_KEY)
        .unwrap()
        .string_value(),
      current.as_ref()
    );
    let engine = buffer.logging_state.workflows_engine().unwrap();
    assert!(
      engine
        .workflow_command_outcome_log(
          &request.completion_token(),
          WorkflowCommandOutcome::Succeeded {
            message: None,
            fields: LogFields::default(),
          },
          buffer.time_provider.now(),
        )
        .is_ok()
    );
  }
}

#[tokio::test]
async fn workflow_command_cancellation_preserves_processing_timestamp() {
  for session_change in [false, true] {
    for explicit_timestamp in [false, true] {
      let mut setup = Setup::new();
      let (_config_tx, config_rx) = mpsc::channel(1);
      let (buffer, _) = setup.make_test_async_log_buffer(config_rx);
      let state_store = TestStore::new().await;
      let state_store = (*state_store).clone();
      let mut buffer = buffer
        .update(setup.make_config_update(command_workflows()), &state_store)
        .await;
      let started_at = OffsetDateTime::UNIX_EPOCH + 2.seconds();
      let clock = Arc::new(TestTimeProvider::new(started_at));
      buffer.time_provider = clock.clone();
      buffer.replayer.forward_logs_to_pipeline = true;
      buffer
        .process_all_logs(normal_log("start"), &state_store, None)
        .await
        .unwrap();
      buffer
        .process_all_logs(normal_log("execute"), &state_store, None)
        .await
        .unwrap();
      let start_index = buffer.replayer.timestamps.len() - 1;
      assert_eq!(buffer.replayer.timestamps[start_index], started_at);

      if session_change {
        setup.session_strategy.start_new_session(None).unwrap();
      }
      let mut context =
        current_process_admission_context(&buffer.metadata_provider, &setup.session_strategy)
          .unwrap();
      // Model ingress admitted before command dispatch but processed after it. Neither this
      // snapshot nor an explicit source-log timestamp may replace the cancellation's own time.
      let admitted_at = OffsetDateTime::UNIX_EPOCH + 1.seconds();
      context.admitted_at = admitted_at;
      context.provider.timestamp = admitted_at;
      let mut boundary = normal_log("start");
      if explicit_timestamp {
        boundary.attributes_overrides = Some(LogAttributesOverrides::OccurredAt(
          OffsetDateTime::UNIX_EPOCH,
        ));
      }
      clock.advance(125.milliseconds());
      buffer
        .process_all_logs(
          boundary,
          &state_store,
          Some(EventContext::CurrentProcess(context)),
        )
        .await
        .unwrap();
      assert_eq!(
        buffer.replayer.timestamps[start_index + 1],
        if explicit_timestamp {
          OffsetDateTime::UNIX_EPOCH
        } else {
          admitted_at
        }
      );
      assert_eq!(
        buffer.replayer.logs.lock().last().unwrap(),
        COMMAND_OUTCOME_MESSAGE
      );
      assert_eq!(buffer.replayer.timestamps.last(), Some(&clock.now()));
      let fields = buffer.replayer.fields.lock();
      let start = &fields[start_index];
      let end = fields.last().unwrap();
      assert_eq!(
        end.get(LOG_FIELD_NAME_SPAN_ID),
        start.get(LOG_FIELD_NAME_SPAN_ID)
      );
      assert_eq!(
        end.get(LOG_FIELD_NAME_DURATION_MS).unwrap().as_str(),
        Some("125")
      );
      assert_eq!(
        end
          .get(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON)
          .unwrap()
          .as_str(),
        Some(
          if session_change {
            "session_changed"
          } else {
            "workflow_reset"
          }
        )
      );
    }
  }
}

#[tokio::test]
async fn workflow_command_session_start_carryover_never_dispatches_invalidated_work() {
  for reset in [false, true] {
    let mut setup = Setup::new();
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, command_rule());
    let start = state("start")
      .declare_transition(
        &command,
        make_state_change_rule(StateScope::FEATURE_FLAG, "ready", Some("yes")),
      )
      .declare_transition(&command, rule!(message_equals("reset")));
    let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
      WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config(),
    ]);
    let (_config_tx, config_rx) = mpsc::channel(1);
    let (buffer, _) = setup.make_test_async_log_buffer(config_rx);
    let state_store = TestStore::new().await;
    let state_store = (*state_store).clone();
    let mut buffer = buffer
      .update(setup.make_config_update(workflows), &state_store)
      .await;
    let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
    buffer.time_provider = clock.clone();
    buffer.replayer.forward_logs_to_pipeline = true;
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = oneshot::channel();
    let (returned_tx, returned_rx) = oneshot::channel();
    let registry = RegisteredCommandDispatcher::default();
    registry.register(
      "handler".into(),
      Arc::new(GatedAttachmentHandler {
        started_tx,
        result_rx: Mutex::new(Some(result_rx)),
        returned_tx: Mutex::new(Some(returned_tx)),
      }),
    );
    let (completion_tx, completion_rx) = mpsc::channel(1);
    buffer.workflow_command_dispatcher = WorkflowCommandDispatcher::new(
      registry,
      CommandExecutionPolicy::new(&setup.runtime),
      completion_tx,
      buffer.workflow_attachment_store(),
    );
    buffer.workflow_command_completion_rx = completion_rx;
    let context =
      current_process_admission_context(&buffer.metadata_provider, &setup.session_strategy)
        .unwrap();
    buffer
      .process_feature_flag_exposure("ready".into(), Some("yes".into()), context, &state_store)
      .await;
    let message = if reset { "reset" } else { "keep" };
    buffer
      .process_all_logs(normal_log(message), &state_store, None)
      .await
      .unwrap();
    if reset {
      assert_eq!(*buffer.replayer.logs.lock(), ["reset"]);
      assert!(matches!(
        started_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
      ));
      // The replacement run may still launch a valid command after the matcher cooldown.
      clock.advance(61.seconds());
      buffer
        .process_all_logs(normal_log("execute"), &state_store, None)
        .await
        .unwrap();
    }
    started_rx.recv().await.unwrap();
    clock.advance(125.milliseconds());
    result_tx
      .send(CommandResult::Completed {
        fields: LogFields::default(),
        attachment: None,
      })
      .unwrap();
    returned_rx.await.unwrap();
    let completion = buffer.workflow_command_completion_rx.recv().await.unwrap();
    buffer
      .process_workflow_command_completion(completion, &state_store)
      .await;
    let logs = buffer.replayer.logs.lock();
    let lifecycle = logs
      .iter()
      .filter(|message| {
        matches!(
          message.as_str(),
          COMMAND_START_MESSAGE | COMMAND_OUTCOME_MESSAGE
        )
      })
      .map(String::as_str)
      .collect::<Vec<_>>();
    assert_eq!(lifecycle, [COMMAND_START_MESSAGE, COMMAND_OUTCOME_MESSAGE]);
    assert!(
      !buffer
        .replayer
        .fields
        .lock()
        .last()
        .unwrap()
        .contains_key(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON)
    );
  }
}

#[tokio::test]
async fn workflow_command_first_ingress_initializes_session_without_rewinds() {
  for change_session in [false, true] {
    let mut setup = Setup::new();
    let terminal = state("terminal");
    let command = state("command").declare_transition(&terminal, command_rule());
    let observer = state("observer").declare_transition_with_actions(
      &terminal,
      make_on_new_session_rule(),
      &[make_emit_counter_action(
        "session_start_observed",
        metric_value(1),
        vec![],
      )],
    );
    let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
      WorkflowBuilder::new("command", &[&command, &terminal]).make_config(),
      WorkflowBuilder::new("observer", &[&observer, &terminal]).make_config(),
    ]);
    let (_config_tx, config_rx) = mpsc::channel(1);
    let (buffer, _) = setup.make_test_async_log_buffer(config_rx);
    let state_store = TestStore::new().await;
    let state_store = (*state_store).clone();
    let mut buffer = buffer
      .update(setup.make_config_update(workflows), &state_store)
      .await;
    let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
    buffer.time_provider = clock.clone();
    buffer.replayer.forward_logs_to_pipeline = true;
    let (started_tx, started_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    let registry = RegisteredCommandDispatcher::default();
    registry.register(
      "handler".into(),
      Arc::new(GatedDirectCommandHandler {
        started_tx: Mutex::new(Some(started_tx)),
        result_rx: Mutex::new(Some(result_rx)),
      }),
    );
    let (completion_tx, completion_rx) = mpsc::channel(1);
    buffer.workflow_command_dispatcher = WorkflowCommandDispatcher::new(
      registry,
      CommandExecutionPolicy::new(&setup.runtime),
      completion_tx,
      buffer.workflow_attachment_store(),
    );
    buffer.workflow_command_completion_rx = completion_rx;
    let owner = setup.session_strategy.session_id().unwrap();
    let mut log = normal_log("direct command telemetry");
    log.log_type = LogType::SPAN;
    buffer
      .process_all_logs_with_completion(
        log,
        &state_store,
        Some(EventContext::Command {
          session_id: owner.clone(),
          provider: None,
          cancelled: false,
        }),
        Some(WorkflowCommandEvent::Log),
      )
      .await
      .map_err(|error| error.error)
      .unwrap();
    let invocation = started_rx.await.unwrap();
    assert_eq!(invocation.session_id, owner.as_ref());
    assert_eq!(buffer.replayer.session_ids.last().unwrap(), owner.as_ref());
    if change_session {
      setup.session_strategy.start_new_session(None).unwrap();
    }
    let current = setup.session_strategy.session_id().unwrap();
    clock.advance(125.milliseconds());
    buffer
      .process_all_logs(normal_log("ordinary ingress"), &state_store, None)
      .await
      .unwrap();
    setup
      .collector
      .assert_workflow_counter_eq(1, "session_start_observed", labels! {});
    // Historical command ingress must not undo the session established by ordinary ingress.
    buffer
      .process_all_logs_with_completion(
        normal_log("historical telemetry"),
        &state_store,
        Some(EventContext::Command {
          session_id: owner.clone(),
          provider: None,
          cancelled: false,
        }),
        Some(WorkflowCommandEvent::Log),
      )
      .await
      .map_err(|error| error.error)
      .unwrap();
    if change_session {
      let completion = buffer.workflow_command_completion_rx.recv().await.unwrap();
      assert!(
        result_tx
          .send(CommandResult::Completed {
            fields: LogFields::default(),
            attachment: None,
          })
          .is_err()
      );
      buffer
        .process_workflow_command_completion(completion, &state_store)
        .await;
    } else {
      result_tx
        .send(CommandResult::Completed {
          fields: LogFields::default(),
          attachment: None,
        })
        .unwrap();
      let completion = buffer.workflow_command_completion_rx.recv().await.unwrap();
      buffer
        .process_workflow_command_completion(completion, &state_store)
        .await;
    }
    buffer
      .process_all_logs(normal_log("still current"), &state_store, None)
      .await
      .unwrap();
    setup
      .collector
      .assert_workflow_counter_eq(1, "session_start_observed", labels! {});
    setup.collector.assert_counter_eq(
      if change_session { 2 } else { 1 },
      "workflows:sessions_total",
      labels! {},
    );
    setup
      .collector
      .assert_counter_eq(0, "workflows:session_thrashes_total", labels! {});
    assert_eq!(
      state_store
        .read()
        .await
        .get(Scope::System, SYSTEM_SESSION_ID_KEY)
        .unwrap()
        .string_value(),
      current.as_ref()
    );
    let logs = buffer.replayer.logs.lock();
    let ends = logs
      .iter()
      .enumerate()
      .filter_map(|(index, message)| (message == COMMAND_OUTCOME_MESSAGE).then_some(index))
      .collect::<Vec<_>>();
    assert_eq!(ends.len(), 1);
    let end = ends[0];
    assert_eq!(buffer.replayer.session_ids[end], owner.as_ref());
    assert_eq!(
      buffer.replayer.fields.lock()[end]
        .get(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON)
        .and_then(DataValue::as_str),
      change_session.then_some("session_changed")
    );
  }
}

#[tokio::test]
async fn direct_commands_use_processed_session_boundary_without_rewinding_logging() {
  for (complete_before_boundary, state_boundary) in
    [(false, false), (true, false), (false, true), (true, true)]
  {
    let mut setup = Setup::new();
    let (_config_tx, config_rx) = mpsc::channel(1);
    let (buffer, sender) = setup.make_test_async_log_buffer(config_rx);
    let state_store = TestStore::new().await;
    let state_store = (*state_store).clone();
    let mut buffer = buffer
      .update(setup.make_config_update(command_workflows()), &state_store)
      .await;
    buffer.replayer.forward_logs_to_pipeline = true;
    buffer
      .process_all_logs(normal_log("session A"), &state_store, None)
      .await
      .unwrap();
    let owner = setup.session_strategy.session_id().unwrap();

    let (started_tx, started_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    let registry = RegisteredCommandDispatcher::default();
    registry.register(
      "handler".into(),
      Arc::new(GatedDirectCommandHandler {
        started_tx: Mutex::new(Some(started_tx)),
        result_rx: Mutex::new(Some(result_rx)),
      }),
    );
    let command_state = Arc::new(DeviceCommandState::new(Arc::new(SystemTimeProvider)));
    let (upload_tx, mut updates) = mpsc::channel(16);
    let (trigger_tx, _trigger_rx) = mpsc::channel(1);
    let dispatcher = DeviceCommandDispatcher::new(
      upload_tx,
      sender,
      trigger_tx,
      setup.session_strategy.clone(),
      Arc::new(MockClient::new()),
      registry,
      CommandExecutionPolicy::new(&setup.runtime),
      command_state.clone(),
      Arc::new(SystemTimeProvider),
    );
    let command_id = Uuid::new_v4();
    dispatcher.dispatch_configuration(vec![DeviceCommandRequest {
      command_id: command_id.to_string().into(),
      sdk_instance_id: command_state.sdk_instance_id.clone().into(),
      expires_at: (OffsetDateTime::now_utc() + 1.days()).into_proto(),
      command_selector: Some(WorkflowCommandSelector {
        command_selector: Some(
          workflow_command_selector::Command_selector::RegisteredCommand(
            workflow_command_selector::RegisteredCommand {
              registered_command_id: "handler".into(),
              ..Default::default()
            },
          ),
        ),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    }]);
    let Some(DataUpload::DeviceCommandUpdate(accepted)) = updates.recv().await else {
      panic!("expected accepted update");
    };
    accepted.response_tx.send(()).unwrap();
    let invocation = started_rx.await.unwrap();
    assert_eq!(invocation.command_id, Some(command_id));
    assert_eq!(invocation.session_id, owner.as_ref());

    setup.session_strategy.start_new_session(None).unwrap();
    let current = setup.session_strategy.session_id().unwrap();
    assert!(!result_tx.is_closed());
    let mut terminal = None;
    if complete_before_boundary {
      result_tx
        .send(CommandResult::Completed {
          fields: LogFields::default(),
          attachment: None,
        })
        .unwrap();
      terminal = updates.recv().await;
    }
    if state_boundary {
      let context =
        current_process_admission_context(&buffer.metadata_provider, &setup.session_strategy)
          .unwrap();
      buffer
        .process_feature_flag_exposure("flag".into(), Some("on".into()), context, &state_store)
        .await;
    } else {
      buffer
        .process_all_logs(normal_log("session B"), &state_store, None)
        .await
        .unwrap();
    }
    let terminal = match terminal {
      Some(terminal) => terminal,
      None => updates.recv().await.unwrap(),
    };
    let DataUpload::DeviceCommandUpdate(terminal) = terminal else {
      panic!("expected terminal update");
    };
    if complete_before_boundary {
      assert!(matches!(
        terminal.payload.update_type,
        Some(device_command_update::Update_type::Completed(_))
      ));
    } else {
      assert!(matches!(
        terminal.payload.update_type,
        Some(device_command_update::Update_type::Failed(_))
      ));
    }
    terminal.response_tx.send(()).unwrap();

    // Replay the old-session span entries after the B log. Neither their start nor their
    // normal/cancelled end may restore A in the engine or the system-session state.
    buffer.event_buffer.mark_startup_gate_ready();
    buffer.event_buffer.start_startup_gate(None);
    let entries = buffer.event_buffer.next_batch(8).await.entries;
    assert_eq!(entries.len(), 2);
    for entry in entries {
      let EventBufferEntry::Ingress(event) = entry else {
        panic!("expected command span");
      };
      let EventContext::Command {
        session_id,
        cancelled,
        ..
      } = &event.context
      else {
        panic!("expected trusted command context");
      };
      assert_eq!(session_id, &owner);
      let command_event = if *cancelled {
        WorkflowCommandEvent::Cancelled
      } else {
        WorkflowCommandEvent::Log
      };
      let LoggerIngressPayload::Log(log) = event.payload else {
        panic!("expected command log");
      };
      buffer
        .process_all_logs_with_completion(
          log,
          &state_store,
          Some(event.context),
          Some(command_event),
        )
        .await
        .map_err(|error| error.error)
        .unwrap();
    }
    assert_eq!(
      state_store
        .read()
        .await
        .get(Scope::System, SYSTEM_SESSION_ID_KEY)
        .unwrap()
        .string_value(),
      current.as_ref()
    );
    buffer
      .process_all_logs(normal_log("still session B"), &state_store, None)
      .await
      .unwrap();
    setup
      .collector
      .assert_counter_eq(2, "workflows:sessions_total", labels! {});
    setup
      .collector
      .assert_counter_eq(0, "workflows:session_thrashes_total", labels! {});
  }
}

#[tokio::test]
async fn recovered_workflow_cancellation_keeps_original_session_after_normalization() {
  let mut setup = Setup::new();
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let (_config_tx, config_rx) = mpsc::channel(1);
  let (buffer, _) = setup.make_test_async_log_buffer(config_rx);
  let mut buffer = buffer
    .update(setup.make_config_update(command_workflows()), &state_store)
    .await;
  let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  buffer.time_provider = clock.clone();
  buffer.replayer.forward_logs_to_pipeline = true;
  let owner = setup.session_strategy.session_id().unwrap();
  buffer
    .process_log(normal_log("start"), &state_store, None, None)
    .await
    .unwrap();
  let request = buffer
    .process_log(normal_log("execute"), &state_store, None, None)
    .await
    .unwrap()
    .workflow_commands_to_start
    .pop()
    .unwrap();
  buffer
    .logging_state
    .workflows_engine()
    .unwrap()
    .maybe_persist(true)
    .await;
  drop(buffer);

  setup.session_strategy.start_new_session(None).unwrap();
  let current = setup.session_strategy.session_id().unwrap();
  assert_ne!(owner, current);
  clock.advance(125.milliseconds());
  let (_config_tx, config_rx) = mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_rx);
  buffer.time_provider = clock;
  buffer.replayer.forward_logs_to_pipeline = true;
  let mut config = setup.make_config_update(command_workflows());
  config.from_cache = true;
  let mut buffer = buffer.update(config, &state_store).await;
  assert_eq!(*buffer.replayer.logs.lock(), [COMMAND_OUTCOME_MESSAGE]);
  assert_eq!(buffer.replayer.session_ids, [owner.to_string()]);
  assert_eq!(setup.session_strategy.session_id().unwrap(), current);
  assert_eq!(buffer.replayer.log_levels, [log_level::ERROR]);
  let end = buffer.replayer.fields.lock()[0].clone();
  assert_eq!(
    end
      .get(LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON)
      .unwrap()
      .as_str(),
    Some("sdk_restart")
  );
  assert_eq!(
    end.get(LOG_FIELD_NAME_DURATION_MS).unwrap().as_str(),
    Some("125")
  );
  assert_eq!(
    end.get(LOG_FIELD_NAME_SPAN_ID),
    request.start_log().fields.get(LOG_FIELD_NAME_SPAN_ID)
  );
  assert_eq!(*buffer.replayer.completion_tokens.lock(), [None]);

  buffer.recover_workflow_commands(&state_store).await;
  buffer
    .process_workflow_command_completion(
      WorkflowCommandCompletion {
        token: request.completion_token(),
        outcome: WorkflowCommandOutcome::Succeeded {
          message: None,
          fields: LogFields::default(),
        },
      },
      &state_store,
    )
    .await;
  assert_eq!(*buffer.replayer.logs.lock(), [COMMAND_OUTCOME_MESSAGE]);
}

#[tokio::test]
async fn workflow_command_span_includes_handler_and_admission_but_not_background_upload() {
  let mut setup = Setup::new();
  setup.drain_buffer_events();
  tokio::fs::create_dir_all(setup.tmp_dir.path().join("buffer"))
    .await
    .unwrap();
  let buffers = BufferConfigList {
    buffer_config: vec![default_buffer_config(
      BufferType::TRIGGER,
      Some(match_message(COMMAND_OUTCOME_MESSAGE)),
    )],
    ..Default::default()
  };
  setup
    .buffer_manager
    .update_from_config(&buffers, false)
    .await
    .unwrap();
  let (_config_update_tx, config_update_rx) = mpsc::channel(1);
  let (buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let workflows = command_workflows();
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let mut config = setup.make_config_update(workflows);
  config.buffer_selector = BufferSelector::new(&buffers).unwrap();
  let mut buffer = buffer.update(config, &state_store).await;
  let started_at = OffsetDateTime::UNIX_EPOCH;
  let clock = Arc::new(TestTimeProvider::new(started_at));
  buffer.time_provider = clock.clone();
  buffer.replayer.forward_logs_to_pipeline = true;

  let (started_tx, mut started_rx) = mpsc::unbounded_channel();
  let (result_tx, result_rx) = oneshot::channel();
  let (returned_tx, returned_rx) = oneshot::channel();
  let registry = RegisteredCommandDispatcher::default();
  registry.register(
    "handler".into(),
    Arc::new(GatedAttachmentHandler {
      started_tx,
      result_rx: Mutex::new(Some(result_rx)),
      returned_tx: Mutex::new(Some(returned_tx)),
    }),
  );
  let (completion_tx, completion_rx) = mpsc::channel(1);
  let attachment_store = buffer.workflow_attachment_store();
  let store = attachment_store.get().await.unwrap();
  let admission_permit = store.block_admissions_for_test().await;
  buffer.workflow_command_dispatcher = WorkflowCommandDispatcher::new(
    registry,
    CommandExecutionPolicy::new(&setup.runtime),
    completion_tx,
    attachment_store,
  );
  buffer.workflow_command_completion_rx = completion_rx;

  buffer
    .process_all_logs(normal_log("start"), &state_store, None)
    .await
    .unwrap();
  buffer
    .process_all_logs(normal_log("execute"), &state_store, None)
    .await
    .unwrap();
  started_rx.recv().await.unwrap();
  let start_fields = buffer.replayer.fields.lock().last().unwrap().clone();
  assert!(!start_fields.contains_key(LOG_FIELD_NAME_DURATION_MS));
  assert_eq!(buffer.replayer.timestamps.last(), Some(&started_at));
  clock.advance(125.milliseconds());
  result_tx
    .send(CommandResult::Completed {
      fields: LogFields::default(),
      attachment: Some(CommandAttachment {
        source: UploadSource::Bytes(b"attachment".to_vec()),
        content_type: Some("application/octet-stream".into()),
      }),
    })
    .unwrap();
  returned_rx.await.unwrap();
  // The handler has returned, but durable attachment admission is still blocked.
  clock.advance(75.milliseconds());
  assert!(matches!(
    buffer.workflow_command_completion_rx.try_recv(),
    Err(mpsc::error::TryRecvError::Empty)
  ));
  drop(admission_permit);
  let completion = buffer.workflow_command_completion_rx.recv().await.unwrap();
  let WorkflowCommandOutcome::SucceededWithAttachment { artifact_id, .. } = &completion.outcome
  else {
    panic!("expected a durably admitted attachment");
  };
  let artifact_id = *artifact_id;
  assert!(!store.metadata(artifact_id).await.unwrap().unwrap().uploaded);
  buffer
    .process_workflow_command_completion(completion, &state_store)
    .await;
  assert_eq!(
    buffer.replayer.logs.lock().as_slice(),
    [
      "start",
      "execute",
      COMMAND_START_MESSAGE,
      COMMAND_OUTCOME_MESSAGE
    ]
  );
  let end_fields = buffer.replayer.fields.lock().last().unwrap().clone();
  assert_eq!(
    end_fields.get(LOG_FIELD_NAME_SPAN_ID),
    start_fields.get(LOG_FIELD_NAME_SPAN_ID)
  );
  assert_eq!(
    end_fields.get(LOG_FIELD_NAME_DURATION_MS).unwrap().as_str(),
    Some("200")
  );
  assert_eq!(clock.now(), started_at + 200.milliseconds());
  assert_eq!(
    buffer.replayer.timestamps.last(),
    Some(&(started_at + 200.milliseconds()))
  );

  // The end is already emitted while the attachment is still awaiting background upload.
  assert!(!store.metadata(artifact_id).await.unwrap().unwrap().uploaded);
  clock.advance(5.seconds());
  store.complete_upload(artifact_id).await.unwrap();
  assert!(store.metadata(artifact_id).await.unwrap().unwrap().uploaded);
  assert_eq!(
    buffer
      .replayer
      .fields
      .lock()
      .last()
      .unwrap()
      .get(LOG_FIELD_NAME_DURATION_MS)
      .unwrap()
      .as_str(),
    Some("200")
  );
}

#[tokio::test]
async fn workflow_command_completion_survives_metadata_failure() {
  let setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let terminal = state("terminal");
  let command = state("command").declare_transition(
    &terminal,
    Rule {
      rule_type: Some(Rule_type::MatchRunCommand(MatchRunCommand {
        command_selector: Some(WorkflowCommandSelector {
          command_selector: Some(
            workflow_command_selector::Command_selector::RegisteredCommand(
              workflow_command_selector::RegisteredCommand {
                registered_command_id: "handler".to_string(),
                ..Default::default()
              },
            ),
          ),
          ..Default::default()
        })
        .into(),
        minimum_execution_interval: Some(protobuf::well_known_types::duration::Duration {
          seconds: 60,
          ..Default::default()
        })
        .into(),
        ..Default::default()
      })),
      ..Default::default()
    },
  );
  let start = state("start").declare_transition(&command, rule!(message_equals("start")));
  let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
    WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config(),
  ]);
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let mut buffer = buffer
    .update(setup.make_config_update(workflows), &state_store)
    .await;
  buffer
    .process_log(normal_log("start"), &state_store, None, None)
    .await
    .unwrap();
  let result = buffer
    .process_log(normal_log("execute"), &state_store, None, None)
    .await
    .unwrap();
  assert_eq!(1, result.workflow_commands_to_start.len());
  let token = result.workflow_commands_to_start[0].completion_token();

  buffer.metadata_collector = MetadataCollector::new(
    Arc::new(FailingMetadataProvider),
    LogFields::default(),
    LogFields::default(),
  );
  buffer
    .process_workflow_command_completion(
      WorkflowCommandCompletion {
        token: token.clone(),
        outcome: WorkflowCommandOutcome::Succeeded {
          message: None,
          fields: LogFields::default(),
        },
      },
      &state_store,
    )
    .await;
  assert!(
    buffer
      .logging_state
      .workflows_engine()
      .unwrap()
      .complete_workflow_command(
        &token,
        WorkflowCommandOutcome::Succeeded {
          message: None,
          fields: LogFields::default(),
        },
        OffsetDateTime::now_utc(),
      )
      .is_err()
  );
}

#[tokio::test]
async fn workflow_command_start_survives_buffer_write_failure() {
  let setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let terminal = state("terminal");
  let command = state("command").declare_transition(
    &terminal,
    Rule {
      rule_type: Some(Rule_type::MatchRunCommand(MatchRunCommand {
        command_selector: Some(WorkflowCommandSelector {
          command_selector: Some(
            workflow_command_selector::Command_selector::RegisteredCommand(
              workflow_command_selector::RegisteredCommand {
                registered_command_id: "handler".to_string(),
                ..Default::default()
              },
            ),
          ),
          ..Default::default()
        })
        .into(),
        minimum_execution_interval: Some(protobuf::well_known_types::duration::Duration {
          seconds: 60,
          ..Default::default()
        })
        .into(),
        ..Default::default()
      })),
      ..Default::default()
    },
  );
  let start = state("start").declare_transition(&command, rule!(message_equals("start")));
  let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
    WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config(),
  ]);
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let mut config_update = setup.make_config_update(workflows);
  config_update.buffer_selector = BufferSelector::new(&BufferConfigList {
    buffer_config: vec![default_buffer_config(
      BufferType::CONTINUOUS,
      Some(match_message("execute")),
    )],
    ..Default::default()
  })
  .unwrap();
  let mut buffer = buffer.update(config_update, &state_store).await;

  buffer
    .process_log(normal_log("start"), &state_store, None, None)
    .await
    .unwrap();
  let result = buffer
    .process_log(normal_log("execute"), &state_store, None, None)
    .await
    .unwrap();

  assert_eq!(1, result.workflow_commands_to_start.len());
}

#[tokio::test]
async fn failed_log_write_uses_synthetic_log_for_workflow_flush() {
  let mut setup = Setup::new();
  setup.drain_buffer_events();
  std::fs::create_dir_all(setup.tmp_dir.path().join("buffer")).unwrap();
  let trigger_config = BufferConfigList {
    buffer_config: vec![default_buffer_config(BufferType::TRIGGER, None)],
    ..Default::default()
  };
  setup
    .buffer_manager
    .update_from_config(&trigger_config, false)
    .await
    .unwrap();

  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let terminal = state("terminal");
  let start = state("start").declare_transition_with_actions(
    &terminal,
    rule!(message_equals("flush")),
    &[make_flush_buffers_action(&["default"], None, "flush")],
  );
  let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
    WorkflowBuilder::new("workflow", &[&start, &terminal]).make_config(),
  ]);
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let mut config_update = setup.make_config_update(workflows);
  let mut missing_buffer =
    default_buffer_config(BufferType::CONTINUOUS, Some(match_message("flush")));
  missing_buffer.id = "missing".to_string();
  config_update.buffer_selector = BufferSelector::new(&BufferConfigList {
    buffer_config: vec![missing_buffer],
    ..Default::default()
  })
  .unwrap();
  let mut buffer = buffer.update(config_update, &state_store).await;

  buffer
    .process_log(normal_log("flush"), &state_store, None, None)
    .await
    .unwrap();

  let trigger_buffer = setup.buffer_manager.buffers().remove("default").unwrap().1;
  let mut consumer = trigger_buffer.new_consumer().unwrap();
  assert_ne!(consumer.start_read(false).unwrap(), [] as [u8; 0]);
}

#[tokio::test]
async fn partially_written_log_does_not_add_synthetic_log_for_workflow_flush() {
  let mut setup = Setup::new();
  setup.drain_buffer_events();
  std::fs::create_dir_all(setup.tmp_dir.path().join("buffer")).unwrap();
  let trigger_config = BufferConfigList {
    buffer_config: vec![default_buffer_config(BufferType::TRIGGER, None)],
    ..Default::default()
  };
  setup
    .buffer_manager
    .update_from_config(&trigger_config, false)
    .await
    .unwrap();

  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let terminal = state("terminal");
  let start = state("start").declare_transition_with_actions(
    &terminal,
    rule!(message_equals("flush")),
    &[make_flush_buffers_action(&["default"], None, "flush")],
  );
  let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
    WorkflowBuilder::new("workflow", &[&start, &terminal]).make_config(),
  ]);
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let mut config_update = setup.make_config_update(workflows);
  let mut missing_buffer =
    default_buffer_config(BufferType::CONTINUOUS, Some(match_message("flush")));
  missing_buffer.id = "missing".to_string();
  config_update.buffer_selector = BufferSelector::new(&BufferConfigList {
    buffer_config: vec![
      default_buffer_config(BufferType::TRIGGER, Some(match_message("flush"))),
      missing_buffer,
    ],
    ..Default::default()
  })
  .unwrap();
  let mut buffer = buffer.update(config_update, &state_store).await;

  buffer
    .process_log(normal_log("flush"), &state_store, None, None)
    .await
    .unwrap();

  let trigger_buffer = setup.buffer_manager.buffers().remove("default").unwrap().1;
  let mut consumer = trigger_buffer.new_consumer().unwrap();
  assert_ne!(consumer.start_read(false).unwrap(), [] as [u8; 0]);
  assert!(consumer.start_read(false).is_err());
}

#[tokio::test]
async fn workflow_command_lifecycle_retains_metadata_after_start_replay_failure() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let terminal = state("terminal");
  let command = state("command").declare_transition(
    &terminal,
    Rule {
      rule_type: Some(Rule_type::MatchRunCommand(MatchRunCommand {
        command_selector: Some(WorkflowCommandSelector {
          command_selector: Some(
            workflow_command_selector::Command_selector::RegisteredCommand(
              workflow_command_selector::RegisteredCommand {
                registered_command_id: "handler".to_string(),
                ..Default::default()
              },
            ),
          ),
          ..Default::default()
        })
        .into(),
        minimum_execution_interval: Some(protobuf::well_known_types::duration::Duration {
          seconds: 60,
          ..Default::default()
        })
        .into(),
        ..Default::default()
      })),
      ..Default::default()
    },
  );
  let start = state("start").declare_transition(&command, rule!(message_equals("start")));
  let workflows = WorkflowsConfiguration::new_with_workflow_configurations(vec![
    WorkflowBuilder::new("workflow", &[&start, &command, &terminal]).make_config(),
  ]);
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  buffer = buffer
    .update(
      setup.make_config_update(WorkflowsConfiguration::default()),
      &state_store,
    )
    .await;
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut workflow_buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  workflow_buffer = workflow_buffer
    .update(setup.make_config_update(workflows), &state_store)
    .await;
  buffer
    .metadata_collector
    .add_field(
      "outcome_metadata".into(),
      DataValue::String("metadata_value".to_string()),
    )
    .unwrap();
  workflow_buffer
    .process_log(normal_log("start"), &state_store, None, None)
    .await
    .unwrap();
  let request = workflow_buffer
    .process_log(normal_log("execute"), &state_store, None, None)
    .await
    .unwrap()
    .workflow_commands_to_start
    .pop()
    .unwrap();
  let mut start_logs = buffer.handle_log_replay_result(LogReplayResult {
    workflow_commands_to_start: vec![request.clone()],
    ..Default::default()
  });
  let (start_log, command_event) = start_logs.pop().unwrap();
  assert_eq!(start_logs, [] as [(Log, Option<WorkflowCommandEvent>); 0]);
  assert_eq!(command_event, Some(WorkflowCommandEvent::Start));
  buffer
    .replayer
    .results
    .lock()
    .push_back(Err(anyhow::anyhow!("start replay failed")));
  let (start_log, context) = workflow_generated_log(
    start_log,
    None,
    Some(LogAttributesOverrides::OccurredAt(request.started_at)),
    true,
  );
  buffer
    .process_all_logs_with_completion(start_log, &state_store, context, command_event)
    .await
    .map_err(|error| error.error)
    .unwrap();
  assert_eq!(
    buffer.replayer.logs.lock().last().unwrap(),
    COMMAND_START_MESSAGE
  );
  assert_eq!(
    buffer.replayer.completion_tokens.lock().last().unwrap(),
    &None
  );
  assert_eq!(
    buffer
      .replayer
      .fields
      .lock()
      .last()
      .unwrap()
      .get("outcome_metadata")
      .unwrap()
      .as_str(),
    Some("metadata_value"),
  );
  buffer
    .replayer
    .results
    .lock()
    .push_back(Ok(LogReplayResult {
      engine_has_debug_workflows: true,
      ..Default::default()
    }));

  buffer
    .process_workflow_command_outcome_logs(
      [Log {
        log_level: log_level::INFO,
        log_type: LogType::SPAN,
        message: COMMAND_OUTCOME_MESSAGE.into(),
        fields: LogFields::default(),
        matching_fields: LogFields::default(),
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        session_id: "session".into(),
        command_attachment: None,
        capture_session: None,
      }],
      &state_store,
    )
    .await;

  assert_eq!(
    None,
    buffer
      .replayer
      .completion_tokens
      .lock()
      .last()
      .cloned()
      .unwrap()
  );
  assert_eq!(
    Some(&DataValue::String("metadata_value".to_string())),
    buffer
      .replayer
      .fields
      .lock()
      .last()
      .unwrap()
      .get("outcome_metadata")
  );
  assert!(buffer.send_workflow_debug_state_delay.is_some());
}

#[tokio::test]
async fn failed_workflow_outcome_replay_releases_attachment() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let attachment_store = buffer.workflow_attachment_store().get().await.unwrap();
  let attachment = attachment_store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  let payload_path = setup
    .tmp_dir
    .path()
    .join("workflow-attachments")
    .join(format!("{}.payload", attachment.id));
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();

  buffer
    .process_workflow_command_outcome_logs(
      [Log {
        log_level: log_level::ERROR,
        log_type: LogType::SPAN,
        message: COMMAND_OUTCOME_MESSAGE.into(),
        session_id: "session".into(),
        occurred_at: OffsetDateTime::now_utc(),
        fields: LogFields::default(),
        matching_fields: LogFields::default(),
        command_attachment: Some(Box::new(LogAttachment {
          artifact_id: attachment.id.to_string(),
          content_type: "application/octet-stream".to_string(),
          size_bytes: 9,
          source: AttachmentSource::WORKFLOW,
        })),
        capture_session: None,
      }],
      &state_store,
    )
    .await;

  assert!(!tokio::fs::try_exists(payload_path).await.unwrap());
}

#[tokio::test]
async fn partially_written_workflow_outcome_keeps_attachment() {
  let mut setup = Setup::new();
  setup.drain_buffer_events();
  std::fs::create_dir_all(setup.tmp_dir.path().join("buffer")).unwrap();
  setup
    .buffer_manager
    .update_from_config(
      &BufferConfigList {
        buffer_config: vec![default_buffer_config(BufferType::CONTINUOUS, None)],
        ..Default::default()
      },
      false,
    )
    .await
    .unwrap();

  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (buffer, _) = setup.make_real_async_log_buffer(config_update_rx);
  let attachment_store = buffer.workflow_attachment_store().get().await.unwrap();
  let attachment = attachment_store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  let attachment_directory = setup.tmp_dir.path().join("workflow-attachments");
  let payload_path = attachment_directory.join(format!("{}.payload", attachment.id));
  let metadata_path = attachment_directory.join(format!("{}.metadata", attachment.id));
  let occurred_at = OffsetDateTime::now_utc();
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  let mut config_update = setup.make_config_update(WorkflowsConfiguration::default());
  let mut missing_buffer = default_buffer_config(
    BufferType::CONTINUOUS,
    Some(match_message(COMMAND_OUTCOME_MESSAGE)),
  );
  missing_buffer.id = "missing".to_string();
  config_update.buffer_selector = BufferSelector::new(&BufferConfigList {
    buffer_config: vec![
      default_buffer_config(
        BufferType::CONTINUOUS,
        Some(match_message(COMMAND_OUTCOME_MESSAGE)),
      ),
      missing_buffer,
    ],
    ..Default::default()
  })
  .unwrap();
  let mut buffer = buffer.update(config_update, &state_store).await;

  buffer
    .process_workflow_command_outcome_logs(
      [Log {
        log_level: log_level::ERROR,
        log_type: LogType::SPAN,
        message: COMMAND_OUTCOME_MESSAGE.into(),
        session_id: "session".into(),
        occurred_at,
        fields: LogFields::default(),
        matching_fields: LogFields::default(),
        command_attachment: Some(Box::new(LogAttachment {
          artifact_id: attachment.id.to_string(),
          content_type: "application/octet-stream".to_string(),
          size_bytes: 9,
          source: AttachmentSource::WORKFLOW,
        })),
        capture_session: None,
      }],
      &state_store,
    )
    .await;

  assert!(tokio::fs::try_exists(payload_path).await.unwrap());
  let metadata = WorkflowAttachmentState::parse_from_bytes(
    &read_checksummed_data(&tokio::fs::read(metadata_path).await.unwrap()).unwrap(),
  )
  .unwrap();
  assert_eq!(
    metadata.occurred_at_micros,
    Some(u64::try_from(occurred_at.unix_timestamp_micros()).unwrap())
  );
}

#[tokio::test]
async fn committed_workflow_outcome_keeps_attachment_after_injected_log_failure() {
  let mut setup = Setup::new();
  let (_config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
  let (mut buffer, _) = setup.make_test_async_log_buffer(config_update_rx);
  let attachment_store = buffer.workflow_attachment_store().get().await.unwrap();
  let attachment = attachment_store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  let attachment_directory = setup.tmp_dir.path().join("workflow-attachments");
  let payload_path = attachment_directory.join(format!("{}.payload", attachment.id));
  let metadata_path = attachment_directory.join(format!("{}.metadata", attachment.id));
  let occurred_at = OffsetDateTime::now_utc();
  let state_store = TestStore::new().await;
  let state_store = (*state_store).clone();
  buffer = buffer
    .update(
      setup.make_config_update(WorkflowsConfiguration::default()),
      &state_store,
    )
    .await;
  buffer.replayer.results.lock().extend([
    Ok(LogReplayResult {
      logs_to_inject: vec![Log {
        log_level: log_level::INFO,
        log_type: LogType::NORMAL,
        message: "Injected workflow log".into(),
        fields: LogFields::default(),
        matching_fields: LogFields::default(),
        occurred_at: OffsetDateTime::now_utc(),
        session_id: "session".into(),
        command_attachment: None,
        capture_session: None,
      }],
      committed_workflow_attachment: true,
      ..Default::default()
    }),
    Err(anyhow::anyhow!("injected workflow log replay failed")),
  ]);

  buffer
    .process_workflow_command_outcome_logs(
      [Log {
        log_level: log_level::ERROR,
        log_type: LogType::SPAN,
        message: COMMAND_OUTCOME_MESSAGE.into(),
        session_id: "session".into(),
        occurred_at,
        fields: LogFields::default(),
        matching_fields: LogFields::default(),
        command_attachment: Some(Box::new(LogAttachment {
          artifact_id: attachment.id.to_string(),
          content_type: "application/octet-stream".to_string(),
          size_bytes: 9,
          source: AttachmentSource::WORKFLOW,
        })),
        capture_session: None,
      }],
      &state_store,
    )
    .await;

  assert!(tokio::fs::try_exists(payload_path).await.unwrap());
  let metadata = WorkflowAttachmentState::parse_from_bytes(
    &read_checksummed_data(&tokio::fs::read(metadata_path).await.unwrap()).unwrap(),
  )
  .unwrap();
  assert_eq!(
    metadata.occurred_at_micros,
    Some(u64::try_from(occurred_at.unix_timestamp_micros()).unwrap())
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
    command_attachment: None,
    capture_session: None,
  };

  sender.try_send_log(log, LogAdmission::Current).unwrap();

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
    command_attachment: None,
    capture_session: None,
  };
  sender.try_send_log(log, LogAdmission::Current).unwrap();

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
    command_attachment: None,
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

  sender_2.try_send_log(log, LogAdmission::Current).unwrap();
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
