// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./async_log_buffer_test.rs"]
mod async_log_buffer_test;

use crate::device_id::DeviceIdInterceptor;
use crate::log_replay::{LogReplay, LogReplayResult};
use crate::logger::{ReportProcessingRequest, with_thread_local_logger_guard};
use crate::logging_state::{ConfigUpdate, LoggingState, UninitializedLoggingContext};
use crate::metadata::MetadataCollector;
use crate::network::{NetworkQualityInterceptor, SystemTimeProvider};
use crate::ordered_receiver::{OrderedMessage, OrderedReceiver, SequencedMessage};
use crate::pre_config_buffer::{PendingStateOperation, PreConfigBuffer, PreConfigItem};
use crate::{Block, internal_report, network};
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_bounded_buffer::{TrySendError, channel};
use bd_buffer::BuffersWithAck;
use bd_client_common::init_lifecycle::{InitLifecycle, InitLifecycleState};
use bd_client_common::{maybe_await, maybe_await_map};
use bd_client_stats::FlushTriggerRequest;
use bd_crash_handler::global_state;
use bd_device::Store;
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::size::MemorySized;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  Log,
  LogFieldValue,
  LogFields,
  LogInterceptor,
  LogLevel,
  LogMessage,
  StringOrBytes,
};
use bd_network_quality::{NetworkQualityMonitor, NetworkQualityResolver};
use bd_proto::protos::client::api::debug_data_request::{
  WorkflowDebugData,
  WorkflowTransitionDebugData,
};
use bd_proto::protos::client::api::{DebugDataRequest, debug_data_request};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::ConfigLoader;
use bd_session_replay::CaptureScreenshotHandler;
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_state::{Scope, StateReader};
use bd_stats_common::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_time::{OffsetDateTimeExt, TimeDurationExt, TimeProvider};
use bd_workflows::workflow::WorkflowDebugStateMap;
use debug_data_request::workflow_transition_debug_data::Transition_type;
use std::collections::{HashMap, VecDeque};
use std::mem::size_of_val;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Sleep;

//
// ReportProcessor
//

/// Abstraction over crash report processing to allow for easier testing.
pub trait ReportProcessor {
  async fn process_all_pending_reports(&self) -> Vec<bd_crash_handler::CrashLog>;
}

impl ReportProcessor for bd_crash_handler::Monitor {
  async fn process_all_pending_reports(&self) -> Vec<bd_crash_handler::CrashLog> {
    self.process_all_pending_reports().await
  }
}

impl ReportProcessor for () {
  async fn process_all_pending_reports(&self) -> Vec<bd_crash_handler::CrashLog> {
    vec![]
  }
}

/// A feature flag entry returned from `GetFeatureFlags`.
#[derive(Debug, Clone)]
pub struct FeatureFlagEntry {
  pub name: String,
  pub variant: String,
}

#[derive(Debug)]
pub enum StateUpdateMessage {
  AddLogField(String, StringOrBytes),
  RemoveLogField(String),
  SetFeatureFlagExposure(String, Option<String>),
  GetFeatureFlags(bd_completion::Sender<Vec<FeatureFlagEntry>>),
  FlushState(Option<bd_completion::Sender<()>>),
}

impl MemorySized for StateUpdateMessage {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Self::AddLogField(key, value) => key.size() + value.size(),
        Self::RemoveLogField(field_name) => field_name.len(),
        Self::SetFeatureFlagExposure(flag, variant) => {
          flag.len() + variant.as_ref().map_or(0, String::len)
        },
        Self::GetFeatureFlags(sender) => size_of_val(sender),
        Self::FlushState(sender) => size_of_val(sender),
      }
  }
}

pub type SequencedStateUpdate = SequencedMessage<StateUpdateMessage>;

impl MemorySized for SequencedStateUpdate {
  fn size(&self) -> usize {
    // Don't count sequence overhead - size() is consistent source of truth for accounting
    self.message.size()
  }
}

#[derive(Debug)]
pub struct EmitLogMessage {
  log: LogLine,
  log_processing_completed_tx: Option<oneshot::Sender<()>>,
}

pub type SequencedLog = SequencedMessage<EmitLogMessage>;

impl MemorySized for SequencedLog {
  fn size(&self) -> usize {
    // Don't count sequence overhead - size() is consistent source of truth for accounting
    self.message.size()
  }
}

impl From<LogLine> for EmitLogMessage {
  fn from(log: LogLine) -> Self {
    Self {
      log,
      log_processing_completed_tx: None,
    }
  }
}

impl MemorySized for EmitLogMessage {
  fn size(&self) -> usize {
    size_of_val(self) + self.log.size()
  }
}

//
// LogLine
//

/// A copy of an incoming log line, used to allow for offloading the
/// processing of the incoming logs to an async run loop.
///
/// The log does not have a `group`, `timestamp` and all of the `fields` yet.
/// These are populated only after the log is dequeued for processing on the
/// run loop and the execution of the program calls into `metadata_provider`
/// to retrieve the aforementioned properties and merge them into the final log
/// before passing them for further processing.
#[derive(Debug)]
pub struct LogLine {
  // Remember to update the implementation
  // of the `MemorySized` trait every
  // time the struct is modified!!!
  pub log_level: LogLevel,
  pub log_type: LogType,
  pub message: LogMessage,
  pub fields: AnnotatedLogFields,
  pub matching_fields: AnnotatedLogFields,
  pub attributes_overrides: Option<LogAttributesOverrides>,

  /// If set, indicates that the log should trigger a session capture. The provided value is an ID
  /// that helps identify why the session should be captured.
  pub capture_session: Option<&'static str>,
}

//
// LogAttributesOverrides
//

#[derive(Debug)]
pub enum LogAttributesOverrides {
  /// The hint that tells the SDK to use the previous session ID if available.
  ///
  /// Use of this override assumes that all relevant metadata has been attached to the log as no
  /// current session metadata will be added.
  PreviousRunSessionID(OffsetDateTime),

  /// Overrides the time when the log occurred at, useful for cases like spans with a provided
  /// time.
  OccurredAt(OffsetDateTime),
}

impl MemorySized for LogLine {
  fn size(&self) -> usize {
    // Add a constant number of bytes (48) to account for the size of `log_processing_completed_tx`.
    // We do not use `size_of_val` or `size_of` to do that as it reports different size of the
    // the field when ran on a server and locally on a laptop. The number was captured by
    // calling `size_of_val(log_processing_completed_tx)` on an M2 Macbook.
    //
    // Add a constant number of bytes (24) to account for field alignments etc. that we do not
    // account for when not using `size_of_val(self)`.
    size_of_val(&self.log_level)
      + size_of_val(&self.log_type)
      + self.message.size()
      + self.fields.size()
      + self.matching_fields.size()
      + size_of_val(&self.attributes_overrides)
      + 48
      + 24
  }
}

#[derive(Clone)]
pub struct Sender {
  log_buffer_tx: bd_bounded_buffer::Sender<SequencedLog>,
  state_buffer_tx: bd_bounded_buffer::Sender<SequencedStateUpdate>,
  sequence: Arc<AtomicU64>,
}

impl Sender {
  #[cfg(test)]
  pub(crate) fn from_parts(
    log_buffer_tx: bd_bounded_buffer::Sender<SequencedLog>,
    state_buffer_tx: bd_bounded_buffer::Sender<SequencedStateUpdate>,
  ) -> Self {
    Self {
      log_buffer_tx,
      state_buffer_tx,
      sequence: Arc::new(AtomicU64::new(0)),
    }
  }

  fn next_sequence(&self) -> u64 {
    self.sequence.fetch_add(1, Ordering::Relaxed)
  }

  pub fn try_send_log(&self, msg: EmitLogMessage) -> Result<(), TrySendError> {
    let sequenced = SequencedMessage {
      sequence: self.next_sequence(),
      message: msg,
    };
    self.log_buffer_tx.try_send(sequenced)
  }

  pub fn try_send_state_update(&self, msg: StateUpdateMessage) -> Result<(), TrySendError> {
    let sequenced = SequencedMessage {
      sequence: self.next_sequence(),
      message: msg,
    };
    self.state_buffer_tx.try_send(sequenced)
  }

  pub fn flush_state(&self, block: Block) -> Result<(), TrySendError> {
    let (completion_tx, completion_rx) = if matches!(block, Block::Yes(_)) {
      let (tx, rx) = bd_completion::Sender::new();
      (Some(tx), Some(rx))
    } else {
      (None, None)
    };

    self.try_send_state_update(StateUpdateMessage::FlushState(completion_tx))?;

    // Wait for the processing to be completed only if passed `blocking` argument is equal to
    // `true`.
    if let Some(completion_rx) = completion_rx
      && let Block::Yes(block_timeout) = block
    {
      match &completion_rx.blocking_recv_with_timeout(block_timeout) {
        Ok(()) => {
          log::debug!("flush state: completion received");
        },
        Err(e) => {
          log::debug!("flush state: received an error when waiting for completion: {e}");
        },
      }
    }
    Ok(())
  }
}

pub type AsyncLogBufferOrderedReceiver = OrderedReceiver<EmitLogMessage, StateUpdateMessage>;

//
// AsyncLogBuffer
//

// Orchestrates buffering of incoming logs and offloading their processing to
// a run loop in an async way.
pub struct AsyncLogBuffer<R: LogReplay> {
  ordered_rx: AsyncLogBufferOrderedReceiver,
  config_update_rx: mpsc::Receiver<ConfigUpdate>,
  report_processor_rx: mpsc::Receiver<ReportProcessingRequest>,
  data_upload_tx: mpsc::Sender<DataUpload>,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,

  session_strategy: Arc<bd_session::Strategy>,
  metadata_collector: MetadataCollector,
  resource_utilization_reporter: bd_resource_utilization::Reporter,

  session_replay_recorder: bd_session_replay::Recorder,
  session_replay_capture_screenshot_handler: CaptureScreenshotHandler,

  events_listener: bd_events::Listener,

  replayer: R,
  interceptors: Vec<Arc<dyn LogInterceptor>>,

  logging_state: LoggingState<PreConfigItem>,
  global_state_tracker: global_state::Tracker,
  global_state_reader: global_state::Reader,
  time_provider: Arc<dyn TimeProvider>,
  lifecycle_state: InitLifecycleState,

  pending_workflow_debug_state: HashMap<String, WorkflowDebugStateMap>,
  send_workflow_debug_state_delay: Option<Pin<Box<Sleep>>>,
}

impl<R: LogReplay + Send + 'static> AsyncLogBuffer<R> {
  pub(crate) fn new(
    uninitialized_logging_context: UninitializedLoggingContext<PreConfigItem>,
    replayer: R,
    session_strategy: Arc<bd_session::Strategy>,
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    resource_utilization_target: Box<dyn bd_resource_utilization::Target + Send + Sync>,
    session_replay_target: Box<dyn bd_session_replay::Target + Send + Sync>,
    events_listener_target: Box<dyn bd_events::ListenerTarget + Send + Sync>,
    config_update_rx: mpsc::Receiver<ConfigUpdate>,
    report_processor_rx: mpsc::Receiver<ReportProcessingRequest>,
    shutdown_trigger_handle: ComponentShutdownTriggerHandle,
    runtime_loader: &Arc<ConfigLoader>,
    log_network_quality_monitor: Arc<dyn NetworkQualityMonitor>,
    network_quality_resolver: Arc<dyn NetworkQualityResolver>,
    device_id: String,
    store: &Arc<Store>,
    time_provider: Arc<dyn TimeProvider>,
    lifecycle_state: InitLifecycleState,
    data_upload_tx: mpsc::Sender<DataUpload>,
  ) -> (Self, Sender) {
    let (log_tx, log_rx) = channel(
      uninitialized_logging_context
        .pre_config_log_buffer
        .max_size(),
    );

    // Larger channel for state updates as they are less frequent and we want
    // to avoid dropping any state updates if possible.
    // Note that the 10 MB is not pre-allocated memory, just the upper limit of data stored within
    // the buffer before backpressure is applied.
    let (state_tx, state_rx) = channel(
      10 * 1024 * 1024, // 10 MB
    );

    let (
      session_replay_recorder,
      session_replay_capture_screenshot_handler,
      screenshot_log_interceptor,
    ) = bd_session_replay::Recorder::new(
      session_replay_target,
      runtime_loader,
      &uninitialized_logging_context.stats.scope,
    );

    let internal_periodic_fields_reporter =
      Arc::new(internal_report::Reporter::new(runtime_loader));
    let bandwidth_usage_tracker = Arc::new(network::HTTPTrafficDataUsageTracker::new(
      Arc::new(SystemTimeProvider),
      log_network_quality_monitor,
    ));
    let network_quality_interceptor =
      Arc::new(NetworkQualityInterceptor::new(network_quality_resolver));
    let device_id_interceptor = Arc::new(DeviceIdInterceptor::new(device_id));

    (
      Self {
        ordered_rx: OrderedReceiver::new(log_rx, state_rx),

        config_update_rx,
        report_processor_rx,
        data_upload_tx,
        shutdown_trigger_handle,

        replayer,

        session_strategy,
        metadata_collector: MetadataCollector::new(metadata_provider),
        resource_utilization_reporter: bd_resource_utilization::Reporter::new(
          resource_utilization_target,
          runtime_loader,
        ),

        session_replay_recorder,
        session_replay_capture_screenshot_handler,

        events_listener: bd_events::Listener::new(events_listener_target, runtime_loader),

        interceptors: vec![
          internal_periodic_fields_reporter,
          bandwidth_usage_tracker,
          network_quality_interceptor,
          Arc::new(screenshot_log_interceptor),
          device_id_interceptor,
        ],

        // The size of the pre-config buffer matches the size of the enclosing
        // async log buffer.
        logging_state: LoggingState::Uninitialized(uninitialized_logging_context),
        global_state_tracker: global_state::Tracker::new(
          store.clone(),
          runtime_loader.register_duration_watch(),
        ),
        global_state_reader: global_state::Reader::new(store.clone()),
        time_provider,
        lifecycle_state,

        pending_workflow_debug_state: HashMap::new(),
        send_workflow_debug_state_delay: None,
      },
      Sender {
        log_buffer_tx: log_tx,
        state_buffer_tx: state_tx,
        sequence: Arc::new(AtomicU64::new(0)),
      },
    )
  }

  pub fn enqueue_log(
    tx: &Sender,
    log_level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    attributes_overrides: Option<LogAttributesOverrides>,
    block: Block,
    capture_session: Option<&'static str>,
  ) -> Result<(), TrySendError> {
    let (log_processing_completed_tx_option, log_processing_completed_rx_option) =
      if matches!(block, Block::Yes(_)) {
        // Create a (sender, receiver) pair only if the caller wants to wait on
        // on the log being pushed through the whole log processing pipeline.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let bd_rx = bd_completion::Receiver::to_bd_completion_rx(rx);
        (Some(tx), Some(bd_rx))
      } else {
        (None, None)
      };

    let log = LogLine {
      log_level,
      log_type,
      message,
      fields,
      matching_fields,
      attributes_overrides,
      capture_session,
    };

    // There is no point in continuing the execution of the method and waiting for the log
    // processing to complete if we failed to enqueue the log. In fact, waiting in such case would
    // lead to infinite waiting.
    //
    // There are two possible reasons for the call to fail:
    // 1. The channel is full due to us hitting the capacity limit.
    // 2. The receiver side has been closed. This should only happen in cases in which the event
    //    loop has shut down, which means that we either errored out and defensively shut down the
    //    loop or explicitly shut it down. In either case it is not helpful to report this as an
    //    unexpected error.
    tx.try_send_log(EmitLogMessage {
      log,
      log_processing_completed_tx: log_processing_completed_tx_option,
    })
    .inspect_err(|e| log::debug!("enqueue_log: sending to channel failed: {e:?}"))?;

    // Wait for log processing to be completed only if passed `blocking`
    // argument is equal to `true` and we created a relevant one shot Tokio channel.
    if let Some(rx) = log_processing_completed_rx_option
      && let Block::Yes(block_timeout) = block
    {
      match &rx.blocking_recv_with_timeout(block_timeout) {
        Ok(()) => {
          log::debug!("enqueue_log: log processing completion received");
        },
        Err(e) => {
          log::debug!(
            "enqueue_log: received an error when waiting for log processing completion: {e}"
          );
        },
      }
    }
    // Report success even if the `blocking == true` part of the
    // implementation above failed.
    Ok(())
  }

  async fn process_all_logs(
    &mut self,
    log: LogLine,
    block: bool,
    state_store: &bd_state::Store,
  ) -> anyhow::Result<()> {
    let mut logs = VecDeque::new();
    logs.push_back(log);
    while let Some(log) = logs.pop_front() {
      let log_replay_result = self.process_log(log, block, state_store).await?;
      logs.extend(log_replay_result.logs_to_inject.into_iter().map(|log| {
        LogLine {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message,
          // TODO(mattklein123): Right now we set all fields as OOTB so they can have reserved
          // naming if desired. This may have to change in the future.
          fields: log
            .fields
            .into_iter()
            .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
            .collect(),
          matching_fields: log
            .matching_fields
            .into_iter()
            .map(|(key, value)| {
              (
                key,
                // TODO(mattklein123): Right now the only matching field set on injected logs is
                // the _generate_log_id field used for subsequent matching. If
                // this ever changes we will need to correctly propagate this
                // through.
                AnnotatedLogField::new_ootb(value),
              )
            })
            .collect(),
          // TODO(mattklein123): Technically we should probably propagate overrides to injected
          // logs as well as cover completion under any generated logs, but this gets complicated
          // and is an extreme edge case so we ignore for now until proven it's an issue.
          attributes_overrides: None,
          capture_session: log.capture_session,
        }
      }));

      self
        .pending_workflow_debug_state
        .extend(log_replay_result.workflow_debug_state);
      // We send a periodic workflow debug state update even if there have been no transitions.
      // For an active debugging session this allows us to allow the UI to know we are actually
      // attached and debugging.
      if log_replay_result.engine_has_debug_workflows
        && self.send_workflow_debug_state_delay.is_none()
      {
        // TODO(mattklein123): In a perfect world every time we transition from not debugging to
        // debugging we should immediately send a debug update so that the server can get the
        // baseline state and begin debugging properly. We can do this in a follow up.
        self.send_workflow_debug_state_delay = Some(Box::pin(1.seconds().sleep()));
      }
    }
    Ok(())
  }

  async fn process_log(
    &mut self,
    log: LogLine,
    block: bool,
    state_store: &bd_state::Store,
  ) -> anyhow::Result<LogReplayResult> {
    // Prevent re-entrancy when we are evaluating the log metadata.
    let result = with_thread_local_logger_guard(|| {
      if let Some(LogAttributesOverrides::PreviousRunSessionID(_)) = &log.attributes_overrides {
        // Since we're mimicing a log from the previous app start we want to use the previous
        // global state instead of calling into the providers at this point.
        self
          .metadata_collector
          .metadata_from_fields_with_previous_global_state(
            log.fields,
            log.matching_fields,
            &self.global_state_reader,
          )
      } else {
        self
          .metadata_collector
          .normalized_metadata_with_extra_fields(
            log.fields,
            log.matching_fields,
            log.log_type,
            &mut self.global_state_tracker,
          )
      }
    });

    match result {
      Ok(metadata) => {
        let (session_id, timestamp, extra_fields) = match log.attributes_overrides {
          Some(LogAttributesOverrides::PreviousRunSessionID(occurred_at)) => {
            // Use the previous session ID if available and the provided timestamp.
            (
              self
                .session_strategy
                .previous_process_session_id()
                .unwrap_or_else(|| self.session_strategy.session_id()),
              occurred_at,
              Some(LogFields::from([(
                "_logged_at".into(),
                LogFieldValue::String(metadata.timestamp.to_string()),
              )])),
            )
          },
          Some(LogAttributesOverrides::OccurredAt(overridden_timestamp)) => {
            // Occurred at override provided. Emit log with overrides applied.
            (
              self.session_strategy.session_id(),
              overridden_timestamp,
              Some(LogFields::from([(
                "_logged_at".into(),
                LogFieldValue::String(metadata.timestamp.to_string()),
              )])),
            )
          },
          None => {
            // No overrides provided. Emit log without any overrides.
            (self.session_strategy.session_id(), metadata.timestamp, None)
          },
        };

        let processed_log = bd_log_primitives::Log {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message,
          fields: if let Some(extra_fields) = extra_fields {
            metadata
              .fields
              .into_iter()
              .chain(extra_fields.into_iter())
              .collect()
          } else {
            metadata.fields
          },
          matching_fields: metadata.matching_fields,
          occurred_at: timestamp,
          session_id,
          capture_session: log.capture_session,
        };

        self.write_log(processed_log, block, state_store).await
      },
      Err(e) => {
        // TODO(Augustyniak): Consider logging as error so that SDK customers can see these
        // errors which are mostly emitted as the result of calls into platform-provided metadata
        // provider.
        anyhow::bail!("failed to process a log inside of process_log section: {e}")
      },
    }
  }

  async fn write_log(
    &mut self,
    log: Log,
    block: bool,
    state_store: &bd_state::Store,
  ) -> anyhow::Result<LogReplayResult> {
    let log_replay_result = match &mut self.logging_state {
      LoggingState::Uninitialized(uninitialized_logging_context) => {
        let result = uninitialized_logging_context
          .pre_config_log_buffer
          .push(PreConfigItem::Log(log));

        uninitialized_logging_context
          .stats
          .pre_config_log_buffer
          .record(&result);
        if let Err(e) = result {
          anyhow::bail!("failed to push log to a pre-config buffer: {e}");
        }

        LogReplayResult::default()
      },
      LoggingState::Initialized(initialized_logging_context) => self
        .replayer
        .replay_log(
          log,
          block,
          &mut initialized_logging_context.processing_pipeline,
          state_store,
          self.time_provider.now(),
        )
        .await
        .map_err(|e| anyhow!("failed to replay async log buffer log: {e}"))?,
    };

    Ok(log_replay_result)
  }

  async fn update(
    mut self,
    config: ConfigUpdate,
  ) -> (Self, Option<PreConfigBuffer<PreConfigItem>>) {
    let (initialized_logging_context, maybe_pre_config_log_buffer) = match self.logging_state {
      LoggingState::Uninitialized(uninitialized_logging_context) => {
        let (initialized_logging_context, pre_config_log_buffer) = uninitialized_logging_context
          .updated(
            config,
            self.session_replay_capture_screenshot_handler.clone(),
          )
          .await;
        (initialized_logging_context, Some(pre_config_log_buffer))
      },
      LoggingState::Initialized(mut initialized_logging_context) => {
        initialized_logging_context.update(config);
        (initialized_logging_context, None)
      },
    };

    self.logging_state = LoggingState::Initialized(initialized_logging_context);

    (self, maybe_pre_config_log_buffer)
  }

  async fn maybe_replay_pre_config_buffer(
    &mut self,
    pre_config_buffer: PreConfigBuffer<PreConfigItem>,
    state_store: &bd_state::Store,
  ) {
    let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state else {
      return;
    };

    let now = self.time_provider.now();

    for item in pre_config_buffer.pop_all() {
      match item {
        PreConfigItem::Log(log) => {
          if let Err(e) = self
            .replayer
            .replay_log(
              log,
              false,
              &mut initialized_logging_context.processing_pipeline,
              state_store,
              now,
            )
            .await
          {
            log::debug!("failed to replay pre-config log: {e}");
          }
        },
        PreConfigItem::StateOperation(operation) => match operation {
          PendingStateOperation::SetFeatureFlagExposure {
            name,
            variant,
            session_id,
          } => {
            initialized_logging_context
              .handle_state_insert(
                state_store,
                &self.metadata_collector,
                &mut self.global_state_tracker,
                &mut self.replayer,
                Scope::FeatureFlagExposure,
                name,
                variant.unwrap_or_default(),
                now,
                &session_id,
              )
              .await;
          },
        },
      }
    }
  }

  pub async fn run(
    self,
    state_store: bd_state::Store,
    report_processor: impl ReportProcessor,
  ) -> Self {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    self
      .run_with_shutdown(
        state_store,
        report_processor,
        shutdown_trigger.make_shutdown(),
      )
      .await
  }

  // TODO(mattklein123): This seems to only be used for tests. Figure out how to clean this up
  // so we don't need this just for tests.
  pub async fn run_with_shutdown(
    mut self,
    state_store: bd_state::Store,
    report_processor: impl ReportProcessor,
    mut shutdown: ComponentShutdown,
  ) -> Self {
    // Processes incoming logs and reacts to workflows config updates.
    //
    // The first workflows config update makes the async log buffer disable
    // pre-config log buffer and results in a replay all of the logs stored
    // by the pre-config log buffer. All of that happens in a way where logs
    // stored in pre-config log buffer are guaranteed to be replayed before
    // the async log buffer goes back to processing incoming logs.

    let local_shutdown = shutdown.cancelled();
    tokio::pin!(local_shutdown);
    let mut self_shutdown = self.shutdown_trigger_handle.make_shutdown();
    let self_shutdown = self_shutdown.cancelled();
    tokio::pin!(self_shutdown);
    loop {
      let initialized_logging_context =
        if let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state {
          Some(initialized_logging_context)
        } else {
          None
        };

      tokio::select! {
        Some(config) = self.config_update_rx.recv() => {
          let (updated_self, maybe_pre_config_buffer)
            = self.update(config).await;

          self = updated_self;
          if let Some(pre_config_buffer) = maybe_pre_config_buffer {
            self.lifecycle_state.set(InitLifecycle::LogProcessingStarted);
            self
              .maybe_replay_pre_config_buffer(pre_config_buffer, &state_store)
              .await;
          }
        },
        Some(ReportProcessingRequest {
           session
        }) = self.report_processor_rx.recv() => {
          // TODO(snowp): Once we move over to using the file watcher we can more accurately pick
          // current vs previous for all reports, but as we need to handle restarts etc we may
          // also want to embed the full information into the report. This should ensure that we
          // can upload files after the fact and intelligently drop them.
          // TODO(snowp): Consider emitting the crash log as part of writing the log instead of
          // emitting it as part of the upload process. This avoids having to mess with time
          // overrides at a later stage.

          for crash_log in report_processor.process_all_pending_reports().await {
            let attributes_overrides = match session {
                crate::ReportProcessingSession::Current => LogAttributesOverrides::OccurredAt(
                  crash_log.timestamp,
                ),
                crate::ReportProcessingSession::PreviousRun =>
                    LogAttributesOverrides::PreviousRunSessionID(
                  crash_log.timestamp)
            }.into();
            let log = LogLine {
              log_type: LogType::LIFECYCLE,
              log_level: crash_log.log_level,
              message: crash_log.message.clone(),
              fields: crash_log.fields,
              matching_fields: [].into(),
              attributes_overrides,
              // Always capture the session when we process a crash log.
              // TODO(snowp): Ideally we should include information like the report and client side
              // grouping here to help make smarter decisions during intent negotiation.
              capture_session: Some("crash_handler"),
            };
            if let Err(e) = self.process_all_logs(log, false, &state_store).await {
              log::debug!("failed to process crash log: {e}");
            }
          }
        },
        Some(ordered_message) = self.ordered_rx.recv() => {
          match ordered_message {
            OrderedMessage::Log(EmitLogMessage { mut log, log_processing_completed_tx }) => {
              for interceptor in &mut self.interceptors {
                interceptor.process(
                  log.log_level,
                  log.log_type,
                  &log.message,
                  &mut log.fields,
                  &mut log.matching_fields,
                );
              }

              if let Err(e) = self.process_all_logs(
                log,
                log_processing_completed_tx.is_some(),
                &state_store,
              ).await {
                log::debug!("failed to process all logs: {e}");
              }

              if let Some(tx) = log_processing_completed_tx && Err(()) == tx.send(()) {
                debug_assert!(false, "failed to send log processing completion");
                log::debug!("failed to send log processing completion");
              }
            },
            OrderedMessage::State(async_log_buffer_message) => {
              match async_log_buffer_message {
                StateUpdateMessage::AddLogField(key, value) => {
                  if let Err(e) = self
                    .metadata_collector
                    .add_field(key.clone().into(), value.clone())
                  {
                    log::warn!("failed to add log field ({key:?}): {e}");
                  }
                },
                StateUpdateMessage::RemoveLogField(field_name) => {
                  self.metadata_collector.remove_field(&field_name);
                },
                StateUpdateMessage::SetFeatureFlagExposure(flag, variant) => {
                  if let LoggingState::Initialized(initialized_logging_context) =
                    &mut self.logging_state
                  {
                    let variant_value = variant.clone().unwrap_or_default();

                    // Initialized: update state store and replay through workflows
                    initialized_logging_context
                      .handle_state_insert(
                        &state_store,
                        &self.metadata_collector,
                        &mut self.global_state_tracker,
                        &mut self.replayer,
                        Scope::FeatureFlagExposure,
                        flag.clone(),
                        variant_value.clone(),
                        self.time_provider.now(),
                        &self.session_strategy.session_id(),
                      )
                      .await;

                    // Emit an internal log for the feature flag change
                    let mut fields: AnnotatedLogFields = AnnotatedLogFields::new();
                    fields.insert(
                      "_feature_flag_name".into(),
                      AnnotatedLogField::new_ootb(flag.clone()),
                    );
                    fields.insert(
                      "_feature_flag_variant".into(),
                      AnnotatedLogField::new_ootb(variant_value),
                    );

                    let log = LogLine {
                      log_level: bd_log_primitives::log_level::INFO,
                      log_type: LogType::INTERNAL_SDK,
                      message: LogMessage::String(format!("Feature flag set: {flag}")),
                      fields,
                      matching_fields: AnnotatedLogFields::new(),
                      attributes_overrides: None,
                      capture_session: None,
                    };

                    if let Err(e) = self.process_all_logs(log, false, &state_store).await {
                      log::debug!("failed to emit feature flag log: {e}");
                    }
                  } else {
                    // Not initialized: queue the operation for later replay
                    if let LoggingState::Uninitialized(uninitialized_logging_context) =
                      &mut self.logging_state
                    {
                      let result = uninitialized_logging_context.pre_config_log_buffer.push(
                        PreConfigItem::StateOperation(PendingStateOperation::SetFeatureFlagExposure{
                            name: flag,
                            variant,
                            session_id: self.session_strategy.session_id()
                        }),
                      );
                      uninitialized_logging_context
                        .stats
                        .pre_config_log_buffer
                        .record(&result);
                      if let Err(e) = result {
                        log::debug!("failed to enqueue state operation to pre-config buffer: {e}");
                      }
                    }
                  }
                },
                StateUpdateMessage::GetFeatureFlags(response_tx) => {
                  let entries = state_store
                    .read()
                    .await
                    .iter()
                    .filter(|entry| entry.scope == Scope::FeatureFlagExposure)
                    .map(|entry| FeatureFlagEntry {
                      name: entry.key.clone(),
                      variant: entry
                        .value
                        .value_type
                        .as_ref()
                        .and_then(|v| match v {
                          bd_state::Value_type::StringValue(s) => Some(s.clone()),
                          _ => None,
                        })
                        .unwrap_or_default(),
                    })
                    .collect();
                  response_tx.send(entries);
                },
                StateUpdateMessage::FlushState(completion_tx) => {
                  let flush_stats_trigger = self.logging_state.flush_stats_trigger().clone();
                  let flush_stats = async move {
                    let (sender, receiver) = bd_completion::Sender::new();
                    if let Err(e) =
                      flush_stats_trigger.flush(
                        FlushTriggerRequest { do_upload: true, completion_tx: Some(sender) }
                      ).await
                    {
                      log::debug!("flushing state: failed to flush stats: {e}");
                    }

                    if let Err(e) = receiver.recv().await {
                      log::debug!("flushing state: failed to wait for stats flush: {e}");
                    }
                  };

                  let flush_buffers_trigger = self.logging_state.flush_buffers_trigger().clone();
                  let flush_buffers = async move {
                    let (sender, receiver) = bd_completion::Sender::new();
                    let buffers_with_ack = BuffersWithAck::new_all_buffers(Some(sender));
                    if let Err(e) = flush_buffers_trigger
                      .send(buffers_with_ack).await
                    {
                      log::debug!("flushing state: failed to flush buffers: {e}");
                    }

                    if let Err(e) = receiver.recv().await {
                      log::debug!("flushing state: failed to wait for buffers flush: {e}");
                    }
                  };

                  // TODO(mattklein123): We need the store interfaces to be async so that this
                  // flush can be async also. For now we do spawn blocking.
                  let session_strategy = self.session_strategy.clone();
                  let flush_session = async {
                    let _ = tokio::task::spawn_blocking(move || {
                      session_strategy.flush();
                    }).await;
                  };

                  let persist_workflows = async {
                    if let Some(workflows_engine) = self.logging_state.workflows_engine() {
                      workflows_engine.maybe_persist(true).await;
                    }
                  };

                  tokio::join!(flush_stats, flush_buffers, flush_session, persist_workflows);

                  if let Some(completion_tx) = completion_tx {
                    completion_tx.send(());
                  }
                },
              }
            },
          }
        },
        () = maybe_await_map(
          initialized_logging_context,
          |initialized_logging_context| async {
            initialized_logging_context.processing_pipeline.run().await;
        })
          => {},
        () = maybe_await(&mut self.send_workflow_debug_state_delay) => {
          self.send_debug_data().await;
        },
        () = self.resource_utilization_reporter.run() => {},
        () = self.session_replay_recorder.run() => {},
        () = self.events_listener.run() => {},
        () = &mut local_shutdown => {
          return self;
        },
        () = &mut self_shutdown => {
          return self;
        },
      }
    }
  }



  async fn send_debug_data(&mut self) {
    log::debug!("sending workflow debug data");
    let mut workflow_debug_data: HashMap<String, WorkflowDebugData> = HashMap::new();
    for (workflow_id, state) in std::mem::take(&mut self.pending_workflow_debug_state) {
      let workflow_entry = workflow_debug_data.entry(workflow_id).or_default();
      for (state_key, data) in state.into_inner().into_iter() {
        let last_transition_time: OffsetDateTime = data.last_transition_time.into();
        match state_key {
          WorkflowDebugStateKey::StartOrReset => {
            let start_reset = workflow_entry.start_reset.mut_or_insert_default();
            start_reset.transition_count += data.count;
            start_reset.last_transition_time = last_transition_time.into_proto();
          },
          WorkflowDebugStateKey::StateTransition {
            state_id,
            transition_type,
          } => {
            workflow_entry
              .states
              .entry(state_id)
              .or_default()
              .transitions
              .push(WorkflowTransitionDebugData {
                transition_type: Some(match transition_type {
                  WorkflowDebugTransitionType::Normal(index) => {
                    Transition_type::TransitionIndex(index.try_into().unwrap_or(0))
                  },
                  WorkflowDebugTransitionType::Timeout => Transition_type::TimeoutTransition(true),
                }),
                transition_count: data.count,
                last_transition_time: last_transition_time.into_proto(),
                ..Default::default()
              });
          },
        }
      }
    }

    let _ = self
      .data_upload_tx
      .send(DataUpload::DebugData(DebugDataRequest {
        workflow_debug_data,
        ..Default::default()
      }))
      .await;
  }
}
