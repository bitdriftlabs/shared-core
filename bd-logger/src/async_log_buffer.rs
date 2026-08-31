// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
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
use crate::pre_config_buffer::{PendingStateOperation, PreConfigBuffer, PreConfigItem};
use crate::{Block, battery, internal_report, network};
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_bounded_buffer::TrySendError;
use bd_buffer::BuffersWithAck;
use bd_client_common::init_lifecycle::{InitLifecycle, InitLifecycleState};
use bd_client_common::{maybe_await, maybe_await_map};
use bd_crash_handler::global_state;
use bd_device::Store;
pub use bd_event_buffer::LoggerControl;
use bd_event_buffer::{
  AdmissionContext,
  AdmissionOutcome,
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventContext,
  LoggerIngressEvent,
  LoggerIngressPayload,
  ProviderSnapshot,
};
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  Log,
  LogFieldValue,
  LogFields,
  LogInterceptor,
  LogLevel,
  LogMessage,
};
pub use bd_log_primitives::{LogAttributesOverrides, LogLine};
use bd_network_quality::{NetworkQualityMonitor, NetworkQualityResolver};
use bd_proto::protos::client::api::debug_data_request::{
  WorkflowDebugData,
  WorkflowTransitionDebugData,
};
use bd_proto::protos::client::api::{DebugDataRequest, debug_data_request};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{self, ConfigLoader, IntWatch};
use bd_session_replay::CaptureScreenshotHandler;
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_state::{
  ENTITY_ID_KEY,
  MEMORY_PRESSURE_LEVEL_KEY,
  SYSTEM_SESSION_ID_KEY,
  Scope,
  string_value,
};
use bd_time::{OffsetDateTimeExt, TimeDurationExt, TimeProvider};
use bd_workflow_stats::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_workflows::workflow::WorkflowDebugStateMap;
use debug_data_request::workflow_transition_debug_data::Transition_type;
use std::collections::{HashMap, VecDeque};
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use tokio::sync::mpsc;
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
  fn process_all_pending_reports(&self) -> impl Future<Output = Vec<bd_crash_handler::CrashLog>> {
    ready(vec![])
  }
}

#[derive(Clone)]
pub struct Sender {
  inner: SenderInner,
}

#[derive(Clone)]
enum SenderInner {
  EventBuffer {
    event_buffer: EventBuffer,
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    session_strategy: Arc<bd_session::Strategy>,
  },
  #[cfg(test)]
  TestEventBuffer { event_buffer: EventBuffer },
}

impl Sender {
  fn new(
    event_buffer: EventBuffer,
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    session_strategy: Arc<bd_session::Strategy>,
  ) -> Self {
    Self {
      inner: SenderInner::EventBuffer {
        event_buffer,
        metadata_provider,
        session_strategy,
      },
    }
  }

  #[cfg(test)]
  pub(crate) fn from_event_buffer(event_buffer: EventBuffer) -> Self {
    Self {
      inner: SenderInner::TestEventBuffer { event_buffer },
    }
  }

  pub fn try_send_log(&self, log: LogLine) -> Result<(), TrySendError> {
    match &self.inner {
      SenderInner::EventBuffer {
        event_buffer,
        metadata_provider,
        session_strategy,
      } => {
        let context = admission_context(
          log.attributes_overrides.as_ref(),
          metadata_provider,
          session_strategy,
        )
        .map_err(|error| {
          log::debug!("failed to capture log admission context: {error}");
          TrySendError::ContextCaptureFailed
        })?;
        admit(
          event_buffer,
          EventBufferEntry::ingress(LoggerIngressEvent::log(log, context, None)),
        )
      },
      #[cfg(test)]
      SenderInner::TestEventBuffer { event_buffer } => admit(
        event_buffer,
        EventBufferEntry::ingress(LoggerIngressEvent::log(
          log,
          EventContext::CurrentProcess(AdmissionContext {
            session_id: "test".into(),
            provider: ProviderSnapshot {
              timestamp: OffsetDateTime::UNIX_EPOCH,
              ootb_fields: LogFields::default(),
              custom_fields: LogFields::default(),
            },
            admitted_at: OffsetDateTime::UNIX_EPOCH,
          }),
          None,
        )),
      ),
    }
  }

  pub fn try_send_log_with_provider_snapshot(
    &self,
    log: LogLine,
    provider: ProviderSnapshot,
  ) -> Result<(), TrySendError> {
    match &self.inner {
      SenderInner::EventBuffer {
        event_buffer,
        session_strategy,
        ..
      } => {
        let context = admission_context_from_provider(
          log.attributes_overrides.as_ref(),
          provider,
          session_strategy,
        )
        .map_err(|error| {
          log::debug!("failed to capture log admission context: {error}");
          TrySendError::ContextCaptureFailed
        })?;
        admit(
          event_buffer,
          EventBufferEntry::ingress(LoggerIngressEvent::log(log, context, None)),
        )
      },
      #[cfg(test)]
      SenderInner::TestEventBuffer { .. } => self.try_send_log(log),
    }
  }

  pub fn try_send_control(&self, msg: LoggerControl) -> Result<(), TrySendError> {
    match &self.inner {
      SenderInner::EventBuffer { event_buffer, .. } => {
        admit(event_buffer, EventBufferEntry::Control(msg))
      },
      #[cfg(test)]
      SenderInner::TestEventBuffer { event_buffer } => {
        admit(event_buffer, EventBufferEntry::Control(msg))
      },
    }
  }

  pub fn try_send_feature_flag_exposure(
    &self,
    flag: String,
    variant: Option<String>,
  ) -> Result<(), TrySendError> {
    match &self.inner {
      SenderInner::EventBuffer {
        event_buffer,
        metadata_provider,
        session_strategy,
      } => {
        let context = current_process_admission_context(metadata_provider, session_strategy)
          .map_err(|error| {
            log::debug!("failed to capture feature flag admission context: {error}");
            TrySendError::ContextCaptureFailed
          })?;
        admit(
          event_buffer,
          EventBufferEntry::ingress(LoggerIngressEvent::feature_flag_exposure(
            flag, variant, context,
          )),
        )
      },
      #[cfg(test)]
      SenderInner::TestEventBuffer { event_buffer } => admit(
        event_buffer,
        EventBufferEntry::ingress(LoggerIngressEvent::feature_flag_exposure(
          flag,
          variant,
          AdmissionContext {
            session_id: "test".into(),
            provider: ProviderSnapshot {
              timestamp: OffsetDateTime::UNIX_EPOCH,
              ootb_fields: LogFields::default(),
              custom_fields: LogFields::default(),
            },
            admitted_at: OffsetDateTime::UNIX_EPOCH,
          },
        )),
      ),
    }
  }

  pub fn flush_state(&self, block: Block) -> Result<(), TrySendError> {
    let (completion_tx, completion_rx) = if matches!(block, Block::Yes { .. }) {
      let (tx, rx) = bd_completion::Sender::new();
      (Some(tx), Some(rx))
    } else {
      (None, None)
    };

    self.try_send_control(LoggerControl::FlushState(completion_tx))?;

    // Wait for the processing to be completed only if passed `blocking` argument is equal to
    // `true`.
    let result = match (block, completion_rx) {
      (
        Block::Yes {
          timeout,
          poll_callback,
        },
        Some(rx),
      ) => Some(rx.blocking_recv_with_timeout_and_callback(
        timeout,
        poll_callback.as_ref().map(AsRef::as_ref),
      )),
      _ => None,
    };
    if let Some(result) = result {
      match &result {
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

fn admission_context(
  attributes_overrides: Option<&LogAttributesOverrides>,
  metadata_provider: &Arc<dyn MetadataProvider + Send + Sync>,
  session_strategy: &Arc<bd_session::Strategy>,
) -> anyhow::Result<EventContext> {
  if matches!(
    attributes_overrides,
    Some(LogAttributesOverrides::PreviousRunSessionID(_))
  ) {
    // Prior-process logs only retain their admission timestamp. Capturing current-process fields
    // would waste work and could drop a crash report if an irrelevant field provider fails.
    let logged_at = with_thread_local_logger_guard(|| metadata_provider.timestamp())?;
    return Ok(EventContext::PreviousProcess { logged_at });
  }

  let provider = provider_snapshot(metadata_provider)?;
  admission_context_from_provider(attributes_overrides, provider, session_strategy)
}

fn current_process_admission_context(
  metadata_provider: &Arc<dyn MetadataProvider + Send + Sync>,
  session_strategy: &Arc<bd_session::Strategy>,
) -> anyhow::Result<AdmissionContext> {
  let provider = provider_snapshot(metadata_provider)?;
  current_process_admission_context_from_provider(provider, session_strategy)
}

fn provider_snapshot(
  metadata_provider: &Arc<dyn MetadataProvider + Send + Sync>,
) -> anyhow::Result<ProviderSnapshot> {
  with_thread_local_logger_guard(|| -> anyhow::Result<ProviderSnapshot> {
    let timestamp = metadata_provider.timestamp()?;
    let (custom_fields, ootb_fields) = metadata_provider.fields()?;
    Ok(ProviderSnapshot {
      timestamp,
      ootb_fields,
      custom_fields,
    })
  })
}

fn admission_context_from_provider(
  attributes_overrides: Option<&LogAttributesOverrides>,
  provider: ProviderSnapshot,
  session_strategy: &Arc<bd_session::Strategy>,
) -> anyhow::Result<EventContext> {
  if matches!(
    attributes_overrides,
    Some(LogAttributesOverrides::PreviousRunSessionID(_))
  ) {
    return Ok(EventContext::PreviousProcess {
      logged_at: provider.timestamp,
    });
  }

  current_process_admission_context_from_provider(provider, session_strategy)
    .map(EventContext::CurrentProcess)
}

fn current_process_admission_context_from_provider(
  provider: ProviderSnapshot,
  session_strategy: &Arc<bd_session::Strategy>,
) -> anyhow::Result<AdmissionContext> {
  let session_id = session_strategy.session_id()?;
  Ok(AdmissionContext {
    session_id,
    admitted_at: provider.timestamp,
    provider,
  })
}

fn admit(event_buffer: &EventBuffer, entry: EventBufferEntry) -> Result<(), TrySendError> {
  match event_buffer.admit(entry) {
    AdmissionOutcome::Admitted => Ok(()),
    AdmissionOutcome::RejectedFull | AdmissionOutcome::RejectedOversized => {
      Err(TrySendError::FullSizeOverflow)
    },
    AdmissionOutcome::Closed => Err(TrySendError::Closed),
  }
}

/// Converts a workflow-generated log into a line for immediate replay after its source log.
///
/// Generated logs do not re-enter `EventBuffer`, but must retain their source's immutable admission
/// context and metadata overrides so they cannot be attributed to a later session or process.
fn workflow_generated_log(
  log: Log,
  context: Option<EventContext>,
  attributes_overrides: Option<LogAttributesOverrides>,
) -> (LogLine, Option<EventContext>) {
  let log = LogLine {
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
          // the _generate_log_id field used for subsequent matching. If this ever changes we
          // will need to correctly propagate this through.
          AnnotatedLogField::new_ootb(value),
        )
      })
      .collect(),
    attributes_overrides,
    capture_session: log.capture_session,
  };
  (log, context)
}

//
// AsyncLogBuffer
//

// Orchestrates buffering of incoming logs and offloading their processing to
// a run loop in an async way.
pub struct AsyncLogBuffer<R: LogReplay> {
  event_buffer: EventBuffer,
  event_buffer_limit_watches: EventBufferLimitWatches,
  config_update_rx: mpsc::Receiver<ConfigUpdate>,
  report_processor_rx: mpsc::Receiver<ReportProcessingRequest>,
  data_upload_tx: mpsc::Sender<DataUpload>,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,

  session_strategy: Arc<bd_session::Strategy>,
  metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
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
  sdk_status_tracker: bd_client_common::sdk_status::SdkStatusTracker,
  pending_workflow_debug_state: HashMap<String, WorkflowDebugStateMap>,
  send_workflow_debug_state_delay: Option<Pin<Box<Sleep>>>,
  last_session_id: Option<Arc<str>>,
}

struct EventBufferLimitWatches {
  log_limit_bytes: IntWatch<runtime::event_buffer::LogLimitBytesFlag>,
  total_limit_bytes: IntWatch<runtime::event_buffer::TotalLimitBytesFlag>,
}

impl EventBufferLimitWatches {
  fn new(runtime_loader: &ConfigLoader) -> Self {
    Self {
      log_limit_bytes: runtime_loader.register_int_watch(),
      total_limit_bytes: runtime_loader.register_int_watch(),
    }
  }

  fn read_mark_update(&mut self) -> EventBufferLimits {
    EventBufferLimits {
      log_limit_bytes: *self.log_limit_bytes.read_mark_update() as usize,
      total_limit_bytes: *self.total_limit_bytes.read_mark_update() as usize,
    }
  }
}

impl<R: LogReplay + Send + 'static> AsyncLogBuffer<R> {
  pub(crate) fn new(
    uninitialized_logging_context: UninitializedLoggingContext<PreConfigItem>,
    replayer: R,
    session_strategy: Arc<bd_session::Strategy>,
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    initial_ootb_fields: LogFields,
    initial_custom_fields: LogFields,
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
    sdk_status_tracker: bd_client_common::sdk_status::SdkStatusTracker,
    data_upload_tx: mpsc::Sender<DataUpload>,
  ) -> (Self, Sender) {
    // The old log and control channels had 1 MiB and 10 MiB byte budgets respectively. Keep
    // those bootstrap limits while moving both flows into one ordered ingress.
    let mut event_buffer_limit_watches = EventBufferLimitWatches::new(runtime_loader);
    let event_buffer = EventBuffer::new(EventBufferLimits {
      log_limit_bytes: uninitialized_logging_context
        .pre_config_log_buffer
        .max_size(),
      total_limit_bytes: 10 * 1024 * 1024,
    });
    // The bootstrap limits cover admission before runtime configuration is available. Stage the
    // current runtime pair as well: this covers a persisted configuration that loaded before ALB
    // was built, while EventBuffer still applies it only at its next admission.
    event_buffer.set_pending_limits(event_buffer_limit_watches.read_mark_update());

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
    let battery_drain_tracker = Arc::new(battery::BatteryDrainTracker::new(
      Arc::new(SystemTimeProvider),
      runtime_loader,
    ));
    let network_quality_interceptor =
      Arc::new(NetworkQualityInterceptor::new(network_quality_resolver));
    let device_id_interceptor = Arc::new(DeviceIdInterceptor::new(device_id));

    (
      Self {
        event_buffer: event_buffer.clone(),
        event_buffer_limit_watches,

        config_update_rx,
        report_processor_rx,
        data_upload_tx,
        shutdown_trigger_handle,

        replayer,

        session_strategy: session_strategy.clone(),
        metadata_provider: metadata_provider.clone(),
        metadata_collector: MetadataCollector::new(
          metadata_provider.clone(),
          initial_ootb_fields,
          initial_custom_fields,
        ),
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
          battery_drain_tracker,
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
        sdk_status_tracker,
        pending_workflow_debug_state: HashMap::new(),
        send_workflow_debug_state_delay: None,
        last_session_id: None,
      },
      Sender::new(event_buffer, metadata_provider, session_strategy),
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
    capture_session: Option<&'static str>,
  ) -> Result<(), TrySendError> {
    let log = LogLine {
      log_level,
      log_type,
      message,
      fields,
      matching_fields,
      attributes_overrides,
      capture_session,
    };

    // There are two possible reasons for the call to fail:
    // 1. The channel is full due to us hitting the capacity limit.
    // 2. The receiver side has been closed. This should only happen in cases in which the event
    //    loop has shut down, which means that we either errored out and defensively shut down the
    //    loop or explicitly shut it down. In either case it is not helpful to report this as an
    //    unexpected error.
    tx.try_send_log(log)
      .inspect_err(|e| log::debug!("enqueue_log: event admission failed: {e:?}"))?;

    Ok(())
  }

  pub fn enqueue_log_with_provider_snapshot(
    tx: &Sender,
    log_level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    attributes_overrides: Option<LogAttributesOverrides>,
    capture_session: Option<&'static str>,
    provider: ProviderSnapshot,
  ) -> Result<(), TrySendError> {
    let log = LogLine {
      log_level,
      log_type,
      message,
      fields,
      matching_fields,
      attributes_overrides,
      capture_session,
    };

    tx.try_send_log_with_provider_snapshot(log, provider)
      .inspect_err(|e| log::debug!("enqueue_log: event admission failed: {e:?}"))?;

    Ok(())
  }

  async fn process_all_logs(
    &mut self,
    log: LogLine,
    state_store: &bd_state::Store,
    context: Option<EventContext>,
  ) -> anyhow::Result<()> {
    let mut logs = VecDeque::new();
    logs.push_back((log, context));
    while let Some((log, context)) = logs.pop_front() {
      let source_context = context.clone();
      let source_attributes_overrides = log.attributes_overrides.clone();
      let log_replay_result = self.process_log(log, state_store, context).await?;
      logs.extend(log_replay_result.logs_to_inject.into_iter().map(|log| {
        workflow_generated_log(
          log,
          source_context.clone(),
          source_attributes_overrides.clone(),
        )
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
    state_store: &bd_state::Store,
    context: Option<EventContext>,
  ) -> anyhow::Result<LogReplayResult> {
    // Prevent re-entrancy when we are evaluating the log metadata.
    let result = with_thread_local_logger_guard(|| {
      match context {
        Some(EventContext::CurrentProcess(context)) => Ok((
          self
            .metadata_collector
            .normalized_metadata_from_provider_snapshot(
              log.fields,
              log.matching_fields,
              log.log_type,
              &mut self.global_state_tracker,
              context.provider,
            ),
          Some(context.session_id),
        )),
        Some(EventContext::PreviousProcess { logged_at }) => Ok((
          MetadataCollector::metadata_from_fields_with_previous_global_state(
            log.fields,
            log.matching_fields,
            &self.global_state_reader,
            logged_at,
          ),
          None,
        )),
        None
          if matches!(
            &log.attributes_overrides,
            Some(LogAttributesOverrides::PreviousRunSessionID(_))
          ) =>
        {
          // Since we're mimicing a log from the previous app start we want to use the previous
          // global state instead of calling into the providers at this point.
          Ok((
            MetadataCollector::metadata_from_fields_with_previous_global_state(
              log.fields,
              log.matching_fields,
              &self.global_state_reader,
              self.metadata_collector.timestamp()?,
            ),
            None,
          ))
        },
        None => self
          .metadata_collector
          .normalized_metadata_with_extra_fields(
            log.fields,
            log.matching_fields,
            log.log_type,
            &mut self.global_state_tracker,
          )
          .map(|metadata| (metadata, None)),
      }
    });

    match result {
      Ok((metadata, session_id)) => {
        let (session_id, timestamp, extra_fields) = match log.attributes_overrides {
          Some(LogAttributesOverrides::PreviousRunSessionID(occurred_at)) => {
            // Use the previous session ID if available and the provided timestamp.
            let session_id = match self.session_strategy.previous_process_session_id() {
              Some(session_id) => session_id,
              None => self.session_strategy.session_id()?,
            };
            (
              session_id,
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
              session_id.map_or_else(|| self.session_strategy.session_id(), Ok)?,
              overridden_timestamp,
              Some(LogFields::from([(
                "_logged_at".into(),
                LogFieldValue::String(metadata.timestamp.to_string()),
              )])),
            )
          },
          None => {
            // No overrides provided. Emit log without any overrides.
            (
              session_id.map_or_else(|| self.session_strategy.session_id(), Ok)?,
              metadata.timestamp,
              None,
            )
          },
        };

        if !matches!(
          log.attributes_overrides,
          Some(LogAttributesOverrides::PreviousRunSessionID(_))
        ) {
          self
            .update_system_session_id(state_store, &session_id)
            .await;
        }

        let processed_log = bd_log_primitives::Log {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message,
          fields: if let Some(extra_fields) = extra_fields {
            metadata.fields.into_iter().chain(extra_fields).collect()
          } else {
            metadata.fields
          },
          matching_fields: metadata.matching_fields,
          occurred_at: timestamp,
          session_id,
          capture_session: log.capture_session,
        };

        self.write_log(processed_log, state_store).await
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
          &mut initialized_logging_context.processing_pipeline,
          state_store,
          self.time_provider.now(),
        )
        .await
        .map_err(|e| anyhow!("failed to replay async log buffer log: {e}"))?,
    };

    Ok(log_replay_result)
  }

  async fn update_system_session_id(
    &mut self,
    state_store: &bd_state::Store,
    session_id: &Arc<str>,
  ) {
    if self.last_session_id.as_ref() == Some(session_id) {
      return;
    }

    self.last_session_id = Some(session_id.clone());

    if let Err(e) = state_store
      .insert(
        Scope::System,
        SYSTEM_SESSION_ID_KEY.to_string(),
        string_value(session_id.to_string()),
      )
      .await
    {
      log::debug!("failed to persist sid in state store: {e}");
    }
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
    if !matches!(self.logging_state, LoggingState::Initialized(_)) {
      return;
    }

    let now = self.time_provider.now();

    for item in pre_config_buffer.pop_all() {
      match item {
        PreConfigItem::Log(log) => {
          self
            .update_system_session_id(state_store, &log.session_id)
            .await;
          let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state
          else {
            return;
          };
          if let Err(e) = self
            .replayer
            .replay_log(
              log,
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
            context,
          } => {
            let AdmissionContext {
              session_id,
              provider,
              admitted_at,
            } = context;
            let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state
            else {
              return;
            };
            initialized_logging_context
              .handle_state_insert(
                state_store,
                &self.metadata_collector,
                &mut self.global_state_tracker,
                &mut self.replayer,
                Scope::FeatureFlagExposure,
                name,
                variant.unwrap_or_default(),
                admitted_at,
                &session_id,
                provider,
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
        _ = self.event_buffer_limit_watches.log_limit_bytes.changed() => {
          self.refresh_event_buffer_limits();
        },
        _ = self.event_buffer_limit_watches.total_limit_bytes.changed() => {
          self.refresh_event_buffer_limits();
        },
        Some(config) = self.config_update_rx.recv() => {
          let (updated_self, maybe_pre_config_buffer)
            = self.update(config).await;

          self = updated_self;
          if let Some(pre_config_buffer) = maybe_pre_config_buffer {
            self.lifecycle_state.set(InitLifecycle::LogProcessingStarted);
            self.sdk_status_tracker.record_running();
            self
              .maybe_replay_pre_config_buffer(pre_config_buffer, &state_store)
              .await;
          }
        },
        Some(ReportProcessingRequest {
           session
        }) = self.report_processor_rx.recv() => {
          let reports = report_processor.process_all_pending_reports().await;
          self.admit_crash_reports(reports, &session);
        },
        // TODO(snowp): Benchmark batched reads. A batched implementation must cooperatively yield
        // between entries and return to this select! so Tokio and ALB's other branches progress.
        event_buffer_entries = self.event_buffer.next_batch(1) => {
          for entry in event_buffer_entries {
            match entry {
              EventBufferEntry::Ingress(event) => {
                let (context, payload, completion) = event.into_parts();
                match payload {
                  LoggerIngressPayload::Log(mut log) => {
                    for interceptor in &mut self.interceptors {
                      interceptor.process(
                        log.log_level,
                        log.log_type,
                        &log.message,
                        &mut log.fields,
                        &mut log.matching_fields,
                      );
                    }

                    if let Err(e) = self.process_all_logs(log, &state_store, Some(context)).await {
                      log::debug!("failed to process all logs: {e}");
                    }
                  },
                  LoggerIngressPayload::FeatureFlagExposure { flag, variant } => {
                    if let EventContext::CurrentProcess(context) = context {
                      self
                        .process_feature_flag_exposure(flag, variant, context, &state_store)
                        .await;
                    } else {
                      log::debug!("dropping feature flag exposure with previous-process context");
                    }
                  },
                }
                if let Some(completion) = completion {
                  completion.send(());
                }
              },
              EventBufferEntry::Control(async_log_buffer_message) => {
                self.process_control(async_log_buffer_message, &state_store).await;
              },
            }
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
        () = &mut local_shutdown => break,
        () = &mut self_shutdown => break,
      }
    }

    self.event_buffer.close();
    self
  }

  fn refresh_event_buffer_limits(&mut self) {
    // EventBuffer records the full pair under its admission mutex. The next producer admission
    // applies both values atomically and performs any required shrink eviction.
    self
      .event_buffer
      .set_pending_limits(self.event_buffer_limit_watches.read_mark_update());
  }

  fn admit_crash_reports(
    &self,
    reports: Vec<bd_crash_handler::CrashLog>,
    session: &crate::ReportProcessingSession,
  ) {
    // Report discovery can perform I/O, so it stays outside EventBuffer. Once reports have been
    // parsed, place the complete group in one admission order without interleaving other ingress.
    let entries = reports
      .into_iter()
      .filter_map(
        |crash_log| match self.crash_report_entry(crash_log, session) {
          Ok(entry) => Some(entry),
          Err(error) => {
            log::debug!("failed to capture crash report admission context: {error}");
            None
          },
        },
      )
      .collect::<Vec<_>>();

    for outcome in self.event_buffer.admit_batch(entries) {
      if outcome != AdmissionOutcome::Admitted {
        log::debug!("failed to admit crash report: {outcome:?}");
      }
    }
  }

  fn crash_report_entry(
    &self,
    crash_log: bd_crash_handler::CrashLog,
    session: &crate::ReportProcessingSession,
  ) -> anyhow::Result<EventBufferEntry> {
    let attributes_overrides = match session {
      crate::ReportProcessingSession::Current => {
        LogAttributesOverrides::OccurredAt(crash_log.timestamp)
      },
      crate::ReportProcessingSession::PreviousRun => {
        LogAttributesOverrides::PreviousRunSessionID(crash_log.timestamp)
      },
    }
    .into();
    let log = LogLine {
      log_type: LogType::LIFECYCLE,
      log_level: crash_log.log_level,
      message: crash_log.message,
      fields: crash_log.fields,
      matching_fields: [].into(),
      attributes_overrides,
      capture_session: Some("crash_handler"),
    };
    let context = admission_context(
      log.attributes_overrides.as_ref(),
      &self.metadata_provider,
      &self.session_strategy,
    )?;

    Ok(EventBufferEntry::ingress(LoggerIngressEvent::log(
      log, context, None,
    )))
  }

  async fn process_control(
    &mut self,
    async_log_buffer_message: LoggerControl,
    state_store: &bd_state::Store,
  ) {
    match async_log_buffer_message {
      LoggerControl::AddLogField(key, value) => {
        if let Err(e) = self.metadata_collector.add_field(key.clone().into(), value) {
          log::warn!("failed to add log field ({key:?}): {e}");
        }
      },
      LoggerControl::UpdateOotbLogField(key, value) => {
        self.metadata_collector.update_ootb_field(key.into(), value);
      },
      LoggerControl::RemoveLogField(field_name) => {
        self.metadata_collector.remove_field(field_name.into());
      },
      LoggerControl::SetMemoryPressureLevel { level } => {
        if let Err(e) = state_store
          .insert(
            Scope::System,
            MEMORY_PRESSURE_LEVEL_KEY.to_string(),
            string_value(level.variant_name().unwrap_or("Unknown").to_string()),
          )
          .await
        {
          log::debug!("failed to persist memory pressure level: {e}");
        }
      },
      LoggerControl::SetEntityId(entity_id) => {
        let result = match entity_id {
          Some(entity_id) => state_store
            .insert(
              Scope::System,
              ENTITY_ID_KEY.to_string(),
              string_value(entity_id),
            )
            .await
            .map(|_| ()),
          None => state_store
            .remove(Scope::System, ENTITY_ID_KEY)
            .await
            .map(|_| ()),
        };

        if let Err(e) = result {
          log::debug!("failed to persist entity ID state: {e}");
        }
      },
      LoggerControl::FlushState(completion_tx) => {
        let flush_stats_trigger = self.logging_state.flush_stats_trigger().clone();
        let flush_stats = async move {
          let completion = match flush_stats_trigger.flush() {
            Ok(completion) => completion,
            Err(e) => {
              log::debug!("flushing state: failed to flush stats: {e}");
              return;
            },
          };

          if let Err(e) = completion.wait().await {
            log::debug!("flushing state: failed to flush stats: {e}");
          }
        };

        let flush_buffers_trigger = self.logging_state.flush_buffers_trigger().clone();
        let flush_buffers = async move {
          let (sender, receiver) = bd_completion::Sender::new();
          let buffers_with_ack = BuffersWithAck::new_all_buffers(Some(sender));
          if let Err(e) = flush_buffers_trigger.send(buffers_with_ack).await {
            log::debug!("flushing state: failed to flush buffers: {e}");
          }

          if let Err(e) = receiver.recv().await {
            log::debug!("flushing state: failed to wait for buffers flush: {e}");
          }
        };

        let flush_session = self.session_strategy.flush();

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
  }

  async fn process_feature_flag_exposure(
    &mut self,
    flag: String,
    variant: Option<String>,
    context: AdmissionContext,
    state_store: &bd_state::Store,
  ) {
    let AdmissionContext {
      session_id,
      provider,
      admitted_at,
    } = context;
    if let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state {
      // Initialized: update state store and replay through workflows.
      initialized_logging_context
        .handle_state_insert(
          state_store,
          &self.metadata_collector,
          &mut self.global_state_tracker,
          &mut self.replayer,
          Scope::FeatureFlagExposure,
          flag,
          variant.unwrap_or_default(),
          admitted_at,
          &session_id,
          provider,
        )
        .await;
    } else if let LoggingState::Uninitialized(uninitialized_logging_context) =
      &mut self.logging_state
    {
      // Not initialized: queue the operation for later replay.
      let result =
        uninitialized_logging_context
          .pre_config_log_buffer
          .push(PreConfigItem::StateOperation(
            PendingStateOperation::SetFeatureFlagExposure {
              name: flag,
              variant,
              context: AdmissionContext {
                session_id,
                provider,
                admitted_at,
              },
            },
          ));
      uninitialized_logging_context
        .stats
        .pre_config_log_buffer
        .record(&result);
      if let Err(e) = result {
        log::debug!("failed to enqueue state operation to pre-config buffer: {e}");
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
