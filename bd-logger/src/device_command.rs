// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Shared execution primitives for direct device commands and workflow command matchers.
//!
//! # Direct commands
//! Fresh server configuration admits an invocation through `DeviceCommandState`, which owns
//! live-SDK deduplication and update tracking. An admitted worker registers its session owner and
//! emits a span start. Valid requests wait for the backend's Accepted acknowledgement before
//! invoking a handler, capturing a screenshot, or starting a prepared buffer upload. The span
//! includes those waits and result uploading, but ends when the local terminal outcome is chosen,
//! before the final update acknowledgement. Reconnects and configuration replacement do not
//! restart or cancel admitted execution; cached configuration must not execute commands after
//! SDK restart.
//!
//! # Workflow commands
//! The engine parks a traversal behind a private completion token and asks
//! [`WorkflowCommandDispatcher`] to execute its selector. The logger emits the start span and
//! receives a candidate outcome, including durable attachment admission when applicable.
//! Completing the handler does not itself advance the workflow: the logging loop must accept
//! the token, replay its end span, and match the optional outcome predicate before the command
//! transition's configured actions run. Workflow attachments upload later, unlike direct results.
//!
//! # Cancellation boundaries
//! Session changes become effective when ordinary log/state ingress is processed, not when an
//! application thread resets the session strategy. [`DeviceCommandRegistry`] cancels unfinished
//! direct executions at that boundary; the engine invalidates pending workflow commands when it
//! observes the new session or resets their owning traversals. A normal terminal outcome already
//! accepted wins over later cancellation, including while its transport acknowledgement is pending.
//! Cancellation emits a failed ERROR end with an explicit reason, retaining the original span and
//! session. Command telemetry never moves the live logging or workflow session back to that owner.
//!
//! # Cancellation limits and teardown
//! Cancellation drops the executor future; native registered-handler bridges use that drop to
//! request cooperative cancellation. [`CommandExecutionPolicy`] also drops timed-out handler or
//! screenshot futures, but reports an ordinary timeout failure rather than a session/reset
//! cancellation. Neither path rolls back screenshot, flush, or upload work already handed off.
//! Span logging is best-effort: SDK teardown may leave a start without an end. On restart,
//! persisted pending workflow traversals are cancelled, not re-executed, while sibling progress is
//! retained.

#[cfg(test)]
#[path = "./device_command_test.rs"]
mod tests;

use crate::async_log_buffer::{LogAdmission, Sender as LogSender};
use crate::profiling::{ProfilingHandler, StackSampleError, StackSampleOutcome};
use crate::workflow_attachment::AttachmentStoreHandle;
use anyhow::anyhow;
use bd_api::upload::{DeviceCommandState, TrackedDeviceCommandUpdate};
use bd_api::{DataUpload, TriggerUpload, TriggerUploadCompletion};
use bd_artifact_upload::UploadSource;
use bd_log_primitives::command_attachment::{AttachmentSource, LogAttachment};
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  LogAttributesOverrides,
  LogFields,
  LogLine,
  SPAN_TYPE_END,
  SPAN_TYPE_START,
  log_level,
};
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
use bd_proto::protos::client::api::device_command_update::completed::{Attachment, attachment};
use bd_proto::protos::client::api::{
  DeviceCommandResultContext,
  DeviceCommandUpdate,
  device_command_update,
};
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{Data, LogType};
use bd_proto::protos::workflow::workflow_command::{
  WellKnownCommandType,
  WorkflowCommandSelector,
  workflow_command_selector,
};
use bd_runtime::runtime::device_command::ExecutionTimeoutFlag;
use bd_runtime::runtime::device_command::stack_sampling::{
  ExecutionTimeoutFlag as StackSamplingTimeoutFlag,
  MaxDurationFlag as StackSamplingMaxDurationFlag,
};
use bd_runtime::runtime::{ConfigLoader, DurationWatch};
use bd_time::{TimeDurationExt, TimeProvider};
use bd_workflows::workflow::{
  COMMAND_OUTCOME_MESSAGE,
  COMMAND_START_MESSAGE,
  CommandArtifactMetadata,
  CommandCancellationReason,
  CommandOutcome,
  CommandSpan,
  WorkflowCommandCompletionToken,
  WorkflowCommandOutcome,
  WorkflowCommandRequest,
};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use protobuf::Message;
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

const MAX_SCREENSHOT_BYTES: usize = 2 * 1024 * 1024;
const REGISTERED_COMMAND_ARTIFACT_TYPE_ID: &str = "device_command_attachment";
const STACK_SAMPLING_ARTIFACT_TYPE_ID: &str = "stack_sampling_trace";
const STACK_SAMPLING_CONTENT_TYPE: &str = "application/vnd.bitdrift.perfetto-trace";
const STACK_SAMPLING_DEFAULT_DURATION: time::Duration = time::Duration::milliseconds(3000);
const STACK_SAMPLING_MIN_DURATION: time::Duration = time::Duration::milliseconds(1000);
/// Measured platform cost of starting the sampler and post-processing its output. The sampling
/// window must leave this much room inside the command's execution timeout.
const STACK_SAMPLING_OVERHEAD: time::Duration = time::Duration::milliseconds(2500);
const STACK_SAMPLING_DEFAULT_FREQUENCY_HZ: u32 = 100;
const STACK_SAMPLING_FREQUENCY_HZ: RangeInclusive<u32> = 10 ..= 250;
const STACK_SAMPLING_DEFAULT_BUFFER_KB: u32 = 2048;
const STACK_SAMPLING_BUFFER_KB: RangeInclusive<u32> = 256 ..= 16384;

//
// TestHooks
//

trait TestHooks: Send + Sync {
  fn admission_sleep_started(&self) {}

  fn admission_sleep_finished(&self) {}
}

//
// DeviceCommandDispatcher
//

/// Owns direct-command admission and execution tasks across configuration and mux lifetimes.
pub struct DeviceCommandDispatcher {
  senders: DeviceCommandSenders,
  trigger_upload_tx: Sender<TriggerUpload>,
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  profiling_handler: ProfilingHandler,
  command_state: Arc<DeviceCommandState>,
  // At most one waiter for capacity, replaced by the next fresh configuration.
  admission_task: Arc<Mutex<Option<JoinHandle<()>>>>,
  test_hooks: Option<Arc<dyn TestHooks>>,
  // Execution belongs to the SDK, not to a configuration or mux stream. None fences a racing
  // admission task after Drop has begun; dropping a JoinHandle alone would detach execution.
  execution_tasks: Arc<Mutex<Option<Vec<JoinHandle<()>>>>>,
  time_provider: Arc<dyn TimeProvider>,
}

impl Drop for DeviceCommandDispatcher {
  fn drop(&mut self) {
    if let Some(task) = self.admission_task.lock().take() {
      task.abort();
    }
    if let Some(tasks) = self.execution_tasks.lock().take() {
      for task in tasks {
        task.abort();
      }
    }
  }
}

//
// DeviceCommandSenders
//

#[derive(Clone)]
struct DeviceCommandSenders {
  data_upload_tx: Sender<DataUpload>,
  log_sender: LogSender,
  command_state: Arc<DeviceCommandState>,
}

//
// DeviceCommandRegistry
//

/// Tracks direct executions at the logging layer's processed-session boundary, not at the
/// application thread's session-strategy reset. Weak entries do not keep SDK execution alive.
#[derive(Clone, Default)]
pub struct DeviceCommandRegistry {
  state: Arc<Mutex<DeviceCommandRegistryState>>,
}

#[derive(Default)]
struct DeviceCommandRegistryState {
  // Before the logger observes its first session, registration falls back to the strategy's ID.
  session_id: Option<Arc<str>>,
  executions: Vec<(Weak<DeviceCommandExecution>, oneshot::Sender<()>)>,
}

impl DeviceCommandRegistry {
  fn register(
    &self,
    mut execution: DeviceCommandExecution,
  ) -> (Arc<DeviceCommandExecution>, oneshot::Receiver<()>) {
    let mut state = self.state.lock();
    if let Some(session_id) = &state.session_id {
      // Registration and processed-session changes share this lock. A task starting after the
      // boundary cannot register an old owner captured earlier from the session strategy.
      execution.session_id = Ok(session_id.clone());
    }
    state.executions.retain(|(execution, _)| {
      execution
        .upgrade()
        .is_some_and(|execution| execution.is_running())
    });
    let execution = Arc::new(execution);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    state
      .executions
      .push((Arc::downgrade(&execution), cancel_tx));
    (execution, cancel_rx)
  }

  /// Called only for a session observed by the logging layer, never directly by a strategy reset.
  pub(crate) fn update_session(&self, session_id: &Arc<str>) {
    let mut state = self.state.lock();
    if state.session_id.as_ref() == Some(session_id) {
      return;
    }
    state.session_id = Some(session_id.clone());
    let mut cancelled = Vec::new();
    for (weak, cancel_tx) in std::mem::take(&mut state.executions) {
      let Some(execution) = weak.upgrade() else {
        continue;
      };
      if execution
        .session_id
        .as_ref()
        .is_ok_and(|owner| owner == session_id)
      {
        state.executions.push((weak, cancel_tx));
      } else if execution.cancel() {
        cancelled.push(cancel_tx);
      }
    }
    drop(state);
    // The terminal decision is already made. Wake workers outside the registry lock; they drop
    // platform work and emit the canceled end only after their start log has been submitted.
    for cancel_tx in cancelled {
      let _ = cancel_tx.send(());
    }
  }
}

//
// DeviceCommandExecution
//

#[derive(PartialEq, Eq)]
enum DeviceCommandStatus {
  /// Includes Accepted-ACK, execution, and result-upload waits, all of which can be cancelled.
  Running,
  /// The local terminal outcome won, independently of end-log admission or final acknowledgement.
  Finished,
  /// The registry reserved cancellation; the worker still owns dropping work and emitting the end.
  Cancelled,
}

struct DeviceCommandExecution {
  senders: DeviceCommandSenders,
  span: CommandSpan,
  session_id: anyhow::Result<Arc<str>>,
  time_provider: Arc<dyn TimeProvider>,
  status: Mutex<DeviceCommandStatus>,
  // Cancellation uses the sequence after the last update handed to the API, even if not yet ACKed.
  last_update_sequence: AtomicU64,
}

impl DeviceCommandExecution {
  fn is_running(&self) -> bool {
    *self.status.lock() == DeviceCommandStatus::Running
  }

  fn is_cancelled(&self) -> bool {
    *self.status.lock() == DeviceCommandStatus::Cancelled
  }

  fn cancel(&self) -> bool {
    let mut status = self.status.lock();
    if *status != DeviceCommandStatus::Running {
      return false;
    }
    *status = DeviceCommandStatus::Cancelled;
    true
  }

  fn emit(&self, log: LogLine) {
    let result = match &self.session_id {
      Ok(session_id) => self.senders.log_sender.try_send_log(
        log,
        LogAdmission::Command {
          session_id: session_id.clone(),
          cancelled: self.is_cancelled(),
        },
      ),
      Err(error) => {
        log::debug!("failed to capture device command span session: {error}");
        return;
      },
    };
    if let Err(error) = result {
      log::debug!("failed to admit device command span log: {error}");
    }
  }

  fn start(&self, now: OffsetDateTime) {
    self.emit(LogLine {
      log_level: log_level::INFO,
      log_type: LogType::SPAN,
      message: COMMAND_START_MESSAGE.into(),
      fields: self
        .span
        .fields(SPAN_TYPE_START, now)
        .into_iter()
        .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
        .collect(),
      matching_fields: AnnotatedLogFields::default(),
      command_attachment: None,
      attributes_overrides: Some(LogAttributesOverrides::OccurredAt(now)),
      capture_session: None,
    });
  }

  /// Reserves a normal terminal end before transport submission. Accepted updates have no end log
  /// and leave the terminal state unchanged; duplicate or cancelled terminal attempts return false.
  fn finish(
    &self,
    update: &DeviceCommandUpdate,
    artifact_metadata: Option<CommandArtifactMetadata>,
  ) -> bool {
    let Some(log) = device_command_outcome_log(
      update,
      artifact_metadata,
      &self.span,
      self.time_provider.now(),
    ) else {
      return true;
    };
    {
      // Only this direct execution's terminal outcome competes with processed-session
      // cancellation. Workflow completions are serialized entirely by the logging loop.
      let mut status = self.status.lock();
      if *status != DeviceCommandStatus::Running {
        return false;
      }
      *status = DeviceCommandStatus::Finished;
    }
    self.emit(log);
    true
  }

  // Called once by the worker after the registry wins cancellation and the work future is dropped.
  // Keeping emission here preserves start/end order even if reset raced initial registration.
  fn cancellation_update(&self, command_id: &str) -> DeviceCommandUpdate {
    let reason = CommandCancellationReason::SessionChanged;
    if let Ok(session_id) = &self.session_id {
      let log = reason.into_log(session_id.to_string(), self.time_provider.now(), &self.span);
      self.emit(LogLine {
        log_level: log.log_level,
        log_type: log.log_type,
        message: log.message,
        fields: log
          .fields
          .into_iter()
          .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
          .collect(),
        matching_fields: AnnotatedLogFields::default(),
        command_attachment: None,
        attributes_overrides: Some(LogAttributesOverrides::OccurredAt(log.occurred_at)),
        capture_session: None,
      });
    }
    log::debug!("device command {command_id} cancelled because its session ended");
    failed_device_command_update(
      command_id,
      self.last_update_sequence.load(Ordering::Relaxed) + 1,
      CommandError::Other(reason.message().to_string()),
    )
  }
}

impl DeviceCommandDispatcher {
  pub fn new(
    data_upload_tx: Sender<DataUpload>,
    log_sender: LogSender,
    trigger_upload_tx: Sender<TriggerUpload>,
    session_strategy: Arc<bd_session::Strategy>,
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    command_dispatcher: RegisteredCommandDispatcher,
    execution_policy: CommandExecutionPolicy,
    remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
    profiling_handler: ProfilingHandler,
    command_state: Arc<DeviceCommandState>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
    Self {
      senders: DeviceCommandSenders {
        data_upload_tx,
        log_sender,
        command_state: command_state.clone(),
      },
      trigger_upload_tx,
      session_strategy,
      artifact_client,
      command_dispatcher,
      execution_policy,
      remote_screenshot_capture_handler,
      profiling_handler,
      command_state,
      admission_task: Arc::default(),
      test_hooks: None,
      execution_tasks: Arc::new(Mutex::new(Some(vec![]))),
      time_provider,
    }
  }

  /// Applies only a fresh server dispatch, never a cached configuration. Replacing the deferred
  /// candidates does not cancel admitted execution or release its deduplication tombstone.
  pub fn dispatch_configuration(&self, commands: Vec<DeviceCommandRequest>) {
    let mut admission_task = self.admission_task.lock();
    if let Some(task) = admission_task.take() {
      task.abort();
    }
    let mut deferred = vec![];
    let mut deferred_bytes = 0;
    for command in commands {
      if self.command_state.admit(&command) {
        // Claim the ID before spawning, so reconnects and repeated configurations cannot start
        // a second handler while this execution is awaiting its Accepted ACK or doing work.
        Self::spawn_execution(&self.execution_tasks, self.execution(command));
      } else if self.command_state.admission_retry_delay(&command).is_some()
        && deferred.len() < 64
        && deferred_bytes + command.compute_size() <= 4 * 1024 * 1024
      {
        // Only capacity rejection is retryable. Bound both retained candidates and payload bytes;
        // the handler future stays unpolled until the registry grants admission.
        deferred_bytes += command.compute_size();
        let execution = self.execution(command.clone());
        deferred.push((command, execution));
      }
    }
    if deferred.is_empty() {
      return;
    }
    let command_state = self.command_state.clone();
    let execution_tasks = self.execution_tasks.clone();
    let test_hooks = self.test_hooks.clone();
    let current_admission = self.admission_task.clone();
    // Configuration processing must return without waiting for tombstone expiry. One owned task
    // waits for all deferred candidates, rather than spawning a timer per rejected command.
    *admission_task = Some(tokio::spawn(async move {
      while let Some(delay) = deferred
        .iter()
        .filter_map(|(command, _)| command_state.admission_retry_delay(command))
        .min()
      {
        log::debug!(
          "device command admission deferred for {delay:?}; pending={}",
          deferred.len()
        );
        let sleep = tokio::time::sleep(delay);
        if let Some(hooks) = &test_hooks {
          hooks.admission_sleep_started();
        }
        sleep.await;
        if let Some(hooks) = &test_hooks {
          hooks.admission_sleep_finished();
        }
        // Abort is cooperative. Serialize the whole admission pass with replacement/teardown,
        // and reject an old task that was already running when its handle was replaced.
        let current_admission = current_admission.lock();
        if current_admission
          .as_ref()
          .is_none_or(|task| task.id() != tokio::task::id())
        {
          return;
        }
        let pending = std::mem::take(&mut deferred);
        for (command, execution) in pending {
          if command_state.admit(&command) {
            Self::spawn_execution(&execution_tasks, execution);
          } else if command_state.admission_retry_delay(&command).is_some() {
            deferred.push((command, execution));
          }
        }
      }
    }));
  }

  fn spawn_execution(
    tasks: &Mutex<Option<Vec<JoinHandle<()>>>>,
    execution: BoxFuture<'static, ()>,
  ) {
    let mut tasks = tasks.lock();
    // Serialize starting execution with SDK teardown. An aborted admission task may already be
    // running on another worker; it must not detach a new handler after Drop drains this list.
    if let Some(tasks) = tasks.as_mut() {
      tasks.retain(|task| !task.is_finished());
      tasks.push(tokio::spawn(execution));
    }
  }

  fn execution(&self, command: DeviceCommandRequest) -> BoxFuture<'static, ()> {
    // Capture dependencies, not the dispatcher itself: SDK destruction must be able to reach
    // Drop and abort these tasks, including handlers waiting for an ACK on a disconnected mux.
    let command_id = command.command_id.to_string();
    let senders = self.senders.clone();
    let trigger_upload_tx = self.trigger_upload_tx.clone();
    let session_strategy = self.session_strategy.clone();
    let artifact_client = self.artifact_client.clone();
    let command_dispatcher = self.command_dispatcher.clone();
    let execution_policy = self.execution_policy.clone();
    let remote_screenshot_capture_handler = self.remote_screenshot_capture_handler.clone();
    let profiling_handler = self.profiling_handler.clone();
    let time_provider = self.time_provider.clone();
    Box::pin(async move {
      let started_at = time_provider.now();
      let (execution, cancel_rx) =
        senders
          .log_sender
          .device_commands()
          .register(DeviceCommandExecution {
            senders: senders.clone(),
            span: CommandSpan {
              id: command_id.clone(),
              started_at: Some(started_at),
            },
            session_id: session_strategy.session_id(),
            time_provider,
            status: Mutex::new(DeviceCommandStatus::Running),
            last_update_sequence: AtomicU64::new(0),
          });
      execution.start(started_at);
      let mut work = Box::pin(execute_device_command(
        command,
        execution.clone(),
        trigger_upload_tx,
        artifact_client,
        command_dispatcher,
        execution_policy,
        remote_screenshot_capture_handler,
        profiling_handler,
      ));
      let result = {
        // A closed sender is not cancellation: pruning a finished registry entry also closes it.
        // Only an explicit signal may interrupt work, including a pending Accepted-ACK wait.
        tokio::select! {
          biased;
          Ok(()) = cancel_rx => Ok(()),
          result = &mut work => result,
        }
      };
      // Drop the handler/adapter before emitting cancellation or waiting on its backend update.
      // This requests platform cancellation without waiting for cooperative platform work to stop.
      drop(work);
      if let Err(error) = result {
        // Delivery can fail after a terminal end was already emitted. finish then does nothing,
        // rather than replacing that outcome or producing a second end.
        execution.finish(
          &failed_device_command_update(&command_id, 1, CommandError::Other(error.to_string())),
          None,
        );
        log::debug!("device command {command_id} execution stopped: {error}");
      }
      if execution.is_cancelled() {
        let update = execution.cancellation_update(&command_id);
        if let Err(error) = submit_device_command_update(&execution, update).await {
          log::debug!("failed to submit cancelled device command {command_id}: {error}");
        }
      }
    })
  }
}

/// An invocation of an application-defined command.
#[derive(Debug, Clone)]
pub struct CommandInvocation {
  /// Present only when the command was directly dispatched to the device.
  pub command_id: Option<Uuid>,
  pub registered_command_id: String,
  pub arguments: HashMap<String, Data>,
  pub session_id: String,
}

/// A locally produced command attachment.
#[derive(Debug)]
pub struct CommandAttachment {
  pub source: UploadSource,
  pub content_type: Option<String>,
}

/// The reason a command failed. Detailed messages retain their existing wire/log representation.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CommandError {
  /// The execution deadline expired. Platform work may finish after its Rust future is cancelled.
  #[error("command timed out")]
  Timeout,
  #[error("command unknown")]
  CommandUnknown,
  /// Returned by the platform when its command concurrency limit is reached.
  #[error("max command concurrency reached")]
  MaxCommandConcurrency,
  /// Returned by the platform when another invocation of the same command is still active.
  #[error("command already executing")]
  AlreadyExecuting,
  /// Returned by the platform when it cannot parse an invocation's arguments.
  #[error("{0}")]
  InvalidArguments(String),
  #[error("{0}")]
  HandlerFailed(String),
  #[error("{0}")]
  Other(String),
}

impl CommandError {
  fn into_message(self) -> String {
    match self {
      Self::InvalidArguments(message) | Self::HandlerFailed(message) | Self::Other(message) => {
        message
      },
      Self::Timeout
      | Self::CommandUnknown
      | Self::MaxCommandConcurrency
      | Self::AlreadyExecuting => self.to_string(),
    }
  }
}

/// The terminal outcome of an application-defined command.
#[derive(Debug)]
pub enum CommandResult {
  Completed {
    fields: LogFields,
    attachment: Option<CommandAttachment>,
  },
  Failed {
    error: CommandError,
    fields: LogFields,
  },
}

/// Handles an opaque command selected by its registered command ID.
/// Direct and workflow invocations have a runtime-configurable execution timeout, defaulting to
/// five seconds. Handlers must yield to allow deadlines and cancellation to be observed.
/// The future can be dropped on timeout, session cancellation, workflow invalidation, or teardown;
/// native bridges request platform cancellation, but already-started external effects may continue.
#[async_trait::async_trait]
pub trait RegisteredCommandHandler: Send + Sync {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult;
}

/// Bounds handler/capture execution, not Accepted-ACK, attachment, or terminal-delivery waits.
#[derive(Clone)]
pub struct CommandExecutionPolicy {
  timeout: DurationWatch<ExecutionTimeoutFlag>,
  stack_sampling_timeout: DurationWatch<StackSamplingTimeoutFlag>,
  stack_sampling_max_duration: DurationWatch<StackSamplingMaxDurationFlag>,
}

impl CommandExecutionPolicy {
  pub(crate) fn new(runtime: &ConfigLoader) -> Self {
    Self {
      timeout: runtime.register_duration_watch(),
      stack_sampling_timeout: runtime.register_duration_watch(),
      stack_sampling_max_duration: runtime.register_duration_watch(),
    }
  }

  async fn execute<F, T>(&self, command: &str, execution: F) -> Result<T, CommandError>
  where
    F: Future<Output = Result<T, CommandError>>,
  {
    let timeout = *self.timeout.read();
    self
      .execute_within(command, timeout, execution)
      .await
      .and_then(|result| result)
  }

  async fn execute_within<F, T>(
    &self,
    command: &str,
    timeout: time::Duration,
    execution: F,
  ) -> Result<T, CommandError>
  where
    F: Future<Output = T>,
  {
    if timeout.is_zero() {
      return Err(CommandError::Timeout);
    }
    log::debug!("command {command} execution started with timeout {timeout}");
    match timeout
      .timeout(AssertUnwindSafe(execution).catch_unwind())
      .await
    {
      Ok(Ok(result)) => Ok(result),
      Ok(Err(_)) => Err(CommandError::Other("command handler panicked".into())),
      Err(_) => {
        log::debug!("command {command} execution timed out after {timeout}");
        Err(CommandError::Timeout)
      },
    }
  }

  /// The longest sampling window a stack sampling command may run: the configured maximum, and
  /// never more than its execution timeout leaves after the platform's fixed overhead.
  pub(crate) fn stack_sampling_limits(&self) -> StackSamplingLimits {
    let timeout = *self.stack_sampling_timeout.read();
    StackSamplingLimits {
      max_duration: (*self.stack_sampling_max_duration.read())
        .min(timeout - STACK_SAMPLING_OVERHEAD),
    }
  }

  /// Dropping the returned future on timeout drops the capture future, which cancels the platform
  /// capture.
  async fn capture_stack_sample(
    &self,
    handler: ProfilingHandler,
    params: StackSamplingParams,
  ) -> Result<StackSampleOutcome, StackSamplingFailure> {
    let timeout = *self.stack_sampling_timeout.read();
    match self
      .execute_within(
        STACK_SAMPLING_ARTIFACT_TYPE_ID,
        timeout,
        handler.capture_stack_sample(params.duration, params.frequency_hz, params.buffer_size_kb),
      )
      .await
    {
      Ok(Ok(outcome)) => Ok(outcome),
      Ok(Err(error)) => Err(StackSamplingFailure::Platform(error)),
      Err(error) => Err(StackSamplingFailure::Command(error)),
    }
  }

  async fn execute_registered(
    &self,
    handler: Arc<dyn RegisteredCommandHandler>,
    invocation: CommandInvocation,
  ) -> CommandResult {
    let registered_command_id = invocation.registered_command_id.clone();
    match self
      .execute(&registered_command_id, async move {
        Ok(handler.execute(invocation).await)
      })
      .await
    {
      Ok(result) => result,
      Err(error) => CommandResult::Failed {
        error,
        fields: LogFields::default(),
      },
    }
  }

  async fn capture_screenshot(
    &self,
    handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  ) -> Result<Vec<u8>, CommandError> {
    self
      .execute("screenshot", async move {
        handler.capture().await.map_err(CommandError::HandlerFailed)
      })
      .await
  }
}

/// Registry of handler definitions, not live invocations. Lookup clones the handler, so replacing
/// or unregistering a definition does not revoke work that already obtained it.
#[derive(Clone, Default)]
pub struct RegisteredCommandDispatcher {
  handlers: Arc<RwLock<HashMap<String, Arc<dyn RegisteredCommandHandler>>>>,
}

impl RegisteredCommandDispatcher {
  pub fn new(handlers: HashMap<String, Arc<dyn RegisteredCommandHandler>>) -> Self {
    Self {
      handlers: Arc::new(RwLock::new(handlers)),
    }
  }

  pub fn register(
    &self,
    registered_command_id: String,
    handler: Arc<dyn RegisteredCommandHandler>,
  ) {
    log::debug!("registered command handler: {registered_command_id}");
    let previous_handler = {
      let mut handlers = self.handlers.write();
      handlers.insert(registered_command_id, handler)
    };
    drop(previous_handler);
  }

  pub fn unregister(&self, registered_command_id: &str) -> bool {
    let removed_handler = {
      let mut handlers = self.handlers.write();
      handlers.remove(registered_command_id)
    };
    let removed = removed_handler.is_some();
    drop(removed_handler);
    if removed {
      log::debug!("unregistered command handler: {registered_command_id}");
    }
    removed
  }

  pub fn get_handler(
    &self,
    registered_command_id: &str,
  ) -> Option<Arc<dyn RegisteredCommandHandler>> {
    self.handlers.read().get(registered_command_id).cloned()
  }
}

/// A candidate result, not authority to advance a traversal. Its token may have been invalidated
/// while execution, attachment admission, or delivery to the logging loop was in progress.
pub struct WorkflowCommandCompletion {
  pub token: WorkflowCommandCompletionToken,
  pub outcome: WorkflowCommandOutcome,
}

/// Runs selectors independently of workflow evaluation. The engine owns terminal decisions and
/// span identity; removing a dispatch token closes its sender and drops unfinished executor work.
#[derive(Clone)]
pub struct WorkflowCommandDispatcher {
  executor: WorkflowCommandExecutor,
  completion_tx: Sender<WorkflowCommandCompletion>,
  executions: Arc<Mutex<HashMap<WorkflowCommandCompletionToken, watch::Sender<()>>>>,
}

#[derive(Clone)]
struct WorkflowCommandExecutor {
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
  attachment_store: AttachmentStoreHandle,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  profiling_handler: ProfilingHandler,
}

impl WorkflowCommandDispatcher {
  pub fn new(
    command_dispatcher: RegisteredCommandDispatcher,
    execution_policy: CommandExecutionPolicy,
    completion_tx: Sender<WorkflowCommandCompletion>,
    attachment_store: AttachmentStoreHandle,
    remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
    profiling_handler: ProfilingHandler,
  ) -> Self {
    Self {
      executor: WorkflowCommandExecutor {
        command_dispatcher,
        execution_policy,
        attachment_store,
        remote_screenshot_capture_handler,
        profiling_handler,
      },
      completion_tx,
      executions: Arc::default(),
    }
  }

  pub fn dispatch(&self, request: &WorkflowCommandRequest) {
    let token = request.completion_token();
    let completion_tx = self.completion_tx.clone();
    let command_selector = request.command_selector.clone();
    let dispatcher = self.executor.clone();
    let attachment_store = self.executor.attachment_store.clone();
    let session_id = request.session_id.clone();
    let (cancel_tx, mut cancel_rx) = watch::channel(());
    // Publish cancellation ownership before spawning so a reset can stop even an unpolled task.
    self.executions.lock().insert(token.clone(), cancel_tx);

    tokio::task::spawn(async move {
      let cancellation = async move {
        let _ = cancel_rx.changed().await;
        // Engine invalidation already records the authoritative end and reason, including
        // SessionChanged. This fallback result is stale once that token has been removed.
        CommandCancellationReason::WorkflowReset
      };
      let outcome = tokio::select! {
        biased;
        reason = cancellation => WorkflowCommandOutcome::Cancelled { reason },
        outcome = dispatcher.execute(command_selector, session_id) => outcome,
      };
      if let Err(error) = completion_tx
        .send(WorkflowCommandCompletion { token, outcome })
        .await
      {
        // Until the logging loop consumes the candidate, it owns no persistent log reference.
        // If delivery fails, release admission here; stale delivered candidates are released there.
        if let WorkflowCommandOutcome::SucceededWithAttachment { artifact_id, .. } = error.0.outcome
        {
          match attachment_store.get().await {
            Ok(store) => {
              if let Err(error) = store.release(artifact_id).await {
                log::warn!("failed to release undelivered workflow attachment: {error}");
              }
            },
            Err(error) => log::warn!("workflow attachment store unavailable for release: {error}"),
          }
        }
        log::debug!("workflow command completion receiver dropped");
      }
    });
  }

  /// Releases the task's ownership slot. Also used after normal completion acceptance; removing
  /// the slot is not itself authority to emit a cancellation end or change the engine's decision.
  pub fn cancel(&self, token: &WorkflowCommandCompletionToken) {
    self.executions.lock().remove(token);
  }
}

impl WorkflowCommandExecutor {
  async fn execute(
    &self,
    command_selector: WorkflowCommandSelector,
    session_id: String,
  ) -> WorkflowCommandOutcome {
    let arguments = command_selector.arguments;
    match command_selector.command_selector {
      Some(workflow_command_selector::Command_selector::RegisteredCommand(command)) => {
        if let Some(handler) = self
          .command_dispatcher
          .get_handler(&command.registered_command_id)
        {
          let result = self
            .execution_policy
            .execute_registered(
              handler,
              CommandInvocation {
                command_id: None,
                registered_command_id: command.registered_command_id,
                arguments,
                session_id,
              },
            )
            .await;
          workflow_command_outcome(result, &self.attachment_store).await
        } else {
          workflow_command_failure(CommandError::CommandUnknown)
        }
      },
      Some(workflow_command_selector::Command_selector::BuiltinCommand(command)) => {
        workflow_builtin_command_outcome(
          command,
          &arguments,
          self.remote_screenshot_capture_handler.clone(),
          self.profiling_handler.clone(),
          &self.execution_policy,
          &self.attachment_store,
        )
        .await
      },
      None => workflow_command_failure(CommandError::CommandUnknown),
    }
  }
}

async fn workflow_builtin_command_outcome(
  command: workflow_command_selector::BuiltinCommand,
  arguments: &HashMap<String, Data>,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  profiling_handler: ProfilingHandler,
  execution_policy: &CommandExecutionPolicy,
  attachment_store: &AttachmentStoreHandle,
) -> WorkflowCommandOutcome {
  match command.type_.enum_value() {
    Ok(WellKnownCommandType::TAKE_SCREENSHOT) if arguments.is_empty() => {
      match execution_policy
        .capture_screenshot(remote_screenshot_capture_handler)
        .await
      {
        Ok(screenshot) if let Err(error) = validate_screenshot(&screenshot) => {
          workflow_command_failure(CommandError::Other(error.into()))
        },
        Ok(screenshot) => {
          workflow_command_outcome(
            CommandResult::Completed {
              fields: LogFields::default(),
              attachment: Some(CommandAttachment {
                source: UploadSource::Bytes(screenshot),
                content_type: Some("image/jpeg".to_string()),
              }),
            },
            attachment_store,
          )
          .await
        },
        Err(error) => workflow_command_failure(error),
      }
    },
    Ok(WellKnownCommandType::STACK_SAMPLING_TRACE) => {
      let params = match stack_sampling_params(arguments, execution_policy.stack_sampling_limits())
      {
        Ok(params) => params,
        Err(error) => return workflow_command_failure(error),
      };
      match execution_policy
        .capture_stack_sample(profiling_handler, params)
        .await
      {
        Ok(outcome) => {
          let result = workflow_command_outcome(
            CommandResult::Completed {
              fields: params.fields(),
              attachment: Some(stack_sampling_attachment(&outcome)),
            },
            attachment_store,
          )
          .await;
          // The workflow store keeps its own copy of the trace.
          discard_trace_file(&outcome.path).await;
          result
        },
        Err(failure) => failure.into_workflow_outcome(),
      }
    },
    _ => workflow_command_failure(CommandError::CommandUnknown),
  }
}

fn workflow_command_failure(error: CommandError) -> WorkflowCommandOutcome {
  WorkflowCommandOutcome::Failed {
    message: Some(error.into_message()),
    fields: LogFields::default(),
  }
}

// A successful workflow result owns a durable local admission, not an uploaded artifact. The
// logger must either persist an outcome reference or release it if replay/token validation fails.
// Cancellation during admission is handled by the store's owned transaction/handoff protocol.
async fn workflow_command_outcome(
  result: CommandResult,
  attachment_store: &AttachmentStoreHandle,
) -> WorkflowCommandOutcome {
  match result {
    CommandResult::Completed { fields, attachment } => {
      if let Some(attachment) = attachment {
        let store = match attachment_store.get().await {
          Ok(store) => store,
          Err(error) => {
            let message = format!("workflow attachment store unavailable: {error}");
            log::warn!("{message}");
            return WorkflowCommandOutcome::Failed {
              message: Some(message),
              fields,
            };
          },
        };
        let admitted = match store
          .admit(attachment.source, attachment.content_type)
          .await
        {
          Ok(admitted) => admitted,
          Err(error) => {
            let message = format!("workflow attachment admission failed: {error}");
            log::warn!("{message}");
            return WorkflowCommandOutcome::Failed {
              message: Some(message),
              fields,
            };
          },
        };
        return WorkflowCommandOutcome::SucceededWithAttachment {
          message: None,
          fields,
          artifact_id: admitted.id,
          artifact_metadata: admitted.artifact_metadata,
        };
      }
      WorkflowCommandOutcome::Succeeded {
        message: None,
        fields,
      }
    },
    CommandResult::Failed { error, fields } => WorkflowCommandOutcome::Failed {
      message: Some(error.into_message()),
      fields,
    },
  }
}

async fn execute_device_command(
  command: DeviceCommandRequest,
  senders: Arc<DeviceCommandExecution>,
  trigger_upload_tx: Sender<TriggerUpload>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  profiling_handler: ProfilingHandler,
) -> anyhow::Result<()> {
  let command_id = command.command_id.to_string();
  let selector = command.command_selector.into_option().unwrap_or_default();
  let arguments = selector.arguments;
  match selector.command_selector {
    Some(workflow_command_selector::Command_selector::RegisteredCommand(command)) => {
      execute_custom_device_command(
        command_id,
        command.registered_command_id.clone(),
        arguments,
        senders,
        artifact_client,
        command_dispatcher,
        execution_policy,
      )
      .await
    },
    Some(workflow_command_selector::Command_selector::BuiltinCommand(command)) => {
      match command.type_.enum_value() {
        Ok(WellKnownCommandType::DUMP_DEVICE_BUFFER) if arguments.is_empty() => {
          execute_buffer_dump_device_command(command_id, senders, trigger_upload_tx).await
        },
        Ok(WellKnownCommandType::TAKE_SCREENSHOT) if arguments.is_empty() => {
          execute_screenshot_device_command(
            command_id,
            senders,
            artifact_client,
            execution_policy,
            remote_screenshot_capture_handler,
          )
          .await
        },
        Ok(WellKnownCommandType::STACK_SAMPLING_TRACE) => {
          execute_stack_sampling_device_command(
            command_id,
            &arguments,
            senders,
            artifact_client,
            execution_policy,
            profiling_handler,
          )
          .await
        },
        _ => {
          send_device_command_update(
            &senders,
            failed_device_command_update(&command_id, 1, CommandError::CommandUnknown),
          )
          .await
        },
      }
    },
    None => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 1, CommandError::CommandUnknown),
      )
      .await
    },
  }
}

async fn execute_screenshot_device_command(
  command_id: String,
  senders: Arc<DeviceCommandExecution>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  execution_policy: CommandExecutionPolicy,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
) -> anyhow::Result<()> {
  send_device_command_update(
    &senders,
    DeviceCommandUpdate {
      command_id: command_id.clone(),
      update_sequence_number: 1,
      update_type: Some(device_command_update::Update_type::Accepted(
        device_command_update::Accepted::default(),
      )),
      ..Default::default()
    },
  )
  .await?;

  let session_id = match &senders.session_id {
    Ok(session_id) => session_id.to_string(),
    Err(error) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 2, CommandError::Other(error.to_string())),
      )
      .await?;
      return Ok(());
    },
  };
  let screenshot = match execution_policy
    .capture_screenshot(remote_screenshot_capture_handler)
    .await
  {
    Ok(screenshot) if let Err(error) = validate_screenshot(&screenshot) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 2, CommandError::Other(error.into())),
      )
      .await?;
      return Ok(());
    },
    Ok(screenshot) => screenshot,
    Err(error) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 2, error),
      )
      .await?;
      return Ok(());
    },
  };

  let attachment = CommandAttachment {
    source: UploadSource::Bytes(screenshot),
    content_type: Some("image/jpeg".to_string()),
  };
  let update = match stage_device_command_attachment(
    attachment,
    "screenshot",
    &command_id,
    &session_id,
    artifact_client,
  )
  .await
  {
    Ok((artifact_id, metadata)) => {
      return send_device_command_update_with_artifact(
        &senders,
        completed_device_command_update(&command_id, false, None, artifact_attachment(artifact_id)),
        Some(metadata),
      )
      .await;
    },
    Err(error) => {
      failed_device_command_update(&command_id, 2, CommandError::Other(error.to_string()))
    },
  };
  send_device_command_update(&senders, update).await
}

//
// Stack sampling
//

/// A stack sampling command's arguments after defaults and clamping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StackSamplingParams {
  duration: time::Duration,
  frequency_hz: u32,
  buffer_size_kb: u32,
}

impl StackSamplingParams {
  /// The result context: what actually ran, so the backend can tell clamped from requested.
  fn fields(&self) -> LogFields {
    [
      ("profiling_type".into(), "STACK_SAMPLING".to_string().into()),
      (
        "duration_ms".into(),
        self.duration.whole_milliseconds().to_string().into(),
      ),
      ("frequency_hz".into(), self.frequency_hz.to_string().into()),
      (
        "buffer_size_kb".into(),
        self.buffer_size_kb.to_string().into(),
      ),
    ]
    .into()
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StackSamplingLimits {
  max_duration: time::Duration,
}

/// Why a stack sampling capture produced no trace: the platform profiler failed, with its own
/// error code and name, or the command itself did (timeout, panic).
#[derive(Debug)]
enum StackSamplingFailure {
  Platform(StackSampleError),
  Command(CommandError),
}

impl StackSamplingFailure {
  fn into_fields(self) -> LogFields {
    match self {
      Self::Platform(error) => [
        ("error".into(), error.message.into()),
        ("profiling_error_code".into(), error.code.to_string().into()),
        ("profiling_error_name".into(), error.name.into()),
      ]
      .into(),
      Self::Command(error) => [("error".into(), error.into_message().into())].into(),
    }
  }

  fn into_workflow_outcome(self) -> WorkflowCommandOutcome {
    match self {
      Self::Platform(error) => WorkflowCommandOutcome::Failed {
        message: Some(error.message),
        fields: [
          ("profiling_error_code".into(), error.code.to_string().into()),
          ("profiling_error_name".into(), error.name.into()),
        ]
        .into(),
      },
      Self::Command(error) => workflow_command_failure(error),
    }
  }
}

fn optional_integer_argument(
  arguments: &HashMap<String, Data>,
  name: &str,
) -> Result<Option<i64>, CommandError> {
  let Some(data) = arguments.get(name) else {
    return Ok(None);
  };
  match &data.data_type {
    Some(Data_type::SintData(value)) => Ok(Some(*value)),
    Some(Data_type::IntData(value)) => i64::try_from(*value)
      .map(Some)
      .map_err(|_| CommandError::InvalidArguments(format!("argument {name} is out of range"))),
    _ => Err(CommandError::InvalidArguments(format!(
      "argument {name} must be an integer"
    ))),
  }
}

fn clamped_u32(value: Option<i64>, default: u32, range: &RangeInclusive<u32>) -> u32 {
  value.map_or(default, |value| {
    u32::try_from(value.clamp(i64::from(*range.start()), i64::from(*range.end())))
      .unwrap_or(default)
  })
}

/// Applies defaults and clamps. Fails only when the arguments are not integers or the limits
/// leave no usable sampling window at all.
pub fn stack_sampling_params(
  arguments: &HashMap<String, Data>,
  limits: StackSamplingLimits,
) -> Result<StackSamplingParams, CommandError> {
  if limits.max_duration < STACK_SAMPLING_MIN_DURATION {
    return Err(CommandError::InvalidArguments(format!(
      "stack sampling needs at least {} ms but the command timeout leaves {} ms",
      STACK_SAMPLING_MIN_DURATION.whole_milliseconds(),
      limits.max_duration.whole_milliseconds().max(0)
    )));
  }
  let duration = optional_integer_argument(arguments, "duration_ms")?
    .map_or(
      STACK_SAMPLING_DEFAULT_DURATION,
      time::Duration::milliseconds,
    )
    .clamp(STACK_SAMPLING_MIN_DURATION, limits.max_duration);
  Ok(StackSamplingParams {
    duration,
    frequency_hz: clamped_u32(
      optional_integer_argument(arguments, "frequency_hz")?,
      STACK_SAMPLING_DEFAULT_FREQUENCY_HZ,
      &STACK_SAMPLING_FREQUENCY_HZ,
    ),
    buffer_size_kb: clamped_u32(
      optional_integer_argument(arguments, "buffer_size_kb")?,
      STACK_SAMPLING_DEFAULT_BUFFER_KB,
      &STACK_SAMPLING_BUFFER_KB,
    ),
  })
}

/// The uploader reads the trace once, compresses it on the way and deletes the source itself.
fn stack_sampling_attachment(outcome: &StackSampleOutcome) -> CommandAttachment {
  CommandAttachment {
    source: UploadSource::Path(outcome.path.clone()),
    content_type: Some(STACK_SAMPLING_CONTENT_TYPE.to_string()),
  }
}

/// Removes a trace the command could not hand to the uploader. A missing file is fine: the
/// uploader deletes the source once it has persisted its copy.
async fn discard_trace_file(path: &std::path::Path) {
  if let Err(error) = tokio::fs::remove_file(path).await
    && error.kind() != std::io::ErrorKind::NotFound
  {
    log::debug!(
      "failed to remove stack sampling trace {}: {error}",
      path.display()
    );
  }
}

async fn execute_stack_sampling_device_command(
  command_id: String,
  arguments: &HashMap<String, Data>,
  senders: Arc<DeviceCommandExecution>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  execution_policy: CommandExecutionPolicy,
  profiling_handler: ProfilingHandler,
) -> anyhow::Result<()> {
  send_device_command_update(
    &senders,
    DeviceCommandUpdate {
      command_id: command_id.clone(),
      update_sequence_number: 1,
      update_type: Some(device_command_update::Update_type::Accepted(
        device_command_update::Accepted::default(),
      )),
      ..Default::default()
    },
  )
  .await?;

  let session_id = match &senders.session_id {
    Ok(session_id) => session_id.to_string(),
    Err(error) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 2, CommandError::Other(error.to_string())),
      )
      .await?;
      return Ok(());
    },
  };
  let params = match stack_sampling_params(arguments, execution_policy.stack_sampling_limits()) {
    Ok(params) => params,
    Err(error) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 2, error),
      )
      .await?;
      return Ok(());
    },
  };
  let outcome = match execution_policy
    .capture_stack_sample(profiling_handler, params)
    .await
  {
    Ok(outcome) => outcome,
    Err(failure) => {
      send_device_command_update(
        &senders,
        failed_device_command_update_with_fields(&command_id, 2, failure.into_fields()),
      )
      .await?;
      return Ok(());
    },
  };

  let update = match stage_device_command_attachment(
    stack_sampling_attachment(&outcome),
    STACK_SAMPLING_ARTIFACT_TYPE_ID,
    &command_id,
    &session_id,
    artifact_client,
  )
  .await
  {
    Ok((artifact_id, metadata)) => {
      return send_device_command_update_with_artifact(
        &senders,
        completed_device_command_update(
          &command_id,
          false,
          Some(device_command_context(params.fields())),
          artifact_attachment(artifact_id),
        ),
        Some(metadata),
      )
      .await;
    },
    Err(error) => {
      discard_trace_file(&outcome.path).await;
      failed_device_command_update(&command_id, 2, CommandError::Other(error.to_string()))
    },
  };
  send_device_command_update(&senders, update).await
}

fn validate_screenshot(bytes: &[u8]) -> Result<(), &'static str> {
  // This is deliberately a cheap, best-effort client-side guard. The server owns full artifact
  // validation and rejects malformed payloads that still satisfy the JPEG framing check below.
  if bytes.len() > MAX_SCREENSHOT_BYTES {
    return Err("screenshot exceeds the maximum artifact size");
  }
  if !bytes.starts_with(&[0xff, 0xd8]) || !bytes.ends_with(&[0xff, 0xd9]) {
    return Err("screenshot is not a JPEG image");
  }
  Ok(())
}

async fn execute_buffer_dump_device_command(
  command_id: String,
  senders: Arc<DeviceCommandExecution>,
  trigger_upload_tx: Sender<TriggerUpload>,
) -> anyhow::Result<()> {
  let session_id = match &senders.session_id {
    Ok(session_id) => session_id.to_string(),
    Err(error) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 1, CommandError::Other(error.to_string())),
      )
      .await?;
      return Ok(());
    },
  };
  let (trigger_upload, admission_rx, completion_rx) =
    TriggerUpload::new_device_command_with_completion(Vec::new(), command_id.clone(), session_id);
  // Preparation discovers the result size without authorizing upload. start_upload_tx is released
  // only after Accepted is ACKed, so cancellation during that wait cannot start the prepared dump.
  if trigger_upload_tx.send(trigger_upload).await.is_err() {
    send_device_command_update(
      &senders,
      failed_device_command_update(
        &command_id,
        1,
        CommandError::Other("trigger upload manager is unavailable".into()),
      ),
    )
    .await?;
    return Ok(());
  }

  let Ok(admission) = admission_rx.await else {
    send_device_command_update(
      &senders,
      failed_device_command_update(
        &command_id,
        1,
        CommandError::Other("buffer dump upload could not be prepared".into()),
      ),
    )
    .await?;
    return Ok(());
  };
  send_device_command_update(
    &senders,
    DeviceCommandUpdate {
      command_id: command_id.clone(),
      update_sequence_number: 1,
      update_type: Some(device_command_update::Update_type::Accepted(
        device_command_update::Accepted {
          total_result_bytes: Some(admission.total_result_bytes),
          ..Default::default()
        },
      )),
      ..Default::default()
    },
  )
  .await?;
  admission
    .start_upload_tx
    .send(())
    .map_err(|()| anyhow!("buffer dump upload admission was interrupted"))?;

  let update = match completion_rx.await {
    Ok(TriggerUploadCompletion::Completed {
      uploaded_log_count,
      output_truncated,
    }) => completed_device_command_update(
      &command_id,
      output_truncated,
      Some(DeviceCommandResultContext {
        fields: HashMap::from([(
          "uploaded_log_count".to_string(),
          Data {
            data_type: Some(Data_type::IntData(uploaded_log_count)),
            ..Default::default()
          },
        )]),
        ..Default::default()
      }),
      log_batches_attachment(admission.total_result_bytes),
    ),
    Ok(TriggerUploadCompletion::Failed) => failed_device_command_update(
      &command_id,
      2,
      CommandError::Other("buffer dump upload failed".into()),
    ),
    Err(_) => failed_device_command_update(
      &command_id,
      2,
      CommandError::Other("buffer dump upload did not complete".into()),
    ),
  };
  send_device_command_update(&senders, update).await
}

async fn execute_custom_device_command(
  command_id: String,
  registered_command_id: String,
  arguments: HashMap<String, Data>,
  senders: Arc<DeviceCommandExecution>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
) -> anyhow::Result<()> {
  let Ok(command_id_uuid) = Uuid::parse_str(&command_id) else {
    send_device_command_update(
      &senders,
      failed_device_command_update(
        &command_id,
        1,
        CommandError::Other("invalid device command id".into()),
      ),
    )
    .await?;
    return Ok(());
  };
  let Some(handler) = command_dispatcher.get_handler(&registered_command_id) else {
    send_device_command_update(
      &senders,
      failed_device_command_update(&command_id, 1, CommandError::CommandUnknown),
    )
    .await?;
    return Ok(());
  };

  send_device_command_update(
    &senders,
    DeviceCommandUpdate {
      command_id: command_id.clone(),
      update_sequence_number: 1,
      update_type: Some(device_command_update::Update_type::Accepted(
        device_command_update::Accepted::default(),
      )),
      ..Default::default()
    },
  )
  .await?;

  let session_id = match &senders.session_id {
    Ok(session_id) => session_id.to_string(),
    Err(error) => {
      send_device_command_update(
        &senders,
        failed_device_command_update(&command_id, 2, CommandError::Other(error.to_string())),
      )
      .await?;
      return Ok(());
    },
  };
  let result = execution_policy
    .execute_registered(
      handler,
      CommandInvocation {
        command_id: Some(command_id_uuid),
        registered_command_id,
        arguments,
        session_id: session_id.clone(),
      },
    )
    .await;

  let mut artifact_metadata = None;
  let update = match result {
    CommandResult::Completed { fields, attachment } => {
      let attachment = match attachment {
        Some(attachment) => {
          match stage_device_command_attachment(
            attachment,
            REGISTERED_COMMAND_ARTIFACT_TYPE_ID,
            &command_id,
            &session_id,
            artifact_client,
          )
          .await
          {
            Ok((artifact_id, metadata)) => {
              artifact_metadata = Some(metadata);
              artifact_attachment(artifact_id)
            },
            Err(error) => {
              return send_device_command_update(
                &senders,
                failed_device_command_update(
                  &command_id,
                  2,
                  CommandError::Other(error.to_string()),
                ),
              )
              .await;
            },
          }
        },
        None => no_attachment(),
      };
      completed_device_command_update(
        &command_id,
        false,
        Some(device_command_context(fields)),
        attachment,
      )
    },
    CommandResult::Failed { error, mut fields } => {
      fields.insert("error".into(), error.into_message().into());
      failed_device_command_update_with_fields(&command_id, 2, fields)
    },
  };
  send_device_command_update_with_artifact(&senders, update, artifact_metadata).await
}

// Direct success requires both durable staging and upload acknowledgement, so its terminal update
// can advertise a ready artifact. Dropping these waits does not retract uploader-owned work.
async fn stage_device_command_attachment(
  attachment: CommandAttachment,
  type_id: &str,
  command_id: &str,
  session_id: &str,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
) -> anyhow::Result<(Uuid, CommandArtifactMetadata)> {
  let content_type = attachment.content_type.clone();
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let (completion_tx, completion_rx) = oneshot::channel();
  let artifact_id = artifact_client.enqueue_command_upload(
    attachment.source,
    type_id.to_string(),
    None,
    session_id.to_string(),
    Vec::new(),
    command_id.to_string(),
    attachment.content_type,
    Some(persisted_tx),
    Some(completion_tx),
  )?;
  let size_bytes = persisted_rx
    .await
    .map_err(|_| anyhow!("device command attachment persistence was interrupted"))??;
  let metadata = CommandArtifactMetadata::new(content_type, size_bytes);
  match completion_rx.await {
    Ok(Ok(())) => Ok((artifact_id, metadata)),
    Ok(Err(error)) => Err(anyhow!(error)),
    Err(_) => Err(anyhow!("device command attachment upload was interrupted")),
  }
}

// Callers finish any declared artifact/log-batch upload before constructing Completed. Backend
// validation checks the declaration against durable results; final update delivery is separate.
fn completed_device_command_update(
  command_id: &str,
  output_truncated: bool,
  context: Option<DeviceCommandResultContext>,
  attachment: Attachment,
) -> DeviceCommandUpdate {
  DeviceCommandUpdate {
    command_id: command_id.to_string(),
    update_sequence_number: 2,
    update_type: Some(device_command_update::Update_type::Completed(
      device_command_update::Completed {
        output_truncated,
        context: context.into(),
        attachment: Some(attachment).into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

fn no_attachment() -> Attachment {
  Attachment {
    attachment_type: Some(attachment::Attachment_type::None(
      attachment::None::default(),
    )),
    ..Default::default()
  }
}

fn artifact_attachment(artifact_id: Uuid) -> Attachment {
  Attachment {
    attachment_type: Some(attachment::Attachment_type::Artifact(
      attachment::Artifact {
        artifact_id: artifact_id.to_string(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

fn log_batches_attachment(total_result_bytes: u64) -> Attachment {
  Attachment {
    attachment_type: Some(attachment::Attachment_type::LogBatches(
      attachment::LogBatches {
        total_result_bytes,
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

fn device_command_context(fields: LogFields) -> DeviceCommandResultContext {
  DeviceCommandResultContext {
    fields: fields
      .into_iter()
      .map(|(key, value)| (key.into_owned(), value.into_proto()))
      .collect(),
    ..Default::default()
  }
}

fn failed_device_command_update(
  command_id: &str,
  update_sequence_number: u64,
  error: CommandError,
) -> DeviceCommandUpdate {
  failed_device_command_update_with_fields(
    command_id,
    update_sequence_number,
    [("error".into(), error.into_message().into())].into(),
  )
}

fn failed_device_command_update_with_fields(
  command_id: &str,
  update_sequence_number: u64,
  fields: LogFields,
) -> DeviceCommandUpdate {
  DeviceCommandUpdate {
    command_id: command_id.to_string(),
    update_sequence_number,
    update_type: Some(device_command_update::Update_type::Failed(
      device_command_update::Failed {
        context: Some(device_command_context(fields)).into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

// Only terminal updates produce an end span; Accepted is protocol progress, not another span log.
// CommandOutcome sanitizes handler fields before inserting the SDK's result and attachment data.
fn device_command_outcome_log(
  update: &DeviceCommandUpdate,
  artifact_metadata: Option<CommandArtifactMetadata>,
  span: &CommandSpan,
  now: OffsetDateTime,
) -> Option<LogLine> {
  let (succeeded, context, artifact_id) = match update.update_type.as_ref()? {
    device_command_update::Update_type::Completed(completed) => {
      let artifact_id = match completed
        .attachment
        .as_ref()
        .and_then(|attachment| attachment.attachment_type.as_ref())
      {
        Some(attachment::Attachment_type::Artifact(artifact)) => {
          Uuid::parse_str(&artifact.artifact_id).ok()
        },
        _ => None,
      };
      (true, completed.context.as_ref(), artifact_id)
    },
    device_command_update::Update_type::Failed(failed) => (false, failed.context.as_ref(), None),
    device_command_update::Update_type::Accepted(_) => return None,
  };
  let mut fields: LogFields = context
    .into_iter()
    .flat_map(|context| &context.fields)
    .filter_map(|(key, value)| {
      DataValue::from_proto(value.clone()).map(|value| (key.clone().into(), value))
    })
    .collect();
  let message = if succeeded {
    None
  } else {
    fields
      .remove("error")
      .and_then(|value| value.as_str().map(str::to_owned))
  };
  let (log_level, mut fields, command_attachment) = CommandOutcome {
    succeeded,
    message,
    fields,
    attachment: artifact_id
      .zip(artifact_metadata)
      .map(|(artifact_id, metadata)| LogAttachment {
        artifact_id: artifact_id.to_string(),
        content_type: metadata.content_type,
        size_bytes: metadata.size_bytes,
        source: AttachmentSource::DIRECT_COMMAND,
      }),
  }
  .into_log_parts();
  fields.extend(span.fields(SPAN_TYPE_END, now));
  Some(LogLine {
    log_level,
    log_type: LogType::SPAN,
    message: COMMAND_OUTCOME_MESSAGE.into(),
    fields: fields
      .into_iter()
      .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
      .collect(),
    matching_fields: AnnotatedLogFields::default(),
    command_attachment: command_attachment.map(Box::new),
    attributes_overrides: Some(LogAttributesOverrides::OccurredAt(now)),
    capture_session: None,
  })
}

async fn send_device_command_update(
  execution: &DeviceCommandExecution,
  update: DeviceCommandUpdate,
) -> anyhow::Result<()> {
  send_device_command_update_with_artifact(execution, update, None).await
}

async fn send_device_command_update_with_artifact(
  execution: &DeviceCommandExecution,
  update: DeviceCommandUpdate,
  artifact_metadata: Option<CommandArtifactMetadata>,
) -> anyhow::Result<()> {
  // Choose the local terminal outcome before awaiting its ACK. A later session boundary or
  // transport error must not replace an already chosen result with a second terminal end.
  if !execution.finish(&update, artifact_metadata) {
    anyhow::bail!("device command already has a terminal outcome");
  }
  submit_device_command_update(execution, update).await
}

// Transport only: an ACK failure does not reopen terminal arbitration or retry the handler.
// The original command expiry bounds submission/ACK waits; execution has its separate policy.
async fn submit_device_command_update(
  execution: &DeviceCommandExecution,
  update: DeviceCommandUpdate,
) -> anyhow::Result<()> {
  let senders = &execution.senders;
  let (update, response_rx) = TrackedDeviceCommandUpdate::new(update.command_id.clone(), update);
  let remaining = senders
    .command_state
    .remaining_time(&update.payload.command_id)
    .ok_or_else(|| anyhow::anyhow!("command expired before update submission"))?;
  if remaining.is_zero() {
    anyhow::bail!("command expired before update submission");
  }
  let update = update.with_remaining_time(remaining);
  // Include channel backpressure in the original expiry budget, not just the ACK wait. Otherwise
  // a full API queue can retain the execution forever even after its waiter has expired.
  tokio::time::timeout(remaining, async {
    let permit = senders
      .data_upload_tx
      .reserve()
      .await
      .map_err(|_| anyhow!("device command update channel closed"))?;
    let update = update.with_byte_budget(&senders.command_state).await?;
    execution
      .last_update_sequence
      .store(update.payload.update_sequence_number, Ordering::Relaxed);
    permit.send(DataUpload::DeviceCommandUpdate(update));
    response_rx
      .await
      .map_err(|_| anyhow!("device command update was not acknowledged"))
  })
  .await
  .map_err(|_| anyhow!("command expired while submitting update"))?
}
