// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./device_command_test.rs"]
mod tests;

use crate::async_log_buffer::Sender as LogSender;
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
use bd_runtime::runtime::{ConfigLoader, DurationWatch};
use bd_time::{TimeDurationExt, TimeProvider};
use bd_workflows::workflow::{
  COMMAND_OUTCOME_MESSAGE,
  COMMAND_START_MESSAGE,
  CommandArtifactMetadata,
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
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

const MAX_SCREENSHOT_BYTES: usize = 2 * 1024 * 1024;
const REGISTERED_COMMAND_ARTIFACT_TYPE_ID: &str = "device_command_attachment";

// A device command is executed only from a freshly delivered configuration. Cached tail
// configuration can preserve log streaming, but must never replay a prior command after restart.

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

pub struct DeviceCommandDispatcher {
  senders: DeviceCommandSenders,
  trigger_upload_tx: Sender<TriggerUpload>,
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
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
// DeviceCommandExecution
//

struct DeviceCommandExecution {
  senders: DeviceCommandSenders,
  span: CommandSpan,
  session_id: anyhow::Result<Arc<str>>,
  time_provider: Arc<dyn TimeProvider>,
  ended: AtomicBool,
}

impl DeviceCommandExecution {
  fn emit(&self, log: LogLine) {
    let result = match &self.session_id {
      Ok(session_id) => self
        .senders
        .log_sender
        .try_send_command_log(log, session_id.clone()),
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

  fn finish(
    &self,
    update: &DeviceCommandUpdate,
    artifact_metadata: Option<CommandArtifactMetadata>,
  ) {
    if let Some(log) = device_command_outcome_log(
      update,
      artifact_metadata,
      &self.span,
      self.time_provider.now(),
    ) && !self.ended.swap(true, Ordering::Relaxed)
    {
      self.emit(log);
    }
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
    let time_provider = self.time_provider.clone();
    Box::pin(async move {
      let started_at = time_provider.now();
      let execution = Arc::new(DeviceCommandExecution {
        senders,
        span: CommandSpan {
          id: command_id.clone(),
          started_at: Some(started_at),
        },
        session_id: session_strategy.session_id(),
        time_provider,
        ended: AtomicBool::new(false),
      });
      execution.start(started_at);
      if let Err(error) = execute_device_command(
        command,
        execution.clone(),
        trigger_upload_tx,
        session_strategy,
        artifact_client,
        command_dispatcher,
        execution_policy,
        remote_screenshot_capture_handler,
      )
      .await
      {
        execution.finish(
          &failed_device_command_update(&command_id, 1, CommandError::Other(error.to_string())),
          None,
        );
        log::debug!("device command {command_id} execution stopped: {error}");
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
#[async_trait::async_trait]
pub trait RegisteredCommandHandler: Send + Sync {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult;
}

#[derive(Clone)]
pub struct CommandExecutionPolicy {
  timeout: DurationWatch<ExecutionTimeoutFlag>,
}

impl CommandExecutionPolicy {
  pub(crate) fn new(runtime: &ConfigLoader) -> Self {
    Self {
      timeout: runtime.register_duration_watch(),
    }
  }

  async fn execute<F, T>(&self, command: &str, execution: F) -> Result<T, CommandError>
  where
    F: Future<Output = Result<T, CommandError>>,
  {
    let timeout = *self.timeout.read();
    if timeout.is_zero() {
      return Err(CommandError::Timeout);
    }
    log::debug!("command {command} execution started with timeout {timeout}");
    match timeout
      .timeout(AssertUnwindSafe(execution).catch_unwind())
      .await
    {
      Ok(Ok(result)) => result,
      Ok(Err(_)) => Err(CommandError::Other("command handler panicked".into())),
      Err(_) => {
        log::debug!("command {command} execution timed out after {timeout}");
        Err(CommandError::Timeout)
      },
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

pub struct WorkflowCommandCompletion {
  pub token: WorkflowCommandCompletionToken,
  pub outcome: WorkflowCommandOutcome,
}

#[derive(Clone)]
pub struct WorkflowCommandDispatcher {
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
  completion_tx: Sender<WorkflowCommandCompletion>,
  attachment_store: AttachmentStoreHandle,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
}

impl WorkflowCommandDispatcher {
  pub fn new(
    command_dispatcher: RegisteredCommandDispatcher,
    execution_policy: CommandExecutionPolicy,
    completion_tx: Sender<WorkflowCommandCompletion>,
    attachment_store: AttachmentStoreHandle,
    remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  ) -> Self {
    Self {
      command_dispatcher,
      execution_policy,
      completion_tx,
      attachment_store,
      remote_screenshot_capture_handler,
    }
  }

  pub fn dispatch(&self, request: &WorkflowCommandRequest) {
    let token = request.completion_token();
    let completion_tx = self.completion_tx.clone();
    let command_selector = request.command_selector.clone();
    let dispatcher = self.clone();
    let attachment_store = self.attachment_store.clone();
    let session_id = request.session_id.clone();

    tokio::task::spawn(async move {
      let outcome = dispatcher.execute(command_selector, session_id).await;
      if let Err(error) = completion_tx
        .send(WorkflowCommandCompletion { token, outcome })
        .await
      {
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
    _ => workflow_command_failure(CommandError::CommandUnknown),
  }
}

fn workflow_command_failure(error: CommandError) -> WorkflowCommandOutcome {
  WorkflowCommandOutcome::Failed {
    message: Some(error.into_message()),
    fields: LogFields::default(),
  }
}

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
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  execution_policy: CommandExecutionPolicy,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
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
        session_strategy,
        artifact_client,
        command_dispatcher,
        execution_policy,
      )
      .await
    },
    Some(workflow_command_selector::Command_selector::BuiltinCommand(command)) => {
      match command.type_.enum_value() {
        Ok(WellKnownCommandType::DUMP_DEVICE_BUFFER) if arguments.is_empty() => {
          execute_buffer_dump_device_command(
            command_id,
            senders,
            trigger_upload_tx,
            session_strategy,
          )
          .await
        },
        Ok(WellKnownCommandType::TAKE_SCREENSHOT) if arguments.is_empty() => {
          execute_screenshot_device_command(
            command_id,
            senders,
            session_strategy,
            artifact_client,
            execution_policy,
            remote_screenshot_capture_handler,
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
  session_strategy: Arc<bd_session::Strategy>,
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

  let session_id = match session_strategy.session_id() {
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
  session_strategy: Arc<bd_session::Strategy>,
) -> anyhow::Result<()> {
  let session_id = match session_strategy.session_id() {
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
  session_strategy: Arc<bd_session::Strategy>,
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

  let session_id = match session_strategy.session_id() {
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

// These helpers construct a completed update only after the corresponding attachment upload was
// acknowledged. The server verifies the same declaration against its durable attachment catalog.
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
  execution.finish(&update, artifact_metadata);
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
    permit.send(DataUpload::DeviceCommandUpdate(update));
    response_rx
      .await
      .map_err(|_| anyhow!("device command update was not acknowledged"))
  })
  .await
  .map_err(|_| anyhow!("command expired while submitting update"))?
}
