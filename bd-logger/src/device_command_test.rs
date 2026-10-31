// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::{
  CommandAttachment,
  CommandError,
  CommandExecutionPolicy,
  CommandId,
  CommandInvocation,
  CommandRegistration,
  CommandResult,
  DeviceCommandDispatcher,
  DeviceCommandExecution,
  DeviceCommandStatus,
  PlatformCommand,
  RegisteredCommandDispatcher,
  RegisteredCommandHandler,
  TestHooks,
  WorkflowCommandDispatcher,
  artifact_attachment,
  completed_device_command_update,
  device_command_context,
  device_command_outcome_log,
  failed_device_command_update_with_fields,
  no_attachment,
  send_device_command_update,
  workflow_command_outcome,
};
use crate::async_log_buffer::Sender as LogSender;
use crate::workflow_attachment::AttachmentStoreHandle;
use bd_api::DataUpload;
use bd_api::upload::{DeviceCommandState, TrackedDeviceCommandUpdate};
use bd_artifact_upload::{Client, MockClient, UploadSource};
use bd_event_buffer::{
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventContext,
  LoggerIngressPayload,
};
use bd_log_primitives::command_attachment::AttachmentSource;
use bd_log_primitives::{
  COMMAND_OUTCOME_MESSAGE,
  DataValue,
  LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON,
  LOG_FIELD_NAME_COMMAND_DETAILS,
  LOG_FIELD_NAME_COMMAND_MESSAGE,
  LOG_FIELD_NAME_DURATION_MS,
  LOG_FIELD_NAME_RESULT,
  LOG_FIELD_NAME_SPAN_ID,
  LOG_FIELD_NAME_SPAN_NAME,
  LOG_FIELD_NAME_SPAN_TYPE,
  LogAttributesOverrides,
  LogFields,
  SPAN_NAME_DEVICE_COMMAND,
  SPAN_RESULT_CANCELED,
  SPAN_RESULT_FAILURE,
  SPAN_RESULT_SUCCESS,
  SPAN_TYPE_END,
  SPAN_TYPE_START,
  log_level,
};
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
use bd_proto::protos::client::api::{DeviceCommandUpdate, device_command_update};
use bd_proto::protos::logging::payload::{Data, LogType};
use bd_proto::protos::workflow::workflow_command::{
  WellKnownCommandType,
  WorkflowCommandSelector,
  workflow_command_selector,
};
use bd_runtime::runtime::attachment::MaxBytes;
use bd_runtime::runtime::device_command::ExecutionTimeoutFlag;
use bd_runtime::runtime::workflow_attachment::MaxOwnedFiles;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::test::no_timeout;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::{OffsetDateTimeExt, SystemTimeProvider, TestTimeProvider};
use bd_workflows::workflow::{
  CommandArtifactMetadata,
  CommandSpan,
  WorkflowCommandOutcome,
  command_details,
};
use parking_lot::Mutex;
use protobuf::{EnumOrUnknown, Message};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::future::{Future, pending, poll_fn};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};
use tokio::sync::{Notify, mpsc, oneshot};
use uuid::Uuid;

// Deliberately absent from WellKnownCommandType to exercise future numeric built-in registration.
const FUTURE_BUILTIN_COMMAND_TYPE: i32 = 4711;

impl RegisteredCommandDispatcher {
  fn get_builtin(&self, command_type: i32) -> Option<PlatformCommand> {
    self
      .commands
      .read()
      .get(&CommandId::Builtin(command_type))
      .cloned()
  }

  fn get_handler(&self, id: &str) -> Option<Arc<dyn RegisteredCommandHandler>> {
    self
      .commands
      .read()
      .get(&CommandId::Registered(id.into()))
      .map(|command| command.handler.clone())
  }
}

#[test]
fn builtin_registration_isolated_from_app_namespace() {
  let dispatcher = RegisteredCommandDispatcher::default();
  dispatcher
    .register(CommandRegistration::Builtin {
      command_type: FUTURE_BUILTIN_COMMAND_TYPE,
      key: "same_name".into(),
      artifact_type_id: "builtin_artifact".into(),
      timeout: None,
      handler: Arc::new(Handler),
    })
    .unwrap();
  for id in [
    "same_name".to_string(),
    FUTURE_BUILTIN_COMMAND_TYPE.to_string(),
  ] {
    dispatcher
      .register(CommandRegistration::Registered(
        id.clone(),
        Arc::new(Handler),
      ))
      .unwrap();
    assert!(dispatcher.unregister(&id));
    assert!(dispatcher.get_handler(&id).is_none());
  }
  assert_eq!(
    dispatcher
      .get_builtin(FUTURE_BUILTIN_COMMAND_TYPE)
      .unwrap()
      .artifact_type_id,
    "builtin_artifact"
  );
}

#[test]
fn builtin_registration_rejects_invalid_metadata_and_core_override() {
  let dispatcher = RegisteredCommandDispatcher::default();
  dispatcher
    .register(CommandRegistration::Builtin {
      command_type: FUTURE_BUILTIN_COMMAND_TYPE,
      key: "original".into(),
      artifact_type_id: "original_artifact".into(),
      timeout: Some(Duration::seconds(15)),
      handler: Arc::new(Handler),
    })
    .unwrap();
  for (command_type, key, artifact_type_id, timeout) in [
    (-1, "test", "test", None),
    (
      WellKnownCommandType::TYPE_UNSPECIFIED as i32,
      "test",
      "test",
      None,
    ),
    (
      WellKnownCommandType::DUMP_DEVICE_BUFFER as i32,
      "test",
      "test",
      None,
    ),
    (FUTURE_BUILTIN_COMMAND_TYPE, "", "test", None),
    (FUTURE_BUILTIN_COMMAND_TYPE, "test", "", None),
    (
      FUTURE_BUILTIN_COMMAND_TYPE,
      "test",
      "test",
      Some(Duration::ZERO),
    ),
    (
      FUTURE_BUILTIN_COMMAND_TYPE,
      "test",
      "test",
      Some(Duration::seconds(-1)),
    ),
  ] {
    assert!(
      dispatcher
        .register(CommandRegistration::Builtin {
          command_type,
          key: key.into(),
          artifact_type_id: artifact_type_id.into(),
          timeout,
          handler: Arc::new(Handler),
        })
        .is_err()
    );
    let original = dispatcher.get_builtin(FUTURE_BUILTIN_COMMAND_TYPE).unwrap();
    assert_eq!(original.key, "original");
    assert_eq!(original.artifact_type_id, "original_artifact");
    assert_eq!(original.timeout, Some(Duration::seconds(15)));
  }
}

const STACK_SAMPLING_COMMAND_KEY: &str = "stack_sampling_trace";

fn register_stack_sampling(
  registry: &RegisteredCommandDispatcher,
  handler: Arc<dyn RegisteredCommandHandler>,
) {
  registry
    .register(CommandRegistration::Builtin {
      command_type: WellKnownCommandType::STACK_SAMPLING_TRACE as i32,
      key: STACK_SAMPLING_COMMAND_KEY.into(),
      artifact_type_id: "stack_sampling_trace".into(),
      timeout: Some(Duration::seconds(15)),
      handler,
    })
    .unwrap();
}

struct Handler;

//
// AdmissionTestHooks
//

#[derive(Default)]
struct AdmissionTestHooks {
  sleep_started: Notify,
  sleep_finished: Notify,
  remove_on_wake: Mutex<Option<Weak<DeviceCommandDispatcher>>>,
}

impl TestHooks for AdmissionTestHooks {
  fn admission_sleep_started(&self) {
    self.sleep_started.notify_one();
  }

  fn admission_sleep_finished(&self) {
    if let Some(dispatcher) = self
      .remove_on_wake
      .lock()
      .take()
      .and_then(|weak| weak.upgrade())
    {
      dispatcher.dispatch_configuration(vec![]);
    }
    self.sleep_finished.notify_one();
  }
}

struct PendingHandler {
  started_tx: mpsc::UnboundedSender<()>,
  dropped_tx: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for PendingHandler {
  async fn execute(&self, _invocation: CommandInvocation) -> CommandResult {
    let _drop_signal = DropSignal(self.dropped_tx.lock().take());
    self.started_tx.send(()).unwrap();
    pending().await
  }
}

fn pending_handler() -> (
  Arc<PendingHandler>,
  mpsc::UnboundedReceiver<()>,
  oneshot::Receiver<()>,
) {
  let (started_tx, started_rx) = mpsc::unbounded_channel();
  let (dropped_tx, dropped_rx) = oneshot::channel();
  (
    Arc::new(PendingHandler {
      started_tx,
      dropped_tx: Mutex::new(Some(dropped_tx)),
    }),
    started_rx,
    dropped_rx,
  )
}

struct ScreenshotTarget;

type DeviceCommandScreenshotCompletion = Box<dyn FnOnce(Result<Vec<u8>, String>) + Send>;

trait Target: Send + Sync {
  fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
    completion(Err("remote screenshot capture is unavailable".into()));
  }
}

struct RemoteScreenshotCaptureHandler(Arc<dyn Target>);

impl RemoteScreenshotCaptureHandler {
  fn new(target: Arc<dyn Target>) -> Self {
    Self(target)
  }
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for RemoteScreenshotCaptureHandler {
  async fn execute(&self, _invocation: CommandInvocation) -> CommandResult {
    let (sender, receiver) = oneshot::channel();
    self
      .0
      .capture_device_command_screenshot(Box::new(move |result| {
        let _ = sender.send(result);
      }));
    match receiver.await.unwrap() {
      Ok(bytes) => CommandResult::Completed {
        fields: LogFields::default(),
        attachment: Some(CommandAttachment {
          source: UploadSource::Bytes(bytes),
          content_type: Some("image/jpeg".into()),
        }),
      },
      Err(message) => CommandResult::Failed {
        error: CommandError::HandlerFailed(message),
        fields: LogFields::default(),
      },
    }
  }
}

fn register_screenshot(
  registry: &RegisteredCommandDispatcher,
  handler: RemoteScreenshotCaptureHandler,
) {
  registry
    .register(CommandRegistration::Builtin {
      command_type: WellKnownCommandType::TAKE_SCREENSHOT as i32,
      key: "take_screenshot".into(),
      artifact_type_id: "screenshot".into(),
      timeout: None,
      handler: Arc::new(handler),
    })
    .unwrap();
}

impl Target for ScreenshotTarget {}

struct DeferredScreenshotTarget {
  started_tx: mpsc::UnboundedSender<()>,
  completions: Mutex<VecDeque<DeviceCommandScreenshotCompletion>>,
}

impl Target for DeferredScreenshotTarget {
  fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
    self.completions.lock().push_back(completion);
    self.started_tx.send(()).unwrap();
  }
}

impl DeferredScreenshotTarget {
  fn complete(&self, result: Result<Vec<u8>, String>) {
    let completion = self.completions.lock().pop_front().unwrap();
    completion(result);
  }
}

fn deferred_screenshot() -> (
  RemoteScreenshotCaptureHandler,
  Arc<DeferredScreenshotTarget>,
  mpsc::UnboundedReceiver<()>,
) {
  let (started_tx, started_rx) = mpsc::unbounded_channel();
  let target = Arc::new(DeferredScreenshotTarget {
    started_tx,
    completions: Mutex::default(),
  });
  (
    RemoteScreenshotCaptureHandler::new(target.clone()),
    target,
    started_rx,
  )
}

fn screenshot_selector() -> WorkflowCommandSelector {
  WorkflowCommandSelector {
    command_selector: Some(workflow_command_selector::Command_selector::BuiltinCommand(
      workflow_command_selector::BuiltinCommand {
        type_: WellKnownCommandType::TAKE_SCREENSHOT.into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

fn registered_selector() -> WorkflowCommandSelector {
  WorkflowCommandSelector {
    command_selector: Some(
      workflow_command_selector::Command_selector::RegisteredCommand(
        workflow_command_selector::RegisteredCommand {
          registered_command_id: "pending".into(),
          ..Default::default()
        },
      ),
    ),
    ..Default::default()
  }
}

fn direct_dispatcher(
  directory: &Path,
  policy: CommandExecutionPolicy,
  registry: RegisteredCommandDispatcher,
  screenshot_handler: RemoteScreenshotCaptureHandler,
  artifacts: Arc<dyn Client>,
) -> (DeviceCommandDispatcher, mpsc::Receiver<DataUpload>) {
  register_screenshot(&registry, screenshot_handler);
  let (upload_tx, upload_rx) = mpsc::channel(16);
  let (trigger_tx, _trigger_rx) = mpsc::channel(1);
  let logs = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 100_000,
    total_limit_bytes: 100_000,
  });
  let session_strategy = no_timeout(directory).strategy();
  (
    DeviceCommandDispatcher::new(
      upload_tx,
      LogSender::from_event_buffer(logs, session_strategy.clone()),
      trigger_tx,
      session_strategy,
      artifacts,
      registry,
      policy,
      Arc::new(DeviceCommandState::new(Arc::new(SystemTimeProvider))),
      Arc::new(SystemTimeProvider),
    ),
    upload_rx,
  )
}

fn direct_execution(
  dispatcher: &DeviceCommandDispatcher,
  command_id: &str,
) -> DeviceCommandExecution {
  DeviceCommandExecution {
    senders: dispatcher.senders.clone(),
    span: CommandSpan {
      id: command_id.to_owned(),
      started_at: Some(dispatcher.time_provider.now()),
      command_details: Some(command_details(Some(&registered_selector()))),
    },
    session_id: dispatcher.session_strategy.session_id(),
    time_provider: dispatcher.time_provider.clone(),
    status: Mutex::new(DeviceCommandStatus::Running),
    last_update_sequence: AtomicU64::new(0),
  }
}

async fn command_event_buffer(dispatcher: &mut DeviceCommandDispatcher) -> EventBuffer {
  let logs = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 100_000,
    total_limit_bytes: 100_000,
  });
  logs.mark_startup_gate_ready();
  logs.start_startup_gate(None);
  let opening = logs.next_batch(64).await;
  assert!(opening.startup_gate_opened.is_some());
  assert!(opening.entries.is_empty());
  dispatcher.senders.log_sender =
    LogSender::from_event_buffer(logs.clone(), dispatcher.session_strategy.clone());
  logs
}

#[tokio::test]
async fn direct_registration_uses_processed_session_even_when_strategy_snapshot_differs() {
  for boundary_processed in [false, true] {
    let (directory, _runtime, policy) = command_runtime();
    let (dispatcher, _updates) = direct_dispatcher(
      directory.path(),
      policy,
      RegisteredCommandDispatcher::default(),
      RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
      Arc::new(MockClient::new()),
    );
    let registry = dispatcher.senders.log_sender.device_commands();
    let first = dispatcher.session_strategy.session_id().unwrap();
    registry.update_session(&first);
    let stale_execution = direct_execution(&dispatcher, "command");
    dispatcher.session_strategy.start_new_session(None).unwrap();
    let second = dispatcher.session_strategy.session_id().unwrap();
    let execution = if boundary_processed {
      // The worker captured A before the logger processed B, but registers only afterward.
      registry.update_session(&second);
      stale_execution
    } else {
      // The strategy already says B, but A is still the effective logging session.
      direct_execution(&dispatcher, "command")
    };
    let (execution, cancelled) = registry.register(execution);
    assert_eq!(
      execution.session_id.as_ref().unwrap(),
      if boundary_processed { &second } else { &first }
    );
    assert!(execution.is_running());
    registry.update_session(&"third-session".into());
    cancelled.await.unwrap();
    assert!(execution.is_cancelled());
    assert!(!execution.finish(
      &completed_device_command_update("command", false, None, no_attachment()),
      None
    ));
  }
}

#[tokio::test]
async fn direct_processed_session_change_cancels_accepted_wait_and_running_handler() {
  for during_handler in [false, true] {
    let (directory, _runtime, policy) = command_runtime();
    let (handler, mut started_rx, dropped_rx) = pending_handler();
    let registry = RegisteredCommandDispatcher::default();
    registry
      .register(CommandRegistration::Registered("pending".into(), handler))
      .unwrap();
    let (mut dispatcher, mut updates) = direct_dispatcher(
      directory.path(),
      policy,
      registry,
      RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
      Arc::new(MockClient::new()),
    );
    let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
    dispatcher.time_provider = clock.clone();
    let logs = command_event_buffer(&mut dispatcher).await;
    let session_id = dispatcher.session_strategy.session_id().unwrap();
    let command = DeviceCommandRequest {
      command_id: Uuid::new_v4().to_string().into(),
      command_selector: Some(registered_selector()).into(),
      sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
      expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
      ..Default::default()
    };
    dispatcher.dispatch_configuration(vec![command.clone()]);
    let Some(DataUpload::DeviceCommandUpdate(accepted)) = updates.recv().await else {
      panic!("expected accepted command");
    };
    let accepted_waiter = if during_handler {
      accepted.response_tx.send(()).unwrap();
      started_rx.recv().await.unwrap();
      None
    } else {
      Some(accepted.response_tx)
    };
    clock.advance(Duration::milliseconds(125));
    dispatcher.session_strategy.start_new_session(None).unwrap();
    assert!(updates.try_recv().is_err());
    // A strategy reset alone is not the boundary. The real logger calls this when processing
    // the first ordinary event from the new session.
    dispatcher
      .senders
      .log_sender
      .device_commands()
      .update_session(&dispatcher.session_strategy.session_id().unwrap());
    let terminal = acknowledge_update(&mut updates).await;
    assert_eq!(terminal.update_sequence_number, 2);
    assert!(matches!(
      terminal.update_type,
      Some(device_command_update::Update_type::Failed(_))
    ));
    if during_handler {
      dropped_rx.await.unwrap();
    }
    drop(accepted_waiter);
    let task = dispatcher
      .execution_tasks
      .lock()
      .as_mut()
      .unwrap()
      .pop()
      .unwrap();
    task.await.unwrap();
    dispatcher.dispatch_configuration(vec![command]);
    assert!(
      dispatcher
        .execution_tasks
        .lock()
        .as_ref()
        .unwrap()
        .is_empty()
    );
    let entries = logs.next_batch(64).await.entries;
    assert_eq!(entries.len(), 2);
    let EventBufferEntry::Ingress(event) = &entries[1] else {
      panic!("expected cancellation span end");
    };
    assert!(matches!(&event.context, EventContext::Command {
      session_id: owner, cancelled: true, ..
    } if owner == &session_id));
    let LoggerIngressPayload::Log(log) = &event.payload else {
      panic!("expected cancellation span end");
    };
    assert_eq!(log.log_level, log_level::ERROR);
    assert_eq!(
      log
        .fields
        .get(LOG_FIELD_NAME_COMMAND_DETAILS)
        .unwrap()
        .value
        .as_str(),
      Some(command_details(Some(&registered_selector())).as_str()),
    );
    for (field, expected) in [
      (LOG_FIELD_NAME_RESULT, SPAN_RESULT_CANCELED),
      (
        LOG_FIELD_NAME_COMMAND_CANCELLATION_REASON,
        "session_changed",
      ),
      (
        LOG_FIELD_NAME_COMMAND_MESSAGE,
        "Command cancelled: session changed",
      ),
      (LOG_FIELD_NAME_DURATION_MS, "125"),
    ] {
      assert_eq!(
        log.fields.get(field).unwrap().value.as_str(),
        Some(expected)
      );
    }
  }
}

#[tokio::test]
async fn direct_command_spans_are_correlated_and_not_repeated_by_ack_failures_or_dispatch() {
  for scenario in [
    "success",
    "unknown",
    "accepted_ack_failure",
    "terminal_ack_failure",
    "terminal_ack_session_reset",
  ] {
    let (directory, _runtime, policy) = command_runtime();
    let registry = RegisteredCommandDispatcher::default();
    registry
      .register(CommandRegistration::Registered(
        "pending".into(),
        Arc::new(Handler),
      ))
      .unwrap();
    let (mut dispatcher, mut updates) = direct_dispatcher(
      directory.path(),
      policy,
      registry,
      RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
      Arc::new(MockClient::new()),
    );
    let started_at = OffsetDateTime::UNIX_EPOCH;
    let clock = Arc::new(TestTimeProvider::new(started_at));
    dispatcher.time_provider = clock.clone();
    let logs = command_event_buffer(&mut dispatcher).await;
    let session_id = dispatcher.session_strategy.session_id().unwrap();
    let command = DeviceCommandRequest {
      command_id: Uuid::new_v4().to_string().into(),
      command_selector: (scenario != "unknown").then(registered_selector).into(),
      sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
      expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
      ..Default::default()
    };
    dispatcher.dispatch_configuration(vec![command.clone()]);
    dispatcher.dispatch_configuration(vec![command.clone()]);
    let Some(DataUpload::DeviceCommandUpdate(first)) = updates.recv().await else {
      panic!("expected command update");
    };
    if scenario == "unknown" {
      assert!(matches!(
        first.payload.update_type,
        Some(device_command_update::Update_type::Failed(_))
      ));
      first.response_tx.send(()).unwrap();
    } else {
      assert!(matches!(
        first.payload.update_type,
        Some(device_command_update::Update_type::Accepted(_))
      ));
      clock.advance(Duration::milliseconds(125));
      if scenario == "accepted_ack_failure" {
        drop(first.response_tx);
      } else {
        first.response_tx.send(()).unwrap();
        let Some(DataUpload::DeviceCommandUpdate(terminal)) = updates.recv().await else {
          panic!("expected terminal command update");
        };
        assert!(matches!(
          terminal.payload.update_type,
          Some(device_command_update::Update_type::Completed(_))
        ));
        clock.advance(Duration::seconds(10));
        if scenario == "terminal_ack_session_reset" {
          dispatcher.session_strategy.start_new_session(None).unwrap();
          dispatcher
            .senders
            .log_sender
            .device_commands()
            .update_session(&dispatcher.session_strategy.session_id().unwrap());
        }
        if scenario == "terminal_ack_failure" {
          drop(terminal.response_tx);
        } else {
          terminal.response_tx.send(()).unwrap();
        }
      }
    }
    let task = dispatcher
      .execution_tasks
      .lock()
      .as_mut()
      .unwrap()
      .pop()
      .unwrap();
    task.await.unwrap();
    dispatcher.dispatch_configuration(vec![command.clone()]);
    assert!(
      dispatcher
        .execution_tasks
        .lock()
        .as_ref()
        .unwrap()
        .is_empty()
    );
    let entries = logs.next_batch(64).await.entries;
    logs.close();
    assert!(logs.next_batch(64).await.entries.is_empty());
    assert_eq!(entries.len(), 2, "{scenario}");
    for (index, entry) in entries.into_iter().enumerate() {
      let EventBufferEntry::Ingress(event) = entry else {
        panic!("expected span log");
      };
      let EventContext::Command {
        session_id: actual_session_id,
        ..
      } = event.context
      else {
        panic!("expected command span context");
      };
      assert_eq!(actual_session_id, session_id);
      let LoggerIngressPayload::Log(log) = event.payload else {
        panic!("expected span log");
      };
      assert_eq!(log.log_type, LogType::SPAN);
      assert_eq!(
        log
          .fields
          .get(LOG_FIELD_NAME_COMMAND_DETAILS)
          .unwrap()
          .value
          .as_str(),
        Some(command_details(command.command_selector.as_ref()).as_str()),
      );
      assert_eq!(
        log
          .fields
          .get(LOG_FIELD_NAME_SPAN_ID)
          .unwrap()
          .value
          .as_str(),
        Some(command.command_id.as_str())
      );
      assert_eq!(
        log
          .fields
          .get(LOG_FIELD_NAME_SPAN_NAME)
          .unwrap()
          .value
          .as_str(),
        Some(SPAN_NAME_DEVICE_COMMAND)
      );
      assert_eq!(
        log
          .fields
          .get(LOG_FIELD_NAME_SPAN_TYPE)
          .unwrap()
          .value
          .as_str(),
        Some(
          if index == 0 {
            SPAN_TYPE_START
          } else {
            SPAN_TYPE_END
          }
        )
      );
      let duration_ms = if scenario == "unknown" { "0" } else { "125" };
      if index == 0 {
        assert!(!log.fields.contains_key(LOG_FIELD_NAME_DURATION_MS));
        assert!(!log.fields.contains_key(LOG_FIELD_NAME_RESULT));
        let Some(LogAttributesOverrides::OccurredAt(time)) = log.attributes_overrides else {
          panic!("expected the original start timestamp");
        };
        assert_eq!(time, started_at);
      } else {
        assert_eq!(
          log
            .fields
            .get(LOG_FIELD_NAME_DURATION_MS)
            .unwrap()
            .value
            .as_str(),
          Some(duration_ms)
        );
        assert_eq!(
          log
            .fields
            .get(LOG_FIELD_NAME_RESULT)
            .unwrap()
            .value
            .as_str(),
          Some(
            if matches!(scenario, "unknown" | "accepted_ack_failure") {
              SPAN_RESULT_FAILURE
            } else {
              SPAN_RESULT_SUCCESS
            }
          ),
        );
      }
    }
  }
}

fn workflow_dispatcher(
  directory: &Path,
  runtime: Arc<ConfigLoader>,
  policy: CommandExecutionPolicy,
  registry: RegisteredCommandDispatcher,
  screenshot_handler: RemoteScreenshotCaptureHandler,
) -> WorkflowCommandDispatcher {
  register_screenshot(&registry, screenshot_handler);
  let (completion_tx, _completion_rx) = mpsc::channel(16);
  WorkflowCommandDispatcher::new(
    registry,
    policy,
    completion_tx,
    AttachmentStoreHandle::new(directory.to_owned(), runtime),
  )
}

impl WorkflowCommandDispatcher {
  async fn execute(
    &self,
    selector: WorkflowCommandSelector,
    session_id: String,
  ) -> WorkflowCommandOutcome {
    self.executor.execute(selector, session_id).await
  }
}

async fn acknowledge_update(updates: &mut mpsc::Receiver<DataUpload>) -> DeviceCommandUpdate {
  let Some(DataUpload::DeviceCommandUpdate(update)) = updates.recv().await else {
    panic!("expected a device command update");
  };
  update.response_tx.send(()).unwrap();
  update.payload
}

fn assert_timeout_update(update: DeviceCommandUpdate) {
  assert_eq!(update.update_sequence_number, 2);
  let Some(device_command_update::Update_type::Failed(failed)) = update.update_type else {
    panic!("expected a failed command update");
  };
  let fields = &failed.context.as_ref().unwrap().fields;
  assert_eq!(fields.len(), 1);
  assert_eq!(
    DataValue::from_proto(fields.get("error").unwrap().clone()),
    Some("command timed out".into())
  );
}

#[tokio::test(start_paused = true)]
async fn direct_registered_command_timeout_reports_one_terminal_failure() {
  let (directory, _runtime, policy) = command_runtime();
  let (handler, mut started_rx, dropped_rx) = pending_handler();
  let registry = RegisteredCommandDispatcher::default();
  registry
    .register(CommandRegistration::Registered("pending".into(), handler))
    .unwrap();
  let (mut dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  dispatcher.time_provider = clock.clone();
  let logs = command_event_buffer(&mut dispatcher).await;
  let command = DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    command_selector: Some(registered_selector()).into(),
    sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
    ..Default::default()
  };
  dispatcher.dispatch_configuration(vec![command.clone()]);
  let accepted = acknowledge_update(&mut updates).await;
  assert_eq!(accepted.update_sequence_number, 1);
  assert!(matches!(
    accepted.update_type,
    Some(device_command_update::Update_type::Accepted(_))
  ));
  started_rx.recv().await.unwrap();
  dispatcher.dispatch_configuration(vec![]);
  dispatcher.dispatch_configuration(vec![command]);
  clock.advance(Duration::seconds(5));
  tokio::time::advance(Duration::seconds(5).unsigned_abs()).await;
  assert_timeout_update(acknowledge_update(&mut updates).await);
  dropped_rx.await.unwrap();
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  let entries = logs.next_batch(64).await.entries;
  assert_eq!(entries.len(), 2);
  let EventBufferEntry::Ingress(event) = &entries[1] else {
    panic!("expected timeout span end");
  };
  let LoggerIngressPayload::Log(log) = &event.payload else {
    panic!("expected timeout span end");
  };
  assert_eq!(log.log_type, LogType::SPAN);
  assert_eq!(
    log
      .fields
      .get(LOG_FIELD_NAME_DURATION_MS)
      .unwrap()
      .value
      .as_str(),
    Some("5000")
  );
  assert_eq!(
    log
      .fields
      .get(LOG_FIELD_NAME_COMMAND_MESSAGE)
      .unwrap()
      .value
      .as_str(),
    Some("command timed out")
  );
}

#[tokio::test(start_paused = true)]
async fn direct_command_update_expiry_bounds_channel_byte_budget_and_ack_wait() {
  let (directory, _runtime, policy) = command_runtime();
  let (mut dispatcher, _updates) = direct_dispatcher(
    directory.path(),
    policy,
    RegisteredCommandDispatcher::default(),
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  let state = Arc::new(DeviceCommandState::new(Arc::new(
    bd_time::TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH),
  )));
  dispatcher.senders.command_state = state.clone();
  let command = DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    sdk_instance_id: state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::UNIX_EPOCH + Duration::seconds(2)).into_proto(),
    ..Default::default()
  };
  assert!(state.admit(&command));
  for blocked in ["ack", "channel", "bytes"] {
    let (upload_tx, mut upload_rx) = mpsc::channel(1);
    dispatcher.senders.data_upload_tx = upload_tx;
    let reservation = if blocked == "channel" {
      Some(dispatcher.senders.data_upload_tx.reserve().await.unwrap())
    } else {
      None
    };
    let occupied = if blocked == "bytes" {
      let update = large_command_update(&Uuid::new_v4().to_string());
      let (tracked, response) = TrackedDeviceCommandUpdate::new(update.command_id.clone(), update);
      Some((tracked.with_byte_budget(&state).await.unwrap(), response))
    } else {
      None
    };
    let update = large_command_update(command.command_id.as_str());
    let started_at = tokio::time::Instant::now();
    let execution = direct_execution(&dispatcher, &command.command_id);
    let error = send_device_command_update(&execution, update)
      .await
      .unwrap_err();
    assert_eq!(error.to_string(), "command expired while submitting update");
    assert_eq!(started_at.elapsed(), std::time::Duration::from_secs(2));
    drop(reservation);
    if blocked == "ack" {
      let DataUpload::DeviceCommandUpdate(update) = upload_rx.try_recv().unwrap() else {
        panic!("expected a command update");
      };
      assert_matches::assert_matches!(update.response_tx.send(()), Err(_));
    } else {
      assert_eq!(
        upload_rx.try_recv().unwrap_err(),
        mpsc::error::TryRecvError::Empty
      );
    }
    drop(occupied);
  }
}

fn large_command_update(command_id: &str) -> DeviceCommandUpdate {
  completed_device_command_update(
    command_id,
    false,
    Some(device_command_context(
      [("result".into(), "x".repeat(3 * 1024 * 1024).into())].into(),
    )),
    no_attachment(),
  )
}

#[tokio::test(start_paused = true)]
async fn direct_command_updates_backpressure_without_losing_large_results() {
  let (directory, _runtime, policy) = command_runtime();
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    RegisteredCommandDispatcher::default(),
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  let first_id = Uuid::new_v4().to_string();
  let second_id = Uuid::new_v4().to_string();
  for command_id in [&first_id, &second_id] {
    assert!(dispatcher.command_state.admit(&DeviceCommandRequest {
      command_id: command_id.clone().into(),
      sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
      expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
      ..Default::default()
    }));
  }
  let first = large_command_update(&first_id);
  let second = large_command_update(&second_id);
  let first_execution = direct_execution(&dispatcher, &first_id);
  let second_execution = direct_execution(&dispatcher, &second_id);
  let mut first_submission = Box::pin(send_device_command_update(&first_execution, first.clone()));
  let pending = poll_fn(|context| Poll::Ready(first_submission.as_mut().poll(context))).await;
  assert_matches::assert_matches!(pending, Poll::Pending);
  let Some(DataUpload::DeviceCommandUpdate(first_tracked)) = updates.recv().await else {
    panic!("expected first command result");
  };
  assert_eq!(first_tracked.payload, first);
  let mut second_submission = Box::pin(send_device_command_update(
    &second_execution,
    second.clone(),
  ));
  let pending = poll_fn(|context| Poll::Ready(second_submission.as_mut().poll(context))).await;
  assert_matches::assert_matches!(pending, Poll::Pending);
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  first_tracked.response_tx.send(()).unwrap();
  first_submission.await.unwrap();
  let pending = poll_fn(|context| Poll::Ready(second_submission.as_mut().poll(context))).await;
  assert_matches::assert_matches!(pending, Poll::Pending);
  let Some(DataUpload::DeviceCommandUpdate(second_tracked)) = updates.recv().await else {
    panic!("expected second command result");
  };
  assert_eq!(second_tracked.payload, second);
  second_tracked.response_tx.send(()).unwrap();
  second_submission.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn direct_command_admission_resumes_only_after_original_capacity_expiry() {
  let (directory, _runtime, policy) = command_runtime();
  let (handler, mut started_rx, dropped_rx) = pending_handler();
  let registry = RegisteredCommandDispatcher::default();
  registry
    .register(CommandRegistration::Registered("pending".into(), handler))
    .unwrap();
  let (mut dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  let clock = Arc::new(bd_time::TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let logs = command_event_buffer(&mut dispatcher).await;
  let state = Arc::new(DeviceCommandState::new(clock.clone()));
  dispatcher.command_state = state.clone();
  dispatcher.senders.command_state = state.clone();
  let command = DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    command_selector: Some(registered_selector()).into(),
    sdk_instance_id: state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::UNIX_EPOCH + Duration::seconds(30)).into_proto(),
    ..Default::default()
  };
  for _ in 0 .. 64 {
    let mut occupied = command.clone();
    occupied.command_id = Uuid::new_v4().to_string().into();
    occupied.expires_at = (OffsetDateTime::UNIX_EPOCH + Duration::seconds(2)).into_proto();
    assert!(state.admit(&occupied));
  }
  let hooks = Arc::new(AdmissionTestHooks::default());
  dispatcher.test_hooks = Some(hooks.clone());
  let mut replaced = command.clone();
  replaced.command_id = Uuid::new_v4().to_string().into();
  dispatcher.dispatch_configuration(vec![replaced]);
  hooks.sleep_started.notified().await;
  let pending =
    poll_fn(|context| Poll::Ready(Box::pin(logs.next_batch(64)).as_mut().poll(context))).await;
  assert!(pending.is_pending());
  dispatcher.dispatch_configuration(vec![command.clone()]);
  hooks.sleep_started.notified().await;
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  assert_eq!(
    started_rx.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  clock.advance(Duration::seconds(2));
  tokio::time::advance(std::time::Duration::from_secs(2)).await;
  let accepted = acknowledge_update(&mut updates).await;
  assert_eq!(accepted.command_id, command.command_id.to_string());
  started_rx.recv().await.unwrap();
  assert_eq!(logs.next_batch(64).await.entries.len(), 1);
  dispatcher.dispatch_configuration(vec![command]);
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  assert_eq!(
    started_rx.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  drop(dispatcher);
  dropped_rx.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn replaced_configuration_fences_already_running_admission_task() {
  let (directory, _runtime, policy) = command_runtime();
  let (mut dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    RegisteredCommandDispatcher::default(),
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  let clock = Arc::new(bd_time::TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let state = Arc::new(DeviceCommandState::new(clock.clone()));
  dispatcher.command_state = state.clone();
  dispatcher.senders.command_state = state.clone();
  let hooks = Arc::new(AdmissionTestHooks::default());
  dispatcher.test_hooks = Some(hooks.clone());
  let dispatcher = Arc::new(dispatcher);
  *hooks.remove_on_wake.lock() = Some(Arc::downgrade(&dispatcher));
  let command = DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    command_selector: Some(registered_selector()).into(),
    sdk_instance_id: state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::UNIX_EPOCH + Duration::seconds(30)).into_proto(),
    ..Default::default()
  };
  for _ in 0 .. 64 {
    let mut occupied = command.clone();
    occupied.command_id = Uuid::new_v4().to_string().into();
    occupied.expires_at = (OffsetDateTime::UNIX_EPOCH + Duration::seconds(2)).into_proto();
    assert!(state.admit(&occupied));
  }
  dispatcher.dispatch_configuration(vec![command.clone()]);
  hooks.sleep_started.notified().await;
  clock.advance(Duration::seconds(2));
  tokio::time::advance(std::time::Duration::from_secs(2)).await;
  hooks.sleep_finished.notified().await;
  // The hook replaces config on the same worker after sleep, so abort cannot interrupt the
  // following synchronous admission pass. No ID may be claimed and no handler may be spawned.
  assert_eq!(dispatcher.execution_tasks.lock().as_ref().unwrap().len(), 0);
  assert!(state.admit(&command));
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
}

#[tokio::test(start_paused = true)]
async fn direct_command_execution_ends_with_original_sdk_dispatcher() {
  let (directory, _runtime, policy) = command_runtime();
  let (handler, mut started_rx, dropped_rx) = pending_handler();
  let registry = RegisteredCommandDispatcher::default();
  registry
    .register(CommandRegistration::Registered("pending".into(), handler))
    .unwrap();
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  dispatcher.dispatch_configuration(vec![DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    command_selector: Some(registered_selector()).into(),
    sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
    ..Default::default()
  }]);
  acknowledge_update(&mut updates).await;
  started_rx.recv().await.unwrap();
  drop(dispatcher);
  dropped_rx.await.unwrap();
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Disconnected
  );
}

#[tokio::test(start_paused = true)]
async fn workflow_registered_command_timeout_reports_failure() {
  let (directory, runtime, policy) = command_runtime();
  let (handler, mut started_rx, dropped_rx) = pending_handler();
  let registry = RegisteredCommandDispatcher::default();
  registry
    .register(CommandRegistration::Registered("pending".into(), handler))
    .unwrap();
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
  );
  let execution = tokio::spawn(async move {
    dispatcher
      .execute(registered_selector(), "session".into())
      .await
  });
  started_rx.recv().await.unwrap();
  tokio::time::advance(Duration::seconds(5).unsigned_abs()).await;
  let WorkflowCommandOutcome::Failed { message, fields } = execution.await.unwrap() else {
    panic!("expected workflow timeout failure");
  };
  assert_eq!(message.as_deref(), Some("command timed out"));
  assert_eq!(fields, LogFields::default());
  dropped_rx.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn direct_screenshot_command_timeout_reports_one_terminal_failure() {
  let (directory, _runtime, policy) = command_runtime();
  let (screenshots, target, mut started_rx) = deferred_screenshot();
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    RegisteredCommandDispatcher::default(),
    screenshots,
    Arc::new(MockClient::new()),
  );
  dispatcher.dispatch_configuration(vec![DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    command_selector: Some(screenshot_selector()).into(),
    sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
    ..Default::default()
  }]);
  let accepted = acknowledge_update(&mut updates).await;
  assert_eq!(accepted.update_sequence_number, 1);
  started_rx.recv().await.unwrap();
  tokio::time::advance(Duration::seconds(5).unsigned_abs()).await;
  assert_timeout_update(acknowledge_update(&mut updates).await);
  target.complete(Ok(vec![0xff, 0xd8, 0xff, 0xd9]));
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
}

#[tokio::test(start_paused = true)]
async fn workflow_screenshot_command_timeout_reports_failure() {
  let (directory, runtime, policy) = command_runtime();
  let (screenshots, target, mut started_rx) = deferred_screenshot();
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    RegisteredCommandDispatcher::default(),
    screenshots,
  );
  let execution = tokio::spawn(async move {
    dispatcher
      .execute(screenshot_selector(), "session".into())
      .await
  });
  started_rx.recv().await.unwrap();
  tokio::time::advance(Duration::seconds(5).unsigned_abs()).await;
  let WorkflowCommandOutcome::Failed { message, fields } = execution.await.unwrap() else {
    panic!("expected workflow screenshot timeout failure");
  };
  assert_eq!(message.as_deref(), Some("command timed out"));
  assert_eq!(fields, LogFields::default());
  target.complete(Ok(vec![0xff, 0xd8, 0xff, 0xd9]));
}

#[tokio::test(start_paused = true)]
async fn screenshot_command_honors_timeout_above_thirty_seconds() {
  let (directory, runtime, policy) = command_runtime();
  runtime
    .update_snapshot(make_simple_update(vec![(
      ExecutionTimeoutFlag::path(),
      ValueKind::Int(60_000),
    )]))
    .await
    .unwrap();
  let (screenshots, target, mut started_rx) = deferred_screenshot();
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    RegisteredCommandDispatcher::default(),
    screenshots,
  );
  let execution = tokio::spawn(async move {
    dispatcher
      .execute(screenshot_selector(), "session".into())
      .await
  });
  started_rx.recv().await.unwrap();
  tokio::time::advance(Duration::seconds(31).unsigned_abs()).await;
  assert!(!execution.is_finished());
  target.complete(Ok(vec![0xff, 0xd8, 0xff, 0xd9]));
  assert!(matches!(
    execution.await.unwrap(),
    WorkflowCommandOutcome::SucceededWithAttachment { .. }
  ));
}

struct AttachmentHandler;

#[async_trait::async_trait]
impl RegisteredCommandHandler for AttachmentHandler {
  async fn execute(&self, _invocation: CommandInvocation) -> CommandResult {
    completed_attachment(UploadSource::Bytes(vec![1, 2]), None)
  }
}

#[tokio::test(start_paused = true)]
async fn command_execution_timeout_excludes_attachment_upload() {
  let (directory, _runtime, policy) = command_runtime();
  let registry = RegisteredCommandDispatcher::default();
  registry
    .register(CommandRegistration::Registered(
      "pending".into(),
      Arc::new(AttachmentHandler),
    ))
    .unwrap();
  let (upload_started_tx, mut upload_started_rx) = mpsc::unbounded_channel();
  let artifact_id = Uuid::new_v4();
  let mut artifacts = MockClient::new();
  artifacts
    .expect_enqueue_command_upload()
    .times(1)
    .returning(move |_, _, _, _, _, _, _, persisted_tx, completion_tx| {
      upload_started_tx
        .send((persisted_tx.unwrap(), completion_tx.unwrap()))
        .unwrap();
      Ok(artifact_id)
    });
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(artifacts),
  );
  dispatcher.dispatch_configuration(vec![DeviceCommandRequest {
    command_id: Uuid::new_v4().to_string().into(),
    command_selector: Some(registered_selector()).into(),
    sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
    ..Default::default()
  }]);
  acknowledge_update(&mut updates).await;
  let (persisted_tx, completion_tx) = upload_started_rx.recv().await.unwrap();
  tokio::time::advance(Duration::seconds(6).unsigned_abs()).await;
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  persisted_tx.send(Ok(2)).unwrap();
  completion_tx.send(Ok(())).unwrap();
  let completed = acknowledge_update(&mut updates).await;
  assert_eq!(completed.update_sequence_number, 2);
  assert!(matches!(
    completed.update_type,
    Some(device_command_update::Update_type::Completed(_))
  ));
}

struct DropSignal(Option<oneshot::Sender<()>>);

impl Drop for DropSignal {
  fn drop(&mut self) {
    if let Some(sender) = self.0.take() {
      let _ = sender.send(());
    }
  }
}

//
// Stack sampling pass-through
//

/// Records what the platform handler was invoked with and answers with one prepared result.
struct RecordingHandler {
  invocations: Mutex<Vec<CommandInvocation>>,
  result: Mutex<Option<CommandResult>>,
}

impl RecordingHandler {
  fn new(result: CommandResult) -> Arc<Self> {
    Arc::new(Self {
      invocations: Mutex::default(),
      result: Mutex::new(Some(result)),
    })
  }
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for RecordingHandler {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
    self.invocations.lock().push(invocation);
    self.result.lock().take().expect("one invocation")
  }
}

fn text_argument(value: &str) -> Data {
  let mut data = Data::new();
  data.set_string_data(value.to_string());
  data
}

fn stack_sampling_arguments() -> HashMap<String, Data> {
  HashMap::from([
    ("duration_ms".to_string(), text_argument("5000")),
    ("tag".to_string(), text_argument("checkout")),
  ])
}

fn stack_sampling_selector(arguments: HashMap<String, Data>) -> WorkflowCommandSelector {
  WorkflowCommandSelector {
    command_selector: Some(workflow_command_selector::Command_selector::BuiltinCommand(
      workflow_command_selector::BuiltinCommand {
        type_: WellKnownCommandType::STACK_SAMPLING_TRACE.into(),
        ..Default::default()
      },
    )),
    arguments,
    ..Default::default()
  }
}

fn stack_sampling_request(
  command_id: Uuid,
  arguments: HashMap<String, Data>,
  dispatcher: &DeviceCommandDispatcher,
) -> DeviceCommandRequest {
  DeviceCommandRequest {
    command_id: command_id.to_string().into(),
    command_selector: Some(stack_sampling_selector(arguments)).into(),
    sdk_instance_id: dispatcher.command_state.sdk_instance_id.clone().into(),
    expires_at: (OffsetDateTime::now_utc() + Duration::days(1)).into_proto(),
    ..Default::default()
  }
}

fn write_trace(directory: &Path) -> PathBuf {
  let path = directory.join("trace.perfetto");
  fs::write(&path, b"trace").unwrap();
  path
}

fn failed_error(update: DeviceCommandUpdate) -> String {
  let Some(device_command_update::Update_type::Failed(failed)) = update.update_type else {
    panic!("expected a failed command update");
  };
  let fields = &failed.context.as_ref().unwrap().fields;
  match DataValue::from_proto(fields.get("error").unwrap().clone()) {
    Some(DataValue::String(error)) => error,
    other => panic!("expected a string error, got {other:?}"),
  }
}

#[tokio::test]
async fn direct_stack_sampling_is_passed_through_to_the_platform_handler() {
  let (directory, _runtime, policy) = command_runtime();
  let trace = write_trace(directory.path());
  let handler = RecordingHandler::new(completed_attachment(
    UploadSource::Path(trace.clone()),
    Some("application/vnd.bitdrift.perfetto-trace".into()),
  ));
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(&registry, handler.clone());
  let (upload_tx, mut upload_rx) = mpsc::unbounded_channel();
  let artifact_id = Uuid::new_v4();
  let mut artifacts = MockClient::new();
  artifacts
    .expect_enqueue_command_upload()
    .times(1)
    .returning(
      move |source, type_id, _, _, _, _, content_type, persisted_tx, completion_tx| {
        let UploadSource::Path(path) = source else {
          panic!("expected the trace to be uploaded from its path");
        };
        upload_tx.send((path, type_id, content_type)).unwrap();
        persisted_tx.unwrap().send(Ok(5)).unwrap();
        completion_tx.unwrap().send(Ok(())).unwrap();
        Ok(artifact_id)
      },
    );
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(artifacts),
  );
  let command_id = Uuid::new_v4();
  dispatcher.dispatch_configuration(vec![stack_sampling_request(
    command_id,
    stack_sampling_arguments(),
    &dispatcher,
  )]);
  assert_eq!(
    acknowledge_update(&mut updates)
      .await
      .update_sequence_number,
    1
  );

  // The attachment goes out under the well-known artifact type, from the handler's file.
  let (path, type_id, content_type) = upload_rx.recv().await.unwrap();
  assert_eq!(path, trace);
  assert_eq!(type_id, "stack_sampling_trace");
  assert_eq!(
    content_type.as_deref(),
    Some("application/vnd.bitdrift.perfetto-trace")
  );
  let completed = acknowledge_update(&mut updates).await;
  assert_eq!(completed.update_sequence_number, 2);
  assert!(matches!(
    completed.update_type,
    Some(device_command_update::Update_type::Completed(_))
  ));

  // The handler saw the well-known key, the command ID and the arguments untouched.
  let invocations = handler.invocations.lock();
  assert_eq!(invocations.len(), 1);
  assert_eq!(
    invocations[0].registered_command_id,
    STACK_SAMPLING_COMMAND_KEY
  );
  assert_eq!(invocations[0].command_id, Some(command_id));
  assert_eq!(invocations[0].arguments, stack_sampling_arguments());
}

#[tokio::test]
async fn direct_stack_sampling_without_a_platform_handler_fails_as_unsupported() {
  let (directory, _runtime, policy) = command_runtime();
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    RegisteredCommandDispatcher::default(),
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  dispatcher.dispatch_configuration(vec![stack_sampling_request(
    Uuid::new_v4(),
    HashMap::new(),
    &dispatcher,
  )]);
  let update = acknowledge_update(&mut updates).await;
  assert_eq!(update.update_sequence_number, 1);
  assert_eq!(failed_error(update), "command unknown");
}

#[tokio::test]
async fn direct_stack_sampling_staging_failure_deletes_the_handed_over_file() {
  let (directory, _runtime, policy) = command_runtime();
  let trace = write_trace(directory.path());
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(
    &registry,
    RecordingHandler::new(completed_attachment(
      UploadSource::Path(trace.clone()),
      None,
    )),
  );
  let mut artifacts = MockClient::new();
  artifacts
    .expect_enqueue_command_upload()
    .times(1)
    .returning(|source, _, _, _, _, _, _, _, _| {
      let UploadSource::Path(path) = source else {
        panic!("expected path")
      };
      fs::remove_file(path).unwrap();
      Err(bd_artifact_upload::EnqueueError::QueueFull)
    });
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(artifacts),
  );
  dispatcher.dispatch_configuration(vec![stack_sampling_request(
    Uuid::new_v4(),
    HashMap::new(),
    &dispatcher,
  )]);
  acknowledge_update(&mut updates).await;
  let update = acknowledge_update(&mut updates).await;
  assert_eq!(update.update_sequence_number, 2);
  assert_eq!(failed_error(update), "upload queue full");
  assert!(!trace.exists(), "a file the uploader never took is deleted");
}

#[tokio::test]
async fn direct_stack_sampling_upload_failure_after_persistence_leaves_the_path_alone() {
  let (directory, _runtime, policy) = command_runtime();
  let trace = write_trace(directory.path());
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(
    &registry,
    RecordingHandler::new(completed_attachment(
      UploadSource::Path(trace.clone()),
      None,
    )),
  );
  let mut artifacts = MockClient::new();
  artifacts
    .expect_enqueue_command_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, _, persisted_tx, completion_tx| {
      // The uploader persisted (and, in production, deleted) the source before the upload failed.
      persisted_tx.unwrap().send(Ok(5)).unwrap();
      completion_tx
        .unwrap()
        .send(Err("upload rejected".to_string()))
        .unwrap();
      Ok(Uuid::new_v4())
    });
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(artifacts),
  );
  dispatcher.dispatch_configuration(vec![stack_sampling_request(
    Uuid::new_v4(),
    HashMap::new(),
    &dispatcher,
  )]);
  acknowledge_update(&mut updates).await;
  let update = acknowledge_update(&mut updates).await;
  assert_eq!(update.update_sequence_number, 2);
  assert_eq!(failed_error(update), "upload rejected");
  // A later capture may reuse the name; the core must not delete what the uploader owns.
  assert!(trace.exists());
}

#[tokio::test(start_paused = true)]
async fn direct_stack_sampling_uses_its_own_execution_timeout() {
  let (directory, _runtime, policy) = command_runtime();
  let (handler, mut started_rx, dropped_rx) = pending_handler();
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(&registry, handler);
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(MockClient::new()),
  );
  dispatcher.dispatch_configuration(vec![stack_sampling_request(
    Uuid::new_v4(),
    HashMap::new(),
    &dispatcher,
  )]);
  acknowledge_update(&mut updates).await;
  started_rx.recv().await.unwrap();

  // The generic five second deadline does not apply to the built-in capture...
  tokio::time::advance(Duration::seconds(6).unsigned_abs()).await;
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
  // ...its own fifteen second one does.
  tokio::time::advance(Duration::seconds(10).unsigned_abs()).await;
  assert_timeout_update(acknowledge_update(&mut updates).await);
  dropped_rx.await.unwrap();
}

#[tokio::test]
async fn cancelling_direct_file_upload_after_persistence_preserves_reused_path() {
  let (directory, _runtime, policy) = command_runtime();
  let trace = write_trace(directory.path());
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(
    &registry,
    RecordingHandler::new(completed_attachment(
      UploadSource::Path(trace.clone()),
      None,
    )),
  );
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let persisted_tx = Mutex::new(Some(persisted_tx));
  let mut artifacts = MockClient::new();
  artifacts
    .expect_enqueue_command_upload()
    .times(1)
    .returning(move |source, _, _, _, _, _, _, persistence, completion| {
      let UploadSource::Path(path) = source else {
        panic!("expected path")
      };
      fs::remove_file(&path).unwrap();
      fs::write(path, b"next capture").unwrap();
      persistence.unwrap().send(Ok(5)).unwrap();
      persisted_tx
        .lock()
        .take()
        .unwrap()
        .send(completion.unwrap())
        .unwrap();
      Ok(Uuid::new_v4())
    });
  let (dispatcher, mut updates) = direct_dispatcher(
    directory.path(),
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    Arc::new(artifacts),
  );
  dispatcher.dispatch_configuration(vec![stack_sampling_request(
    Uuid::new_v4(),
    HashMap::new(),
    &dispatcher,
  )]);
  acknowledge_update(&mut updates).await;
  let _upload_ack = persisted_rx.await.unwrap();
  let tasks = dispatcher.execution_tasks.lock().take().unwrap();
  for task in tasks {
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
  }
  assert_eq!(fs::read(trace).unwrap(), b"next capture");
}

#[tokio::test]
async fn serialized_unknown_builtin_id_executes_without_enum_knowledge() {
  let (directory, runtime, policy) = command_runtime();
  let registry = RegisteredCommandDispatcher::default();
  registry
    .register(CommandRegistration::Builtin {
      command_type: FUTURE_BUILTIN_COMMAND_TYPE,
      key: "future_builtin".into(),
      artifact_type_id: "future_artifact".into(),
      timeout: None,
      handler: Arc::new(Handler),
    })
    .unwrap();
  let selector = WorkflowCommandSelector {
    command_selector: Some(workflow_command_selector::Command_selector::BuiltinCommand(
      workflow_command_selector::BuiltinCommand {
        type_: EnumOrUnknown::from_i32(FUTURE_BUILTIN_COMMAND_TYPE),
        ..Default::default()
      },
    )),
    ..Default::default()
  };
  let decoded =
    WorkflowCommandSelector::parse_from_bytes(&selector.write_to_bytes().unwrap()).unwrap();
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
  );
  assert!(matches!(
    dispatcher.execute(decoded, "session".into()).await,
    WorkflowCommandOutcome::Succeeded { .. }
  ));
}

#[test]
fn unconsumed_native_result_deletes_owned_file() {
  let (directory, _runtime, _policy) = command_runtime();
  let path = write_trace(directory.path());
  let (sender, receiver) = oneshot::channel();
  assert!(
    sender
      .send(super::CommandResultGuard::new(completed_attachment(
        UploadSource::Path(path.clone()),
        None,
      )))
      .is_ok()
  );
  assert!(path.exists());
  drop(receiver);
  assert!(!path.exists());
}

#[tokio::test]
async fn workflow_stack_sampling_is_passed_through_and_the_handed_over_file_is_removed() {
  let (directory, runtime, policy) = command_runtime();
  let trace = write_trace(directory.path());
  let handler = RecordingHandler::new(completed_attachment(
    UploadSource::Path(trace.clone()),
    Some("application/vnd.bitdrift.perfetto-trace".into()),
  ));
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(&registry, handler.clone());
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
  );

  let WorkflowCommandOutcome::SucceededWithAttachment { fields, .. } = dispatcher
    .execute(
      stack_sampling_selector(stack_sampling_arguments()),
      "session".into(),
    )
    .await
  else {
    panic!("expected a workflow attachment");
  };
  assert_eq!(fields.get("handler_field"), Some(&"handler_value".into()));
  assert!(
    !trace.exists(),
    "the workflow store keeps its own copy, so the handed over file is removed"
  );
  let invocations = handler.invocations.lock();
  assert_eq!(
    invocations[0].registered_command_id,
    STACK_SAMPLING_COMMAND_KEY
  );
  assert_eq!(invocations[0].command_id, None);
  assert_eq!(invocations[0].session_id, "session");
  assert_eq!(invocations[0].arguments, stack_sampling_arguments());
}

#[tokio::test]
async fn workflow_stack_sampling_without_a_platform_handler_fails_as_unsupported() {
  let (directory, runtime, policy) = command_runtime();
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    RegisteredCommandDispatcher::default(),
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
  );
  let WorkflowCommandOutcome::Failed { message, .. } = dispatcher
    .execute(stack_sampling_selector(HashMap::new()), "session".into())
    .await
  else {
    panic!("expected a failure");
  };
  assert_eq!(message.as_deref(), Some("command unknown"));
}

#[tokio::test]
async fn workflow_owned_file_admission_failure_deletes_source() {
  let (directory, runtime, policy) = command_runtime();
  runtime
    .update_snapshot(make_simple_update(vec![
      (MaxBytes::path(), ValueKind::Int(1)),
      (MaxOwnedFiles::path(), ValueKind::Int(1)),
    ]))
    .await
    .unwrap();
  let trace = write_trace(directory.path());
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(
    &registry,
    RecordingHandler::new(completed_attachment(
      UploadSource::Path(trace.clone()),
      None,
    )),
  );
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
  );

  let outcome = dispatcher
    .execute(stack_sampling_selector(HashMap::new()), "session".into())
    .await;
  let WorkflowCommandOutcome::Failed { message, fields } = outcome else {
    panic!("expected workflow attachment admission failure");
  };
  assert!(message.unwrap().contains("within size limit"));
  assert_eq!(fields.get("handler_field"), Some(&"handler_value".into()));
  assert!(!trace.exists());

  let store = dispatcher.executor.attachment_store.get().await.unwrap();
  let next = store
    .admit(UploadSource::Bytes(vec![1]), None)
    .await
    .unwrap();
  store.release(next.id).await.unwrap();
  assert_eq!(
    fs::read_dir(directory.path().join("workflow-attachments"))
      .unwrap()
      .count(),
    0
  );
}

#[tokio::test]
async fn workflow_owned_file_admission_cancellation_deletes_source() {
  let (directory, runtime, policy) = command_runtime();
  runtime
    .update_snapshot(make_simple_update(vec![(
      MaxOwnedFiles::path(),
      ValueKind::Int(1),
    )]))
    .await
    .unwrap();
  let trace = write_trace(directory.path());
  let registry = RegisteredCommandDispatcher::default();
  register_stack_sampling(
    &registry,
    RecordingHandler::new(completed_attachment(
      UploadSource::Path(trace.clone()),
      None,
    )),
  );
  let dispatcher = workflow_dispatcher(
    directory.path(),
    runtime,
    policy,
    registry,
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
  );
  let store = dispatcher.executor.attachment_store.get().await.unwrap();
  let permit = store.block_admissions_for_test().await;
  let mut execution =
    Box::pin(dispatcher.execute(stack_sampling_selector(HashMap::new()), "session".into()));
  assert!(futures_util::poll!(&mut execution).is_pending());
  assert!(trace.exists());

  drop(execution);
  assert!(!trace.exists());
  drop(permit);

  let next = store
    .admit(UploadSource::Bytes(vec![1]), None)
    .await
    .unwrap();
  store.release(next.id).await.unwrap();
  assert_eq!(
    fs::read_dir(directory.path().join("workflow-attachments"))
      .unwrap()
      .count(),
    0
  );
}

fn command_runtime() -> (TempDir, Arc<ConfigLoader>, CommandExecutionPolicy) {
  fs::create_dir_all(".tmp").unwrap();
  let directory = TempDir::with_prefix_in("command", ".tmp").unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let policy = CommandExecutionPolicy::new(&runtime);
  (directory, runtime, policy)
}

#[tokio::test]
async fn registered_command_execution_preserves_success() {
  let (_directory, _runtime, policy) = command_runtime();
  let result = policy
    .execute_registered(
      Arc::new(Handler),
      CommandInvocation {
        command_id: None,
        registered_command_id: "fast".into(),
        arguments: HashMap::new(),
        session_id: "session".into(),
      },
      None,
    )
    .await;
  assert!(matches!(
    result,
    CommandResult::Completed {
      attachment: None,
      ..
    }
  ));
}

#[tokio::test(start_paused = true)]
async fn command_execution_timeout_drops_future() {
  let (_directory, _runtime, policy) = command_runtime();
  let (started_tx, started_rx) = oneshot::channel();
  let (dropped_tx, mut dropped_rx) = oneshot::channel();
  let execution = tokio::spawn(async move {
    policy
      .execute("blocking", async move {
        let _drop_signal = DropSignal(Some(dropped_tx));
        started_tx.send(()).unwrap();
        pending::<Result<(), CommandError>>().await
      })
      .await
  });
  started_rx.await.unwrap();
  tokio::time::advance(Duration::seconds(4).unsigned_abs()).await;
  assert!(!execution.is_finished());
  assert_eq!(
    dropped_rx.try_recv(),
    Err(oneshot::error::TryRecvError::Empty)
  );
  tokio::time::advance(Duration::seconds(1).unsigned_abs()).await;
  assert_eq!(execution.await.unwrap(), Err(CommandError::Timeout));
  dropped_rx.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancelling_command_execution_drops_future() {
  let (_directory, _runtime, policy) = command_runtime();
  let (started_tx, started_rx) = oneshot::channel();
  let (dropped_tx, dropped_rx) = oneshot::channel();
  let execution = tokio::spawn(async move {
    policy
      .execute("blocking", async move {
        let _drop_signal = DropSignal(Some(dropped_tx));
        started_tx.send(()).unwrap();
        pending::<Result<(), CommandError>>().await
      })
      .await
  });
  started_rx.await.unwrap();
  execution.abort();
  assert!(execution.await.unwrap_err().is_cancelled());
  dropped_rx.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn command_execution_timeout_snapshots_runtime_per_invocation() {
  let (_directory, runtime, policy) = command_runtime();
  let (started_tx, started_rx) = oneshot::channel();
  let first_policy = policy.clone();
  let first = tokio::spawn(async move {
    first_policy
      .execute("first", async move {
        started_tx.send(()).unwrap();
        pending::<Result<(), CommandError>>().await
      })
      .await
  });
  started_rx.await.unwrap();
  runtime
    .update_snapshot(make_simple_update(vec![(
      ExecutionTimeoutFlag::path(),
      ValueKind::Int(1000),
    )]))
    .await
    .unwrap();
  let (started_tx, started_rx) = oneshot::channel();
  let second = tokio::spawn(async move {
    policy
      .execute("second", async move {
        started_tx.send(()).unwrap();
        pending::<Result<(), CommandError>>().await
      })
      .await
  });
  started_rx.await.unwrap();
  tokio::time::advance(Duration::seconds(1).unsigned_abs()).await;
  assert_eq!(second.await.unwrap(), Err(CommandError::Timeout));
  assert!(!first.is_finished());
  tokio::time::advance(Duration::seconds(4).unsigned_abs()).await;
  assert_eq!(first.await.unwrap(), Err(CommandError::Timeout));
}

#[tokio::test]
async fn zero_command_execution_timeout_does_not_start_work() {
  let (_directory, runtime, policy) = command_runtime();
  runtime
    .update_snapshot(make_simple_update(vec![(
      ExecutionTimeoutFlag::path(),
      ValueKind::Int(0),
    )]))
    .await
    .unwrap();
  let calls = Arc::new(AtomicUsize::new(0));
  let execution_calls = calls.clone();
  let result = policy
    .execute("zero", async move {
      execution_calls.fetch_add(1, Ordering::SeqCst);
      Ok(())
    })
    .await;
  assert_eq!(result, Err(CommandError::Timeout));
  assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn command_execution_preserves_ready_result_at_timeout_boundary() {
  let (_directory, _runtime, policy) = command_runtime();
  let (started_tx, started_rx) = oneshot::channel();
  let execution = tokio::spawn(async move {
    policy
      .execute("boundary", async move {
        let completion = tokio::time::sleep(Duration::seconds(5).unsigned_abs());
        started_tx.send(()).unwrap();
        completion.await;
        Ok(())
      })
      .await
  });
  started_rx.await.unwrap();
  tokio::time::advance(Duration::seconds(5).unsigned_abs()).await;
  assert_eq!(execution.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn command_execution_polls_borrowed_future_in_calling_task() {
  let (_directory, _runtime, policy) = command_runtime();
  let command = String::from("inline");
  let caller = tokio::task::try_id();
  let result = policy
    .execute(&command, async {
      Ok((tokio::task::try_id(), command.as_str()))
    })
    .await;
  assert_eq!(result, Ok((caller, "inline")));
}

#[tokio::test]
async fn typed_command_errors_preserve_workflow_messages_and_fields() {
  let (directory, runtime, _policy) = command_runtime();
  let store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  for (error, expected) in [
    (CommandError::Timeout, "command timed out"),
    (CommandError::CommandUnknown, "command unknown"),
    (
      CommandError::MaxCommandConcurrency,
      "max command concurrency reached",
    ),
    (CommandError::AlreadyExecuting, "command already executing"),
    (
      CommandError::InvalidArguments("argument 'verbose' has an invalid value".into()),
      "argument 'verbose' has an invalid value",
    ),
    (
      CommandError::HandlerFailed("capture failed".into()),
      "capture failed",
    ),
    (
      CommandError::Other("store unavailable".into()),
      "store unavailable",
    ),
  ] {
    let update = super::failed_device_command_update("command", 1, error.clone());
    let Some(device_command_update::Update_type::Failed(failed)) = update.update_type else {
      panic!("expected a typed command failure update");
    };
    let fields = &failed.context.as_ref().unwrap().fields;
    assert_eq!(fields.len(), 1);
    assert_eq!(
      DataValue::from_proto(fields.get("error").unwrap().clone()),
      Some(expected.into())
    );
    let outcome = workflow_command_outcome(
      CommandResult::Failed {
        error,
        fields: [("reason".into(), "detail".into())].into(),
      },
      &store,
    )
    .await;
    let WorkflowCommandOutcome::Failed { message, fields } = outcome else {
      panic!("expected command failure");
    };
    assert_eq!(message.as_deref(), Some(expected));
    assert_eq!(fields, [("reason".into(), "detail".into())].into());
  }
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for Handler {
  async fn execute(&self, _invocation: CommandInvocation) -> CommandResult {
    CommandResult::Completed {
      fields: LogFields::default(),
      attachment: None,
    }
  }
}

struct ReenterOnDrop {
  dispatcher: RegisteredCommandDispatcher,
  drops: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for ReenterOnDrop {
  async fn execute(&self, _invocation: CommandInvocation) -> CommandResult {
    CommandResult::Completed {
      fields: LogFields::default(),
      attachment: None,
    }
  }
}

impl Drop for ReenterOnDrop {
  fn drop(&mut self) {
    assert!(self.dispatcher.commands.try_write().is_some());
    self
      .dispatcher
      .register(CommandRegistration::Registered(
        "reentered".to_string(),
        Arc::new(Handler),
      ))
      .unwrap();
    self.drops.fetch_add(1, Ordering::SeqCst);
  }
}

#[test]
fn replacing_or_removing_handler_allows_destructor_to_reenter() {
  let dispatcher = RegisteredCommandDispatcher::default();
  let drops = Arc::new(AtomicUsize::new(0));

  dispatcher
    .register(CommandRegistration::Registered(
      "custom".to_string(),
      Arc::new(ReenterOnDrop {
        dispatcher: dispatcher.clone(),
        drops: drops.clone(),
      }),
    ))
    .unwrap();
  dispatcher
    .register(CommandRegistration::Registered(
      "custom".to_string(),
      Arc::new(Handler),
    ))
    .unwrap();
  assert_eq!(drops.load(Ordering::SeqCst), 1);

  dispatcher
    .register(CommandRegistration::Registered(
      "custom".to_string(),
      Arc::new(ReenterOnDrop {
        dispatcher: dispatcher.clone(),
        drops: drops.clone(),
      }),
    ))
    .unwrap();
  assert!(dispatcher.unregister("custom"));
  assert_eq!(drops.load(Ordering::SeqCst), 2);
  assert!(dispatcher.get_handler("reentered").is_some());
}

#[test]
fn cloned_command_dispatchers_share_registrations() {
  let dispatcher = RegisteredCommandDispatcher::default();
  let clone = dispatcher.clone();
  let original: Arc<dyn RegisteredCommandHandler> = Arc::new(Handler);
  let replacement: Arc<dyn RegisteredCommandHandler> = Arc::new(Handler);

  dispatcher
    .register(CommandRegistration::Registered(
      "custom".to_string(),
      original.clone(),
    ))
    .unwrap();
  let selected = clone.get_handler("custom").unwrap();
  assert!(Arc::ptr_eq(&selected, &original));

  clone
    .register(CommandRegistration::Registered(
      "custom".to_string(),
      replacement.clone(),
    ))
    .unwrap();
  assert!(Arc::ptr_eq(
    &dispatcher.get_handler("custom").unwrap(),
    &replacement
  ));
  assert!(Arc::ptr_eq(&selected, &original));

  assert!(dispatcher.unregister("custom"));
  assert!(clone.get_handler("custom").is_none());
  assert!(!clone.unregister("custom"));
}

fn completed_attachment(source: UploadSource, content_type: Option<String>) -> CommandResult {
  CommandResult::Completed {
    fields: [("handler_field".into(), "handler_value".into())].into(),
    attachment: Some(CommandAttachment {
      source,
      content_type,
    }),
  }
}

#[test]
fn terminal_device_command_updates_produce_outcome_logs() {
  let command_id = Uuid::new_v4().to_string();
  let artifact_id = Uuid::new_v4();
  let started_at = OffsetDateTime::UNIX_EPOCH;
  let span = CommandSpan {
    id: command_id.clone(),
    started_at: Some(started_at),
    command_details: Some(command_details(Some(&registered_selector()))),
  };
  let now = started_at + Duration::milliseconds(125);
  let context = super::device_command_context(
    [
      ("count".into(), DataValue::U64(7)),
      (LOG_FIELD_NAME_RESULT.into(), "spoofed".into()),
    ]
    .into(),
  );
  let completed = completed_device_command_update(
    &command_id,
    false,
    Some(context),
    artifact_attachment(artifact_id),
  );
  let log = device_command_outcome_log(
    &completed,
    Some(CommandArtifactMetadata::new(Some("image/jpeg".into()), 123)),
    &span,
    now,
  )
  .unwrap();
  assert_eq!(log.log_level, log_level::INFO);
  assert_eq!(log.log_type, LogType::SPAN);
  assert_eq!(log.message.as_str(), Some(COMMAND_OUTCOME_MESSAGE));
  assert_eq!(
    log
      .fields
      .get(LOG_FIELD_NAME_RESULT)
      .unwrap()
      .value
      .as_str(),
    Some(SPAN_RESULT_SUCCESS)
  );
  assert_eq!(log.fields.get("count").unwrap().value, DataValue::U64(7));
  let attachment = log.command_attachment.as_ref().unwrap();
  assert_eq!(attachment.source, AttachmentSource::DIRECT_COMMAND);
  assert_eq!(attachment.artifact_id, artifact_id.to_string());
  assert!(!log.fields.contains_key(LOG_FIELD_NAME_COMMAND_MESSAGE));
  assert_eq!(attachment.content_type, "image/jpeg");
  assert_eq!(attachment.size_bytes, 123);

  let failed = failed_device_command_update_with_fields(
    &command_id,
    1,
    [
      ("error".into(), "not registered".into()),
      ("reason".into(), "missing".into()),
    ]
    .into(),
  );
  let log = device_command_outcome_log(&failed, None, &span, now).unwrap();
  assert_eq!(log.log_level, log_level::ERROR);
  assert_eq!(
    log
      .fields
      .get(LOG_FIELD_NAME_RESULT)
      .unwrap()
      .value
      .as_str(),
    Some(SPAN_RESULT_FAILURE)
  );
  assert_eq!(
    log
      .fields
      .get(LOG_FIELD_NAME_COMMAND_MESSAGE)
      .unwrap()
      .value
      .as_str(),
    Some("not registered")
  );
  assert_eq!(
    log.fields.get("reason").unwrap().value.as_str(),
    Some("missing")
  );
  assert!(!log.fields.contains_key("error"));
  assert!(!log.fields.contains_key("_command_artifact_id"));
  assert!(!log.fields.contains_key("_command_artifact_content_type"));
  assert!(!log.fields.contains_key("_command_artifact_size_bytes"));

  let completed_without_attachment =
    completed_device_command_update(&command_id, false, None, no_attachment());
  let log = device_command_outcome_log(&completed_without_attachment, None, &span, now).unwrap();
  assert!(!log.fields.contains_key("_command_artifact_id"));
  assert!(!log.fields.contains_key("_command_artifact_content_type"));
  assert!(!log.fields.contains_key("_command_artifact_size_bytes"));
  assert!(
    device_command_outcome_log(
      &DeviceCommandUpdate {
        update_type: Some(device_command_update::Update_type::Accepted(
          device_command_update::Accepted::default(),
        )),
        ..Default::default()
      },
      None,
      &span,
      now
    )
    .is_none()
  );
}

#[tokio::test]
async fn workflow_screenshot_preserves_jpeg_content_type() {
  struct ScreenshotTarget;

  impl Target for ScreenshotTarget {
    fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
      completion(Ok(vec![0xff, 0xd8, 0xff, 0xd9]));
    }
  }

  let directory = tempfile::tempdir().unwrap();
  let store = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  let handler = RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget));
  let outcome = workflow_command_outcome(
    handler
      .execute(CommandInvocation {
        command_id: None,
        registered_command_id: "take_screenshot".into(),
        arguments: HashMap::new(),
        session_id: "session".into(),
      })
      .await,
    &store,
  )
  .await;
  let WorkflowCommandOutcome::SucceededWithAttachment { artifact_id, .. } = outcome else {
    panic!("expected screenshot attachment");
  };
  assert_eq!(
    store
      .get()
      .await
      .unwrap()
      .content_type(artifact_id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
  let restarted = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  assert_eq!(
    restarted
      .get()
      .await
      .unwrap()
      .content_type(artifact_id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
}

#[tokio::test]
async fn workflow_registered_handler_preserves_attachment_content_type() {
  struct AttachmentHandler;

  #[async_trait::async_trait]
  impl RegisteredCommandHandler for AttachmentHandler {
    async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
      assert_eq!(invocation.registered_command_id, "custom");
      assert_eq!(invocation.command_id, None);
      completed_attachment(
        UploadSource::Bytes(b"attachment".to_vec()),
        Some("application/vnd.example.capture".into()),
      )
    }
  }

  let directory = tempfile::tempdir().unwrap();
  let store = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  let dispatcher = RegisteredCommandDispatcher::default();
  dispatcher
    .register(CommandRegistration::Registered(
      "custom".to_string(),
      Arc::new(AttachmentHandler),
    ))
    .unwrap();
  let result = dispatcher
    .get_handler("custom")
    .unwrap()
    .execute(CommandInvocation {
      command_id: None,
      registered_command_id: "custom".to_string(),
      arguments: HashMap::new(),
      session_id: "session".to_string(),
    })
    .await;
  let outcome = workflow_command_outcome(result, &store).await;
  let WorkflowCommandOutcome::SucceededWithAttachment {
    artifact_id,
    fields,
    ..
  } = outcome
  else {
    panic!("expected registered command attachment");
  };
  assert_eq!(Some(&"handler_value".into()), fields.get("handler_field"));
  assert_eq!(
    store
      .get()
      .await
      .unwrap()
      .content_type(artifact_id)
      .await
      .unwrap()
      .as_deref(),
    Some("application/vnd.example.capture")
  );
  let restarted = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  assert_eq!(
    restarted
      .get()
      .await
      .unwrap()
      .content_type(artifact_id)
      .await
      .unwrap()
      .as_deref(),
    Some("application/vnd.example.capture")
  );
}

#[test]
fn command_artifact_metadata_defaults_content_type() {
  for content_type in [None, Some(String::new())] {
    let metadata = CommandArtifactMetadata::new(content_type, 7);
    assert_eq!(metadata.content_type, "application/octet-stream");
    assert_eq!(metadata.size_bytes, 7);
  }
}

#[tokio::test]
async fn workflow_attachment_admission_failure_reports_command_failure() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  runtime
    .update_snapshot(make_simple_update(vec![(
      (MaxBytes::path()),
      ValueKind::Int(1),
    )]))
    .await
    .unwrap();
  let store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);

  let outcome = workflow_command_outcome(
    completed_attachment(UploadSource::Bytes(vec![0; 2]), None),
    &store,
  )
  .await;

  match outcome {
    WorkflowCommandOutcome::Failed {
      message: Some(message),
      fields,
    } => {
      assert!(message.contains("workflow attachment admission failed"));
      assert_eq!(Some(&"handler_value".into()), fields.get("handler_field"));
    },
    _ => panic!("expected workflow command failure"),
  }
}

#[tokio::test]
async fn unavailable_workflow_attachment_store_reports_command_failure() {
  let directory = tempfile::tempdir().unwrap();
  let invalid_sdk_directory = directory.path().join("sdk-file");
  tokio::fs::write(&invalid_sdk_directory, b"not a directory")
    .await
    .unwrap();
  let runtime = ConfigLoader::new(&invalid_sdk_directory);
  let store = AttachmentStoreHandle::new(invalid_sdk_directory, runtime);

  let outcome = workflow_command_outcome(
    completed_attachment(UploadSource::Bytes(vec![0]), None),
    &store,
  )
  .await;

  match outcome {
    WorkflowCommandOutcome::Failed {
      message: Some(message),
      ..
    } => assert!(message.contains("workflow attachment store unavailable")),
    _ => panic!("expected workflow command failure"),
  }
}
