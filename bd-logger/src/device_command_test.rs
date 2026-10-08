// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  CommandAttachment,
  CommandError,
  CommandExecutionPolicy,
  CommandInvocation,
  CommandResult,
  DeviceCommandDispatcher,
  RegisteredCommandDispatcher,
  RegisteredCommandHandler,
  WorkflowCommandDispatcher,
  artifact_attachment,
  completed_device_command_update,
  device_command_outcome_log,
  failed_device_command_update_with_fields,
  no_attachment,
  workflow_builtin_command_outcome,
  workflow_command_outcome,
};
use crate::async_log_buffer::Sender as LogSender;
use crate::workflow_attachment::AttachmentStoreHandle;
use bd_api::DataUpload;
use bd_artifact_upload::{Client, MockClient, UploadSource};
use bd_event_buffer::{EventBuffer, EventBufferLimits};
use bd_log_primitives::{DataValue, LogFields, log_level};
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
use bd_proto::protos::client::api::{DeviceCommandUpdate, device_command_update};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::workflow::workflow_command::{
  WellKnownCommandType,
  WorkflowCommandSelector,
  workflow_command_selector,
};
use bd_runtime::runtime::attachment::MaxBytes;
use bd_runtime::runtime::device_command::ExecutionTimeoutFlag;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session::test::no_timeout;
use bd_session_replay::{
  DeviceCommandScreenshotCompletion,
  RemoteScreenshotCaptureHandler,
  Target,
};
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_workflows::workflow::{CommandArtifactMetadata, WorkflowCommandOutcome};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::future::pending;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;
use time::Duration;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

struct Handler;

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

impl Target for ScreenshotTarget {
  fn capture_screen(&self) {}
}

struct DeferredScreenshotTarget {
  started_tx: mpsc::UnboundedSender<()>,
  completions: Mutex<VecDeque<DeviceCommandScreenshotCompletion>>,
}

impl Target for DeferredScreenshotTarget {
  fn capture_screen(&self) {}

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
  let (upload_tx, upload_rx) = mpsc::channel(16);
  let (trigger_tx, _trigger_rx) = mpsc::channel(1);
  let logs = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 100_000,
    total_limit_bytes: 100_000,
  });
  (
    DeviceCommandDispatcher::new(
      upload_tx,
      LogSender::from_event_buffer(logs),
      trigger_tx,
      no_timeout(directory).strategy(),
      artifacts,
      registry,
      policy,
      screenshot_handler,
    ),
    upload_rx,
  )
}

fn workflow_dispatcher(
  directory: &Path,
  runtime: Arc<ConfigLoader>,
  policy: CommandExecutionPolicy,
  registry: RegisteredCommandDispatcher,
  screenshot_handler: RemoteScreenshotCaptureHandler,
) -> WorkflowCommandDispatcher {
  let (completion_tx, _completion_rx) = mpsc::channel(16);
  WorkflowCommandDispatcher::new(
    registry,
    policy,
    completion_tx,
    AttachmentStoreHandle::new(directory.to_owned(), runtime),
    screenshot_handler,
  )
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
  registry.register("pending".into(), handler);
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
    ..Default::default()
  }]);
  let accepted = acknowledge_update(&mut updates).await;
  assert_eq!(accepted.update_sequence_number, 1);
  assert!(matches!(
    accepted.update_type,
    Some(device_command_update::Update_type::Accepted(_))
  ));
  started_rx.recv().await.unwrap();
  tokio::time::advance(Duration::seconds(5).unsigned_abs()).await;
  assert_timeout_update(acknowledge_update(&mut updates).await);
  dropped_rx.await.unwrap();
  assert_eq!(
    updates.try_recv().unwrap_err(),
    mpsc::error::TryRecvError::Empty
  );
}

#[tokio::test(start_paused = true)]
async fn workflow_registered_command_timeout_reports_failure() {
  let (directory, runtime, policy) = command_runtime();
  let (handler, mut started_rx, dropped_rx) = pending_handler();
  let registry = RegisteredCommandDispatcher::default();
  registry.register("pending".into(), handler);
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
  registry.register("pending".into(), Arc::new(AttachmentHandler));
  let (upload_started_tx, mut upload_started_rx) = mpsc::unbounded_channel();
  let artifact_id = Uuid::new_v4();
  let mut artifacts = MockClient::new();
  artifacts
    .expect_enqueue_command_upload()
    .times(1)
    .returning(move |_, _, _, _, _, _, _, _, persisted_tx, completion_tx| {
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
    assert!(self.dispatcher.handlers.try_write().is_some());
    self
      .dispatcher
      .register("reentered".to_string(), Arc::new(Handler));
    self.drops.fetch_add(1, Ordering::SeqCst);
  }
}

#[test]
fn replacing_or_removing_handler_allows_destructor_to_reenter() {
  let dispatcher = RegisteredCommandDispatcher::default();
  let drops = Arc::new(AtomicUsize::new(0));

  dispatcher.register(
    "custom".to_string(),
    Arc::new(ReenterOnDrop {
      dispatcher: dispatcher.clone(),
      drops: drops.clone(),
    }),
  );
  dispatcher.register("custom".to_string(), Arc::new(Handler));
  assert_eq!(drops.load(Ordering::SeqCst), 1);

  dispatcher.register(
    "custom".to_string(),
    Arc::new(ReenterOnDrop {
      dispatcher: dispatcher.clone(),
      drops: drops.clone(),
    }),
  );
  assert!(dispatcher.unregister("custom"));
  assert_eq!(drops.load(Ordering::SeqCst), 2);
  assert!(dispatcher.get_handler("reentered").is_some());
}

#[test]
fn cloned_command_dispatchers_share_registrations() {
  let dispatcher = RegisteredCommandDispatcher::new(HashMap::new());
  let clone = dispatcher.clone();
  let original: Arc<dyn RegisteredCommandHandler> = Arc::new(Handler);
  let replacement: Arc<dyn RegisteredCommandHandler> = Arc::new(Handler);

  dispatcher.register("custom".to_string(), original.clone());
  let selected = clone.get_handler("custom").unwrap();
  assert!(Arc::ptr_eq(&selected, &original));

  clone.register("custom".to_string(), replacement.clone());
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
      state: LogFields::default(),
    }),
  }
}

#[test]
fn terminal_device_command_updates_produce_outcome_logs() {
  let command_id = Uuid::new_v4().to_string();
  let artifact_id = Uuid::new_v4();
  let context = super::device_command_context(
    [
      ("count".into(), DataValue::U64(7)),
      ("_command_status".into(), "spoofed".into()),
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
  )
  .unwrap();
  assert_eq!(log.log_level, log_level::INFO);
  assert_eq!(log.log_type, LogType::NORMAL);
  assert_eq!(log.message.as_str(), Some("Command completed"));
  assert_eq!(
    log.fields.get("_command_status").unwrap().value.as_str(),
    Some("success")
  );
  assert_eq!(
    log.fields.get("_command_id").unwrap().value.as_str(),
    Some(command_id.as_str())
  );
  assert_eq!(log.fields.get("count").unwrap().value, DataValue::U64(7));
  assert_eq!(
    log
      .fields
      .get("_command_artifact_id")
      .unwrap()
      .value
      .as_str(),
    Some(artifact_id.to_string().as_str())
  );
  assert!(!log.fields.contains_key("_command_message"));
  assert_eq!(
    log
      .fields
      .get("_command_artifact_content_type")
      .unwrap()
      .value
      .as_str(),
    Some("image/jpeg")
  );
  assert_eq!(
    log
      .fields
      .get("_command_artifact_size_bytes")
      .unwrap()
      .value,
    DataValue::U64(123)
  );

  let failed = failed_device_command_update_with_fields(
    &command_id,
    1,
    [
      ("error".into(), "not registered".into()),
      ("reason".into(), "missing".into()),
      ("_command_id".into(), "spoofed".into()),
    ]
    .into(),
  );
  let log = device_command_outcome_log(&failed, None).unwrap();
  assert_eq!(log.log_level, log_level::ERROR);
  assert_eq!(
    log.fields.get("_command_status").unwrap().value.as_str(),
    Some("failure")
  );
  assert_eq!(
    log.fields.get("_command_message").unwrap().value.as_str(),
    Some("not registered")
  );
  assert_eq!(
    log.fields.get("reason").unwrap().value.as_str(),
    Some("missing")
  );
  assert_eq!(
    log.fields.get("_command_id").unwrap().value.as_str(),
    Some(command_id.as_str())
  );
  assert!(!log.fields.contains_key("error"));
  assert!(!log.fields.contains_key("_command_artifact_id"));
  assert!(!log.fields.contains_key("_command_artifact_content_type"));
  assert!(!log.fields.contains_key("_command_artifact_size_bytes"));

  let completed_without_attachment =
    completed_device_command_update(&command_id, false, None, no_attachment());
  let log = device_command_outcome_log(&completed_without_attachment, None).unwrap();
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
      None
    )
    .is_none()
  );
}

#[tokio::test]
async fn workflow_screenshot_preserves_jpeg_content_type() {
  struct ScreenshotTarget;

  impl Target for ScreenshotTarget {
    fn capture_screen(&self) {
      panic!("unexpected periodic screen capture");
    }

    fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
      completion(Ok(vec![0xff, 0xd8, 0xff, 0xd9]));
    }
  }

  let directory = tempfile::tempdir().unwrap();
  let store = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  let outcome = workflow_builtin_command_outcome(
    workflow_command_selector::BuiltinCommand {
      type_: WellKnownCommandType::TAKE_SCREENSHOT.into(),
      ..Default::default()
    },
    &HashMap::new(),
    RemoteScreenshotCaptureHandler::new(Arc::new(ScreenshotTarget)),
    &CommandExecutionPolicy::new(&ConfigLoader::new(directory.path())),
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
  dispatcher.register("custom".to_string(), Arc::new(AttachmentHandler));
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
