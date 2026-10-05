// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  CommandAttachment,
  CommandInvocation,
  CommandResult,
  RegisteredCommandDispatcher,
  RegisteredCommandHandler,
  artifact_attachment,
  completed_device_command_update,
  device_command_outcome_log,
  failed_device_command_update_with_fields,
  no_attachment,
  workflow_builtin_command_outcome,
  workflow_command_outcome,
};
use crate::workflow_attachment::AttachmentStoreHandle;
use bd_artifact_upload::UploadSource;
use bd_log_primitives::{DataValue, LogFields, log_level};
use bd_proto::protos::client::api::{DeviceCommandUpdate, device_command_update};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::workflow::workflow_command::{
  WellKnownCommandType,
  workflow_command_selector,
};
use bd_runtime::runtime::attachment::MaxBytes;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_session_replay::{
  DeviceCommandScreenshotCompletion,
  RemoteScreenshotCaptureHandler,
  Target,
};
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_workflows::workflow::{CommandArtifactMetadata, WorkflowCommandOutcome};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

struct Handler;

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
