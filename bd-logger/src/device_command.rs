// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./device_command_test.rs"]
mod device_command_test;

use crate::workflow_attachment::AttachmentStoreHandle;
use anyhow::anyhow;
use bd_api::upload::TrackedDeviceCommandUpdate;
use bd_api::{DataUpload, TriggerUpload, TriggerUploadCompletion};
use bd_artifact_upload::UploadSource;
use bd_log_primitives::LogFields;
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
use bd_proto::protos::bdtail::bdtail_config::device_command_request::Command_type;
use bd_proto::protos::client::api::device_command_update::completed::{Attachment, attachment};
use bd_proto::protos::client::api::{
  DeviceCommandResultContext,
  DeviceCommandUpdate,
  device_command_update,
};
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::workflow::workflow_command::workflow_command_selector;
use bd_workflows::workflow::{
  WorkflowCommandCompletionToken,
  WorkflowCommandOutcome,
  WorkflowCommandRequest,
};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use uuid::Uuid;

const MAX_SCREENSHOT_BYTES: usize = 2 * 1024 * 1024;

// A device command is executed only from a freshly delivered configuration. Cached tail
// configuration can preserve log streaming, but must never replay a prior command after restart.
// TODO: Replace this with durable invocation recovery and server-owned redelivery once
// command execution and terminal updates survive SDK restart and mux reconnect.

//
// DeviceCommandDispatcher
//

pub struct DeviceCommandDispatcher {
  data_upload_tx: Sender<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  active_command_ids: Mutex<HashSet<String>>,
}

impl DeviceCommandDispatcher {
  pub fn new(
    data_upload_tx: Sender<DataUpload>,
    trigger_upload_tx: Sender<TriggerUpload>,
    session_strategy: Arc<bd_session::Strategy>,
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    handlers: HashMap<String, Arc<dyn RegisteredCommandHandler>>,
    remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
  ) -> Self {
    Self {
      data_upload_tx,
      trigger_upload_tx,
      session_strategy,
      artifact_client,
      command_dispatcher: RegisteredCommandDispatcher::new(handlers),
      remote_screenshot_capture_handler,
      active_command_ids: Mutex::default(),
    }
  }

  pub fn dispatch_configuration(&self, commands: Vec<DeviceCommandRequest>) {
    let configured_command_ids = commands
      .iter()
      .map(|command| command.command_id.to_string())
      .collect::<HashSet<_>>();
    let new_commands = {
      let mut active_command_ids = self.active_command_ids.lock();
      active_command_ids.retain(|command_id| configured_command_ids.contains(command_id));
      commands
        .into_iter()
        .filter(|command| active_command_ids.insert(command.command_id.to_string()))
        .collect::<Vec<_>>()
    };

    for command in new_commands {
      self.dispatch(command);
    }
  }

  fn dispatch(&self, command: DeviceCommandRequest) {
    let command_id = command.command_id.to_string();
    let data_upload_tx = self.data_upload_tx.clone();
    let trigger_upload_tx = self.trigger_upload_tx.clone();
    let session_strategy = self.session_strategy.clone();
    let artifact_client = self.artifact_client.clone();
    let command_dispatcher = self.command_dispatcher.clone();
    let remote_screenshot_capture_handler = self.remote_screenshot_capture_handler.clone();
    tokio::task::spawn(async move {
      if let Err(error) = execute_device_command(
        command,
        data_upload_tx,
        trigger_upload_tx,
        session_strategy,
        artifact_client,
        command_dispatcher,
        remote_screenshot_capture_handler,
      )
      .await
      {
        log::debug!("device command {command_id} execution stopped: {error}");
      }
    });
  }
}

/// An invocation of an application-defined command.
#[derive(Debug, Clone)]
pub struct CommandInvocation {
  /// Present only when the command was directly dispatched to the device.
  pub command_id: Option<Uuid>,
  pub registered_command_id: String,
  pub session_id: String,
}

/// A locally produced command attachment.
#[derive(Debug)]
pub struct CommandAttachment {
  pub source: UploadSource,
  pub type_id: String,
  pub state: LogFields,
}

/// The terminal outcome of an application-defined command.
#[derive(Debug)]
pub enum CommandResult {
  Completed {
    fields: LogFields,
    attachment: Option<CommandAttachment>,
  },
  Failed {
    error: String,
    fields: LogFields,
  },
}

/// Handles an opaque command selected by its registered command ID.
#[async_trait::async_trait]
pub trait RegisteredCommandHandler: Send + Sync {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult;
}

#[derive(Clone)]
pub struct RegisteredCommandDispatcher {
  handlers: HashMap<String, Arc<dyn RegisteredCommandHandler>>,
}

impl RegisteredCommandDispatcher {
  pub fn new(handlers: HashMap<String, Arc<dyn RegisteredCommandHandler>>) -> Self {
    Self { handlers }
  }

  pub fn has_handler(&self, registered_command_id: &str) -> bool {
    self.handlers.contains_key(registered_command_id)
  }

  pub async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
    let Some(handler) = self.handlers.get(&invocation.registered_command_id) else {
      return CommandResult::Failed {
        error: "unregistered command".to_string(),
        fields: LogFields::default(),
      };
    };

    handler.execute(invocation).await
  }
}

pub struct WorkflowCommandCompletion {
  pub token: WorkflowCommandCompletionToken,
  pub outcome: WorkflowCommandOutcome,
}

pub struct WorkflowCommandDispatcher {
  command_dispatcher: RegisteredCommandDispatcher,
  completion_tx: Sender<WorkflowCommandCompletion>,
  attachment_store: AttachmentStoreHandle,
}

impl WorkflowCommandDispatcher {
  pub fn new(
    handlers: HashMap<String, Arc<dyn RegisteredCommandHandler>>,
    completion_tx: Sender<WorkflowCommandCompletion>,
    attachment_store: AttachmentStoreHandle,
  ) -> Self {
    Self {
      command_dispatcher: RegisteredCommandDispatcher::new(handlers),
      completion_tx,
      attachment_store,
    }
  }

  pub fn dispatch(&self, request: &WorkflowCommandRequest) {
    let token = request.completion_token();
    let completion_tx = self.completion_tx.clone();
    let registered_command_id = match &request.command_selector.command_selector {
      Some(workflow_command_selector::Command_selector::RegisteredCommand(command)) => {
        Some(command.registered_command_id.clone())
      },
      Some(workflow_command_selector::Command_selector::BuiltinCommand(_)) | None => None,
    };
    let command_dispatcher = self.command_dispatcher.clone();
    let attachment_store = self.attachment_store.clone();
    let session_id = request.session_id.clone();

    tokio::task::spawn(async move {
      let outcome = match registered_command_id {
        Some(registered_command_id) if command_dispatcher.has_handler(&registered_command_id) => {
          let execution = tokio::task::spawn(async move {
            command_dispatcher
              .execute(CommandInvocation {
                command_id: None,
                registered_command_id,
                session_id,
              })
              .await
          });
          match execution.await {
            Ok(result) => workflow_command_outcome(result, &attachment_store).await,
            Err(error) if error.is_panic() => {
              workflow_command_failure("workflow command handler panicked")
            },
            Err(_) => workflow_command_failure("workflow command handler stopped"),
          }
        },
        Some(_) => workflow_command_failure("unregistered workflow command"),
        None => workflow_command_failure("unsupported workflow command"),
      };
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
}

fn workflow_command_failure(message: &str) -> WorkflowCommandOutcome {
  WorkflowCommandOutcome::Failed {
    message: Some(message.to_string()),
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
        match attachment_store.get().await {
          Ok(store) => match store.admit(attachment.source).await {
            Ok(admitted) => {
              return WorkflowCommandOutcome::SucceededWithAttachment {
                message: None,
                fields,
                artifact_id: admitted.id,
              };
            },
            Err(error) => {
              log::warn!("workflow attachment admission failed: {error}");
              return WorkflowCommandOutcome::Failed {
                message: Some(format!("workflow attachment admission failed: {error}")),
                fields,
              };
            },
          },
          Err(error) => {
            log::warn!("workflow attachment store unavailable: {error}");
            return WorkflowCommandOutcome::Failed {
              message: Some(format!("workflow attachment store unavailable: {error}")),
              fields,
            };
          },
        }
      }
      WorkflowCommandOutcome::Succeeded {
        message: None,
        fields,
      }
    },
    CommandResult::Failed { error, fields } => WorkflowCommandOutcome::Failed {
      message: Some(error),
      fields,
    },
  }
}

async fn execute_device_command(
  command: DeviceCommandRequest,
  data_upload_tx: Sender<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
) -> anyhow::Result<()> {
  let command_id = command.command_id.to_string();
  match command.command_type {
    Some(Command_type::DumpDeviceBuffer(_)) => {
      execute_buffer_dump_device_command(
        command_id,
        data_upload_tx,
        trigger_upload_tx,
        session_strategy,
      )
      .await
    },
    Some(Command_type::CommandSelector(selector)) => match selector.command_selector {
      Some(workflow_command_selector::Command_selector::RegisteredCommand(command)) => {
        execute_custom_device_command(
          command_id,
          command.registered_command_id.clone(),
          data_upload_tx,
          session_strategy,
          artifact_client,
          command_dispatcher,
        )
        .await
      },
      Some(workflow_command_selector::Command_selector::BuiltinCommand(command)) => {
        match command.command_type {
          Some(workflow_command_selector::builtin_command::Command_type::TakeScreenshot(_)) => {
            execute_screenshot_device_command(
              command_id,
              data_upload_tx,
              session_strategy,
              artifact_client,
              remote_screenshot_capture_handler,
            )
            .await
          },
          None => {
            send_device_command_update(
              &data_upload_tx,
              failed_device_command_update(&command_id, 1, "unsupported device command"),
            )
            .await
          },
        }
      },
      None => {
        send_device_command_update(
          &data_upload_tx,
          failed_device_command_update(&command_id, 1, "unsupported device command"),
        )
        .await
      },
    },
    _ => {
      send_device_command_update(
        &data_upload_tx,
        failed_device_command_update(&command_id, 1, "unsupported device command"),
      )
      .await
    },
  }
}

async fn execute_screenshot_device_command(
  command_id: String,
  data_upload_tx: Sender<DataUpload>,
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
) -> anyhow::Result<()> {
  send_device_command_update(
    &data_upload_tx,
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
        &data_upload_tx,
        failed_device_command_update(&command_id, 2, &error.to_string()),
      )
      .await?;
      return Ok(());
    },
  };
  let screenshot = match remote_screenshot_capture_handler.capture().await {
    Ok(screenshot) if let Err(error) = validate_screenshot(&screenshot) => {
      send_device_command_update(
        &data_upload_tx,
        failed_device_command_update(&command_id, 2, error),
      )
      .await?;
      return Ok(());
    },
    Ok(screenshot) => screenshot,
    Err(error) => {
      send_device_command_update(
        &data_upload_tx,
        failed_device_command_update(&command_id, 2, &error),
      )
      .await?;
      return Ok(());
    },
  };

  let attachment = CommandAttachment {
    source: UploadSource::Bytes(screenshot),
    type_id: "screenshot".to_string(),
    state: LogFields::default(),
  };
  let update =
    match stage_device_command_attachment(attachment, &command_id, &session_id, artifact_client)
      .await
    {
      Ok(artifact_id) => {
        completed_device_command_update(&command_id, false, None, artifact_attachment(artifact_id))
      },
      Err(error) => failed_device_command_update(&command_id, 2, &error.to_string()),
    };
  send_device_command_update(&data_upload_tx, update).await
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
  data_upload_tx: Sender<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,
  session_strategy: Arc<bd_session::Strategy>,
) -> anyhow::Result<()> {
  let session_id = match session_strategy.session_id() {
    Ok(session_id) => session_id.to_string(),
    Err(error) => {
      send_device_command_update(
        &data_upload_tx,
        failed_device_command_update(&command_id, 1, &error.to_string()),
      )
      .await?;
      return Ok(());
    },
  };
  let (trigger_upload, admission_rx, completion_rx) =
    TriggerUpload::new_device_command_with_completion(Vec::new(), command_id.clone(), session_id);
  if trigger_upload_tx.send(trigger_upload).await.is_err() {
    send_device_command_update(
      &data_upload_tx,
      failed_device_command_update(&command_id, 1, "trigger upload manager is unavailable"),
    )
    .await?;
    return Ok(());
  }

  let Ok(admission) = admission_rx.await else {
    send_device_command_update(
      &data_upload_tx,
      failed_device_command_update(&command_id, 1, "buffer dump upload could not be prepared"),
    )
    .await?;
    return Ok(());
  };
  send_device_command_update(
    &data_upload_tx,
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
    Ok(TriggerUploadCompletion::Failed) => {
      failed_device_command_update(&command_id, 2, "buffer dump upload failed")
    },
    Err(_) => failed_device_command_update(&command_id, 2, "buffer dump upload did not complete"),
  };
  send_device_command_update(&data_upload_tx, update).await
}

async fn execute_custom_device_command(
  command_id: String,
  registered_command_id: String,
  data_upload_tx: Sender<DataUpload>,
  session_strategy: Arc<bd_session::Strategy>,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  command_dispatcher: RegisteredCommandDispatcher,
) -> anyhow::Result<()> {
  let Ok(command_id_uuid) = Uuid::parse_str(&command_id) else {
    send_device_command_update(
      &data_upload_tx,
      failed_device_command_update(&command_id, 1, "invalid device command id"),
    )
    .await?;
    return Ok(());
  };
  if !command_dispatcher.has_handler(&registered_command_id) {
    send_device_command_update(
      &data_upload_tx,
      failed_device_command_update(&command_id, 1, "unregistered device command"),
    )
    .await?;
    return Ok(());
  }

  send_device_command_update(
    &data_upload_tx,
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
        &data_upload_tx,
        failed_device_command_update(&command_id, 2, &error.to_string()),
      )
      .await?;
      return Ok(());
    },
  };
  let result = command_dispatcher
    .execute(CommandInvocation {
      command_id: Some(command_id_uuid),
      registered_command_id,
      session_id: session_id.clone(),
    })
    .await;

  let update = match result {
    CommandResult::Completed { fields, attachment } => {
      let attachment = match attachment {
        Some(attachment) => {
          match stage_device_command_attachment(
            attachment,
            &command_id,
            &session_id,
            artifact_client,
          )
          .await
          {
            Ok(artifact_id) => artifact_attachment(artifact_id),
            Err(error) => {
              return send_device_command_update(
                &data_upload_tx,
                failed_device_command_update(&command_id, 2, &error.to_string()),
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
      fields.insert("error".into(), error.into());
      failed_device_command_update_with_fields(&command_id, 2, fields)
    },
  };
  send_device_command_update(&data_upload_tx, update).await
}

async fn stage_device_command_attachment(
  attachment: CommandAttachment,
  command_id: &str,
  session_id: &str,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
) -> anyhow::Result<Uuid> {
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let (completion_tx, completion_rx) = oneshot::channel();
  let artifact_id = artifact_client.enqueue_command_upload(
    attachment.source,
    attachment.type_id,
    attachment.state,
    None,
    session_id.to_string(),
    Vec::new(),
    command_id.to_string(),
    Some(persisted_tx),
    Some(completion_tx),
  )?;
  persisted_rx
    .await
    .map_err(|_| anyhow!("device command attachment persistence was interrupted"))??;
  match completion_rx.await {
    Ok(Ok(())) => Ok(artifact_id),
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
  error: &str,
) -> DeviceCommandUpdate {
  failed_device_command_update_with_fields(
    command_id,
    update_sequence_number,
    [("error".into(), error.to_string().into())].into(),
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

async fn send_device_command_update(
  data_upload_tx: &Sender<DataUpload>,
  update: DeviceCommandUpdate,
) -> anyhow::Result<()> {
  let (update, response_rx) = TrackedDeviceCommandUpdate::new(update.command_id.clone(), update);
  data_upload_tx
    .send(DataUpload::DeviceCommandUpdate(update))
    .await
    .map_err(|_| anyhow!("device command update channel closed"))?;
  response_rx
    .await
    .map_err(|_| anyhow!("device command update was not acknowledged"))
}
