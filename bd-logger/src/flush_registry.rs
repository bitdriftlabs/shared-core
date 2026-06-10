// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./flush_registry_test.rs"]
mod flush_registry_test;

use bd_api::TriggerUploadSource;
use bd_client_common::file::{
  read_compressed_protobuf_file_if_exists,
  write_compressed_protobuf_file,
};
use bd_client_common::file_system::delete_file_if_exists_async;
use bd_macros::proto_serializable;
use bd_proto::protos::workflow::workflow::workflow::action::action_flush_buffers;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const PENDING_TRIGGER_UPLOADS_FILE_NAME: &str = "pending_trigger_uploads_snapshot.1.pb";

//
// PersistedTriggerUploadLifecycle
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedTriggerUploadLifecycle {
  ReadyToUpload,
  UploadingFromBuffer,
  Completed,
  Failed,
}

//
// PersistedTriggerUploadSource
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PersistedTriggerUploadSource {
  WorkflowAction(String),
  ExplicitSessionCapture(String),
  RemoteCommand(String),
}

impl From<&TriggerUploadSource> for PersistedTriggerUploadSource {
  fn from(value: &TriggerUploadSource) -> Self {
    match value {
      TriggerUploadSource::WorkflowAction(id) => Self::WorkflowAction(id.clone()),
      TriggerUploadSource::ExplicitSessionCapture(id) => Self::ExplicitSessionCapture(id.clone()),
      TriggerUploadSource::RemoteCommand(id) => Self::RemoteCommand(id.clone()),
    }
  }
}

impl PersistedTriggerUploadSource {
  pub fn to_trigger_upload_source(&self) -> TriggerUploadSource {
    match self {
      Self::WorkflowAction(id) => TriggerUploadSource::WorkflowAction(id.clone()),
      Self::ExplicitSessionCapture(id) => TriggerUploadSource::ExplicitSessionCapture(id.clone()),
      Self::RemoteCommand(id) => TriggerUploadSource::RemoteCommand(id.clone()),
    }
  }
}

//
// PersistedTriggerUpload
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedTriggerUpload {
  pub id: String,
  pub source: PersistedTriggerUploadSource,
  pub session_id: String,
  pub buffers: Vec<PersistedTriggerUploadBufferProgress>,
  pub streaming: Option<PersistedTriggerUploadStreaming>,
  pub lifecycle: PersistedTriggerUploadLifecycle,
}

impl PersistedTriggerUpload {
  pub fn buffer_ids(&self) -> Vec<String> {
    self
      .buffers
      .iter()
      .map(|buffer| buffer.buffer_id.clone())
      .collect()
  }

  pub fn streaming_proto(&self) -> Option<action_flush_buffers::Streaming> {
    self
      .streaming
      .as_ref()
      .map(PersistedTriggerUploadStreaming::to_proto)
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedTriggerUploadBufferLifecycle {
  Pending,
  UploadingFromBuffer,
  Completed,
  Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedTriggerUploadBufferProgress {
  pub buffer_id: String,
  pub lifecycle: PersistedTriggerUploadBufferLifecycle,
  pub uploaded_batches_count: u64,
  pub uploaded_logs_count: u64,
}

impl PersistedTriggerUploadBufferProgress {
  pub fn new(buffer_id: String) -> Self {
    Self {
      buffer_id,
      lifecycle: PersistedTriggerUploadBufferLifecycle::Pending,
      uploaded_batches_count: 0,
      uploaded_logs_count: 0,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedTriggerUploadStreaming {
  pub destination_buffer_ids: Vec<String>,
  pub max_logs_count: Option<u64>,
}

impl From<&action_flush_buffers::Streaming> for PersistedTriggerUploadStreaming {
  fn from(streaming: &action_flush_buffers::Streaming) -> Self {
    let max_logs_count = streaming.termination_criteria.iter().find_map(|criterion| {
      criterion.type_.as_ref().map(
        |action_flush_buffers::streaming::termination_criterion::Type::LogsCount(logs_count)| {
          logs_count.max_logs_count
        },
      )
    });

    Self {
      destination_buffer_ids: streaming.destination_streaming_buffer_ids.clone(),
      max_logs_count,
    }
  }
}

impl PersistedTriggerUploadStreaming {
  pub fn to_proto(&self) -> action_flush_buffers::Streaming {
    let termination_criteria = self.max_logs_count.map_or_else(Vec::new, |max_logs_count| {
      vec![action_flush_buffers::streaming::TerminationCriterion {
        type_: Some(
          action_flush_buffers::streaming::termination_criterion::Type::LogsCount(
            action_flush_buffers::streaming::termination_criterion::LogsCount {
              max_logs_count,
              ..Default::default()
            },
          ),
        ),
        ..Default::default()
      }]
    });

    action_flush_buffers::Streaming {
      destination_streaming_buffer_ids: self.destination_buffer_ids.clone(),
      termination_criteria,
      ..Default::default()
    }
  }
}

//
// PendingTriggerUploadsSnapshot
//

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PendingTriggerUploadsSnapshot {
  #[field(id = 1)]
  uploads: Vec<PersistedTriggerUploadRecord>,
}

//
// PersistedTriggerUploadRecord
//

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PersistedTriggerUploadRecord {
  #[field(id = 1)]
  id: String,
  #[field(id = 2)]
  source: PersistedTriggerUploadSourceRecord,
  #[field(id = 3)]
  session_id: String,
  #[field(id = 4)]
  streaming: Option<PersistedTriggerUploadStreamingRecord>,
  #[field(id = 5)]
  buffers: Vec<PersistedTriggerUploadBufferProgressRecord>,
  #[field(id = 6)]
  lifecycle: PersistedTriggerUploadLifecycleRecord,
}

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PersistedTriggerUploadSourceRecord {
  #[field(id = 1)]
  kind: PersistedTriggerUploadSourceKindRecord,
  #[field(id = 2)]
  id: String,
}

#[proto_serializable]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PersistedTriggerUploadSourceKindRecord {
  #[field(id = 1)]
  #[field(deserialize)]
  WorkflowAction,
  #[field(id = 2)]
  #[field(deserialize)]
  ExplicitSessionCapture,
  #[field(id = 3)]
  #[field(deserialize)]
  #[default]
  RemoteCommand,
}

#[proto_serializable]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PersistedTriggerUploadLifecycleRecord {
  #[field(id = 1)]
  #[field(deserialize)]
  #[default]
  ReadyToUpload,
  #[field(id = 2)]
  #[field(deserialize)]
  UploadingFromBuffer,
  #[field(id = 3)]
  #[field(deserialize)]
  Completed,
  #[field(id = 4)]
  #[field(deserialize)]
  Failed,
}

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PersistedTriggerUploadBufferProgressRecord {
  #[field(id = 1)]
  buffer_id: String,
  #[field(id = 2)]
  lifecycle: PersistedTriggerUploadBufferLifecycleRecord,
  #[field(id = 3)]
  uploaded_batches_count: u64,
  #[field(id = 4)]
  uploaded_logs_count: u64,
}

#[proto_serializable]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PersistedTriggerUploadBufferLifecycleRecord {
  #[field(id = 1)]
  #[field(deserialize)]
  #[default]
  Pending,
  #[field(id = 2)]
  #[field(deserialize)]
  UploadingFromBuffer,
  #[field(id = 3)]
  #[field(deserialize)]
  Completed,
  #[field(id = 4)]
  #[field(deserialize)]
  Failed,
}

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PersistedTriggerUploadStreamingRecord {
  #[field(id = 1)]
  destination_buffer_ids: Vec<String>,
  #[field(id = 2)]
  max_logs_count: Option<u64>,
}

impl From<&PersistedTriggerUpload> for PersistedTriggerUploadRecord {
  fn from(upload: &PersistedTriggerUpload) -> Self {
    Self {
      id: upload.id.clone(),
      source: (&upload.source).into(),
      session_id: upload.session_id.clone(),
      streaming: upload.streaming.as_ref().map(Into::into),
      buffers: upload.buffers.iter().map(Into::into).collect(),
      lifecycle: upload.lifecycle.into(),
    }
  }
}

impl From<PersistedTriggerUploadRecord> for PersistedTriggerUpload {
  fn from(upload: PersistedTriggerUploadRecord) -> Self {
    Self {
      id: upload.id,
      source: upload.source.into(),
      session_id: upload.session_id,
      buffers: upload.buffers.into_iter().map(Into::into).collect(),
      streaming: upload.streaming.map(Into::into),
      lifecycle: upload.lifecycle.into(),
    }
  }
}

impl From<&PersistedTriggerUploadBufferProgress> for PersistedTriggerUploadBufferProgressRecord {
  fn from(buffer: &PersistedTriggerUploadBufferProgress) -> Self {
    Self {
      buffer_id: buffer.buffer_id.clone(),
      lifecycle: buffer.lifecycle.into(),
      uploaded_batches_count: buffer.uploaded_batches_count,
      uploaded_logs_count: buffer.uploaded_logs_count,
    }
  }
}

impl From<PersistedTriggerUploadBufferProgressRecord> for PersistedTriggerUploadBufferProgress {
  fn from(buffer: PersistedTriggerUploadBufferProgressRecord) -> Self {
    Self {
      buffer_id: buffer.buffer_id,
      lifecycle: buffer.lifecycle.into(),
      uploaded_batches_count: buffer.uploaded_batches_count,
      uploaded_logs_count: buffer.uploaded_logs_count,
    }
  }
}

impl From<PersistedTriggerUploadBufferLifecycle> for PersistedTriggerUploadBufferLifecycleRecord {
  fn from(lifecycle: PersistedTriggerUploadBufferLifecycle) -> Self {
    match lifecycle {
      PersistedTriggerUploadBufferLifecycle::Pending => Self::Pending,
      PersistedTriggerUploadBufferLifecycle::UploadingFromBuffer => Self::UploadingFromBuffer,
      PersistedTriggerUploadBufferLifecycle::Completed => Self::Completed,
      PersistedTriggerUploadBufferLifecycle::Failed => Self::Failed,
    }
  }
}

impl From<PersistedTriggerUploadBufferLifecycleRecord> for PersistedTriggerUploadBufferLifecycle {
  fn from(lifecycle: PersistedTriggerUploadBufferLifecycleRecord) -> Self {
    match lifecycle {
      PersistedTriggerUploadBufferLifecycleRecord::Pending => Self::Pending,
      PersistedTriggerUploadBufferLifecycleRecord::UploadingFromBuffer => Self::UploadingFromBuffer,
      PersistedTriggerUploadBufferLifecycleRecord::Completed => Self::Completed,
      PersistedTriggerUploadBufferLifecycleRecord::Failed => Self::Failed,
    }
  }
}

impl From<&PersistedTriggerUploadStreaming> for PersistedTriggerUploadStreamingRecord {
  fn from(streaming: &PersistedTriggerUploadStreaming) -> Self {
    Self {
      destination_buffer_ids: streaming.destination_buffer_ids.clone(),
      max_logs_count: streaming.max_logs_count,
    }
  }
}

impl From<PersistedTriggerUploadStreamingRecord> for PersistedTriggerUploadStreaming {
  fn from(streaming: PersistedTriggerUploadStreamingRecord) -> Self {
    Self {
      destination_buffer_ids: streaming.destination_buffer_ids,
      max_logs_count: streaming.max_logs_count,
    }
  }
}

impl From<&PersistedTriggerUploadSource> for PersistedTriggerUploadSourceRecord {
  fn from(source: &PersistedTriggerUploadSource) -> Self {
    match source {
      PersistedTriggerUploadSource::WorkflowAction(id) => Self {
        kind: PersistedTriggerUploadSourceKindRecord::WorkflowAction,
        id: id.clone(),
      },
      PersistedTriggerUploadSource::ExplicitSessionCapture(id) => Self {
        kind: PersistedTriggerUploadSourceKindRecord::ExplicitSessionCapture,
        id: id.clone(),
      },
      PersistedTriggerUploadSource::RemoteCommand(id) => Self {
        kind: PersistedTriggerUploadSourceKindRecord::RemoteCommand,
        id: id.clone(),
      },
    }
  }
}

impl From<PersistedTriggerUploadSourceRecord> for PersistedTriggerUploadSource {
  fn from(source: PersistedTriggerUploadSourceRecord) -> Self {
    match source.kind {
      PersistedTriggerUploadSourceKindRecord::WorkflowAction => Self::WorkflowAction(source.id),
      PersistedTriggerUploadSourceKindRecord::ExplicitSessionCapture => {
        Self::ExplicitSessionCapture(source.id)
      },
      PersistedTriggerUploadSourceKindRecord::RemoteCommand => Self::RemoteCommand(source.id),
    }
  }
}

impl From<PersistedTriggerUploadLifecycle> for PersistedTriggerUploadLifecycleRecord {
  fn from(lifecycle: PersistedTriggerUploadLifecycle) -> Self {
    match lifecycle {
      PersistedTriggerUploadLifecycle::ReadyToUpload => Self::ReadyToUpload,
      PersistedTriggerUploadLifecycle::UploadingFromBuffer => Self::UploadingFromBuffer,
      PersistedTriggerUploadLifecycle::Completed => Self::Completed,
      PersistedTriggerUploadLifecycle::Failed => Self::Failed,
    }
  }
}

impl From<PersistedTriggerUploadLifecycleRecord> for PersistedTriggerUploadLifecycle {
  fn from(lifecycle: PersistedTriggerUploadLifecycleRecord) -> Self {
    match lifecycle {
      PersistedTriggerUploadLifecycleRecord::ReadyToUpload => Self::ReadyToUpload,
      PersistedTriggerUploadLifecycleRecord::UploadingFromBuffer => Self::UploadingFromBuffer,
      PersistedTriggerUploadLifecycleRecord::Completed => Self::Completed,
      PersistedTriggerUploadLifecycleRecord::Failed => Self::Failed,
    }
  }
}

#[derive(Default)]
struct Inner {
  loaded: bool,
  uploads: Vec<PersistedTriggerUpload>,
}

//
// PendingTriggerUploadsStore
//

#[derive(Clone)]
pub struct PendingTriggerUploadsStore {
  inner: Arc<Mutex<Inner>>,
  state_path: Arc<PathBuf>,
}

impl PendingTriggerUploadsStore {
  pub fn new(sdk_directory: impl AsRef<Path>) -> Self {
    Self {
      inner: Arc::new(Mutex::new(Inner::default())),
      state_path: Arc::new(
        sdk_directory
          .as_ref()
          .join("state")
          .join("logger")
          .join(PENDING_TRIGGER_UPLOADS_FILE_NAME),
      ),
    }
  }

  pub async fn upsert(&self, upload: PersistedTriggerUpload) {
    self
      .mutate(|uploads| {
        uploads.retain(|existing| existing.id != upload.id);
        uploads.push(upload);
      })
      .await;
  }

  pub async fn remove(&self, id: &str) {
    self
      .mutate(|uploads| uploads.retain(|existing| existing.id != id))
      .await;
  }

  pub async fn mark_uploading(&self, id: &str, buffer_id: &str) {
    self
      .mutate(|uploads| {
        for upload in uploads {
          if upload.id == id {
            upload.lifecycle = PersistedTriggerUploadLifecycle::UploadingFromBuffer;
            if let Some(buffer) = upload
              .buffers
              .iter_mut()
              .find(|buffer| buffer.buffer_id == buffer_id)
            {
              buffer.lifecycle = PersistedTriggerUploadBufferLifecycle::UploadingFromBuffer;
            }
          }
        }
      })
      .await;
  }

  pub async fn record_uploaded_chunk(&self, id: &str, buffer_id: &str, uploaded_logs_count: u64) {
    self
      .mutate(|uploads| {
        for upload in uploads {
          if upload.id == id {
            upload.lifecycle = PersistedTriggerUploadLifecycle::UploadingFromBuffer;
            if let Some(buffer) = upload
              .buffers
              .iter_mut()
              .find(|buffer| buffer.buffer_id == buffer_id)
            {
              buffer.lifecycle = PersistedTriggerUploadBufferLifecycle::UploadingFromBuffer;
              buffer.uploaded_batches_count += 1;
              buffer.uploaded_logs_count += uploaded_logs_count;
            }
          }
        }
      })
      .await;
  }

  pub async fn pending_uploads(&self) -> Vec<PersistedTriggerUpload> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await;
    inner.uploads.clone()
  }

  async fn mutate(&self, mutate: impl FnOnce(&mut Vec<PersistedTriggerUpload>)) {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await;
    mutate(&mut inner.uploads);

    if let Err(e) = Self::persist(&self.state_path, &inner.uploads).await {
      log::warn!(
        "failed to persist pending trigger uploads to {}: {e}",
        self.state_path.display()
      );
    }
  }

  async fn ensure_loaded(state_path: &Path, inner: &mut Inner) {
    if !inner.loaded {
      inner.uploads = Self::load(state_path).await;
      inner.loaded = true;
    }
  }

  async fn load(state_path: &Path) -> Vec<PersistedTriggerUpload> {
    match read_compressed_protobuf_file_if_exists::<PendingTriggerUploadsSnapshot>(state_path).await
    {
      Ok(None) => Vec::new(),
      Ok(Some(snapshot)) => snapshot.uploads.into_iter().map(Into::into).collect(),
      Err(e) => {
        log::warn!(
          "failed to deserialize pending trigger uploads from {}: {e}",
          state_path.display()
        );
        let _ignored = delete_file_if_exists_async(state_path).await;
        Vec::new()
      },
    }
  }

  async fn persist(state_path: &Path, uploads: &[PersistedTriggerUpload]) -> anyhow::Result<()> {
    if uploads.is_empty() {
      return delete_file_if_exists_async(state_path).await;
    }

    write_compressed_protobuf_file(
      state_path,
      &PendingTriggerUploadsSnapshot {
        uploads: uploads.iter().map(Into::into).collect(),
      },
    )
    .await
  }
}
