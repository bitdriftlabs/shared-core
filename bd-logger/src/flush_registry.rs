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
use bd_workflows::config::FlushBufferId;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const PENDING_TRIGGER_UPLOADS_FILE_NAME: &str = "pending_trigger_uploads_snapshot.1.pb";

//
// PersistedTriggerUploadLifecycle
//

// This lifecycle tracks the upload as a whole. Individual buffers also carry their own lifecycle
// so restart recovery can distinguish "the upload exists" from "which buffers have already moved
// from live-buffer reads into durable artifact replay".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedTriggerUploadLifecycle {
  ReadyToUpload,
  UploadingFromBuffer,
  UploadingFromArtifact,
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

  pub fn to_flush_buffer_id(&self) -> FlushBufferId {
    match self {
      Self::WorkflowAction(id) => FlushBufferId::WorkflowActionId(id.clone()),
      Self::ExplicitSessionCapture(id) => FlushBufferId::ExplicitSessionCapture(id.clone()),
      Self::RemoteCommand(id) => FlushBufferId::RemoteCommand(id.clone()),
    }
  }
}

pub fn flush_buffer_id_from_trigger_upload_source(source: &TriggerUploadSource) -> FlushBufferId {
  match source {
    TriggerUploadSource::WorkflowAction(id) => FlushBufferId::WorkflowActionId(id.clone()),
    TriggerUploadSource::ExplicitSessionCapture(id) => {
      FlushBufferId::ExplicitSessionCapture(id.clone())
    },
    TriggerUploadSource::RemoteCommand(id) => FlushBufferId::RemoteCommand(id.clone()),
  }
}

//
// PersistedTriggerUpload
//

// Durable description of a trigger upload that survives process restart. This is the source of
// truth for logger-side recovery: it records which logical flush triggered the upload, which
// buffers were admitted to it in this process, the originating session, any deferred streaming
// activation, and per-buffer progress.
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
    // Streaming is stored in a persistence-friendly shape so the registry stays decoupled from the
    // full workflow proto surface. Recovery converts it back only when replay needs to re-issue
    // the upload request.
    self
      .streaming
      .as_ref()
      .map(PersistedTriggerUploadStreaming::to_proto)
  }
}

// Result of pruning one buffer out of older persisted uploads before a fresh attempt for that
// same buffer is admitted. The consumer uses this to remove matching artifact files and, when an
// upload loses its last remaining buffer, to complete the corresponding process-local flush ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrunedTriggerUploadBuffer {
  pub upload_id: String,
  pub source: PersistedTriggerUploadSource,
  pub buffer_id: String,
  pub removed_upload: bool,
}

// This lifecycle is tracked per buffer so a single trigger upload can be partially progressed and
// later resumed. That matters because a multi-buffer flush may stage and upload some buffers before
// the process dies, leaving recovery to continue only the remaining work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedTriggerUploadBufferLifecycle {
  Pending,
  UploadingFromBuffer,
  UploadingFromArtifact,
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
    // Newly persisted uploads start with zero durable progress. The consumer updates these counts
    // only after artifact-backed chunks have actually been uploaded successfully.
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
    // The registry only needs the subset of streaming configuration required to reconstruct the
    // deferred activation after restart. Everything else stays in the workflow-side proto.
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
    // Reconstruct just enough proto state for recovery to re-issue remote-streaming activation.
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

// The on-disk file contains one compressed protobuf snapshot holding every pending upload. We do
// not shard by upload ID because the set is expected to stay small and recovery wants the full
// picture at startup.
#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PendingTriggerUploadsSnapshot {
  #[field(id = 1)]
  uploads: Vec<PersistedTriggerUploadRecord>,
}

//
// PersistedTriggerUploadRecord
//

// These `*Record` types are the protobuf serialization boundary for the registry. The higher-level
// Rust types above are the in-memory API used by the logger; the record layer exists so we can
// evolve storage without leaking protobuf concerns into the rest of the upload pipeline.
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
  UploadingFromArtifact,
  #[field(id = 4)]
  #[field(deserialize)]
  Completed,
  #[field(id = 5)]
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
  UploadingFromArtifact,
  #[field(id = 4)]
  #[field(deserialize)]
  Completed,
  #[field(id = 5)]
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
      PersistedTriggerUploadBufferLifecycle::UploadingFromArtifact => Self::UploadingFromArtifact,
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
      PersistedTriggerUploadBufferLifecycleRecord::UploadingFromArtifact => {
        Self::UploadingFromArtifact
      },
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
      PersistedTriggerUploadLifecycle::UploadingFromArtifact => Self::UploadingFromArtifact,
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
      PersistedTriggerUploadLifecycleRecord::UploadingFromArtifact => Self::UploadingFromArtifact,
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

// Durable registry of trigger uploads that have been admitted by the logger but not yet fully
// completed. `ProcessLocalPendingFlushState` in bd-workflows is only an in-memory mirror of flush
// IDs; this store is the authoritative restart-safe state that recovery reads back on startup.
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
    // Upsert by logical upload ID so there is at most one durable registry entry for a given
    // source ID. Repeated scheduling/recovery of that same logical flush rewrites this one record
    // instead of accumulating stale copies, while distinct source IDs still remain distinct.
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
    // This transition records that the consumer is still reading live buffer contents for the
    // given buffer and has not yet switched over to replaying staged artifact batches.
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

  pub async fn mark_uploading_from_artifact(&self, id: &str, buffer_id: &str) {
    // Once a batch is durably staged and replayed from artifact storage, restart recovery should
    // resume from that artifact backlog first rather than from the raw ring buffer head.
    self
      .mutate(|uploads| {
        for upload in uploads {
          if upload.id == id {
            upload.lifecycle = PersistedTriggerUploadLifecycle::UploadingFromArtifact;
            if let Some(buffer) = upload
              .buffers
              .iter_mut()
              .find(|buffer| buffer.buffer_id == buffer_id)
            {
              buffer.lifecycle = PersistedTriggerUploadBufferLifecycle::UploadingFromArtifact;
            }
          }
        }
      })
      .await;
  }

  pub async fn record_uploaded_chunk(&self, id: &str, buffer_id: &str, uploaded_logs_count: u64) {
    // Progress is recorded only for successfully uploaded artifact-backed chunks. This means the
    // registry reflects durable upload history rather than speculative reads from the live buffer.
    self
      .mutate(|uploads| {
        for upload in uploads {
          if upload.id == id {
            upload.lifecycle = PersistedTriggerUploadLifecycle::UploadingFromArtifact;
            if let Some(buffer) = upload
              .buffers
              .iter_mut()
              .find(|buffer| buffer.buffer_id == buffer_id)
            {
              buffer.lifecycle = PersistedTriggerUploadBufferLifecycle::UploadingFromArtifact;
              buffer.uploaded_batches_count += 1;
              buffer.uploaded_logs_count += uploaded_logs_count;
            }
          }
        }
      })
      .await;
  }

  pub async fn prune_buffer_from_other_uploads(
    &self,
    current_upload_id: &str,
    buffer_id: &str,
  ) -> Vec<PrunedTriggerUploadBuffer> {
    // Before the logger admits a fresh non-active attempt for buffer B, it may need to evict any
    // older durable upload state that still refers to B. We do that buffer-by-buffer rather than
    // rekeying the whole registry by buffer ID because the surrounding upload model is still
    // source-scoped: one logical upload can own multiple buffers, optional streaming state, and a
    // single process-local flush ID. Pruning therefore removes only the conflicting buffer from
    // older uploads, and only deletes the whole upload record when that buffer was its last
    // member. That distinction is what lets us uphold both sides of the design at once:
    // - buffer-scoped replacement ensures a fresh attempt for B does not coexist with stale durable
    //   history for B;
    // - source-scoped aggregate uploads still preserve any other buffers, deferred streaming
    //   payloads, and completion semantics owned by the older upload.
    //
    // The caller is responsible for acting on the returned `PrunedTriggerUploadBuffer` records.
    // Those records tell the consumer which artifact files must be deleted for the removed buffer,
    // and whether pruning removed the upload's final buffer and therefore completed that upload's
    // process-local flush ID entirely.
    let mut pruned_buffers = Vec::new();
    self
      .mutate(|uploads| {
        let mut index = 0;
        while index < uploads.len() {
          if uploads[index].id == current_upload_id {
            index += 1;
            continue;
          }

          let Some(buffer_index) = uploads[index]
            .buffers
            .iter()
            .position(|buffer| buffer.buffer_id == buffer_id)
          else {
            index += 1;
            continue;
          };

          let removed_upload = uploads[index].buffers.len() == 1;
          let source = uploads[index].source.clone();
          let upload_id = uploads[index].id.clone();

          if removed_upload {
            uploads.remove(index);
          } else {
            uploads[index].buffers.remove(buffer_index);
            index += 1;
          }

          pruned_buffers.push(PrunedTriggerUploadBuffer {
            upload_id,
            source,
            buffer_id: buffer_id.to_string(),
            removed_upload,
          });
        }
      })
      .await;
    pruned_buffers
  }

  pub async fn pending_uploads(&self) -> Vec<PersistedTriggerUpload> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await;
    inner.uploads.clone()
  }

  pub async fn pending_upload(&self, id: &str) -> Option<PersistedTriggerUpload> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await;
    inner.uploads.iter().find(|upload| upload.id == id).cloned()
  }

  async fn mutate(&self, mutate: impl FnOnce(&mut Vec<PersistedTriggerUpload>)) {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await;
    mutate(&mut inner.uploads);

    // Keep the in-memory mirror and on-disk snapshot coupled under the same mutex so every public
    // mutation either persists the new state or logs a warning about the failed durability step.
    if let Err(e) = Self::persist(&self.state_path, &inner.uploads).await {
      log::warn!(
        "failed to persist pending trigger uploads to {}: {e}",
        self.state_path.display()
      );
    }
  }

  async fn ensure_loaded(state_path: &Path, inner: &mut Inner) {
    if !inner.loaded {
      // The store is lazy-loaded because most processes only need this state when recovery or a
      // trigger upload first touches it. After the first load, the mutex-protected in-memory copy
      // is treated as the working set for the remainder of the process.
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
        // A corrupt snapshot is treated as unrecoverable stale state. We delete it and proceed
        // empty rather than trapping the logger in a permanent startup failure loop.
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
    // An empty registry deletes the snapshot entirely so the absence of the file means "no pending
    // uploads" without requiring a serialized empty protobuf.
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
