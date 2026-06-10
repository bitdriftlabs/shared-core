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
  Uploading,
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
  pub buffer_ids: Vec<String>,
  pub has_streaming: bool,
  pub lifecycle: PersistedTriggerUploadLifecycle,
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
  buffer_ids: Vec<String>,
  #[field(id = 4)]
  has_streaming: bool,
  #[field(id = 5)]
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
  Uploading,
}

impl From<&PersistedTriggerUpload> for PersistedTriggerUploadRecord {
  fn from(upload: &PersistedTriggerUpload) -> Self {
    Self {
      id: upload.id.clone(),
      source: (&upload.source).into(),
      buffer_ids: upload.buffer_ids.clone(),
      has_streaming: upload.has_streaming,
      lifecycle: upload.lifecycle.into(),
    }
  }
}

impl From<PersistedTriggerUploadRecord> for PersistedTriggerUpload {
  fn from(upload: PersistedTriggerUploadRecord) -> Self {
    Self {
      id: upload.id,
      source: upload.source.into(),
      buffer_ids: upload.buffer_ids,
      has_streaming: upload.has_streaming,
      lifecycle: upload.lifecycle.into(),
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
      PersistedTriggerUploadLifecycle::Uploading => Self::Uploading,
    }
  }
}

impl From<PersistedTriggerUploadLifecycleRecord> for PersistedTriggerUploadLifecycle {
  fn from(lifecycle: PersistedTriggerUploadLifecycleRecord) -> Self {
    match lifecycle {
      PersistedTriggerUploadLifecycleRecord::ReadyToUpload => Self::ReadyToUpload,
      PersistedTriggerUploadLifecycleRecord::Uploading => Self::Uploading,
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

  pub async fn mark_uploading(&self, id: &str) {
    self
      .mutate(|uploads| {
        for upload in uploads {
          if upload.id == id {
            upload.lifecycle = PersistedTriggerUploadLifecycle::Uploading;
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
