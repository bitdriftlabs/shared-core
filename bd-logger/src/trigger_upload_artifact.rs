// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./trigger_upload_artifact_test.rs"]
mod trigger_upload_artifact_test;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bd_api::upload::TrackedLogBatch;
use bd_client_common::file::{
  read_compressed_protobuf_file_if_exists,
  write_compressed_protobuf_file,
};
use bd_client_common::file_system::delete_file_if_exists_async;
use bd_macros::proto_serializable;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const TRIGGER_UPLOAD_ARTIFACTS_DIRECTORY: &str = "trigger_upload_artifacts";

//
// PersistedTriggerUploadArtifactBatch
//

// Concrete log batch that has been durably staged for a single trigger upload / buffer pair.
// `upload_uuid` is persisted with the logs so retries and restart recovery keep using the same
// upload identity instead of minting a fresh UUID for the same payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedTriggerUploadArtifactBatch {
  pub upload_uuid: String,
  pub logs: Vec<Vec<u8>>,
}

//
// TriggerUploadArtifactStore
//

// The store keeps at most two artifact states per trigger upload buffer:
// - `queued_batch`: durably staged but not yet promoted to the currently replayed batch
// - `inflight_batch`: the batch the consumer should upload or resume uploading first
// This split lets the consumer stage a batch before advancing the live buffer cursor, then promote
// that batch into the single artifact that restart recovery must resume before reading new logs.
#[derive(Default)]
struct Inner {
  loaded: bool,
  snapshot: TriggerUploadArtifactSnapshot,
}

#[derive(Clone)]
pub struct TriggerUploadArtifactStore {
  inner: Arc<Mutex<Inner>>,
  state_path: Arc<PathBuf>,
}

impl TriggerUploadArtifactStore {
  pub fn new(
    logger_state_directory: impl AsRef<Path>,
    trigger_upload_id: &str,
    buffer_id: &str,
  ) -> Self {
    // Each trigger-upload artifact file is keyed by logical upload ID and buffer ID so multi-buffer
    // flushes can recover each buffer independently without collisions. The identifiers are base64
    // encoded because logical IDs may contain filesystem-hostile characters.
    let encoded_trigger_upload_id = URL_SAFE_NO_PAD.encode(trigger_upload_id);
    let encoded_buffer_id = URL_SAFE_NO_PAD.encode(buffer_id);
    let state_path = logger_state_directory
      .as_ref()
      .join(TRIGGER_UPLOAD_ARTIFACTS_DIRECTORY)
      .join(format!(
        "{encoded_trigger_upload_id}.{encoded_buffer_id}.1.pb"
      ));

    Self {
      inner: Arc::new(Mutex::new(Inner::default())),
      state_path: Arc::new(state_path),
    }
  }

  pub async fn stage_batch(
    &self,
    logs: Vec<Vec<u8>>,
  ) -> anyhow::Result<PersistedTriggerUploadArtifactBatch> {
    // Staging is the durable handoff point between the live ring buffer and restart-safe replay.
    // The consumer writes the batch here before advancing the live buffer cursor.
    let batch = PersistedTriggerUploadArtifactBatch {
      upload_uuid: TrackedLogBatch::upload_uuid(),
      logs,
    };

    self
      .mutate(|snapshot| {
        snapshot.queued_batch = Some(batch.clone().into());
      })
      .await?;

    Ok(batch)
  }

  pub async fn queued_batch(&self) -> anyhow::Result<Option<PersistedTriggerUploadArtifactBatch>> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await?;
    Ok(inner.snapshot.queued_batch.clone().map(Into::into))
  }

  pub async fn remove_queued_batch(&self) -> anyhow::Result<()> {
    self
      .mutate(|snapshot| {
        snapshot.queued_batch = None;
      })
      .await
  }

  pub async fn promote_queued_batch_to_inflight(
    &self,
  ) -> anyhow::Result<Option<PersistedTriggerUploadArtifactBatch>> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await?;

    // Promotion moves a staged batch into the single artifact that the consumer should actively
    // upload or recover first. This preserves the original upload UUID while making restart order
    // unambiguous.
    let Some(batch) = inner.snapshot.queued_batch.take() else {
      return Ok(None);
    };
    inner.snapshot.inflight_batch = Some(batch.clone());

    Self::persist(&self.state_path, &inner.snapshot).await?;
    Ok(Some(batch.into()))
  }

  pub async fn inflight_batch(
    &self,
  ) -> anyhow::Result<Option<PersistedTriggerUploadArtifactBatch>> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await?;
    Ok(inner.snapshot.inflight_batch.clone().map(Into::into))
  }

  pub async fn clear_inflight_batch(&self) -> anyhow::Result<()> {
    self.mutate(|snapshot| snapshot.inflight_batch = None).await
  }

  async fn mutate(
    &self,
    mutate: impl FnOnce(&mut TriggerUploadArtifactSnapshot),
  ) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    Self::ensure_loaded(&self.state_path, &mut inner).await?;
    mutate(&mut inner.snapshot);

    // The mutex protects the in-memory snapshot and its persisted form together so each mutation
    // observes and stores a consistent queued/inflight pair.
    Self::persist(&self.state_path, &inner.snapshot).await
  }

  async fn ensure_loaded(state_path: &Path, inner: &mut Inner) -> anyhow::Result<()> {
    if !inner.loaded {
      // Artifact state is lazy-loaded because most processes never need it unless a trigger upload
      // is staged or recovered. After the first access, the mutex-guarded snapshot becomes the
      // working set for the remainder of the process.
      inner.snapshot = Self::load(state_path).await?;
      inner.loaded = true;
    }

    Ok(())
  }

  async fn load(state_path: &Path) -> anyhow::Result<TriggerUploadArtifactSnapshot> {
    Ok(
      read_compressed_protobuf_file_if_exists::<TriggerUploadArtifactSnapshot>(state_path)
        .await?
        .unwrap_or_default(),
    )
  }

  async fn persist(
    state_path: &Path,
    snapshot: &TriggerUploadArtifactSnapshot,
  ) -> anyhow::Result<()> {
    // Deleting the file when both slots are empty makes absence of the snapshot mean "no artifact
    // backlog" without needing to serialize an empty protobuf.
    if snapshot.queued_batch.is_none() && snapshot.inflight_batch.is_none() {
      return delete_file_if_exists_async(state_path).await;
    }

    write_compressed_protobuf_file(state_path, snapshot).await
  }
}

//
// TriggerUploadArtifactSnapshot
//

// Serialized representation of the artifact backlog for one trigger upload / buffer pair. There is
// at most one queued and one inflight batch on disk because the consumer stages and promotes work
// incrementally while draining the live buffer.
#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TriggerUploadArtifactSnapshot {
  #[field(id = 1)]
  queued_batch: Option<PersistedTriggerUploadArtifactBatchRecord>,
  #[field(id = 2)]
  inflight_batch: Option<PersistedTriggerUploadArtifactBatchRecord>,
}

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PersistedTriggerUploadArtifactLog {
  #[field(id = 1)]
  data: Vec<u8>,
}

// Protobuf record layer for the persisted batch. Like the flush registry, this keeps the storage
// format isolated from the higher-level Rust API used by the consumer.
#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PersistedTriggerUploadArtifactBatchRecord {
  #[field(id = 1)]
  upload_uuid: String,
  #[field(id = 2)]
  logs: Vec<PersistedTriggerUploadArtifactLog>,
}

impl From<PersistedTriggerUploadArtifactBatchRecord> for PersistedTriggerUploadArtifactBatch {
  fn from(batch: PersistedTriggerUploadArtifactBatchRecord) -> Self {
    Self {
      upload_uuid: batch.upload_uuid,
      logs: batch.logs.into_iter().map(|log| log.data).collect(),
    }
  }
}

impl From<PersistedTriggerUploadArtifactBatch> for PersistedTriggerUploadArtifactBatchRecord {
  fn from(batch: PersistedTriggerUploadArtifactBatch) -> Self {
    Self {
      upload_uuid: batch.upload_uuid,
      logs: batch
        .logs
        .into_iter()
        .map(|data| PersistedTriggerUploadArtifactLog { data })
        .collect(),
    }
  }
}
