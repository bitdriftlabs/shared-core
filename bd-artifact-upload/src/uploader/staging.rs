// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! The submission-to-index transaction for chunked artifacts.
//!
//! A preparation ledger records only paths actually touched by this attempt. Before publication it
//! supports rollback; after publication it must not unlink payloads, even if fsync fails. Borrowed
//! workflow payloads never enter the ledger. Restart recovery uses the persisted descriptors
//! instead, so no additional journal or on-disk schema is needed.

use super::{
  ArtifactStore,
  EnqueueError,
  NewUpload,
  UploadArtifactRequest,
  UploadSource,
  Uploader,
  WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID,
  retained_persistence_error,
};
use bd_client_common::file_system::OwnedFile;
use bd_stats_common::Counter as _;
use protobuf::Message as _;
use std::path::PathBuf;
use tokio::sync::oneshot;

#[derive(Default)]
pub(super) struct StagedFiles {
  pub(super) created: Vec<PathBuf>,
  pub(super) remove_source: Option<PathBuf>,
  published: bool,
}

impl Uploader {
  pub(super) async fn track_upload(&mut self, upload: NewUpload) {
    let NewUpload {
      source,
      request,
      persisted_tx,
      completion_tx,
      source_lease,
      mut source_cleanup,
    } = upload;
    let id = request.artifact_id.clone();
    let retained = matches!(source, UploadSource::Retained(_));
    if let Some(hooks) = &self.test_hooks {
      hooks.preparation_started(&source, &request);
    }
    let mut files = StagedFiles::default();
    log::debug!("preparing chunked artifact {id}");
    let result = self
      .stage_upload(source, request, &mut source_cleanup, &mut files)
      .await;
    let success = result.is_ok();
    if !retained
      && matches!(
        &result,
        Err(EnqueueError::Other(_) | EnqueueError::QueueFull)
      )
    {
      self.stats.dropped.inc();
    }
    if !success && !files.published {
      self.rollback_files(&files).await;
    }

    // Keep the synchronous guard only for cancellation/rejection. Normal cleanup runs off the
    // executor, and still completes before the persistence acknowledgement.
    let guarded_source = source_cleanup.is_some();
    if let Some(source) = source_cleanup
      && let Err(error) = source.remove().await
    {
      log::warn!("failed to remove owned artifact source: {error}");
    }
    if success {
      if !guarded_source
        && let Some(path) = files.remove_source.take()
        && let Err(error) = self.file_system.delete_file(&path).await
      {
        log::warn!(
          "failed to remove copied artifact source {}: {error}",
          path.display()
        );
      }
      self.clean_sources().await;
    }
    if success && let Some(completion_tx) = completion_tx {
      // Accepted work can outlive its waiter. A retry may replace a closed waiter, but must not
      // steal notification from a live one; failed persistence does not register a success waiter.
      if self
        .upload_completions
        .get(&id)
        .is_none_or(oneshot::Sender::is_closed)
      {
        self.upload_completions.insert(id.clone(), completion_tx);
      }
    }
    if let Err(error) = &result {
      log::warn!("artifact {id} staging failed (retained={retained}): {error}");
    }
    // The durable lease is visible before a workflow cleanup-exclusion permit is released.
    drop(source_lease);
    if let Some(tx) = persisted_tx {
      tx.send(result);
    }
    if let Some(hooks) = &self.test_hooks {
      hooks.staging_completed(&id, success).await;
    }
  }

  async fn stage_upload(
    &mut self,
    source: UploadSource,
    mut request: UploadArtifactRequest,
    source_cleanup: &mut Option<OwnedFile>,
    files: &mut StagedFiles,
  ) -> Result<Option<u64>, EnqueueError> {
    // Reconcile an earlier ambiguous commit before admitting more work or returning idempotent
    // success. Otherwise a matching in-memory lease could be acknowledged without a durable index.
    if self.index_dirty {
      self
        .write_index()
        .await
        .map_err(|error| EnqueueError::RetryablePersistence(error.into()))?;
    }
    super::chunked::validate_capture_request(&request)?;
    request.state_metadata =
      super::scrub_metadata(&self.scrub_rules.borrow(), &request.state_metadata);
    let id = &request.artifact_id;
    if let Some(existing) = self
      .chunked
      .entries
      .iter()
      .find(|entry| entry.artifact_id == *id)
    {
      if request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        && existing.type_id == request.type_id
        && existing.session_id == request.session_id
      {
        return Ok(existing.logical_bytes);
      }
      return Err(EnqueueError::Other(anyhow::anyhow!(
        "artifact ID already has chunked ownership"
      )));
    }
    let retained = matches!(source, UploadSource::Retained(_));
    let artifact_id = request.artifact_id.clone();
    if self
      .index_proto()
      .compute_size()
      .saturating_add(request.compute_size())
      .saturating_add(super::chunked::CHUNK_DESCRIPTOR_RESERVE_BYTES)
      > super::chunked::MAX_INDEX_BYTES
    {
      return Err(anyhow::anyhow!("insufficient bounded index space for artifact metadata").into());
    }
    let _preparation = ArtifactStore::lock().await.map_err(EnqueueError::Other)?;
    let entry = self
      .prepare_chunked(source, request, source_cleanup, files)
      .await
      .map_err(|error| {
        if retained {
          retained_persistence_error(error)
        } else {
          EnqueueError::Other(error)
        }
      })?;
    let size = entry.logical_bytes;
    self.chunked.entries.push(entry);
    if let Some(hooks) = &self.test_hooks {
      hooks.payload_prepared();
    }
    match self.write_index().await {
      Ok(()) => {
        files.published = true;
        log::debug!(
          "durably staged chunked artifact: artifact_id={artifact_id} logical_bytes={size:?}"
        );
        Ok(size)
      },
      Err(error) if error.published => {
        // The renamed index may already reference these files. Keep the entry and payloads, and
        // retry that exact index; rollback here would turn a durability error into payload loss.
        files.published = true;
        Err(EnqueueError::RetryablePersistence(error.into()))
      },
      Err(error) => {
        self.chunked.entries.pop();
        Err(
          if retained {
            retained_persistence_error(error.error)
          } else {
            EnqueueError::Other(error.error)
          },
        )
      },
    }
  }

  async fn rollback_files(&mut self, files: &StagedFiles) {
    if let Err(error) = self.delete_paths(&files.created).await {
      log::warn!("failed to roll back unpublished artifact files; will retry: {error}");
    }
  }
}
