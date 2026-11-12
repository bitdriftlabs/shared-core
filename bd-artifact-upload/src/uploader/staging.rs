// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! The submission-to-index transaction shared by whole-file and chunked artifacts.
//!
//! Admission policy remains format-specific: whole-file reports may evict other reports, while
//! snapshots and workflow leases cannot be evicted; chunk admission accounts for physical bytes and
//! slots. Payload preparation also stays concrete rather than hiding encoding behind a strategy
//! framework. What is shared is the ownership algorithm: reject collisions, prepare bytes, register
//! one entry, publish the common index, release sources, and acknowledge persistence.
//!
//! A preparation ledger records only paths actually touched by this attempt. Before publication it
//! supports rollback (including restoring a moved snapshot); after publication it must not unlink
//! payloads, even if fsync fails. Borrowed workflow payloads never enter the ledger. Restart
//! recovery uses the persisted descriptors instead, so no additional journal or on-disk schema is
//! needed.

use super::{
  ARTIFACT_UPLOAD_DIRECTORY,
  Artifact,
  ArtifactPayloadEncoding,
  ArtifactType,
  ChunkedFileAdmission,
  EnqueueError,
  NewUpload,
  PreparationMode,
  StorageFormat,
  UploadArtifactRequest,
  UploadSource,
  Uploader,
  WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID,
  retained_persistence_error,
};
use bd_client_common::file::{
  async_write_checksummed_data,
  read_and_compress_limited_with_size,
  write_checksummed_data,
};
use bd_client_common::file_system::OwnedFile;
use bd_stats_common::Counter as _;
use bd_time::OffsetDateTimeExt;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

#[derive(Default)]
pub(super) struct StagedFiles {
  pub(super) created: Vec<PathBuf>,
  original: Option<(PathBuf, bool)>,
  published: bool,
}

impl Uploader {
  pub(super) async fn track_upload(&mut self, upload: NewUpload) {
    let NewUpload {
      source,
      request,
      mode,
      persisted_tx,
      completion_tx,
      source_lease,
      mut source_cleanup,
    } = upload;
    let mut request = request;
    if mode == PreparationMode::Chunked {
      // The scrubbed Begin is immutable after admission, including across restart/config changes.
      request.state_metadata =
        super::scrub_metadata(&self.scrub_rules.borrow(), &request.state_metadata);
    }
    let id = request.artifact_id.clone();
    let retained = matches!(source, UploadSource::Retained(_));
    let mut files = StagedFiles::default();
    log::debug!("preparing artifact {id} ({mode:?})");
    let result = self
      .stage_upload(source, request, mode, &mut source_cleanup, &mut files)
      .await;
    let success = result.is_ok();
    if !success && !files.published {
      self.rollback_files(&mut files).await;
    }

    // Both synchronous rejection and async failure consume owned command files before notifying
    // the caller. A guard for a moved source has already been disarmed, so it cannot unlink a
    // replacement created at the producer's old pathname.
    let guarded_source = source_cleanup.is_some();
    drop(source_cleanup);
    if success {
      if !guarded_source
        && let Some((path, false)) = files.original.take()
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
    request: UploadArtifactRequest,
    mode: PreparationMode,
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
    let id = &request.artifact_id;
    if let Some(existing) = self.chunked.entries.iter().find(|entry| {
      entry
        .request
        .as_ref()
        .is_some_and(|entry| entry.artifact_id == *id)
    }) {
      if mode == PreparationMode::Chunked
        && request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        && let Some(existing) = existing.request.as_ref()
        && existing.type_id == request.type_id
        && existing.session_id == request.session_id
      {
        return Ok(existing.logical_bytes);
      }
      return Err(EnqueueError::Other(anyhow::anyhow!(
        "artifact ID already has chunked ownership"
      )));
    }
    if let Some(existing) = self.index.iter().find(|entry| entry.name == *id) {
      if mode == PreparationMode::WholeFile
        && existing.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
        && request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        && existing.session_id == request.session_id
      {
        return Ok(None);
      }
      return Err(EnqueueError::Other(anyhow::anyhow!(
        "workflow attachment ID belongs to another session or artifact type"
      )));
    }
    let retained = matches!(source, UploadSource::Retained(_));
    let _preparation = if mode == PreparationMode::Chunked {
      Some(
        ChunkedFileAdmission::lock()
          .await
          .map_err(EnqueueError::Other)?,
      )
    } else {
      None
    };
    let size = match mode {
      PreparationMode::Chunked => {
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
        let size = entry
          .request
          .as_ref()
          .and_then(|request| request.logical_bytes);
        self.chunked.entries.push(entry);
        self.index_sync_required = true;
        size
      },
      PreparationMode::WholeFile => {
        self.admit_whole_file().await?;
        if let Some(hooks) = &self.test_hooks {
          hooks.whole_file_preparation_started(&source, &request);
        }
        let (entry, size) = self
          .prepare_whole_file(source, request, files)
          .await
          .map_err(|error| {
            if retained {
              retained_persistence_error(error)
            } else {
              EnqueueError::Other(error)
            }
          })?;
        self.index.push_back(entry);
        self.index_sync_required |= retained;
        size
      },
    };
    if let Some(hooks) = &self.test_hooks {
      hooks.payload_prepared();
    }
    match self.write_index().await {
      Ok(()) => {
        files.published = true;
        log::debug!("durably staged artifact");
        Ok(size)
      },
      Err(error) if error.published => {
        // The renamed index may already reference these files. Keep the entry and payloads, and
        // retry that exact index; rollback here would turn a durability error into payload loss.
        files.published = true;
        Err(EnqueueError::RetryablePersistence(error.into()))
      },
      Err(error) => {
        match mode {
          PreparationMode::WholeFile => {
            self.index.pop_back();
          },
          PreparationMode::Chunked => {
            self.chunked.entries.pop();
          },
        }
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

  async fn admit_whole_file(&mut self) -> Result<(), EnqueueError> {
    // Preserve whole-file eviction semantics without teaching chunk admission about report policy.
    // Snapshots hydrate scheduled logs and workflow payloads have independent retention owners.
    if self.index.len() < *self.max_entries.read() as usize {
      return Ok(());
    }
    self.stats.dropped.inc();
    let Some(index) = self.index.iter().position(|entry| {
      entry.type_id.as_deref() != Some(ArtifactType::StateSnapshot.to_type_id())
        && entry.type_id.as_deref() != Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
    }) else {
      return Err(EnqueueError::QueueFull);
    };
    if index == 0 {
      self.stop_current_upload();
    }
    if let Some(entry) = self.index.remove(index) {
      self
        .discard_upload(
          entry,
          "artifact upload was evicted from the upload queue".into(),
        )
        .await;
    }
    // Retirement may have failed to publish. Do not prepare a new payload until the same owner has
    // reconciled that failure; the byte inventory must never run ahead of durable ownership.
    if self.index_dirty {
      self
        .write_index()
        .await
        .map_err(|error| EnqueueError::RetryablePersistence(error.into()))?;
    }
    Ok(())
  }

  async fn prepare_whole_file(
    &self,
    source: UploadSource,
    request: UploadArtifactRequest,
    files: &mut StagedFiles,
  ) -> anyhow::Result<(Artifact, Option<u64>)> {
    let retained = matches!(source, UploadSource::Retained(_));
    let compressed_attachment = super::is_compressed_attachment(&request);
    let target = ARTIFACT_UPLOAD_DIRECTORY.join(&request.artifact_id);
    let mut size_bytes = None;
    // Register the target before its first write: a short write is still an owned file requiring
    // rollback. Borrowed workflow data is never written, copied, restored or unlinked here.
    if !retained {
      files.created.push(target.clone());
    }
    let format = match source {
      source if compressed_attachment => {
        if let UploadSource::Path(path) = &source {
          files.original = Some((path.clone(), false));
        }
        let limit = u64::from(*self.max_attachment_bytes.read());
        let reader = source.into_reader(self.file_system.as_ref(), limit).await?;
        let (encoded, size) = read_and_compress_limited_with_size(reader, limit).await?;
        let mut output = self.file_system.create_file(&target).await?;
        output.write_all(&encoded).await?;
        output.flush().await?;
        size_bytes = Some(size);
        StorageFormat::RAW
      },
      UploadSource::Bytes(bytes) => {
        let mut output = self.file_system.create_file(&target).await?;
        output.write_all(&write_checksummed_data(&bytes)).await?;
        output.flush().await?;
        StorageFormat::CHECKSUMMED
      },
      UploadSource::File(file) => {
        let output = self.file_system.create_file(&target).await?;
        async_write_checksummed_data(tokio::fs::File::from_std(file), output).await?;
        StorageFormat::CHECKSUMMED
      },
      UploadSource::Path(path) => {
        if let Err(error) = self.file_system.rename_file(&path, &target).await {
          log::debug!("failed to move artifact source, falling back to copy: {error}");
          let mut input = self.file_system.open_regular_file(&path).await?;
          let mut output = self.file_system.create_file(&target).await?;
          // A Path payload is already self-validating. Copy its bytes exactly, matching rename;
          // adding a CRC here would corrupt RAW snapshot uploads on cross-filesystem fallback.
          tokio::io::copy(&mut input, &mut output).await?;
          output.flush().await?;
          files.original = Some((path, false));
        } else {
          files.original = Some((path, true));
        }
        StorageFormat::RAW
      },
      UploadSource::Retained(path) => {
        self.file_system.sync_file_and_parent(&path).await?;
        StorageFormat::RAW
      },
    };
    let payload_encoding = if compressed_attachment
      || (retained && request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
    {
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
    } else {
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
    };
    Ok((
      Artifact {
        name: request.artifact_id,
        type_id: Some(
          if request.type_id.is_empty() {
            ArtifactType::default().to_type_id().into()
          } else {
            request.type_id
          },
        ),
        time: if request.time.is_some() {
          request.time
        } else {
          self.time_provider.now().into_proto()
        },
        session_id: request.session_id,
        pending_intent_negotiation: true,
        metadata: request.state_metadata,
        storage_format: format.into(),
        feature_flags: request.feature_flags,
        command_id: request.command_id,
        content_type: request.content_type,
        payload_encoding: payload_encoding.into(),
        crash_profile: request.crash_profile,
        ..Default::default()
      },
      size_bytes,
    ))
  }

  async fn rollback_files(&self, files: &mut StagedFiles) {
    if let Some((original, true)) = files.original.take()
      && let Some(target) = files.created.first()
      && let Err(error) = self.file_system.rename_file(target, &original).await
    {
      // Do not turn a failed restore into deletion of the only remaining snapshot copy.
      log::warn!(
        "failed to restore moved artifact source {}: {error}",
        original.display()
      );
      return;
    }
    if let Err(error) = self.delete_paths(&files.created).await {
      log::warn!("failed to roll back unpublished artifact files; will retry: {error}");
    }
  }
}
