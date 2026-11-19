// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! The submission-to-index transaction shared by whole-file and chunked artifacts.
//!
//! All representations share physical-byte and artifact-count admission. Accepted entries are
//! never evicted to make room for a different format. Payload preparation stays concrete rather
//! than hiding encoding behind a strategy framework. What is shared is the ownership algorithm:
//! reject collisions, prepare bytes, register one entry, publish the common index, release sources,
//! and acknowledge persistence.
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
  ArtifactStore,
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
use crate::input::Input;
use bd_client_common::file::async_write_checksummed_data;
use bd_client_common::file_system::OwnedFile;
use bd_stats_common::Counter as _;
use bd_time::OffsetDateTimeExt;
use protobuf::Message as _;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    let id = request.artifact_id.clone();
    let retained = matches!(source, UploadSource::Retained(_));
    let mut files = StagedFiles::default();
    log::debug!("preparing artifact {id} ({mode:?})");
    let result = self
      .stage_upload(source, request, mode, &mut source_cleanup, &mut files)
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
      self.rollback_files(&mut files).await;
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
    mut request: UploadArtifactRequest,
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
    let chunked = match mode {
      PreparationMode::WholeFile => false,
      PreparationMode::Chunked => true,
    };
    if chunked {
      super::chunked::validate_capture_request(&request)?;
      // Retain only scrubbed metadata; transfer also applies the rules current at each Begin.
      request.state_metadata =
        super::scrub_metadata(&self.scrub_rules.borrow(), &request.state_metadata);
    }
    let id = &request.artifact_id;
    if let Some(existing) = self
      .chunked
      .entries
      .iter()
      .find(|entry| entry.artifact_id == *id)
    {
      if chunked
        && request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
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
      if !chunked
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
    let artifact_id = request.artifact_id.clone();
    if self
      .index_proto()
      .compute_size()
      .saturating_add(request.compute_size())
      .saturating_add(64)
      > super::chunked::MAX_INDEX_BYTES
    {
      return Err(anyhow::anyhow!("insufficient bounded index space for artifact metadata").into());
    }
    let _preparation = ArtifactStore::lock().await.map_err(EnqueueError::Other)?;
    let size = if chunked {
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
      self.index_sync_required = true;
      size
    } else {
      if let Some(hooks) = &self.test_hooks {
        hooks.whole_file_preparation_started(&source);
      }
      let entry = self
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
      self.index_sync_required = true;
      None
    };
    if let Some(hooks) = &self.test_hooks {
      hooks.payload_prepared();
    }
    match self.write_index().await {
      Ok(()) => {
        files.published = true;
        log::debug!(
          "durably staged artifact: artifact_id={artifact_id} chunked={chunked} \
           logical_bytes={size:?}"
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
        if chunked {
          self.chunked.entries.pop();
        } else {
          self.index.pop_back();
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

  async fn prepare_whole_file(
    &self,
    source: UploadSource,
    request: UploadArtifactRequest,
    files: &mut StagedFiles,
  ) -> anyhow::Result<Artifact> {
    let retained = matches!(source, UploadSource::Retained(_));
    let target = ARTIFACT_UPLOAD_DIRECTORY.join(&request.artifact_id);
    let format = match source {
      UploadSource::Retained(path) => {
        self.file_system.sync_file_and_parent(&path).await?;
        StorageFormat::RAW
      },
      source => {
        let original = match &source {
          UploadSource::Path(path) => Some(path.clone()),
          _ => None,
        };
        let mut input = Input::open(source, self.file_system.as_ref()).await?;
        let size = input.size();
        let identity = match &input {
          Input::File { file, .. } if original.is_some() => {
            super::chunked::file_identity(&file.metadata().await?)
          },
          _ => None,
        };
        self
          .chunked
          .admission
          .check_capacity(
            self.file_system.as_ref(),
            size,
            if original.is_some() { size } else { 0 },
            identity,
            size.saturating_add(4096),
          )
          .await?;
        // Register before writing or moving: even a partial payload needs rollback.
        files.created.push(target.clone());
        let format = if let Some(path) = original {
          if let Err(error) = self.file_system.rename_file(&path, &target).await {
            log::debug!("failed to move artifact source, falling back to copy: {error}");
            let mut output = self.file_system.create_file(&target).await?;
            // Legacy snapshots are already self-validating. Copy exactly the bytes a rename
            // preserves, without adding a storage checksum or another compression layer.
            tokio::io::copy(&mut input.reader().take(size + 1), &mut output).await?;
            output.flush().await?;
            files.original = Some((path, false));
          } else {
            files.original = Some((path, true));
          }
          StorageFormat::RAW
        } else {
          let mut output = self.file_system.create_file(&target).await?;
          async_write_checksummed_data(input.reader().take(size + 1), &mut output).await?;
          StorageFormat::CHECKSUMMED
        };
        input.validate().await?;
        let expected_size = size
          + if format == StorageFormat::CHECKSUMMED {
            4
          } else {
            0
          };
        if self
          .file_system
          .open_regular_file(&target)
          .await?
          .metadata()
          .await?
          .len()
          != expected_size
        {
          anyhow::bail!("artifact changed during preparation");
        }
        self.file_system.sync_file_and_parent(&target).await?;
        format
      },
    };
    let payload_encoding = if retained && request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID {
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
    } else {
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
    };
    Ok(Artifact {
      name: request.artifact_id,
      type_id: Some(
        if request.type_id.is_empty() {
          super::CLIENT_REPORT_ARTIFACT_TYPE_ID.into()
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
    })
  }

  async fn rollback_files(&mut self, files: &mut StagedFiles) {
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
