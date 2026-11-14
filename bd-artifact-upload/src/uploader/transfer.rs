// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! One bounded chunk-mode RPC at a time, round-robin across durable artifacts. Receipt progress is
//! queried again after restart or a failed attempt; retained uploads have no overall timeout.

use super::Uploader;
use super::chunked::{CHUNK_BYTES, chunk_path};
use super::storage::StoredArtifact;
use bd_api::DataUpload;
use bd_api::upload::TrackedArtifactUpload;
use bd_backoff::{ExponentialBackoff, InfiniteBackoff};
use bd_client_common::file::read_limited;
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{UploadArtifactRequest, UploadArtifactResponse};
use bd_proto::protos::client::artifact::artifact_upload_index::ChunkedArtifact;
use bd_stats_common::Counter as _;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

//
// Progress
//

pub(super) struct Progress {
  begin: bool,
  receipts: Vec<u8>,
  ready: Instant,
  backoff: ExponentialBackoff,
}

impl Progress {
  fn new(backoff: ExponentialBackoff) -> Self {
    Self {
      begin: true,
      receipts: Vec::new(),
      ready: Instant::now(),
      backoff,
    }
  }

  fn request(&self, template: &UploadArtifactRequest) -> UploadArtifactRequest {
    let upload_uuid = TrackedArtifactUpload::upload_uuid();
    if self.begin {
      return UploadArtifactRequest {
        upload_uuid,
        ..template.clone()
      };
    }
    UploadArtifactRequest {
      upload_uuid,
      artifact_id: template.artifact_id.clone(),
      command_id: template.command_id.clone(),
      logical_bytes: template.logical_bytes,
      chunk_index: (0_u32 ..)
        .zip(&template.chunks)
        .find(|(index, _)| self.receipts[*index as usize / 8] & (1 << (index % 8)) == 0)
        .map(|(index, _)| index),
      ..Default::default()
    }
  }
}

//
// Attempt
//

pub(super) struct Attempt {
  id: String,
  uuid: String,
  result: anyhow::Result<UploadArtifactResponse>,
}

fn validate_ack(
  entry: &ChunkedArtifact,
  uuid: &str,
  response: &UploadArtifactResponse,
) -> anyhow::Result<State> {
  anyhow::ensure!(
    response.upload_uuid == uuid,
    "uncorrelated artifact acknowledgement"
  );
  let state = response
    .state
    .enum_value()
    .map_err(|value| anyhow::anyhow!("unknown artifact acknowledgement state {value}"))?;
  if matches!(state, State::STATE_RETRYABLE | State::STATE_REJECTED) {
    anyhow::ensure!(
      !response.error.is_empty()
        && response.error.len() <= 1024
        && response.received_chunks.is_empty(),
      "invalid artifact error acknowledgement"
    );
    return Ok(state);
  }
  anyhow::ensure!(
    matches!(
      state,
      State::STATE_RECEIVING | State::STATE_FINALIZING | State::STATE_FINALIZED
    ) && response.error.is_empty(),
    "invalid artifact progress state"
  );
  let request = entry
    .request
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("missing request"))?;
  let count = request.chunks.len();
  anyhow::ensure!(
    response.received_chunks.len() == count.div_ceil(8)
      && (count.is_multiple_of(8)
        || response
          .received_chunks
          .last()
          .is_some_and(|last| { last & !((1_u8 << (count % 8)) - 1) == 0 })),
    "invalid artifact receipt bitmap"
  );
  if matches!(state, State::STATE_FINALIZING | State::STATE_FINALIZED) {
    anyhow::ensure!(
      (0 .. count).all(|index| response.received_chunks[index / 8] & (1 << (index % 8)) != 0),
      "artifact finalization has incomplete receipts"
    );
  }
  Ok(state)
}

impl Uploader {
  pub(super) fn next_chunk_retry(&self) -> Option<Instant> {
    if self.chunk_task_handle.is_some() || self.index_dirty {
      return None;
    }
    self
      .chunk_progress
      .values()
      .map(|progress| progress.ready)
      .min()
  }

  pub(super) fn start_chunk_attempt(&mut self) {
    if self.chunk_task_handle.is_some() || self.index_dirty {
      return;
    }
    let count = self.chunked.entries.len();
    for offset in 0 .. count {
      let index = (self.chunk_cursor + offset) % count;
      let entry = &self.chunked.entries[index];
      let Some(template) = entry.request.as_ref() else {
        continue;
      };
      let progress = self
        .chunk_progress
        .entry(template.artifact_id.clone())
        .or_insert_with(|| Progress::new(self.backoff_policy.backoff_mark_update()));
      if progress.ready > Instant::now() {
        continue;
      }
      let mut request = progress.request(template);
      let descriptor = request
        .chunk_index
        .map(|index| template.chunks[index as usize].clone());
      let filesystem = self.file_system.clone();
      let sender = self.data_upload_tx.clone();
      let capacity = self.artifact_rpc_capacity.clone();
      let hooks = self.test_hooks.clone();
      self.chunk_cursor = index + 1;
      self.chunk_task_handle = Some(tokio::spawn(async move {
        let id = request.artifact_id.clone();
        let uuid = request.upload_uuid.clone();
        let result = async {
          if let Some(hooks) = hooks {
            hooks.before_chunk_transfer().await;
          }
          if let Some(descriptor) = descriptor {
            let path = chunk_path(&id, request.chunk_index.unwrap_or_default() as usize);
            let mut file = filesystem.open_regular_file(&path).await?;
            let contents = read_limited(&mut file, CHUNK_BYTES).await?;
            request.contents = tokio::task::spawn_blocking(move || {
              anyhow::ensure!(
                contents.len() == descriptor.encoded_bytes as usize
                  && Sha256::digest(&contents).as_slice() == descriptor.encoded_sha256,
                "staged chunk changed before transfer"
              );
              Ok::<_, anyhow::Error>(contents)
            })
            .await??;
          }
          // Local reads/verification do not occupy the fair network permit. Whole-file attempts
          // share this capacity and release it during retry backoff.
          let _permit = capacity.acquire_owned().await?;
          log::debug!(
            "sending chunked artifact operation: artifact_id={id}, begin={}, chunk={:?}",
            request.time.is_some(),
            request.chunk_index
          );
          let (tracked, response) = TrackedArtifactUpload::new(uuid.clone(), request);
          sender
            .send(DataUpload::ArtifactUpload(tracked))
            .await
            .map_err(|_| anyhow::anyhow!("artifact transport shut down"))?;
          Ok(response.await?)
        }
        .await;
        Attempt { id, uuid, result }
      }));
      break;
    }
  }

  pub(super) async fn handle_chunk_attempt(&mut self, attempt: Attempt) -> anyhow::Result<()> {
    let Some(index) = self.chunked.entries.iter().position(|entry| {
      entry
        .request
        .as_ref()
        .is_some_and(|request| request.artifact_id == attempt.id)
    }) else {
      return Ok(());
    };
    let entry = &mut self.chunked.entries[index];
    let response = attempt.result.and_then(|response| {
      validate_ack(entry, &attempt.uuid, &response).map(|state| (response, state))
    });
    match response {
      Ok((_, State::STATE_FINALIZED)) => {
        log::debug!("chunked artifact finalized: artifact_id={}", attempt.id);
        self.retire_chunk(index, Ok(())).await?;
        self.stats.uploaded.inc();
        if let Some(hooks) = &self.test_hooks {
          hooks.upload_complete(&attempt.id).await;
        }
      },
      Ok((response, State::STATE_REJECTED)) => {
        log::warn!(
          "chunked artifact rejected: artifact_id={}, error={}",
          attempt.id,
          response.error
        );
        self.retire_chunk(index, Err(response.error)).await?;
      },
      result => {
        let progress = self
          .chunk_progress
          .get_mut(&attempt.id)
          .ok_or_else(|| anyhow::anyhow!("missing chunk transfer progress"))?;
        match result {
          Ok((response, State::STATE_RECEIVING | State::STATE_FINALIZING)) => {
            progress.begin = false;
            progress.receipts = response.received_chunks;
            // Complete receipts are not completion. Poll without monopolizing upload capacity.
            if (0 .. entry.request.chunks.len())
              .all(|index| progress.receipts[index / 8] & (1 << (index % 8)) != 0)
            {
              progress.ready = Instant::now() + progress.backoff.next_backoff().unsigned_abs();
            }
          },
          retry => {
            // A failed attempt or RETRYABLE reconciles receipts using the exact durable Begin,
            // never a fresh identity or a whole-file fallback.
            log::debug!(
              "chunked artifact needs resume: artifact_id={}, result={retry:?}",
              attempt.id
            );
            progress.begin = true;
            progress.ready = Instant::now() + progress.backoff.next_backoff().unsigned_abs();
          },
        }
      },
    }
    Ok(())
  }

  async fn retire_chunk(&mut self, index: usize, result: Result<(), String>) -> anyhow::Result<()> {
    let entry = self.chunked.entries.remove(index);
    let id = entry.request.artifact_id.clone();
    let paths = StoredArtifact::Chunked(&entry).owned_paths()?;
    self.chunk_progress.remove(&id);
    self.index_sync_required = true;
    // On a failed index commit keep bytes charged and let reconciliation finish their cleanup.
    let retirement = self.retire_files(&paths).await;
    self.complete_upload(&id, result);
    retirement
  }
}
