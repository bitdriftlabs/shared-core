// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! One bounded chunk-mode RPC at a time, round-robin across durable artifacts. Receipt progress is
//! reconciled by Begin after restart or a failed attempt; retained uploads have no overall timeout.

use super::Uploader;
use super::chunked::{CHUNK_BYTES, chunk_path};
use super::storage::owned_paths;
use bd_api::DataUpload;
use bd_api::upload::TrackedArtifactUpload;
use bd_backoff::{ExponentialBackoff, InfiniteBackoff};
use bd_client_common::file::read_limited;
use bd_metadata::artifact::{
  ARTIFACT_BEGIN_METADATA_KEYS,
  MAX_ARTIFACT_ATTACHED_METADATA_BYTES,
  MAX_ARTIFACT_MANIFEST_BYTES,
  MAX_ARTIFACT_ORIGIN_BYTES,
};
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{UploadArtifactRequest, UploadArtifactResponse};
use bd_stats_common::Counter as _;
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::io;
use tokio::time::Instant;

//
// Progress
//

pub(super) struct Progress {
  begin: bool,
  receipts: Vec<u8>,
  intent_observed: bool,
  ready: Instant,
  backoff: ExponentialBackoff,
}

impl Progress {
  fn new(backoff: ExponentialBackoff) -> Self {
    Self {
      begin: true,
      receipts: Vec::new(),
      intent_observed: false,
      ready: Instant::now(),
      backoff,
    }
  }

  fn request(&self, template: &UploadArtifactRequest) -> UploadArtifactRequest {
    let upload_uuid = TrackedArtifactUpload::upload_uuid();
    let chunk_index = if self.begin {
      None
    } else {
      (0_u32 ..)
        .zip(&template.chunks)
        .find(|(index, _)| self.receipts[*index as usize / 8] & (1 << (index % 8)) == 0)
        .map(|(index, _)| index)
    };
    if let Some(index) = chunk_index {
      let is_final = index as usize + 1 == template.chunks.len();
      UploadArtifactRequest {
        upload_uuid,
        artifact_id: template.artifact_id.clone(),
        command_id: template.command_id.clone(),
        logical_bytes: template.logical_bytes,
        chunk_index: Some(index),
        state_metadata: if is_final {
          template.state_metadata.clone()
        } else {
          std::collections::HashMap::new()
        },
        feature_flags: if is_final {
          template.feature_flags.clone()
        } else {
          Vec::new()
        },
        ..Default::default()
      }
    } else {
      let mut begin = UploadArtifactRequest {
        upload_uuid,
        feature_flags: Vec::new(),
        ..template.clone()
      };
      // Non-empty uploads carry full scrubbed state_metadata and feature_flags on the final chunk.
      // Begin carries only the bounded identity/policy keys needed for admission and sampling.
      if !template.chunks.is_empty() {
        begin
          .state_metadata
          .retain(|key, _| ARTIFACT_BEGIN_METADATA_KEYS.contains(&key.as_str()));
      }
      begin
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

#[derive(Debug, thiserror::Error)]
#[error("invalid staged fragment: {0}")]
struct InvalidFragment(anyhow::Error);

#[derive(Debug, thiserror::Error)]
#[error("scrubbed artifact metadata exceeds upload limits")]
struct OversizedMetadata;

// The metadata budgets leave room for the bounded fragment, the ApiRequest wrapper and framing
// under the server's request cap.
fn check_scrubbed_metadata(request: &UploadArtifactRequest) -> Result<(), OversizedMetadata> {
  let limit = if request.chunk_index.is_none() {
    if request.state_metadata.len() > 32
      || request
        .state_metadata
        .values()
        .any(|value| value.string_data().len() > 255)
      || request
        .state_metadata
        .iter()
        .map(|(key, value)| key.len() + value.string_data().len())
        .sum::<usize>()
        > MAX_ARTIFACT_ORIGIN_BYTES
    {
      return Err(OversizedMetadata);
    }
    MAX_ARTIFACT_MANIFEST_BYTES
  } else {
    MAX_ARTIFACT_ATTACHED_METADATA_BYTES
  };
  let contents_bytes = if request.contents.is_empty() {
    0
  } else {
    protobuf::rt::bytes_size(3, &request.contents)
  };
  if request.compute_size() - contents_bytes > u64::from(limit) {
    return Err(OversizedMetadata);
  }
  Ok(())
}

fn fragment_read_error(error: impl Into<anyhow::Error>) -> anyhow::Error {
  let error = error.into();
  // Accepted fragments are immutable; missing or invalid bytes cannot heal on transport retries.
  if error.downcast_ref::<io::Error>().is_some_and(|error| {
    matches!(
      error.kind(),
      io::ErrorKind::NotFound | io::ErrorKind::InvalidData
    )
  }) {
    InvalidFragment(error).into()
  } else {
    error
  }
}

fn validate_ack(
  request: &UploadArtifactRequest,
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
  if state == State::STATE_DROPPED {
    anyhow::ensure!(
      response.error.is_empty() && response.received_chunks.is_empty(),
      "invalid artifact intent drop acknowledgement"
    );
    return Ok(state);
  }
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
    matches!(state, State::STATE_RECEIVING | State::STATE_FINALIZED) && response.error.is_empty(),
    "invalid artifact progress state"
  );
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
  if state == State::STATE_FINALIZED {
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
      let template = &self.chunked.entries[index];
      let progress = self
        .chunk_progress
        .entry(template.artifact_id.clone())
        .or_insert_with(|| Progress::new(self.backoff_policy.backoff_mark_update()));
      if progress.ready > Instant::now() {
        continue;
      }
      let mut request = progress.request(template);
      let scrub_rules = self.scrub_rules.clone();
      let descriptor = request
        .chunk_index
        .map(|index| template.chunks[index as usize].clone());
      let filesystem = self.file_system.clone();
      let sender = self.data_upload_tx.clone();
      let hooks = self.test_hooks.clone();
      self.chunk_cursor = index + 1;
      self.chunk_task_handle = Some(tokio::spawn(async move {
        let id = request.artifact_id.clone();
        let uuid = request.upload_uuid.clone();
        let operation = if request.chunk_index.is_some() {
          "put_chunk"
        } else {
          "begin"
        };
        let chunk_index = request.chunk_index;
        let started = Instant::now();
        let result = async {
          if let Some(hooks) = hooks {
            hooks.before_chunk_transfer().await;
          }
          if let Some(descriptor) = descriptor {
            let path = chunk_path(&id, request.chunk_index.unwrap_or_default() as usize);
            let mut file = filesystem
              .open_regular_file(&path)
              .await
              .map_err(fragment_read_error)?;
            let contents = read_limited(&mut file, CHUNK_BYTES)
              .await
              .map_err(fragment_read_error)?;
            request.contents = tokio::task::spawn_blocking(move || {
              anyhow::ensure!(
                contents.len() == descriptor.encoded_bytes as usize
                  && Sha256::digest(&contents).as_slice() == descriptor.encoded_sha256,
                InvalidFragment(anyhow::anyhow!("staged chunk changed before transfer"))
              );
              Ok::<_, anyhow::Error>(contents)
            })
            .await??;
          }
          request.state_metadata =
            super::scrub_metadata(&scrub_rules.borrow(), &request.state_metadata);
          check_scrubbed_metadata(&request)?;
          log::debug!(
            "sending chunked artifact: artifact_id={id} upload_uuid={uuid} operation={operation} \
             chunk_index={chunk_index:?} payload_bytes={}",
            request.contents.len(),
          );
          let (tracked, response) = TrackedArtifactUpload::new(uuid.clone(), request);
          sender
            .send(DataUpload::ArtifactUpload(tracked))
            .await
            .map_err(|_| anyhow::anyhow!("artifact transport shut down"))?;
          let response = response.await?;
          log::debug!(
            "chunked artifact ACK: artifact_id={id} upload_uuid={uuid} operation={operation} \
             chunk_index={chunk_index:?} state={} elapsed_ms={}",
            response.state.value(),
            started.elapsed().as_millis(),
          );
          Ok(response)
        }
        .await;
        if let Err(error) = &result {
          log::debug!(
            "chunked artifact attempt failed: artifact_id={id} upload_uuid={uuid} \
             operation={operation} chunk_index={chunk_index:?} elapsed_ms={} error={error}",
            started.elapsed().as_millis(),
          );
        }
        Attempt { id, uuid, result }
      }));
      break;
    }
  }

  pub(super) async fn handle_chunk_attempt(&mut self, attempt: Attempt) -> anyhow::Result<()> {
    let Some(index) = self
      .chunked
      .entries
      .iter()
      .position(|entry| entry.artifact_id == attempt.id)
    else {
      return Ok(());
    };
    let entry = &mut self.chunked.entries[index];
    let response = attempt.result.and_then(|response| {
      validate_ack(entry, &attempt.uuid, &response).map(|state| (response, state))
    });
    if matches!(
      &response,
      Ok((_, State::STATE_RECEIVING | State::STATE_FINALIZED))
    ) {
      let progress = self
        .chunk_progress
        .get_mut(&attempt.id)
        .ok_or_else(|| anyhow::anyhow!("missing chunk transfer progress"))?;
      if !progress.intent_observed {
        progress.intent_observed = true;
        self.stats.accepted_intent.inc();
      }
    }
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
      Ok((_, State::STATE_DROPPED)) => {
        log::debug!("artifact intent dropped: artifact_id={}", attempt.id);
        self.stats.dropped_intent.inc();
        self
          .retire_chunk(
            index,
            Err("artifact dropped during intent negotiation".into()),
          )
          .await?;
      },
      Err(error) if error.is::<InvalidFragment>() || error.is::<OversizedMetadata>() => {
        log::warn!(
          "retiring invalid chunked artifact: artifact_id={} error={error}",
          attempt.id
        );
        self.retire_chunk(index, Err(error.to_string())).await?;
      },
      result => {
        let progress = self
          .chunk_progress
          .get_mut(&attempt.id)
          .ok_or_else(|| anyhow::anyhow!("missing chunk transfer progress"))?;
        match result {
          Ok((response, State::STATE_RECEIVING)) => {
            progress.begin = false;
            progress.receipts = response.received_chunks;
            log::debug!(
              "chunked artifact progress: artifact_id={} upload_uuid={} received_chunks={} \
               total_chunks={}",
              attempt.id,
              attempt.uuid,
              progress
                .receipts
                .iter()
                .map(|byte| byte.count_ones())
                .sum::<u32>(),
              entry.chunks.len(),
            );
            // Complete receipts are not completion. Resume without monopolizing upload capacity.
            if (0 .. entry.chunks.len())
              .all(|index| progress.receipts[index / 8] & (1 << (index % 8)) != 0)
            {
              let delay = progress.backoff.next_backoff().unsigned_abs();
              progress.ready = Instant::now() + delay;
              log::debug!(
                "chunked artifact awaiting publication: artifact_id={} retry_delay_ms={}",
                attempt.id,
                delay.as_millis(),
              );
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
            let delay = progress.backoff.next_backoff().unsigned_abs();
            progress.ready = Instant::now() + delay;
            log::debug!(
              "chunked artifact resume scheduled: artifact_id={} upload_uuid={} retry_delay_ms={}",
              attempt.id,
              attempt.uuid,
              delay.as_millis(),
            );
          },
        }
      },
    }
    Ok(())
  }

  async fn retire_chunk(&mut self, index: usize, result: Result<(), String>) -> anyhow::Result<()> {
    let entry = self.chunked.entries.remove(index);
    let id = entry.artifact_id.clone();
    let paths: Vec<_> = owned_paths(&entry).collect();
    self.chunk_progress.remove(&id);
    // On a failed index commit keep bytes charged and let reconciliation finish their cleanup.
    let retirement = self.retire_files(&paths).await;
    log::debug!(
      "retiring chunked artifact: artifact_id={id} succeeded={} owned_files={} cleanup_complete={}",
      result.is_ok(),
      paths.len(),
      retirement.is_ok(),
    );
    self.complete_upload(&id, result);
    retirement
  }
}
