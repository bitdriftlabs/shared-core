// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./consumer_test.rs"]
mod consumer_test;

use crate::flush_registry::{
  PendingTriggerUploadsStore,
  PersistedTriggerUpload,
  PersistedTriggerUploadBufferProgress,
  PersistedTriggerUploadLifecycle,
  PersistedTriggerUploadSource,
  flush_buffer_id_from_trigger_upload_source,
};
use crate::service::{self, UploadRequest, UploadResult};
use crate::state_upload::StateUploadHandle;
use crate::trigger_upload_artifact::{
  PersistedTriggerUploadArtifactBatch,
  TriggerUploadArtifactStore,
};
use bd_api::upload::LogBatch;
use bd_api::{TriggerUpload, TriggerUploadSource, TriggerUploadStreaming};
use bd_buffer::{AbslCode, Buffer, BufferEvent, BufferEventWithResponse, Consumer, Error};
use bd_client_common::error::InvariantError;
use bd_client_common::maybe_await;
use bd_client_stats_store::{Counter, Scope};
use bd_error_reporter::reporter::handle_unexpected_error_with_details;
use bd_log_primitives::EncodableLog;
use bd_resilient_kv::RetentionHandle;
use bd_runtime::runtime::{ConfigLoader, DurationWatch, IntWatch, Watch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_stats_common::Counter as _;
use bd_time::OffsetDateTimeExt;
use bd_workflows::engine::ProcessLocalPendingFlushState;
use futures_util::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{Instant, Sleep, sleep};
use tower::{Service, ServiceExt};
use tracing::Instrument as _;
use unwrap_infallible::UnwrapInfallible;

// Feature flags used to control the upload parameters.
#[derive(Clone)]
struct Flags {
  // The maximum number of logs allowed per batch size.
  max_batch_size_logs: IntWatch<bd_runtime::runtime::log_upload::BatchSizeFlag>,

  // The maximum number of bytes allowed per batch size.
  max_match_size_bytes: IntWatch<bd_runtime::runtime::log_upload::BatchSizeBytesFlag>,

  // The duration to wait before uploading an incomplete batch. The batch will be uploaded
  // at this point regardless of log activity.
  batch_deadline: DurationWatch<bd_runtime::runtime::log_upload::BatchDeadlineFlag>,

  // The duration to wait before uploading an incomplete batch for streaming logs.
  streaming_batch_deadline:
    DurationWatch<bd_runtime::runtime::log_upload::StreamingBatchDeadlineFlag>,

  // The lookback window for the flush buffer uploads.
  upload_lookback_window_feature_flag:
    Watch<time::Duration, bd_runtime::runtime::log_upload::FlushBufferLookbackWindow>,
}

pub struct RemoteFlushStreamingRequest {
  pub(crate) id: String,
  pub(crate) buffer_ids: Vec<String>,
  pub(crate) session_id: String,
  pub(crate) streaming: TriggerUploadStreaming,
}

impl RemoteFlushStreamingRequest {
  fn new(
    id: String,
    buffer_ids: Vec<String>,
    session_id: String,
    streaming: TriggerUploadStreaming,
  ) -> Self {
    Self {
      id,
      buffer_ids,
      session_id,
      streaming,
    }
  }
}

#[derive(Debug)]
struct TriggerBatchUploadCanceled;

impl Display for TriggerBatchUploadCanceled {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_str("trigger batch upload canceled")
  }
}

impl std::error::Error for TriggerBatchUploadCanceled {}

//
// TriggerUploadIdentity
//

#[derive(Clone, Debug)]
struct TriggerUploadIdentity {
  // Durable local identifier used for persistence, artifact naming, and restart replay.
  durable_upload_id: String,

  // Optional outward-facing identifier copied onto LogUploadRequest for intent-negotiated trigger
  // uploads.
  request_trigger_uuid: Option<String>,
}

impl TriggerUploadIdentity {
  fn new(source: &TriggerUploadSource, request_trigger_uuid: Option<String>) -> Self {
    let durable_upload_id = request_trigger_uuid
      .clone()
      .unwrap_or_else(|| source.logical_id().to_string());

    Self {
      durable_upload_id,
      request_trigger_uuid,
    }
  }

  fn durable_upload_id(&self) -> &str {
    &self.durable_upload_id
  }
}

// Responsible for managing the lifetime of upload tasks as buffers are added/removed via dynamic
// reconfiguration. When continuous buffers are added, a new task is spawned which is responsible
// for performing continuous uploads of said buffer. On removal or on shutdown, these tasks are
// gracefully shut down.
pub struct BufferUploadManager {
  log_upload_service: service::Upload,

  // Feature flags used
  feature_flags: Flags,

  // Used to notify the consumer to shut down processing.
  shutdown: ComponentShutdown,

  // The receiver for updates to the set of ring buffers.
  buffer_event_rx: Receiver<BufferEventWithResponse>,

  // Receiver for a request to upload the contents of a specific ring buffer.
  trigger_upload_rx: Receiver<TriggerUpload>,

  // Sender for remote flush streaming requests that should be activated in the processing
  // pipeline once a concrete set of trigger buffers has been scheduled.
  remote_flush_streaming_tx: Sender<RemoteFlushStreamingRequest>,

  // The map of active trigger buffers. The uploader receives a request to upload a buffer by name,
  // which is looked up in this map and initiated if the buffer is found.
  trigger_buffers: HashMap<String, Arc<Buffer>>,

  // Mapping from buffer id to the shutdown and join handle for the consumer task corresponding
  // to said buffer. This is used to track shutdowns for both continuous and trigger uploads.
  shutdowns: HashMap<String, ComponentShutdownTrigger>,

  // Keeps track of triggers with active uploads. This is used to de-dupe requests to upload a
  // buffer which is already being uploaded.
  active_trigger_uploads: HashSet<String>,

  // Used to log internal logs to the ring buffer.
  logging: Arc<dyn bd_internal_logging::Logger>,

  // Shutdown trigger for the stream buffer upload task, allowing us to cancel the task once the
  // buffer is no longer needed.
  stream_buffer_shutdown_trigger: Option<ComponentShutdownTrigger>,

  old_logs_dropped: Counter,

  // State upload handle for uploading state snapshots before logs.
  state_upload_handle: Option<Arc<StateUploadHandle>>,

  // Durable registry of trigger uploads that have been scheduled but not yet completed.
  pending_trigger_uploads: PendingTriggerUploadsStore,

  // Process-local workflow mirror of pending flush IDs. `PendingTriggerUploadsStore` remains the
  // durable source of truth across restart.
  process_local_pending_flush_state: Arc<ProcessLocalPendingFlushState>,

  logger_state_directory: Arc<PathBuf>,
}

impl BufferUploadManager {
  pub(crate) fn new(
    data_upload_tx: tokio::sync::mpsc::Sender<bd_api::DataUpload>,
    runtime_loader: &Arc<ConfigLoader>,
    sdk_directory: &Path,
    shutdown: ComponentShutdown,
    buffer_event_rx: Receiver<BufferEventWithResponse>,
    trigger_upload_rx: Receiver<TriggerUpload>,
    remote_flush_streaming_tx: Sender<RemoteFlushStreamingRequest>,
    stats: &Scope,
    logging: Arc<dyn bd_internal_logging::Logger>,
    state_upload_handle: Option<Arc<StateUploadHandle>>,
    pending_trigger_uploads: PendingTriggerUploadsStore,
    process_local_pending_flush_state: Arc<ProcessLocalPendingFlushState>,
  ) -> Self {
    let logger_state_directory = Arc::new(sdk_directory.join("state").join("logger"));

    Self {
      log_upload_service: service::new(data_upload_tx, shutdown.clone(), runtime_loader, stats),
      feature_flags: Flags {
        max_batch_size_logs: runtime_loader.register_int_watch(),
        max_match_size_bytes: runtime_loader.register_int_watch(),
        batch_deadline: runtime_loader.register_duration_watch(),
        upload_lookback_window_feature_flag: runtime_loader.register_duration_watch(),
        streaming_batch_deadline: runtime_loader.register_duration_watch(),
      },
      shutdown,
      buffer_event_rx,
      trigger_upload_rx,
      remote_flush_streaming_tx,
      trigger_buffers: HashMap::new(),
      shutdowns: HashMap::new(),
      active_trigger_uploads: HashSet::new(),
      logging,
      stream_buffer_shutdown_trigger: None,
      old_logs_dropped: stats.counter("old_logs_dropped"),
      state_upload_handle,
      pending_trigger_uploads,
      process_local_pending_flush_state,
      logger_state_directory,
    }
  }

  #[tracing::instrument(level = "debug", skip(self), name = "BufferUploadManager")]
  pub async fn run(mut self) -> anyhow::Result<()> {
    let recovered_pending_trigger_uploads =
      self.pending_trigger_uploads.pending_uploads().await.len();
    if recovered_pending_trigger_uploads != 0 {
      log::debug!(
        "found {recovered_pending_trigger_uploads} persisted trigger uploads pending recovery"
      );
    }

    // For each spawned task, track the cancellation token and join handle,
    // allowing us to cancel the task when a buffer has been removed.

    // Used to notify the upload manager about trigger uploads completing, allowing it to clear up
    // state allowing for new uploads to be scheduled for a buffer.
    let (trigger_upload_complete_tx, mut trigger_upload_complete_rx) = channel(1);

    loop {
      tokio::select! {
        Some(event) = self.buffer_event_rx.recv() => {
          self.handle_buffer_event(event, &trigger_upload_complete_tx).await?;
        },
        Some(trigger) = self.trigger_upload_rx.recv() =>
          self.handle_trigger_uploads(trigger, &trigger_upload_complete_tx).await?,
        Some(completed_trigger_buffer) = trigger_upload_complete_rx.recv() => {
          self.active_trigger_uploads.remove(&completed_trigger_buffer);
        },
        () = self.shutdown.cancelled() => {
          self.shutdown().await;
          return Ok(());
        },
        else => {
          self.shutdown().await;
          return Ok(());
        }
      }
    }
  }

  // Checks to see if the buffers we're trying to upload are already being uploaded, and if not
  // spawn a task to consume the entire buffer.
  async fn handle_trigger_uploads(
    &mut self,
    trigger_upload: TriggerUpload,
    trigger_upload_complete_tx: &Sender<String>,
  ) -> anyhow::Result<()> {
    log::debug!("received trigger upload request");

    let TriggerUpload {
      buffer_ids,
      streaming,
      source,
      request_trigger_uuid,
      session_id,
    } = trigger_upload;

    // Intent-negotiated trigger uploads have a per-trigger UUID that the server can use for
    // completion deduplication. Trigger uploads without intent negotiation still need a durable
    // internal key for retries and replay, so they fall back to the logical trigger ID.
    let trigger_upload_identity = TriggerUploadIdentity::new(&source, request_trigger_uuid);
    let trigger_upload_source = PersistedTriggerUploadSource::from(&source);
    let trigger_upload_streaming = streaming.as_ref().map(Into::into);
    let is_remote_streaming_activation = matches!(source, TriggerUploadSource::RemoteCommand(_));
    let mut buffer_upload_completions = vec![];
    let mut eligible_buffer_ids = vec![];
    let mut scheduled_consumers = vec![];
    let buffer_ids = if buffer_ids.is_empty() {
      self.trigger_buffers.keys().cloned().collect::<Vec<_>>()
    } else {
      buffer_ids
    };

    // Trigger uploads are coordinated in two phases. First we decide which buffers can actually
    // participate in this upload and register a consumer for each of them. Only after that do we
    // persist a single upload record describing the concrete buffer set. Persisting the filtered
    // set rather than the original request means restart recovery can replay exactly the work that
    // was scheduled in this process.
    //
    // There is one additional invariant layered on top of the source-scoped upload model: before a
    // fresh non-active attempt for buffer B is admitted, we evict any older durable trigger-upload
    // state that still refers to B. The overall registry remains keyed by logical source ID
    // because aggregate completion, optional streaming activation, and restart replay are still
    // tracked per logical flush. The cleanup is buffer-scoped because the correctness problem we
    // are solving is narrower: once we are about to upload buffer B again, we should not preserve
    // stale durable history that could cause B to be replayed twice from two different uploads.

    for buffer_id in buffer_ids {
      // Admission is also deduped by buffer while a trigger upload is already active. If buffer B
      // is currently flushing because some earlier source ID admitted it, a later request for B is
      // dropped here before persistence rather than stacking up a second pending upload for that
      // same buffer. A partially overlapping request can still proceed for any other buffers that
      // are not already active.
      if self.active_trigger_uploads.contains(&buffer_id) {
        log::debug!("ignoring upload for {buffer_id}, already in progress");
        continue;
      }

      // Initiate a new consumer if we are aware of a buffer by this name. There might not be a
      // buffer here if a buffer has just been added/removed, i.e. the thread local loggers
      // are not in sync with the state within the upload manager. This can also happen if the
      // match configuration targets a buffer that doesn't have a corresponding buffer config,
      // which is not validated.
      if let Some(buffer) = self.trigger_buffers.get(&buffer_id).cloned() {
        // At this point buffer_id is not currently active in memory, so it is safe to prune older
        // durable references to that same buffer before we admit a fresh attempt. This does not
        // collapse the entire upload model down to buffer IDs: older uploads can keep any other
        // buffers they still own, and aggregate source-scoped completion remains intact unless the
        // pruned buffer was their last member.
        Self::prune_stale_trigger_upload_buffers(
          self.pending_trigger_uploads.clone(),
          self.logger_state_directory.clone(),
          self.process_local_pending_flush_state.clone(),
          trigger_upload_identity.durable_upload_id(),
          &buffer_id,
        )
        .await;

        let trigger_consumer =
          self.new_trigger_consumer(&buffer_id, trigger_upload_identity.clone(), &buffer)?;

        self.active_trigger_uploads.insert(buffer_id.clone());
        eligible_buffer_ids.push(buffer_id.clone());

        self
          .logging
          .log_internal(&format!("starting trigger upload for buffer {buffer_id:?}"));

        scheduled_consumers.push((buffer_id, trigger_consumer));
      } else {
        log::debug!("ignoring upload for {buffer_id}, unknown buffer");
      }
    }

    if !eligible_buffer_ids.is_empty() {
      // Persist the upload before any background worker can complete. That gives restart recovery
      // a durable description of the upload even if the process dies after tasks are spawned but
      // before any individual buffer batch has been uploaded. If an overlapping request was
      // filtered by the active-buffer gate above, this durable record only contains the eligible
      // subset that this process actually admitted.
      self
        .pending_trigger_uploads
        .upsert(PersistedTriggerUpload {
          id: trigger_upload_identity.durable_upload_id.clone(),
          source: trigger_upload_source,
          session_id: session_id.clone(),
          buffers: eligible_buffer_ids
            .iter()
            .cloned()
            .map(PersistedTriggerUploadBufferProgress::new)
            .collect(),
          streaming: trigger_upload_streaming,
          lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
        })
        .await;
      self
        .process_local_pending_flush_state
        .mark_pending(flush_buffer_id_from_trigger_upload_source(&source));
    }

    // Remote-command streaming should only become visible once the associated trigger upload has
    // actually completed. We therefore carry the activation request alongside the upload and only
    // deliver it from the aggregate completion task below.
    let remote_streaming_request = if is_remote_streaming_activation {
      streaming.map(|streaming| {
        RemoteFlushStreamingRequest::new(
          trigger_upload_identity.durable_upload_id.clone(),
          eligible_buffer_ids.clone(),
          session_id.clone(),
          streaming,
        )
      })
    } else {
      None
    };

    for (buffer_id, trigger_consumer) in scheduled_consumers {
      let upload_complete_tx = trigger_upload_complete_tx.clone();
      let buffer_id_clone = buffer_id.clone();
      let internal_logger = self.logging.clone();
      let shutdown_trigger = ComponentShutdownTrigger::default();
      let shutdown = shutdown_trigger.make_shutdown();
      let (single_upload_complete_tx, single_upload_complete_rx) = tokio::sync::oneshot::channel();
      buffer_upload_completions.push(single_upload_complete_rx);
      tokio::task::spawn(
        async move {
          // Handle the error here so that we can fire the complete message even on failure.
          let upload_completed = match trigger_consumer.run().await {
            Ok(logs_count) => {
              internal_logger.log_internal(&format!(
                "completed trigger upload for buffer {buffer_id_clone:?}, uploaded logs count: \
                 {logs_count:?}",
              ));
              true
            },
            Err(e) => {
              internal_logger.log_internal(&format!(
                "failed trigger upload for buffer {buffer_id_clone:?}: {e:?}"
              ));
              if !e.is::<TriggerBatchUploadCanceled>() {
                handle_unexpected_error_with_details(e, "", || None);
              }
              false
            },
          };

          single_upload_complete_tx
            .send(upload_completed)
            .map_err(|_| InvariantError::Invariant)?;

          // TODO(mattklein123): Should we pass this into the trigger consumer and actually
          // try to bail quickly if it's taking a long time and we are trying to shutdown?
          drop(shutdown);
          upload_complete_tx
            .send(buffer_id_clone)
            .await
            .map_err(|_| InvariantError::Invariant)
        }
        .instrument(tracing::debug_span!(
          "trigger_consumer",
          buffer_id = &*buffer_id
        )),
      );

      // We never cancel the trigger uploads directly, so just reuse the top level shutdown
      // token.
      self.shutdowns.insert(buffer_id.clone(), shutdown_trigger);
    }

    // Since we have one completion handler for the entire set of trigger uploads, we need to
    // spawn a task to wait for all of the individual trigger uploads to complete before we can
    // signal that the trigger uploads are complete. The durable upload entry stays source-scoped
    // even though stale replacement above happens buffer-by-buffer: one logical upload can still
    // own multiple admitted buffers, one deferred remote-streaming payload, and one
    // process-local flush ID that should only complete once every surviving buffer in that upload
    // has succeeded or the entire upload has been pruned away.
    let pending_trigger_uploads = self.pending_trigger_uploads.clone();
    let process_local_pending_flush_state = self.process_local_pending_flush_state.clone();
    let remote_flush_streaming_tx = self.remote_flush_streaming_tx.clone();
    let tracked_flush_id = flush_buffer_id_from_trigger_upload_source(&source);
    tokio::spawn(async move {
      let completed_uploads = match try_join_all(buffer_upload_completions).await {
        Ok(completed_uploads) => completed_uploads,
        Err(e) => {
          log::debug!("failed to wait for trigger uploads to complete: {e:?}");
          return;
        },
      };

      if completed_uploads.iter().any(|completed| !completed) {
        log::debug!(
          "preserving pending trigger upload state for {durable_trigger_upload_id} because at \
           least one buffer upload did not complete successfully",
          durable_trigger_upload_id = trigger_upload_identity.durable_upload_id,
        );
        return;
      }

      if let Some(remote_streaming_request) = remote_streaming_request
        && remote_flush_streaming_tx
          .send(remote_streaming_request)
          .await
          .is_err()
      {
        log::debug!(
          "preserving pending trigger upload state for {durable_trigger_upload_id} because remote \
           streaming activation could not be delivered",
          durable_trigger_upload_id = trigger_upload_identity.durable_upload_id,
        );
        return;
      }

      // Make completion visible to workflows only after the durable registry entry is gone. That
      // keeps the process-local pending set aligned with the persisted source of truth.
      pending_trigger_uploads
        .remove(&trigger_upload_identity.durable_upload_id)
        .await;
      process_local_pending_flush_state.mark_completed(&tracked_flush_id);

      log::debug!("signaling all trigger uploads complete");
    });

    Ok(())
  }

  // Handles a single buffer event, where a buffer is either added or removed.
  async fn handle_buffer_event(
    &mut self,
    event: BufferEventWithResponse,
    trigger_upload_complete_tx: &Sender<String>,
  ) -> anyhow::Result<()> {
    match &event.event {
      // When a new buffer is created, spawn a new task which is responsible for
      // consuming logs from this buffer continuously.
      BufferEvent::ContinuousBufferCreated(id, buffer) => {
        log::debug!("creating new continuous upload task for buffer {id:?}");
        let (uploader, shutdown_trigger) = self.new_continous_consumer(id, buffer)?;
        tokio::spawn(
          uploader
            .consume_continuous_logs()
            .instrument(tracing::info_span!(
              "consume_continuous_logs",
              buffer_id = id
            )),
        );
        self.shutdowns.insert(id.clone(), shutdown_trigger);
      },
      BufferEvent::TriggerBufferCreated(id, buffer) => {
        log::debug!("registering new trigger buffer {id}");

        self.trigger_buffers.insert(id.clone(), buffer.clone());
      },
      // When a buffer is removed, cancel the consumption task for this buffer.
      BufferEvent::BufferRemoved(id) => {
        // The removed buffer will either be a trigger buffer or a continuous buffer, so try to
        // cancel/remove either.
        if let Some(shutdown_trigger) = self.shutdowns.remove(id) {
          log::debug!("terminating consume task for buffer {id}");
          shutdown_trigger.shutdown().await;
        }

        // TODO(snowp): Consider terminating the upload task for a buffer if it gets removed.
        self.trigger_buffers.remove(id);
      },
      BufferEvent::StreamBufferAdded(buffer) => {
        log::debug!("initializing stream buffer consumer");

        debug_assert!(self.stream_buffer_shutdown_trigger.is_none());

        let shutdown_trigger = ComponentShutdownTrigger::default();
        let shutdown = shutdown_trigger.make_shutdown();
        self.stream_buffer_shutdown_trigger = Some(shutdown_trigger);

        let log_upload_service = self.log_upload_service.clone();
        let consumer = buffer.clone().register_consumer()?;

        let batch_builder = BatchBuilder::new(self.feature_flags.clone());
        tokio::task::spawn(async move {
          StreamedBufferUpload {
            consumer,
            log_upload_service,
            batch_builder,
            shutdown,
          }
          .start()
          .await
        });
      },
      BufferEvent::StreamBufferRemoved(_) => {
        log::debug!("shutting down stream buffer consumer");
        debug_assert!(self.stream_buffer_shutdown_trigger.is_some());

        if let Some(shutdown_trigger) = self.stream_buffer_shutdown_trigger.take() {
          shutdown_trigger.shutdown().await;
        }
      },
      BufferEvent::TriggerBufferConfigUpdated(configured_trigger_buffer_ids) => {
        self
          .recover_pending_trigger_uploads(
            configured_trigger_buffer_ids.iter().cloned().collect(),
            trigger_upload_complete_tx,
          )
          .await?;
      },
    }

    Ok(())
  }

  async fn prune_stale_trigger_upload_buffers(
    pending_trigger_uploads: PendingTriggerUploadsStore,
    logger_state_directory: Arc<PathBuf>,
    process_local_pending_flush_state: Arc<ProcessLocalPendingFlushState>,
    current_upload_id: &str,
    buffer_id: &str,
  ) {
    // This helper is deliberately static over owned clones rather than borrowing `self`. The
    // upload manager runs inside a spawned task, so keeping this path free of long-lived borrows
    // avoids making the manager future non-Send while still letting us share the same cleanup
    // invariant between live scheduling and restart recovery.
    let pruned_uploads = pending_trigger_uploads
      .prune_buffer_from_other_uploads(current_upload_id, buffer_id)
      .await;

    if pruned_uploads.is_empty() {
      return;
    }

    log::debug!(
      "pruning stale trigger upload state for buffer {buffer_id} before admitting upload \
       {current_upload_id}: {:?}",
      pruned_uploads
        .iter()
        .map(|pruned| (&pruned.upload_id, pruned.removed_upload))
        .collect::<Vec<_>>()
    );

    for pruned_upload in pruned_uploads {
      Self::clear_trigger_upload_artifact(
        logger_state_directory.as_ref(),
        &pruned_upload.upload_id,
        &pruned_upload.buffer_id,
      )
      .await;

      if pruned_upload.removed_upload {
        // Pruning the last remaining buffer out of an older upload means that logical flush no
        // longer has any durable work left anywhere in the logger. At that point the old
        // process-local pending ID must be completed as well so workflows and remote streaming do
        // not wait forever on an upload whose final buffer has been intentionally replaced.
        process_local_pending_flush_state
          .mark_completed(&pruned_upload.source.to_flush_buffer_id());
      }
    }
  }

  async fn recover_pending_trigger_uploads(
    &mut self,
    configured_trigger_buffer_ids: HashSet<String>,
    trigger_upload_complete_tx: &Sender<String>,
  ) -> anyhow::Result<()> {
    // Recovery treats the durable registry as authoritative. We first reconcile any persisted
    // upload against the current trigger-buffer config, then feed the surviving work back through
    // the normal scheduling path so live and recovered uploads share one execution path.
    //
    // Recovery iterates newest-first because admitting a newer upload for buffer B may prune older
    // durable references to B before those older uploads are considered. By reloading the current
    // upload record by ID on each iteration, recovery observes the post-prune truth instead of the
    // stale snapshot captured at startup. This is what keeps restart replay aligned with the live
    // invariant that a fresh non-active attempt for B replaces older durable state for B instead of
    // letting multiple upload histories for B coexist.
    let pending_upload_ids = self
      .pending_trigger_uploads
      .pending_uploads()
      .await
      .into_iter()
      .map(|upload| upload.id)
      .collect::<Vec<_>>();

    for pending_upload_id in pending_upload_ids.into_iter().rev() {
      let Some(pending_upload) = self
        .pending_trigger_uploads
        .pending_upload(&pending_upload_id)
        .await
      else {
        continue;
      };

      let missing_buffer_ids: Vec<_> = pending_upload
        .buffers
        .iter()
        .filter(|buffer| !configured_trigger_buffer_ids.contains(&buffer.buffer_id))
        .map(|buffer| buffer.buffer_id.clone())
        .collect();

      let pending_upload = if missing_buffer_ids.is_empty() {
        pending_upload
      } else {
        let retained_buffers: Vec<_> = pending_upload
          .buffers
          .iter()
          .filter(|buffer| configured_trigger_buffer_ids.contains(&buffer.buffer_id))
          .cloned()
          .collect();

        Self::clear_trigger_upload_artifacts(
          self.logger_state_directory.as_ref(),
          &pending_upload.id,
          &missing_buffer_ids,
        )
        .await;

        if retained_buffers.is_empty() {
          log::debug!(
            "abandoning persisted trigger upload {} because configured trigger buffers are \
             missing {:?}",
            pending_upload.id,
            missing_buffer_ids,
          );
          self
            .pending_trigger_uploads
            .remove(&pending_upload.id)
            .await;
          self
            .process_local_pending_flush_state
            .mark_completed(&pending_upload.source.to_flush_buffer_id());
          continue;
        }

        // Rewrite the durable record with only the buffers that still exist in config. This keeps
        // restart recovery from retrying buffers that can no longer be resolved in the current
        // process.
        log::debug!(
          "pruning persisted trigger upload {} due to missing configured trigger buffers {:?}; \
           retaining {:?}",
          pending_upload.id,
          missing_buffer_ids,
          retained_buffers
            .iter()
            .map(|buffer| buffer.buffer_id.clone())
            .collect::<Vec<_>>(),
        );

        let mut updated_upload = pending_upload;
        updated_upload.buffers = retained_buffers;
        self
          .pending_trigger_uploads
          .upsert(updated_upload.clone())
          .await;
        updated_upload
      };

      if pending_upload
        .buffers
        .iter()
        .any(|buffer| self.active_trigger_uploads.contains(&buffer.buffer_id))
      {
        continue;
      }

      if matches!(
        pending_upload.lifecycle,
        PersistedTriggerUploadLifecycle::Completed | PersistedTriggerUploadLifecycle::Failed
      ) {
        log::debug!(
          "skipping persisted trigger upload {} with lifecycle {:?}",
          pending_upload.id,
          pending_upload.lifecycle
        );
        continue;
      }

      log::debug!(
        "recovering persisted trigger upload {} for buffers {:?}",
        pending_upload.id,
        pending_upload.buffer_ids()
      );

      self
        .handle_trigger_uploads(
          match pending_upload.source {
            PersistedTriggerUploadSource::RemoteCommand(_) => TriggerUpload::new(
              pending_upload.buffer_ids(),
              pending_upload.streaming(),
              pending_upload.source.to_trigger_upload_source(),
              pending_upload.session_id.clone(),
            ),
            PersistedTriggerUploadSource::WorkflowAction(_)
            | PersistedTriggerUploadSource::ExplicitSessionCapture(_) => {
              TriggerUpload::new_with_request_trigger_uuid(
                pending_upload.buffer_ids(),
                pending_upload.streaming(),
                pending_upload.source.to_trigger_upload_source(),
                pending_upload.id.clone(),
                pending_upload.session_id.clone(),
              )
            },
          },
          trigger_upload_complete_tx,
        )
        .await?;
    }

    Ok(())
  }

  async fn clear_trigger_upload_artifacts(
    logger_state_directory: &Path,
    trigger_upload_id: &str,
    buffer_ids: &[String],
  ) {
    for buffer_id in buffer_ids {
      Self::clear_trigger_upload_artifact(logger_state_directory, trigger_upload_id, buffer_id)
        .await;
    }
  }

  async fn clear_trigger_upload_artifact(
    logger_state_directory: &Path,
    trigger_upload_id: &str,
    buffer_id: &str,
  ) {
    let artifact_store =
      TriggerUploadArtifactStore::new(logger_state_directory, trigger_upload_id, buffer_id);

    if let Err(e) = artifact_store.remove_queued_batch().await {
      log::debug!(
        "failed to clear queued artifact for abandoned trigger upload {trigger_upload_id} buffer \
         {buffer_id}: {e}",
      );
    }

    if let Err(e) = artifact_store.clear_inflight_batch().await {
      log::debug!(
        "failed to clear inflight artifact for abandoned trigger upload {trigger_upload_id} \
         buffer {buffer_id}: {e}",
      );
    }
  }

  async fn shutdown(self) {
    // We're shutting down, so cancel all tasks.
    for (_, shutdown_trigger) in self.shutdowns {
      shutdown_trigger.shutdown().await;
    }

    log::debug!("all consumers terminated");
  }

  // Creates a new ContinousBufferUploader with an associated cancel token which can be used to
  // cancel the consumption task.
  fn new_continous_consumer(
    &self,
    buffer_name: &str,
    buffer: &Arc<Buffer>,
  ) -> anyhow::Result<(ContinuousBufferUploader, ComponentShutdownTrigger)> {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let consumer = buffer.create_continous_consumer()?;
    let retention_handle = consumer.retention_handle();

    Ok((
      ContinuousBufferUploader::new(
        consumer,
        retention_handle,
        self.log_upload_service.clone(),
        self.feature_flags.clone(),
        shutdown_trigger.make_shutdown(),
        buffer_name.to_string(),
        self.state_upload_handle.clone(),
      ),
      shutdown_trigger,
    ))
  }

  // Creates a new CompleteBufferUpload which will attempt to consume the specified buffer until the
  // end, shutting down once there are no more records to read.
  fn new_trigger_consumer(
    &self,
    buffer_name: &str,
    trigger_upload_identity: TriggerUploadIdentity,
    buffer: &Arc<Buffer>,
  ) -> anyhow::Result<CompleteBufferUpload> {
    let artifact_store = TriggerUploadArtifactStore::new(
      self.logger_state_directory.as_ref(),
      trigger_upload_identity.durable_upload_id(),
      buffer_name,
    );

    Ok(CompleteBufferUpload::new(
      buffer.new_consumer()?,
      self.feature_flags.clone(),
      self.log_upload_service.clone(),
      buffer_name.to_string(),
      self.old_logs_dropped.clone(),
      self.state_upload_handle.clone(),
      self.pending_trigger_uploads.clone(),
      trigger_upload_identity,
      artifact_store,
      buffer.clone(),
    ))
  }
}

//
// BatchSizeLimiter
//

// Helper to keep track of a batch of logs being prepared, and evaluating the current size against
// the limits specified via runtime flags.
struct BatchBuilder {
  flags: Flags,
  total_bytes: usize,
  logs: Vec<Vec<u8>>,
  oldest_micros: Option<u64>,
  newest_micros: Option<u64>,
}

impl BatchBuilder {
  const fn new(flags: Flags) -> Self {
    Self {
      flags,
      total_bytes: 0,
      logs: Vec::new(),
      oldest_micros: None,
      newest_micros: None,
    }
  }

  fn add_log(&mut self, data: Vec<u8>) {
    if let Some(ts) = EncodableLog::extract_timestamp(&data) {
      let ts_micros = ts.unix_timestamp_micros().cast_unsigned();
      self.oldest_micros = Some(self.oldest_micros.map_or(ts_micros, |o| o.min(ts_micros)));
      self.newest_micros = Some(self.newest_micros.map_or(ts_micros, |n| n.max(ts_micros)));
    }
    self.total_bytes += data.len();
    self.logs.push(data);
  }

  fn limit_reached(&self) -> bool {
    if self.logs.is_empty() {
      return false;
    }

    let max_batch_size_logs = *self.flags.max_batch_size_logs.read() as usize;
    let max_batch_size_bytes = *self.flags.max_match_size_bytes.read() as usize;

    max_batch_size_bytes <= self.total_bytes || max_batch_size_logs <= self.logs.len()
  }

  /// Returns the timestamp range (oldest, newest) of logs added to the current batch,
  /// or `None` if no logs with extractable timestamps have been added.
  fn timestamp_range(&self) -> Option<(u64, u64)> {
    self.oldest_micros.zip(self.newest_micros)
  }

  /// Consumes the current batch, resetting all accounting.
  fn take(&mut self) -> Vec<Vec<u8>> {
    self.total_bytes = 0;
    self.oldest_micros = None;
    self.newest_micros = None;
    self.logs.drain(..).collect()
  }
}

//
// ContinuousBufferUploader
//

// The buffer uploader manages continuous uploads of a single ring buffer.
struct ContinuousBufferUploader {
  // A consumer for the ring buffer which should be uploaded.
  consumer: bd_buffer::CursorConsumer,

  log_upload_service: service::Upload,

  // Used to notify the consumer to shut down processing.
  shutdown: ComponentShutdown,

  // Used to track when we should force flush a partial batch.
  flush_batch_sleep: Option<Pin<Box<Sleep>>>,

  // Used to construct the log payload and enforce batch limits.
  batch_builder: BatchBuilder,

  feature_flags: Flags,

  buffer_id: String,

  // State upload handle for uploading state snapshots before logs.
  state_upload_handle: Option<Arc<StateUploadHandle>>,
  retention_handle: RetentionHandle,
}

impl ContinuousBufferUploader {
  pub fn new(
    consumer: bd_buffer::CursorConsumer,
    retention_handle: RetentionHandle,
    log_upload_service: service::Upload,
    feature_flags: Flags,
    shutdown: ComponentShutdown,
    buffer_id: String,
    state_upload_handle: Option<Arc<StateUploadHandle>>,
  ) -> Self {
    Self {
      consumer,
      log_upload_service,
      shutdown,
      flush_batch_sleep: None,
      batch_builder: BatchBuilder::new(feature_flags.clone()),
      feature_flags,
      buffer_id,
      state_upload_handle,
      retention_handle,
    }
  }
  // Attempts to upload all logs in the provided buffer. For every polling interval we
  // attempt to drain as many logs as possible, up to to the batch size. When a batch is
  // ready to upload, we'll repeatedly try to upload this batch until completion.
  async fn consume_continuous_logs(mut self) -> anyhow::Result<()> {
    log::debug!("starting continous log upload");

    loop {
      tokio::select! {
        () = self.shutdown.cancelled() => return Ok(()),
        entry = self.consumer.read() => {
            debug_assert!(entry.is_ok(), "consumer should not fail");
            self.batch_builder.add_log(entry?.to_vec());
            if let Some(oldest) = self.consumer.oldest_timestamp_micros() {
              self.retention_handle.update_retention_micros(oldest);
            }
        },
        () = maybe_await(&mut self.flush_batch_sleep) => {
            log::debug!("flushing logs due to deadline hit");
            self.flush_current_batch().await?;
            continue;
          },
        // If either of the size flags change we want to re-evaluate the current batch.
        _ = self.feature_flags.max_batch_size_logs.changed() => {},
        _ = self.feature_flags.max_match_size_bytes.changed() => {},
      }

      if self.batch_builder.limit_reached() {
        self.flush_current_batch().await?;
      }

      // Arm the flush timer if we have pending logs and there isn't already one present. This
      // ensures that logs won't sit in the current batch for more than the specified
      // deadline.
      if !self.batch_builder.logs.is_empty() && self.flush_batch_sleep.is_none() {
        self.flush_batch_sleep = Some(Box::pin(sleep(
          self.feature_flags.batch_deadline.read().unsigned_abs(),
        )));
      }
    }
  }

  // Consumes the current_batch and performs a log flush.
  async fn flush_current_batch(&mut self) -> anyhow::Result<()> {
    // Disarm the deadline which forces a partial flush to fire.
    self.flush_batch_sleep = None;

    let timestamp_range = self.batch_builder.timestamp_range();
    let logs = self.batch_builder.take();
    let logs_len = logs.len();
    log::debug!("flushing {logs_len} logs");

    if let (Some(handle), Some((oldest, newest))) = (&self.state_upload_handle, timestamp_range) {
      handle.notify_upload_needed(oldest, newest);
    }

    // Attempt to perform an upload of these buffers, with retries ++. See logger/service.rs for
    // details about retry policies etc.
    let upload_future = async {
      self
        .log_upload_service
        .ready()
        .await
        .unwrap_infallible()
        .call(UploadRequest::new(
          LogBatch {
            logs,
            buffer_id: self.buffer_id.clone(),
          },
          false,
        ))
        .await
        .unwrap_infallible()
    };

    // Bail on the upload if we are shutting down this task to avoid deadlocking the API task.
    let result = tokio::select! {
      result = upload_future => result,
      () = self.shutdown.cancelled() => return Ok(()),
    };

    log::debug!("completed continuous upload with result: {result:?}");

    // Regardless of the outcome of the upload, advance the read cursor to mark the records as
    // written.
    for _ in 0 .. logs_len {
      self.consumer.advance_read_cursor()?;
    }
    match self.consumer.oldest_timestamp_micros() {
      Some(oldest_micros) => self.retention_handle.update_retention_micros(oldest_micros),
      None => self
        .retention_handle
        .update_retention_micros(RetentionHandle::RETENTION_NONE),
    }

    Ok(())
  }
}

// StreamedBufferUpload is used to manage uploading streamed logs. Unlike the other upload
// strategies, streamed logs are immediately uploaded without any batching and continuous to upload
// logs from the buffer forever.
struct StreamedBufferUpload {
  // The ring buffer consumer to use to consume logs. We use the cursor consumer to get us async
  // reads, we could technically use the non-cursor version.
  // TODO(mattklein123): I don't think this provides enough value in the streaming case. We should
  // probably just use a memory bound channel for this?
  consumer: Box<dyn bd_buffer::RingBufferConsumer>,

  // Service used to send logs to upload to the uploader.
  log_upload_service: service::Upload,

  batch_builder: BatchBuilder,

  shutdown: ComponentShutdown,
}

impl StreamedBufferUpload {
  async fn start(mut self) -> anyhow::Result<()> {
    loop {
      log::debug!("awaiting stream log");

      let first_log = tokio::select! {
        log = self.consumer.read() => log,
        () = self.shutdown.cancelled() => return Ok(()),
      }?;

      log::debug!("received first log, starting stream upload");

      debug_assert!(self.batch_builder.logs.is_empty());
      self.batch_builder.add_log(first_log.to_vec());

      self.consumer.finish_read()?;
      let deadline = tokio::time::sleep_until(
        Instant::now()
          + self
            .batch_builder
            .flags
            .streaming_batch_deadline
            .read()
            .unsigned_abs(),
      );
      tokio::pin!(deadline);

      loop {
        if self.batch_builder.limit_reached() {
          log::debug!("batch size reached, uploading batch");
          break;
        }

        let result = tokio::select! {
          () = &mut deadline => {
            log::debug!("streaming batch deadline hit, uploading batch");
            break;
          },
          () = self.shutdown.cancelled() => return Ok(()),
          result = self.consumer.read() => {
            result
          },
        };

        match result {
          Ok(log) => {
            self.batch_builder.add_log(log.to_vec());
            self.consumer.finish_read()?;
          },
          Err(e) => {
            anyhow::bail!("failed to read from stream buffer: {e:?}")
          },
        }
      }

      // TODO(snowp): Handle streaming state updates.

      let upload_future = async {
        self
          .log_upload_service
          .ready()
          .await
          .unwrap_infallible()
          .call(UploadRequest::new(
            LogBatch {
              logs: self.batch_builder.take(),
              buffer_id: "streamed".to_string(),
            },
            true,
          ))
          .await
          .unwrap_infallible()
      };

      // Bail on the upload if we are shutting down this task to avoid deadlocking the API task.
      let result = tokio::select! {
        result = upload_future => result,
        () = self.shutdown.cancelled() => return Ok(()),
      };

      log::debug!("completed stream upload with result: {result:?}");
    }
  }
}

// CompleteBufferUpload is used to manage a one-off complete upload of a buffer, caused by a trigger
// being hit or the server requesting the buffer to be uploaded.
struct CompleteBufferUpload {
  // The ring buffer consumer to use to consume logs.
  consumer: Consumer,

  // Used to construct the current trigger-upload batch before it is durably staged.
  batch_builder: BatchBuilder,

  // Service used to send logs to upload to the uploader.
  log_upload_service: service::Upload,

  // The buffer that is being uploaded.
  buffer_id: String,

  lookback_window: Option<time::OffsetDateTime>,

  old_logs_dropped: Counter,

  // State upload handle for uploading state snapshots before logs.
  state_upload_handle: Option<Arc<StateUploadHandle>>,

  // The trigger upload persists in two layers while it is in flight: the registry tracks lifecycle
  // and per-buffer progress, while the artifact store holds concrete log batches that are safe to
  // replay after restart. The remaining fields track the live, not-yet-staged batch being built
  // from the buffer in this process.
  pending_trigger_uploads: PendingTriggerUploadsStore,
  trigger_upload_identity: TriggerUploadIdentity,
  has_marked_uploading_from_buffer: bool,
  pending_batch_reads: usize,
  artifact_store: TriggerUploadArtifactStore,
  buffer: Arc<Buffer>,
}

impl CompleteBufferUpload {
  fn new(
    consumer: Consumer,
    runtime_flags: Flags,
    log_upload_service: service::Upload,
    buffer_id: String,
    old_logs_dropped: Counter,
    state_upload_handle: Option<Arc<StateUploadHandle>>,
    pending_trigger_uploads: PendingTriggerUploadsStore,
    trigger_upload_identity: TriggerUploadIdentity,
    artifact_store: TriggerUploadArtifactStore,
    buffer: Arc<Buffer>,
  ) -> Self {
    let lookback_window_limit = *runtime_flags.upload_lookback_window_feature_flag.read();

    let lookback_window = if lookback_window_limit.is_zero() {
      None
    } else {
      Some(OffsetDateTime::now_utc() - lookback_window_limit)
    };

    Self {
      consumer,
      batch_builder: BatchBuilder::new(runtime_flags),
      log_upload_service,
      buffer_id,
      lookback_window,
      old_logs_dropped,
      state_upload_handle,
      pending_trigger_uploads,
      trigger_upload_identity,
      has_marked_uploading_from_buffer: false,
      pending_batch_reads: 0,
      artifact_store,
      buffer,
    }
  }

  async fn run(mut self) -> anyhow::Result<i32> {
    log::debug!("starting trigger consumption task");

    // TODO(snowp): Consider tokio::task::yield_now to avoid starving other tasks if the batch size
    // is large.

    // A recovered upload may already have durable batches waiting in the artifact store. We first
    // reconcile any queued artifact with the current buffer head, then flush persisted work before
    // reading new logs. That ordering avoids re-uploading batches that were already staged before
    // the previous process died.
    self.discard_duplicate_queued_batch().await?;
    self.flush_persisted_batches().await?;

    let mut total_logs = 0;
    loop {
      let entry = self.consumer.start_read(false);

      match entry {
        // Accumulate a full trigger batch in memory, then persist it once before the buffer is
        // advanced in bulk.
        Ok(log) => {
          if let Some(lookback_window) = self.lookback_window {
            // We are defensive here as we can't be sure the log is well formed.
            if let Some(ts) = EncodableLog::extract_timestamp(&log)
              && ts < lookback_window
            {
              log::debug!("skipping log, outside lookback window");
              self.old_logs_dropped.inc();
              self.consumer.finish_read()?;
              continue;
            }
          }

          if !self.has_marked_uploading_from_buffer {
            self
              .pending_trigger_uploads
              .mark_uploading(
                self.trigger_upload_identity.durable_upload_id(),
                &self.buffer_id,
              )
              .await;
            self.has_marked_uploading_from_buffer = true;
          }

          self.batch_builder.add_log(log);
          self.pending_batch_reads += 1;
          total_logs += 1;

          if self.batch_builder.limit_reached() {
            self.persist_and_flush_current_batch().await?;
          }
        },
        // No more logs available, flush any durable artifact backlog.
        Err(Error::AbslStatus(AbslCode::Unavailable, _)) => {
          self.persist_and_flush_current_batch().await?;
          self.flush_persisted_batches().await?;

          log::debug!("trigger upload complete, sent {total_logs} logs");
          return Ok(total_logs);
        },
        // Unexpected error, bubble up.
        Err(e) => return Err(e.into()),
      }
    }
  }

  #[allow(clippy::needless_pass_by_ref_mut)]
  async fn discard_duplicate_queued_batch(&mut self) -> anyhow::Result<()> {
    let Some(queued_batch) = self.artifact_store.queued_batch().await? else {
      return Ok(());
    };
    let Some(buffer_log) = self.buffer.peek_oldest_record()? else {
      return Ok(());
    };

    // A queued batch has been durably staged but has not yet been promoted to inflight. If the
    // current buffer head still matches the first log in that artifact, we know this process has
    // not advanced past the staged batch, so it is safe to drop the queued artifact and rebuild it
    // from the live buffer instead of replaying a stale duplicate copy.
    if queued_batch
      .logs
      .first()
      .is_some_and(|log| *log == buffer_log)
    {
      log::debug!(
        "dropping recovered queued trigger batch that still exists in buffer {}",
        self.buffer_id
      );
      self.artifact_store.remove_queued_batch().await?;
    }

    Ok(())
  }

  async fn persist_and_flush_current_batch(&mut self) -> anyhow::Result<()> {
    if self.batch_builder.logs.is_empty() {
      return Ok(());
    }

    // The critical ordering here is: stage the concrete logs durably, then advance the live buffer
    // cursor, then promote the staged batch to the single inflight artifact. That guarantees a
    // crash cannot lose logs silently: after restart we will either find them still readable in the
    // buffer or recover them from the artifact store.
    self
      .artifact_store
      .stage_batch(self.batch_builder.take())
      .await?;
    self.consumer.finish_reads(self.pending_batch_reads)?;
    self.pending_batch_reads = 0;

    let batch = self
      .artifact_store
      .promote_queued_batch_to_inflight()
      .await?
      .ok_or_else(|| anyhow::anyhow!("staged trigger batch missing from artifact store"))?;

    log::debug!(
      "staged trigger batch of {} logs before advancing buffer {}",
      batch.logs.len(),
      self.buffer_id
    );

    self.flush_batch(batch, false).await
  }

  async fn flush_persisted_batches(&mut self) -> anyhow::Result<()> {
    // Recovery may leave both an inflight artifact and additional queued artifacts on disk. We
    // always flush the inflight batch first, then keep promoting queued batches until the durable
    // backlog is empty before returning to live buffer reads.
    if let Some(batch) = self.artifact_store.inflight_batch().await? {
      self.flush_batch(batch, true).await?;
    }

    while let Some(batch) = self
      .artifact_store
      .promote_queued_batch_to_inflight()
      .await?
    {
      self.flush_batch(batch, true).await?;
    }

    Ok(())
  }

  async fn flush_batch(
    &mut self,
    batch: PersistedTriggerUploadArtifactBatch,
    discard_recovered_prefix: bool,
  ) -> anyhow::Result<()> {
    let logs_len = u64::try_from(batch.logs.len()).unwrap_or(u64::MAX);
    let uploaded_logs = batch.logs.clone();
    let timestamp_range = timestamp_range_for_logs(&batch.logs);
    log::debug!("flushing {logs_len} logs from trigger artifact");

    // Once a batch is uploaded from durable artifact storage, the registry should reflect that the
    // buffer is no longer being read directly from the live ring buffer. This distinction matters
    // for restart because subsequent recovery should resume from the artifact backlog first.
    self
      .pending_trigger_uploads
      .mark_uploading_from_artifact(
        self.trigger_upload_identity.durable_upload_id(),
        &self.buffer_id,
      )
      .await;

    // Upload state snapshot if needed before uploading logs
    if let (Some(handle), Some((oldest, newest))) = (&self.state_upload_handle, timestamp_range) {
      handle.notify_upload_needed(oldest, newest);
    }

    // Attempt to perform an upload of these buffers, with retries ++. See logger/service.rs for
    // details about retry policies etc.
    let result = self
      .log_upload_service
      .ready()
      .await
      .unwrap_infallible()
      .call(
        UploadRequest::new_with_uuid(
          batch.upload_uuid,
          LogBatch {
            logs: batch.logs,
            buffer_id: self.buffer_id.clone(),
          },
          false,
        )
        .with_request_trigger_uuid(self.trigger_upload_identity.request_trigger_uuid.clone()),
      )
      .await
      .unwrap_infallible();

    match result {
      UploadResult::Success => {
        self.artifact_store.clear_inflight_batch().await?;

        // Record progress before attempting any buffer-prefix cleanup. The uploaded batch is now
        // durable history regardless of whether the current buffer still matches its contents.
        self
          .pending_trigger_uploads
          .record_uploaded_chunk(
            self.trigger_upload_identity.durable_upload_id(),
            &self.buffer_id,
            logs_len,
          )
          .await;

        if discard_recovered_prefix {
          self.discard_uploaded_buffer_prefix(&uploaded_logs)?;
        }

        log::debug!("completed trigger batch upload with result: {result:?}");
        Ok(())
      },
      UploadResult::Failure => {
        anyhow::bail!("trigger batch upload failed")
      },
      UploadResult::Canceled => Err(TriggerBatchUploadCanceled.into()),
    }
  }

  fn discard_uploaded_buffer_prefix(&mut self, uploaded_logs: &[Vec<u8>]) -> anyhow::Result<()> {
    if uploaded_logs.is_empty() {
      return Ok(());
    }

    // This path only runs for recovered artifact uploads. The previous process may have staged and
    // even partially uploaded logs before dying, so the live buffer can contain a prefix that is
    // now redundant. We only retire the longest matching prefix and stop on the first divergence so
    // newly written logs behind the recovered artifact remain intact.
    let mut matched_logs = 0;
    for uploaded_log in uploaded_logs {
      match self.consumer.start_read(false) {
        Ok(buffer_log) if buffer_log == *uploaded_log => {
          matched_logs += 1;
        },
        Ok(_) => {
          log::debug!(
            "stopped discarding recovered trigger artifact prefix for {} after {matched_logs} \
             matched logs because the buffer diverged",
            self.buffer_id,
          );
          break;
        },
        Err(Error::AbslStatus(AbslCode::Unavailable, _)) => {
          break;
        },
        Err(e) => {
          return Err(e.into());
        },
      }
    }

    if matched_logs != 0 {
      self.consumer.finish_reads(matched_logs)?;
      log::debug!(
        "discarded {matched_logs} buffered logs already uploaded from recovered trigger artifact"
      );
    }

    Ok(())
  }
}

fn timestamp_range_for_logs(logs: &[Vec<u8>]) -> Option<(u64, u64)> {
  let mut oldest_micros = None;
  let mut newest_micros = None;

  for log in logs {
    if let Some(ts) = EncodableLog::extract_timestamp(log) {
      let ts_micros = ts.unix_timestamp_micros().cast_unsigned();
      oldest_micros = Some(oldest_micros.map_or(ts_micros, |oldest: u64| oldest.min(ts_micros)));
      newest_micros = Some(newest_micros.map_or(ts_micros, |newest: u64| newest.max(ts_micros)));
    }
  }

  oldest_micros.zip(newest_micros)
}
