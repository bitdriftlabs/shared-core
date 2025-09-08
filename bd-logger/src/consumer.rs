// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./consumer_test.rs"]
mod consumer_test;

use crate::service::{self, UploadRequest};
use bd_api::upload::LogBatch;
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::{AbslCode, Buffer, BufferEvent, BufferEventWithResponse, Consumer, Error};
use bd_client_common::error::InvariantError;
use bd_client_common::fb::root_as_log;
use bd_client_common::maybe_await;
use bd_client_stats_store::{Counter, Scope};
use bd_error_reporter::reporter::handle_unexpected_error_with_details;
use bd_runtime::runtime::{ConfigLoader, IntWatch, Watch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use futures_util::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{Sleep, sleep};
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
  batch_deadline_watch: IntWatch<bd_runtime::runtime::log_upload::BatchDeadlineFlag>,

  // The maximum number of logs to upload in a single streaming batch.
  streaming_batch_size: IntWatch<bd_runtime::runtime::log_upload::StreamingBatchSizeFlag>,

  // The lookback window for the flush buffer uploads.
  upload_lookback_window_feature_flag:
    Watch<time::Duration, bd_runtime::runtime::log_upload::FlushBufferLookbackWindow>,
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
}

impl BufferUploadManager {
  pub(crate) fn new(
    data_upload_tx: Sender<DataUpload>,
    runtime_loader: &Arc<ConfigLoader>,
    shutdown: ComponentShutdown,
    buffer_event_rx: Receiver<BufferEventWithResponse>,
    trigger_upload_rx: Receiver<TriggerUpload>,
    stats: &Scope,
    logging: Arc<dyn bd_internal_logging::Logger>,
  ) -> Self {
    Self {
      log_upload_service: service::new(data_upload_tx, shutdown.clone(), runtime_loader, stats),
      feature_flags: Flags {
        max_batch_size_logs: runtime_loader.register_int_watch(),
        max_match_size_bytes: runtime_loader.register_int_watch(),
        batch_deadline_watch: runtime_loader.register_int_watch(),
        upload_lookback_window_feature_flag: runtime_loader.register_duration_watch(),
        streaming_batch_size: runtime_loader.register_int_watch(),
      },
      shutdown,
      buffer_event_rx,
      trigger_upload_rx,
      trigger_buffers: HashMap::new(),
      shutdowns: HashMap::new(),
      active_trigger_uploads: HashSet::new(),
      logging,
      stream_buffer_shutdown_trigger: None,
      old_logs_dropped: stats.counter("old_logs_dropped"),
    }
  }

  #[tracing::instrument(level = "debug", skip(self), name = "BufferUploadManager")]
  pub async fn run(mut self) -> anyhow::Result<()> {
    // For each spawned task, track the cancellation token and join handle,
    // allowing us to cancel the task when a buffer has been removed.

    // Used to notify the upload manager about trigger uploads completing, allowing it to clear up
    // state allowing for new uploads to be scheduled for a buffer.
    let (trigger_upload_complete_tx, mut trigger_upload_complete_rx) = channel(1);

    loop {
      tokio::select! {
        Some(event) = self.buffer_event_rx.recv() => self.handle_buffer_event(event).await?,
        Some(trigger) = self.trigger_upload_rx.recv() =>
          self.handle_trigger_uploads(trigger, &trigger_upload_complete_tx)?,
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
  fn handle_trigger_uploads(
    &mut self,
    trigger_upload: TriggerUpload,
    trigger_upload_complete_tx: &Sender<String>,
  ) -> anyhow::Result<()> {
    log::debug!("received trigger upload request");


    let mut buffer_upload_completions = vec![];

    for buffer_id in trigger_upload.buffer_ids {
      // If there is already an active upload for this buffer, do nothing. This may happen if an
      // aggressive workflow is used which can match a second (or more) log within the time it takes
      // to upload the log in response to the first trigger. Because we lock the buffer, this log
      // likely didn't even make it into the buffer so we just drop it here.
      if self.active_trigger_uploads.contains(&buffer_id) {
        log::debug!("ignoring upload for {buffer_id}, already in progress");
        continue;
      }

      // Initiate a new consumer if we are aware of a buffer by this name. There might not be a
      // buffer here if a buffer has just been added/removed, i.e. the thread local loggers
      // are not in sync with the state within the upload manager. This can also happen if the
      // match configuration targets a buffer that doesn't have a corresponding buffer config,
      // which is not validated.
      if let Some(buffer) = self.trigger_buffers.get(&buffer_id) {
        let trigger_consumer = self.new_trigger_consumer(&buffer_id, buffer)?;

        self.active_trigger_uploads.insert(buffer_id.clone());

        self
          .logging
          .log_internal(&format!("starting trigger upload for buffer {buffer_id:?}"));

        let upload_complete_tx = trigger_upload_complete_tx.clone();
        let buffer_id_clone = buffer_id.clone();
        let internal_logger = self.logging.clone();
        let shutdown_trigger = ComponentShutdownTrigger::default();
        let shutdown = shutdown_trigger.make_shutdown();
        let (single_upload_complete_tx, single_upload_complete_rx) =
          tokio::sync::oneshot::channel();
        buffer_upload_completions.push(single_upload_complete_rx);
        tokio::task::spawn(
          async move {
            // Handle the error here so that we can fire the complete message even on failure.
            match trigger_consumer.run().await {
              Ok(logs_count) => internal_logger.log_internal(&format!(
                "completed trigger upload for buffer {buffer_id_clone:?}, uploaded logs count: \
                 {logs_count:?}",
              )),
              Err(e) => {
                internal_logger.log_internal(&format!(
                  "failed trigger upload for buffer {buffer_id_clone:?}: {e:?}"
                ));
                handle_unexpected_error_with_details(e, "", || None);
              },
            }

            single_upload_complete_tx
              .send(())
              .map_err(|()| InvariantError::Invariant)?;

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
        self
          .shutdowns
          .insert(buffer_id.to_string(), shutdown_trigger);
      } else {
        log::debug!("ignoring upload for {buffer_id}, unknown buffer");
      }
    }

    // Since we have one completion handler for the entire set of trigger uploads, we need to
    // spawn a task to wait for all of the individual trigger uploads to complete before we can
    // signal that the trigger uploads are complete.
    tokio::spawn(async move {
      if let Err(e) = try_join_all(buffer_upload_completions).await {
        log::debug!("failed to wait for trigger uploads to complete: {e:?}");
      }

      if let Err(e) = trigger_upload.response_tx.send(()) {
        log::debug!("failed to send trigger upload response: {e:?}");
      }

      log::debug!("signaling all trigger uploads complete");
    });

    Ok(())
  }

  // Handles a single buffer event, where a buffer is either added or removed.
  async fn handle_buffer_event(&mut self, event: BufferEventWithResponse) -> anyhow::Result<()> {
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

        let batch_size = self.feature_flags.streaming_batch_size.clone();
        tokio::task::spawn(async move {
          StreamedBufferUpload {
            consumer,
            log_upload_service,
            batch_size,
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
    }

    Ok(())
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

    Ok((
      ContinuousBufferUploader::new(
        buffer.create_continous_consumer()?,
        self.log_upload_service.clone(),
        self.feature_flags.clone(),
        shutdown_trigger.make_shutdown(),
        buffer_name.to_string(),
      ),
      shutdown_trigger,
    ))
  }

  // Creates a new CompleteBufferUpload which will attempt to consume the specified buffer until the
  // end, shutting down once there are no more records to read.
  fn new_trigger_consumer(
    &self,
    buffer_name: &str,
    buffer: &Arc<Buffer>,
  ) -> anyhow::Result<CompleteBufferUpload> {
    Ok(CompleteBufferUpload::new(
      buffer.new_consumer()?,
      self.feature_flags.clone(),
      self.log_upload_service.clone(),
      buffer_name.to_string(),
      self.old_logs_dropped.clone(),
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
}

impl BatchBuilder {
  const fn new(flags: Flags) -> Self {
    Self {
      flags,
      total_bytes: 0,
      logs: Vec::new(),
    }
  }

  fn add_log(&mut self, data: Vec<u8>) {
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

  /// Consumes the current batch, resetting all accounting.
  fn take(&mut self) -> Vec<Vec<u8>> {
    self.total_bytes = 0;
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
}

impl ContinuousBufferUploader {
  pub fn new(
    consumer: bd_buffer::CursorConsumer,
    log_upload_service: service::Upload,
    feature_flags: Flags,
    shutdown: ComponentShutdown,
    buffer_id: String,
  ) -> Self {
    Self {
      consumer,
      log_upload_service,
      shutdown,
      flush_batch_sleep: None,
      batch_builder: BatchBuilder::new(feature_flags.clone()),
      feature_flags,
      buffer_id,
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
        self.flush_batch_sleep = Some(Box::pin(sleep(Duration::from_millis(
          (*self.feature_flags.batch_deadline_watch.read()).into(),
        ))));
      }
    }
  }

  // Consumes the current_batch and performs a log flush.
  async fn flush_current_batch(&mut self) -> anyhow::Result<()> {
    // Disarm the deadline which forces a partial flush to fire.
    self.flush_batch_sleep = None;

    let logs = self.batch_builder.take();
    let logs_len = logs.len();

    log::debug!("flushing {logs_len} logs");

    // Attempt to perform an upload of these buffers, with retries ++. See logger/service.rs for
    // details about retry policies etc.
    let upload_future = async {
      self
        .log_upload_service
        .ready()
        .await
        .unwrap_infallible()
        .call(UploadRequest::new(LogBatch {
          logs,
          buffer_id: self.buffer_id.clone(),
        }))
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

    Ok(())
  }
}

// StreamedBufferUpload is used to manage uploading streamed logs. Unlike the other upload
// strategies, streamed logs are immediately uploaded without any batching and continuous to upload
// logs from the buffer forever.
struct StreamedBufferUpload {
  // The ring buffer consumer to use to consume logs. We use the cursor consumer to get us async
  // reads, we could technically use the non-cursor version.
  consumer: Box<dyn bd_buffer::RingBufferConsumer>,

  // Service used to send logs to upload to the uploader.
  log_upload_service: service::Upload,

  batch_size: IntWatch<bd_runtime::runtime::log_upload::StreamingBatchSizeFlag>,

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

      let mut logs = vec![first_log.to_vec()];

      self.consumer.finish_read()?;

      loop {
        if logs.len() >= *self.batch_size.read() as usize {
          log::debug!("batch size reached, uploading batch");
          break;
        }

        match self.consumer.start_read(false) {
          Ok(log) => {
            logs.push(log.to_vec());
            self.consumer.finish_read()?;
          },
          Err(bd_buffer::Error::AbslStatus(AbslCode::Unavailable, _)) => {
            break;
          },
          Err(e) => {
            anyhow::bail!("failed to read from stream buffer: {e:?}")
          },
        }
      }

      let upload_future = async {
        self
          .log_upload_service
          .ready()
          .await
          .unwrap_infallible()
          .call(UploadRequest::new(LogBatch {
            logs,
            buffer_id: "streamed".to_string(),
          }))
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

  // Used to construct the log payload and enforce batch limits.
  batch_builder: BatchBuilder,

  // Service used to send logs to upload to the uploader.
  log_upload_service: service::Upload,

  // The buffer that is being uploaded.
  buffer_id: String,

  lookback_window: Option<time::OffsetDateTime>,

  old_logs_dropped: Counter,
}

impl CompleteBufferUpload {
  fn new(
    consumer: Consumer,
    runtime_flags: Flags,
    log_upload_service: service::Upload,
    buffer_id: String,
    old_logs_dropped: Counter,
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
    }
  }

  async fn run(mut self) -> anyhow::Result<i32> {
    log::debug!("starting trigger consumption task");

    // TODO(snowp): Consider tokio::task::yield_now to avoid starving other tasks if the batch size
    // is large.

    let mut total_logs = 0;
    loop {
      let entry = self.consumer.try_read();

      match entry {
        // Log available, add to batch and flush if batch size hit.
        Ok(log) => {
          if let Some(lookback_window) = self.lookback_window {
            // The buffer producer/consumer API doesn't limit the input to flatbuffer logs, so we
            // defensively check that we get back a valid log before trying to access the
            // timestamp.
            if let Ok(log) = root_as_log(&log) {
              // There should always be a timestamp on the log, but this relies on the log being
              // correctly constructed so we stay on the safe side and check for None.
              if let Some(ts) = log.timestamp() {
                let ts = OffsetDateTime::from_unix_timestamp(ts.seconds())?
                  + time::Duration::nanoseconds(i64::from(ts.nanos()));
                if ts < lookback_window {
                  log::debug!("skipping log, outside lookback window");
                  self.old_logs_dropped.inc();
                  continue;
                }
              }
            }
          }

          total_logs += 1;
          self.batch_builder.add_log(log);
        },
        // No more logs available, flush current batch if there are any logs.
        Err(Error::AbslStatus(AbslCode::Unavailable, _)) => {
          if !self.batch_builder.logs.is_empty() {
            self.flush_batch().await?;
          }

          log::debug!("trigger upload complete, sent {total_logs} logs");
          return Ok(total_logs);
        },
        // Unexpected error, bubble up.
        Err(e) => return Err(e.into()),
      }

      if self.batch_builder.limit_reached() {
        self.flush_batch().await?;
      }
    }
  }

  async fn flush_batch(&mut self) -> anyhow::Result<()> {
    let logs = self.batch_builder.take();

    log::debug!("flushing {} logs", logs.len());

    // Attempt to perform an upload of these buffers, with retries ++. See logger/service.rs for
    // details about retry policies etc.
    let result = self
      .log_upload_service
      .ready()
      .await
      .unwrap_infallible()
      .call(UploadRequest::new(LogBatch {
        logs,
        buffer_id: self.buffer_id.clone(),
      }))
      .await
      .unwrap_infallible();

    // Note that we don't keep a shutdown handle for trigger uploads since we don't ever try to
    // cancel them, so we don't anything extra here to avoid deadlocking like in the other two
    // consumer tasks.

    log::debug!("completed trigger batch upload with result: {result:?}");

    Ok(())
  }
}
