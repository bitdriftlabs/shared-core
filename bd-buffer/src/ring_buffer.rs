// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./ring_buffer_test.rs"]
mod ring_buffer_test;

use crate::buffer::{
  self,
  AggregateRingBuffer,
  AllowOverwrite,
  LockHandle,
  PerRecordCrc32Check,
  RingBuffer as RingBufferInterface,
  RingBufferConsumer,
  RingBufferCursorConsumer,
  RingBufferProducer,
  RingBufferStats,
  VolatileRingBuffer,
};
use crate::ffi::AbslCode;
use crate::{Error, Result};
use anyhow::anyhow;
use bd_client_common::error::handle_unexpected;
use bd_client_stats_store::{Counter, Scope};
use bd_proto::protos::config::v1::config::{buffer_config, BufferConfigList};
use bd_stats_common::labels;
use futures::future::join_all;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

// TODO(snowp): This file is growing large, consider splitting trigger and continuous into their own
// modules.

// Events emitted by the buffer manager informing about changes made to the active buffers.
pub enum BufferEvent {
  // A new continuous buffer has been created with the given name.
  ContinuousBufferCreated(String, Arc<RingBuffer>),

  // A new trigger buffer has been created with the given name.
  TriggerBufferCreated(String, Arc<RingBuffer>),

  // The buffer with the given name has been removed. Any buffer removed should first have been
  // added either via ContinousBufferCreated or TriggerBufferCreated.
  BufferRemoved(String),

  StreamBufferAdded(Arc<dyn buffer::RingBuffer>),

  StreamBufferRemoved(Arc<dyn buffer::RingBuffer>),
}

impl Debug for BufferEvent {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ContinuousBufferCreated(arg0, arg1) => f
        .debug_tuple("ContinuousBufferCreated")
        .field(arg0)
        .field(arg1)
        .finish(),
      Self::TriggerBufferCreated(arg0, arg1) => f
        .debug_tuple("TriggerBufferCreated")
        .field(arg0)
        .field(arg1)
        .finish(),
      Self::BufferRemoved(arg0) => f.debug_tuple("BufferRemoved").field(arg0).finish(),
      Self::StreamBufferAdded(_) => f.debug_tuple("StreamBufferAdded").finish(),
      Self::StreamBufferRemoved(_) => f.debug_tuple("StreamBufferRemoved").finish(),
    }
  }
}

/// A buffer event alongside a channel used to signal that the event has been processed. This
/// allows for the configuration update to ensure that by the time the configuration has been
/// applied, the rest of the system is aware of the new buffer configurations.
#[derive(Debug)]
pub struct BufferEventWithResponse {
  /// The buffer event to process.
  pub event: BufferEvent,

  on_processed_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for BufferEventWithResponse {
  fn drop(&mut self) {
    let _ignored = self.on_processed_tx.take().unwrap().send(());
  }
}

impl BufferEventWithResponse {
  #[must_use]
  pub fn new(event: BufferEvent) -> (Self, tokio::sync::oneshot::Receiver<()>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (
      Self {
        event,
        on_processed_tx: Some(tx),
      },
      rx,
    )
  }
}

#[derive(Debug)]
pub struct BuffersWithAck {
  buffers: Vec<String>,
  completed_tx: Option<bd_completion::Sender<()>>,
}

impl BuffersWithAck {
  // Flush specific buffers. Empty list of buffers means flush all buffers.
  #[must_use]
  pub fn new(buffers: Vec<String>, completed_tx: Option<bd_completion::Sender<()>>) -> Self {
    Self {
      buffers,
      completed_tx,
    }
  }

  // Flush all buffers.
  #[must_use]
  pub fn new_all_buffers(completed_tx: Option<bd_completion::Sender<()>>) -> Self {
    Self {
      buffers: Vec::new(),
      completed_tx,
    }
  }
}

// A map of all the disk-based buffers an an optional stream buffer for use for streaming when
// there are active stream listeners.
type AllBuffers = (
  HashMap<String, (buffer_config::Type, Arc<RingBuffer>)>,
  Option<Arc<buffer::VolatileRingBuffer>>,
);

// Responsible for managing multiple ring buffers and applying dynamic configuration updates.
pub struct Manager {
  // Both the file-based ring buffers and the RAM-only stream buffer are kept within an RwLock in
  // order to allow modifications to be done in the updater while allowing the flush channel
  // process flush requests. We keep both kinds of buffer within one lock in order to avoid
  // having to lock multiple times during an update.
  // TODO(snowp): It might be nicer to use a dyn trait to differentiate the two different
  // disk-based buffers, but it wasn't obvious how to do that and also share a lot of the
  // implementation between the two.
  buffers: parking_lot::Mutex<AllBuffers>,

  // The directory to store buffers in.
  buffer_directory: PathBuf,

  buffer_event_tx: Sender<BufferEventWithResponse>,

  scope: Scope,

  stream_buffer_size_flag:
    bd_runtime::runtime::Watch<u32, bd_runtime::runtime::buffers::StreamBufferSizeBytes>,
}

impl Manager {
  pub fn new(
    buffer_directory: PathBuf,
    stats: &Scope,
    runtime: &bd_runtime::runtime::ConfigLoader,
  ) -> (
    Arc<Self>,
    tokio::sync::mpsc::Receiver<BufferEventWithResponse>,
  ) {
    let scope = stats.scope("ring_buffer");
    let (buffer_event_tx, buffer_event_rx) = tokio::sync::mpsc::channel(1);

    (
      Arc::new(Self {
        buffers: parking_lot::Mutex::new((HashMap::new(), None)),
        buffer_directory,
        buffer_event_tx,
        scope,
        stream_buffer_size_flag: runtime.register_watch().unwrap(),
      }),
      buffer_event_rx,
    )
  }

  #[tracing::instrument(level = "debug", skip(self))]
  pub async fn process_flushes(
    &self,
    mut flush_buffer_rx: Receiver<BuffersWithAck>,
  ) -> anyhow::Result<()> {
    loop {
      let Some(buffers_with_ack) = flush_buffer_rx.recv().await else {
        log::debug!("shutting down buffer flush task");
        return Ok(());
      };

      let flush_all_buffers = buffers_with_ack.buffers.is_empty();

      // We flush all buffers if no specific buffers are provided.
      let buffers = if flush_all_buffers {
        self.buffers.lock().0.keys().cloned().collect()
      } else {
        buffers_with_ack.buffers
      };

      for buffer_id in buffers {
        if let Some((_, buffer)) = self.buffers.lock().0.get(&buffer_id).cloned() {
          log::debug!("buffer_id={buffer_id} signaled to flush");
          buffer.flush();
        } else {
          log::debug!("buffer_id={buffer_id} signaled to flush not found");
          return Ok(());
        }
      }

      if flush_all_buffers {
        if let Some(stream_buffer) = &self.buffers.lock().1 {
          stream_buffer.flush();
        }
      }

      if let Some(completed_tx) = buffers_with_ack.completed_tx {
        completed_tx.send(());
      }
    }
  }

  // Applies a new buffer config, creating new buffer handles for new config and removing
  // buffers that are no longer referenced.
  pub async fn update_from_config(
    &self,
    config: &BufferConfigList,
    streaming: bool,
  ) -> anyhow::Result<Option<Arc<VolatileRingBuffer>>> {
    // Clone the set of ring buffers for us to reconcile changes. We clone here to avoid mutating
    // self during the reconciliation process as an error might leave this in a bad state.
    // We are not locking this structure for the whole duration of the method since no one else is
    // writing to it
    let (mut current_buffers, stream_buffer) = self.buffers.lock().clone();

    let mut updated_buffers = HashMap::new();
    let mut new_buffers = Vec::new();

    for buffer in &config.buffer_config {
      let buffer_type = bd_client_common::error::required_proto_enum(buffer.type_, "buffer type")?;
      if let Some((buffer_type, existing_buffer)) = current_buffers.remove(&buffer.id) {
        updated_buffers.insert(buffer.id.clone(), (buffer_type, existing_buffer.clone()));
      } else {
        // If the buffer is a trigger buffer, create the buffer in "allow overwrite" mode.
        // This is the only difference in the underlying buffer, and is what allows us to upload
        // trigger buffers occasionally with the latest set of logs, while trying to avoid dropping
        // logs for continuous buffers when log uploading is slow.
        let allow_overwrite = buffer.type_ == buffer_config::Type::TRIGGER.into();

        // TODO(snowp): Returning early here might leave previously created buffers on disk
        // without the manager keeping track of them. This would result in leaking the buffers
        // should there be no subsequent update which takes ownership over the file. We should
        // add some kind of cleanup logic (maybe on startup) which cleans up unreferenced buffers.

        // TODO(snowp): Make these fields required.
        let volatile_buffer_size = buffer
          .buffer_sizes
          .as_ref()
          .map_or(10_000, |sizes| sizes.volatile_buffer_size_bytes);
        let non_volatile_buffer_size = buffer
          .buffer_sizes
          .as_ref()
          .map_or(100_000, |sizes| sizes.non_volatile_buffer_size_bytes);
        log::debug!(
          "creating buffer with volatile_size={}, non_volatile_size={}",
          volatile_buffer_size,
          non_volatile_buffer_size
        );

        let (ring_buffer, _) = RingBuffer::new(
          &buffer.name,
          volatile_buffer_size,
          self.buffer_directory.join(buffer.id.as_str()),
          non_volatile_buffer_size,
          allow_overwrite,
          self
            .scope
            .counter_with_labels("record_write", labels! {"buffer_id" => &buffer.id}),
          self
            .scope
            .counter_with_labels("record_write_failure", labels! {"buffer_id" => &buffer.id}),
          self
            .scope
            .counter_with_labels("volatile_overwrite", labels! {"buffer_id" => &buffer.id}),
          self
            .scope
            .counter_with_labels("record_corrupted", labels! {"buffer_id" => &buffer.id}),
          self
            .scope
            .counter_with_labels("total_data_loss", labels! {"buffer_id" => &buffer.id}),
          None,
        )?;

        updated_buffers.insert(buffer.id.clone(), (buffer_type, ring_buffer.clone()));
        new_buffers.push((buffer.id.clone(), (buffer_type, ring_buffer)));
      }
    }

    // First we resolve all the aggregate buffers based on the explicit config.
    let mut update_acks = self
      .resolve_buffer_updates(current_buffers, new_buffers)
      .await;

    // Add in an update for the streaming buffer if we end up creating/destroying the stream buffer
    // as part of this update.
    let updated_stream_buffer = if let Some((rx, stream_buffer)) = self
      .resolve_stream_buffer_update(stream_buffer.clone(), streaming)
      .await
    {
      update_acks.push(rx);
      stream_buffer
    } else {
      stream_buffer
    };

    // Update the self state at the very end once we know that there won't be any more errors.
    *self.buffers.lock() = (updated_buffers, updated_stream_buffer.clone());

    // TODO(snowp): Consider using a single aggregated event object to avoid having a number of
    // channels to join on.
    // In order to ensure that all the configurations have been applied once this function
    // completes, we wait here for all of the events to have been acknowledged.
    join_all(update_acks).await;

    Ok(updated_stream_buffer)
  }

  // Compares the new and old buffer maps to determine which buffers were added and which were
  // removed. Fires up the update event and returns the list of update acks that should be awaited
  // to ensure that the update has been processed.
  async fn resolve_buffer_updates(
    &self,
    current_buffers: HashMap<String, (buffer_config::Type, Arc<RingBuffer>)>,
    new_buffers: Vec<(String, (buffer_config::Type, Arc<RingBuffer>))>,
  ) -> Vec<tokio::sync::oneshot::Receiver<()>> {
    let mut update_acks = Vec::new();

    // Mark the underlying buffer for deletion once all references to the buffer has been removed.
    // Attempting to delete it immediately runs into lifetime issues as the thread local loggers may
    // still hold a reference to the buffer.
    //
    // Issue a BufferRemoved event, which informs the uploader about this buffer removal and allows
    // it to clean up upload state related to the buffer.
    for (id, (_, unreferenced_buffer)) in current_buffers {
      log::debug!("notifying about buffer removal for {}", id);

      unreferenced_buffer
        .delete_on_drop
        .store(true, Ordering::Relaxed);

      let (event, rx) = BufferEventWithResponse::new(BufferEvent::BufferRemoved(id.clone()));
      // Returning an error triggers a config nack, so handle the error immediately.
      handle_unexpected(
        self
          .buffer_event_tx
          .send(event)
          .await
          .map_err(|_| anyhow!("buffer events")),
        "buffer removal",
      );
      update_acks.push(rx);
    }

    // The creation of buffers must be announced to allow the uploader to pick them
    // up and process them. Do so for all new buffers.
    for (id, (buffer_type, buffer)) in new_buffers {
      log::debug!("notifying about buffer creation for {}", id);

      // We wrap the buffer we send to the uploader with a use case specific type, allowing the
      // uploader to create the right kind of consumer.
      let event = match buffer_type {
        buffer_config::Type::CONTINUOUS => {
          BufferEvent::ContinuousBufferCreated(id.clone(), buffer.clone())
        },
        buffer_config::Type::TRIGGER => {
          BufferEvent::TriggerBufferCreated(id.clone(), buffer.clone())
        },
      };

      let (event, rx) = BufferEventWithResponse::new(event);
      // Returning an error triggers a config nack, so handle the error immediately.
      handle_unexpected(
        self
          .buffer_event_tx
          .send(event)
          .await
          .map_err(|_| anyhow!("buffer events")),
        "buffer addition",
      );

      update_acks.push(rx);
    }

    update_acks
  }

  async fn resolve_stream_buffer_update(
    &self,
    mut stream_buffer: Option<Arc<buffer::VolatileRingBuffer>>,
    streaming: bool,
  ) -> Option<(
    tokio::sync::oneshot::Receiver<()>,
    Option<Arc<buffer::VolatileRingBuffer>>,
  )> {
    let mut updated_stream_buffer = None;

    let (event, rx) = match (streaming, &stream_buffer) {
      (true, Some(_)) | (false, None) => {
        // We are already at the desired state, do nothing.
        None
      },
      (true, None) => {
        updated_stream_buffer = Some(buffer::VolatileRingBuffer::new(
          "bd tail".to_string(),
          self.stream_buffer_size_flag.read(),
          Arc::new(RingBufferStats::default()),
        ));

        Some(BufferEventWithResponse::new(
          BufferEvent::StreamBufferAdded(updated_stream_buffer.clone().unwrap()),
        ))
      },
      (false, Some(_)) => Some(BufferEventWithResponse::new(
        BufferEvent::StreamBufferRemoved(stream_buffer.take().unwrap()),
      )),
    }?;

    // Returning an error triggers a config nack, so handle the error immediately.
    handle_unexpected(
      self
        .buffer_event_tx
        .send(event)
        .await
        .map_err(|_| anyhow!("buffer events")),
      "buffer addition",
    );

    Some((rx, updated_stream_buffer))
  }

  // Returns the active set of ring buffers.
  #[must_use]
  pub fn buffers(
    &self,
  ) -> HashMap<
    String,
    (
      bd_proto::protos::config::v1::config::buffer_config::Type,
      Arc<RingBuffer>,
    ),
  > {
    // This should be small, so a simple clone seems good enough.
    self
      .buffers
      .lock()
      .0
      .iter()
      .map(|(key, (buffer_type, buffer))| (key.clone(), (*buffer_type, buffer.clone())))
      .collect()
  }

  #[must_use]
  #[allow(clippy::option_if_let_else)]
  pub fn stream_buffer(&self) -> Option<Arc<dyn buffer::RingBuffer>> {
    // A plain .clone() doesn't work well with the dyn indirection.
    match &self.buffers.lock().1 {
      Some(buffer) => Some(buffer.clone()),
      None => None,
    }
  }
}

//
// CursorConsumer
//

// Adapter for the new cursor impl. To be removed.
pub struct CursorConsumer {
  cursor_consumer: Box<dyn RingBufferCursorConsumer>,

  // TODO(mattklein123): This is not actually required in the new code but some tests seem to
  // depend on this. Clean this up during the old code purge.
  _buffer: Arc<RingBuffer>,
}

impl CursorConsumer {
  pub async fn read(&mut self) -> anyhow::Result<&[u8]> {
    self
      .cursor_consumer
      .read()
      .await
      .map_err(|e| anyhow!("cursor consumer buffer read error occurred: {e}"))
  }

  pub fn advance_read_cursor(&mut self) -> anyhow::Result<()> {
    self
      .cursor_consumer
      .advance_read_pointer()
      .map_err(|e| anyhow!("cursor consumer buffer read error occurred: {e}"))
  }
}

//
// Consumer
//

// Adapter for the new consumer. To be removed.
pub struct Consumer {
  consumer: Box<dyn RingBufferConsumer>,
  _lock_handle: Box<dyn LockHandle>,

  // TODO(mattklein123): This is not actually required in the new code but some tests seem to
  // depend on this. Clean this up during the old code purge.
  _buffer: Arc<RingBuffer>,
}

impl Consumer {
  pub fn try_read(&mut self) -> Result<Vec<u8>> {
    let reserved = self.consumer.start_read(false)?;
    let result = reserved.to_vec();
    self.consumer.finish_read()?;
    Ok(result)
  }

  pub async fn read(&mut self) -> Result<Vec<u8>> {
    self.consumer.read().await.map(<[u8]>::to_vec)
  }
}

// A wrapper around a ring buffer. A shared type is used here to support being able to write
// into any kind of buffer, regardless of upload strategy.
pub struct RingBuffer {
  // The file where the disk component of this ring buffer is stored.
  filename: PathBuf,

  // If true, the underlying file should be deleted on Drop.
  delete_on_drop: AtomicBool,

  // The underlying buffer.
  buffer: Arc<AggregateRingBuffer>,
}

impl Debug for RingBuffer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "ring buffer: {}", self.filename.display())
  }
}

impl RingBuffer {
  fn make_buffer(
    name: &str,
    volatile_size: u32,
    filename: &str,
    non_volatile_size: u32,
    allow_overwrite: bool,
    overwrite_counter: Counter,
    corrupted_record_counter: Counter,
    total_data_loss_counter: Counter,
    volatile_records_written: Counter,
    volatile_records_refused: Counter,
    non_volatile_records_written: Option<Counter>,
  ) -> Result<Arc<AggregateRingBuffer>> {
    // TODO(mattklein123): Right now we expose a very limited set of stats. Given it's much easier
    // now to inject stats we can consider exposing the rest. For now just duplicate what we
    // had previously.
    let volatile_stats = RingBufferStats {
      records_overwritten: Some(overwrite_counter),
      records_written: Some(volatile_records_written),
      records_refused: Some(volatile_records_refused),
      ..Default::default()
    };
    let non_volatile_stats = RingBufferStats {
      records_corrupted: Some(corrupted_record_counter),
      total_data_loss: Some(total_data_loss_counter),
      records_written: non_volatile_records_written,
      ..Default::default()
    };

    AggregateRingBuffer::new(
      name,
      volatile_size,
      filename,
      non_volatile_size,
      PerRecordCrc32Check::Yes,
      if allow_overwrite {
        AllowOverwrite::Yes
      } else {
        AllowOverwrite::Block
      },
      Arc::new(volatile_stats),
      Arc::new(non_volatile_stats),
    )
  }

  /// Creates a new ring buffer with the provided parameters. Returns a handle to the newly created
  /// buffer and whether we deleted the old file (used for testing purposes).
  pub fn new(
    name: &str,
    volatile_size: u32,
    non_volatile_filename: PathBuf,
    non_volatile_size: u32,
    allow_overwrite: bool,
    write_counter: Counter,
    write_failure_counter: Counter,
    overwrite_counter: Counter,
    corrupted_record_counter: Counter,
    total_data_loss_counter: Counter,
    non_volatile_records_written: Option<Counter>,
  ) -> Result<(Arc<Self>, bool)> {
    let filename = non_volatile_filename
      .to_str()
      .ok_or(Error::InvalidFileName)?
      .to_string();

    let mut buffer = Self::make_buffer(
      name,
      volatile_size,
      &filename,
      non_volatile_size,
      allow_overwrite,
      overwrite_counter.clone(),
      corrupted_record_counter.clone(),
      total_data_loss_counter.clone(),
      write_counter.clone(),
      write_failure_counter.clone(),
      non_volatile_records_written.clone(),
    );

    let mut deleted = false;
    // If we fail to create the buffer with a data loss error, the buffer is corrupt and must be
    // re-created.
    if let Err(Error::AbslStatus(AbslCode::DataLoss, _)) = buffer {
      log::debug!("buffer corrupted, removing and trying again");

      // Ignore errors here. If we fail to delete the file there's nothing much we can do. We'll
      // then fail to create the buffer when we retry below due to the same corruption.
      handle_unexpected(
        std::fs::remove_file(&non_volatile_filename)
          .map_err(|e| anyhow!("An io error ocurred: {e}")),
        "deleting corrupted buffer",
      );

      deleted = true;

      buffer = Self::make_buffer(
        name,
        volatile_size,
        &filename,
        non_volatile_size,
        allow_overwrite,
        overwrite_counter,
        corrupted_record_counter,
        total_data_loss_counter,
        write_counter,
        write_failure_counter,
        non_volatile_records_written,
      );
    }

    buffer
      .map_err(|e| Error::BufferCreation(non_volatile_filename.clone(), Box::new(e)))
      .map(|buffer| {
        (
          Arc::new(Self {
            filename: non_volatile_filename,
            delete_on_drop: AtomicBool::new(false),
            buffer,
          }),
          deleted,
        )
      })
  }

  // Returns a new thread local producer that can be used to write new entries into the ring buffer.
  pub fn new_thread_local_producer(
    self: &Arc<Self>,
  ) -> anyhow::Result<Box<dyn RingBufferProducer>> {
    self
      .buffer
      .clone()
      .register_producer()
      .map_err(|e| anyhow!("failed to register thread local producer: {e}"))
  }

  // Creates a new continuous consumer that might be used to read logs from the buffer.
  pub fn create_continous_consumer(self: &Arc<Self>) -> anyhow::Result<CursorConsumer> {
    Ok(CursorConsumer {
      cursor_consumer: self.buffer.clone().register_cursor_consumer()?,
      _buffer: self.clone(),
    })
  }

  // Creates a new consumer which can be used to consume all the logs in the buffer.
  pub fn new_consumer(self: &Arc<Self>) -> anyhow::Result<Consumer> {
    let lock_handle = self.buffer.clone().lock();
    lock_handle.await_reservations_drained();
    self.buffer.flush();

    Ok(Consumer {
      consumer: self.buffer.clone().register_consumer()?,
      _lock_handle: lock_handle,
      _buffer: self.clone(),
    })
  }

  // Flush the underlying buffer.
  pub fn flush(&self) {
    self.buffer.flush();
  }
}

impl Drop for RingBuffer {
  fn drop(&mut self) {
    if self.delete_on_drop.load(Ordering::Relaxed) {
      handle_unexpected::<(), anyhow::Error>(
        std::fs::remove_file(&self.filename).map_err(|e| anyhow!("An io error ocurred: {e}")),
        "deleting ring buffer",
      );
    }
  }
}
