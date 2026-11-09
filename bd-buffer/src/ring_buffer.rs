// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
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
  RingBufferCursorConsumer,
  RingBufferProducer,
  RingBufferStats,
  VolatileRingBuffer,
};
use crate::ffi::AbslCode;
use crate::{Error, Result};
use anyhow::anyhow;
use bd_client_stats_store::{Counter, Scope};
use bd_error_reporter::reporter::handle_unexpected;
use bd_log_primitives::EncodableLog;
use bd_proto::protos::config::v1::config::{BufferConfigList, buffer_config};
use bd_stats_common::labels;
use bd_time::OffsetDateTimeExt as _;
use bd_versioned_kv::{RetentionHandle, RetentionRegistry};
use futures::future::join_all;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

  // The full configured set of trigger buffers after a config update has been applied.
  TriggerBufferConfigUpdated(Vec<String>),

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
      Self::TriggerBufferConfigUpdated(arg0) => f
        .debug_tuple("TriggerBufferConfigUpdated")
        .field(arg0)
        .finish(),
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
    if let Some(on_processed_tx) = self.on_processed_tx.take() {
      let _ignored = on_processed_tx.send(());
    }
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
  pub const fn new(buffers: Vec<String>, completed_tx: Option<bd_completion::Sender<()>>) -> Self {
    Self {
      buffers,
      completed_tx,
    }
  }

  // Flush all buffers.
  #[must_use]
  pub const fn new_all_buffers(completed_tx: Option<bd_completion::Sender<()>>) -> Self {
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
  // Both the file-based ring buffers and the RAM-only stream buffer are kept within a mutex in
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
    bd_runtime::runtime::IntWatch<bd_runtime::runtime::buffers::StreamBufferSizeBytes>,

  retention_registry: Arc<RetentionRegistry>,
}

impl Manager {
  pub fn new(
    buffer_directory: PathBuf,
    stats: &Scope,
    runtime: &bd_runtime::runtime::ConfigLoader,
    retention_registry: Arc<RetentionRegistry>,
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
        stream_buffer_size_flag: runtime.register_int_watch(),
        retention_registry,
      }),
      buffer_event_rx,
    )
  }

  pub async fn process_flushes(&self, mut flush_buffer_rx: Receiver<BuffersWithAck>) {
    loop {
      let Some(buffers_with_ack) = flush_buffer_rx.recv().await else {
        log::debug!("shutting down buffer flush task");
        return;
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
          return;
        }
      }

      if flush_all_buffers && let Some(stream_buffer) = &self.buffers.lock().1 {
        stream_buffer.flush();
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
          "creating buffer with volatile_size={volatile_buffer_size}, \
           non_volatile_size={non_volatile_buffer_size}"
        );

        let retention_handle = self.retention_registry.create_handle().await;
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
          retention_handle.clone(),
        )?;

        updated_buffers.insert(buffer.id.clone(), (buffer_type, ring_buffer.clone()));
        new_buffers.push((buffer.id.clone(), (buffer_type, ring_buffer)));
      }
    }

    // First we resolve all the aggregate buffers based on the explicit config.
    let configured_trigger_buffer_ids = updated_buffers
      .iter()
      .filter_map(|(id, (buffer_type, _))| {
        (*buffer_type == buffer_config::Type::TRIGGER).then_some(id.clone())
      })
      .collect();

    let mut update_acks = self
      .resolve_buffer_updates(current_buffers, new_buffers, configured_trigger_buffer_ids)
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
    configured_trigger_buffer_ids: Vec<String>,
  ) -> Vec<tokio::sync::oneshot::Receiver<()>> {
    let mut update_acks = Vec::new();

    // Mark the underlying buffer for deletion once all references to the buffer has been removed.
    // Attempting to delete it immediately runs into lifetime issues as the thread local loggers may
    // still hold a reference to the buffer.
    //
    // Issue a BufferRemoved event, which informs the uploader about this buffer removal and allows
    // it to clean up upload state related to the buffer.
    for (id, (_, unreferenced_buffer)) in current_buffers {
      log::debug!("notifying about buffer removal for {id}");

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
      log::debug!("notifying about buffer creation for {id}");

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

    let (event, rx) = BufferEventWithResponse::new(BufferEvent::TriggerBufferConfigUpdated(
      configured_trigger_buffer_ids,
    ));
    handle_unexpected(
      self
        .buffer_event_tx
        .send(event)
        .await
        .map_err(|_| anyhow!("buffer events")),
      "trigger buffer config update",
    );
    update_acks.push(rx);

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
          *self.stream_buffer_size_flag.read(),
          Arc::new(RingBufferStats::default()),
          |_| {},
          |_| {},
          None::<fn(Option<&[u8]>)>,
        ));

        Some(BufferEventWithResponse::new(
          BufferEvent::StreamBufferAdded(updated_stream_buffer.clone()?),
        ))
      },
      (false, Some(_)) => Some(BufferEventWithResponse::new(
        BufferEvent::StreamBufferRemoved(stream_buffer.take()?),
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
  consumer: Box<dyn RingBufferCursorConsumer>,
  retention_handle: RetentionHandle,

  buffer: Arc<RingBuffer>,
}

impl CursorConsumer {
  pub async fn read(&mut self) -> anyhow::Result<&[u8]> {
    self
      .consumer
      .read()
      .await
      .map_err(|e| anyhow!("cursor consumer buffer read error occurred: {e}"))
  }

  pub fn advance_read_cursor(&mut self) -> anyhow::Result<()> {
    self
      .consumer
      .advance_read_pointers(1)
      .map_err(|e| anyhow!("cursor consumer buffer read error occurred: {e}"))?;
    if self.buffer.trigger_retention.is_some() {
      self.buffer.refresh_disk_retention();
    }
    Ok(())
  }

  #[must_use]
  pub fn oldest_timestamp_micros(&self) -> Option<u64> {
    match self.buffer.buffer.peek_oldest_record(|record_data| {
      EncodableLog::extract_timestamp(record_data)
        .and_then(|ts| u64::try_from(ts.unix_timestamp_micros()).ok())
    }) {
      Ok(Some(Some(micros))) => Some(micros),
      Ok(Some(None) | None) => None,
      Err(error) => {
        log::debug!("failed to peek oldest record for retention update: {error}");
        None
      },
    }
  }

  #[must_use]
  pub fn retention_handle(&self) -> RetentionHandle {
    self.retention_handle.clone()
  }
}

//
// Consumer
//

// Adapter for the new consumer. To be removed.
#[allow(clippy::struct_field_names)]
pub struct Consumer {
  cursor_consumer: Box<dyn RingBufferCursorConsumer>,
  _lock_handle: Box<dyn LockHandle>,

  // The consumer holds the aggregate producer lock, so the non-volatile buffer is stable while
  // callers take the read-only snapshot used by device-command upload progress.
  buffer: Arc<RingBuffer>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemainingPayloadStats {
  pub payload_bytes: u64,
  pub record_count: u64,
}

impl Consumer {
  pub fn remaining_payload_stats(&self) -> Result<RemainingPayloadStats> {
    let (payload_bytes, record_count) = self
      .buffer
      .buffer
      .non_volatile_buffer()
      .locked_cursor_remaining_stats()?;
    Ok(RemainingPayloadStats {
      payload_bytes,
      record_count,
    })
  }

  pub fn start_read(&mut self, block: bool) -> Result<Vec<u8>> {
    self.cursor_consumer.start_read(block).map(<[u8]>::to_vec)
  }

  pub fn finish_read(&mut self) -> Result<()> {
    self.cursor_consumer.advance_read_pointers(1)?;
    self.buffer.refresh_disk_retention();
    Ok(())
  }

  pub fn finish_reads(&mut self, count: usize) -> Result<()> {
    self.cursor_consumer.advance_read_pointers(count)?;
    if count > 0 {
      self.buffer.refresh_disk_retention();
    }
    Ok(())
  }

  pub fn try_read(&mut self) -> Result<Vec<u8>> {
    let result = self.start_read(false)?;
    self.finish_read()?;
    Ok(result)
  }

  pub async fn read(&mut self) -> Result<Vec<u8>> {
    self.cursor_consumer.read().await.map(<[u8]>::to_vec)
  }
}

//
// TriggerRetention
//

struct TriggerRetentionState {
  oldest_disk: u64,
  oldest_ram: u64,
}

// Trigger buffers retain both durable logs and accepted logs awaiting the flush thread. A single
// timestamp per tier tracks the front of each ring; the registry sees the earlier timestamp so
// neither tier can prematurely release attachments or state. Callbacks only take this mutex while
// holding their own ring lock, never the other ring's lock.
// This assumes timestamps follow ring order. Logs with older event times behind newer records can
// still lose attachment or state retention prematurely when the front advances; fixing that known
// limitation requires moving to a non-timestamp based solution which will be done in a follow up.
struct TriggerRetention {
  handle: RetentionHandle,
  state: Mutex<TriggerRetentionState>,
  disk_head_dirty: AtomicBool,
}

impl TriggerRetention {
  fn new(handle: RetentionHandle) -> Self {
    Self {
      handle,
      state: Mutex::new(TriggerRetentionState {
        oldest_disk: RetentionHandle::RETENTION_PENDING,
        oldest_ram: RetentionHandle::RETENTION_NONE,
      }),
      disk_head_dirty: AtomicBool::new(true),
    }
  }

  fn timestamp(record: &[u8]) -> u64 {
    EncodableLog::extract_timestamp(record)
      .and_then(|ts| u64::try_from(ts.unix_timestamp_micros()).ok())
      .unwrap_or(0)
  }

  fn publish(&self, state: &TriggerRetentionState) {
    // TODO: If coordinator contention remains costly, coalesce forward-only releases with an idle
    // timer. A new or earlier retention requirement must still be published synchronously.
    let retention = if state.oldest_disk == RetentionHandle::RETENTION_PENDING {
      RetentionHandle::RETENTION_PENDING
    } else {
      state.oldest_ram.min(state.oldest_disk)
    };
    self.handle.update_retention_micros(retention);
  }

  // Commit protects a log immediately, even before it becomes visible to the flush thread. A
  // later overwrite or finish_read notification replaces this bound with the actual RAM front.
  fn committed(&self, record: &[u8]) {
    let mut state = self.state.lock();
    state.oldest_ram = state.oldest_ram.min(Self::timestamp(record));
    self.publish(&state);
  }

  // Invoked under the volatile lock after overwrite or finish_read. finish_read runs for both
  // successful flushes and records dropped during a non-volatile reset, so either way the RAM
  // bound cannot keep a retired record alive indefinitely.
  fn oldest_ram_changed(&self, oldest: Option<&[u8]>) {
    let mut state = self.state.lock();
    state.oldest_ram = oldest.map_or(RetentionHandle::RETENTION_NONE, Self::timestamp);
    self.publish(&state);
  }

  fn evicted_from_disk(&self, record: &[u8]) {
    let mut state = self.state.lock();
    self.disk_head_dirty.store(true, Ordering::Relaxed);
    if state.oldest_disk != RetentionHandle::RETENTION_PENDING {
      state.oldest_disk = state.oldest_disk.max(Self::timestamp(record));
      self.publish(&state);
    }
  }

  // Disk commit precedes volatile finish_read, so the new durable bound is published before RAM
  // releases its copy. No cross-buffer lock is acquired from either callback.
  // TODO: Report a changed disk head during the existing disk lock to avoid re-locking it here.
  fn flushed(&self, oldest_disk_record: Option<&[u8]>) {
    let mut state = self.state.lock();
    self
      .disk_head_dirty
      .store(oldest_disk_record.is_none(), Ordering::Relaxed);
    state.oldest_disk = oldest_disk_record.map_or(RetentionHandle::RETENTION_NONE, Self::timestamp);
    self.publish(&state);
  }

  fn should_inspect_disk_head(&self) -> bool {
    self.disk_head_dirty.swap(false, Ordering::Relaxed)
  }

  fn set_disk(&self, oldest: Option<u64>) {
    let mut state = self.state.lock();
    self
      .disk_head_dirty
      .store(oldest.is_none(), Ordering::Relaxed);
    state.oldest_disk = oldest.unwrap_or(RetentionHandle::RETENTION_NONE);
    self.publish(&state);
  }
}

//
// RingBuffer
//

// A wrapper around a ring buffer. A shared type is used here to support being able to write
// into any kind of buffer, regardless of upload strategy.
pub struct RingBuffer {
  // The file where the disk component of this ring buffer is stored.
  filename: PathBuf,

  // If true, the underlying file should be deleted on Drop.
  delete_on_drop: AtomicBool,

  // The underlying buffer.
  buffer: Arc<AggregateRingBuffer>,

  retention_handle: RetentionHandle,
  trigger_retention: Option<Arc<TriggerRetention>>,
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
    trigger_retention: Option<Arc<TriggerRetention>>,
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
      {
        let retention = trigger_retention.clone();
        move |record_data| {
          if let Some(retention) = retention.as_ref() {
            retention.committed(record_data);
          }
        }
      },
      |_| {},
      {
        let retention = trigger_retention.clone();
        move |record_data| {
          if let Some(retention) = retention.as_ref() {
            retention.evicted_from_disk(record_data);
          }
        }
      },
      trigger_retention
        .clone()
        .map(|retention| move |oldest: Option<&[u8]>| retention.flushed(oldest)),
      trigger_retention
        .clone()
        .map(|retention| move || retention.should_inspect_disk_head()),
      trigger_retention
        .clone()
        .map(|retention| move |oldest: Option<&[u8]>| retention.oldest_ram_changed(oldest)),
      trigger_retention.map(|retention| move || retention.set_disk(None)),
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
    retention_handle: RetentionHandle,
  ) -> Result<(Arc<Self>, bool)> {
    let filename = non_volatile_filename
      .to_str()
      .ok_or(Error::InvalidFileName)?
      .to_string();

    let trigger_retention =
      allow_overwrite.then(|| Arc::new(TriggerRetention::new(retention_handle.clone())));
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
      trigger_retention.clone(),
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
        trigger_retention.clone(),
      );
    }

    buffer
      .map_err(|e| Error::BufferCreation(non_volatile_filename.clone(), Box::new(e)))
      .map(|buffer| {
        let result = Arc::new(Self {
          filename: non_volatile_filename,
          delete_on_drop: AtomicBool::new(false),
          buffer,
          retention_handle,
          trigger_retention,
        });
        result.refresh_disk_retention();
        (result, deleted)
      })
  }

  fn refresh_disk_retention(&self) {
    let result = self
      .buffer
      .non_volatile_buffer()
      .inspect_oldest_record(|oldest| {
        let oldest = oldest.map(TriggerRetention::timestamp);
        if let Some(retention) = &self.trigger_retention {
          retention.set_disk(oldest);
        } else {
          self
            .retention_handle
            .update_retention_micros(oldest.unwrap_or(RetentionHandle::RETENTION_NONE));
        }
      });
    if let Err(error) = result {
      log::debug!("failed to peek oldest record for retention update: {error}");
    }
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
      consumer: self.buffer.clone().register_cursor_consumer()?,
      retention_handle: self.retention_handle.clone(),
      buffer: self.clone(),
    })
  }

  // Creates a new consumer which can be used to consume all the logs in the buffer.
  pub fn new_consumer(self: &Arc<Self>) -> anyhow::Result<Consumer> {
    // Trigger uploads rely on this lock to stop new writes before they begin draining the
    // non-volatile buffer. The aggregate lock targets the volatile producer side, then flushes RAM
    // to disk before the non-volatile consumer is registered, so the resulting consumer sees a
    // stable snapshot for one-off upload.
    let lock_handle = self.buffer.clone().lock();
    lock_handle.await_reservations_drained();
    self.buffer.flush();

    Ok(Consumer {
      cursor_consumer: self
        .buffer
        .non_volatile_buffer()
        .clone()
        .register_locked_cursor_consumer()?,
      _lock_handle: lock_handle,
      buffer: self.clone(),
    })
  }

  pub fn peek_oldest_record(&self) -> anyhow::Result<Option<Vec<u8>>> {
    self
      .buffer
      .peek_oldest_record(<[u8]>::to_vec)
      .map_err(|e| anyhow!("failed to peek oldest record: {e}"))
  }

  // Flush the underlying buffer.
  pub fn flush(&self) {
    self.buffer.flush();
  }

  #[cfg(test)]
  pub fn thread_synchronizer(
    &self,
  ) -> Arc<crate::buffer::test::thread_synchronizer::ThreadSynchronizer> {
    self.buffer.thread_synchronizer()
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
