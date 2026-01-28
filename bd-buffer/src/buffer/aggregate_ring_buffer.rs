// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./aggregate_ring_buffer_test.rs"]
mod aggregate_ring_buffer_test;

use super::common_ring_buffer::AllowOverwrite;
use super::non_volatile_ring_buffer::{
  BlockWhenReservingIntoConcurrentRead,
  FileHeader,
  PerRecordCrc32Check,
};
use super::{
  LockHandle,
  NonVolatileRingBuffer,
  RingBuffer,
  RingBufferConsumer,
  RingBufferCursorConsumer,
  RingBufferProducer,
  RingBufferStats,
  VolatileRingBuffer,
};
#[cfg(test)]
use crate::buffer::test::thread_synchronizer::ThreadSynchronizer;
use crate::{AbslCode, Error, Result};
use bd_log_primitives::LossyIntToU32;
use parking_lot::Mutex;
#[cfg(test)]
use std::any::Any;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

//
// SharedData
//

struct SharedData {
  volatile_buffer: Arc<VolatileRingBuffer>,
  non_volatile_buffer: Arc<NonVolatileRingBuffer>,
  shutdown_flush_thread: AtomicBool,
  allow_overwrite: AllowOverwrite,

  #[cfg(test)]
  thread_synchronizer: Arc<ThreadSynchronizer>,
}

impl SharedData {
  fn flush_thread_func(&self) -> Result<()> {
    log::debug!("starting flush thread func");
    let mut consumer = self.volatile_buffer.clone().register_consumer()?;
    let mut producer = self.non_volatile_buffer.clone().register_producer()?;

    while !self.shutdown_flush_thread.load(Ordering::Relaxed) {
      #[cfg(test)]
      self
        .thread_synchronizer
        .sync_point("thread_func_start_read");

      let read_reservation = match consumer.as_mut().start_read(true) {
        Ok(read_reservation) => read_reservation,
        Err(e) => {
          debug_assert!(matches!(e, Error::AbslStatus(AbslCode::Aborted, _)));
          debug_assert!(self.shutdown_flush_thread.load(Ordering::Relaxed));
          break;
        },
      };

      // The following will block if a reader is currently reading or if operating in no overwrite
      // mode if there is no space for the reservation. This will apply natural back pressure into
      // the volatile buffer.
      match producer
        .as_mut()
        .reserve(read_reservation.len().to_u32_lossy(), true)
      {
        Ok(write_reservation) => {
          write_reservation.copy_from_slice(read_reservation);
          producer.as_mut().commit()?;
        },
        Err(Error::AbslStatus(AbslCode::Unavailable, ref message)) => {
          // In this case we drop the write if the buffer is unavailable due to total data loss
          // reset processing.
          debug_assert_eq!(message, "pending total data loss reset");
        },
        Err(e) => {
          debug_assert!(self.shutdown_flush_thread.load(Ordering::Relaxed));
          debug_assert!(
            matches!(e, Error::AbslStatus(AbslCode::Aborted, _))
              || matches!(e, Error::AbslStatus(AbslCode::FailedPrecondition, message)
                        if message == "buffer is locked")
          );

          break;
        },
      }

      // The order of the commit and the finish read operations matters for properly implementing
      // flush(). The commit to the underlying non volatile storage must come before we finish the
      // read which may unblock anyone waiting on flush callback.
      consumer.as_mut().finish_read()?;
    }
    log::debug!("stopping flush thread func");
    Ok(())
  }
}

//
// RingBufferImpl
//

// An implementation of ring buffer with the following high level semantics:
// 1) Wraps both a volatile and non-volatile internal buffer.
// 2) Allows multiple producers to write into the internal volatile space.
// 3) Runs a flush thread to flush data from the volatile buffer to the non-volatile buffer.
// 4) Allows a single consumer to read from the internal non-volatile space.
pub struct RingBufferImpl {
  shared_data: Arc<SharedData>,
  flush_thread: Option<std::thread::JoinHandle<Result<()>>>,
  shutdown_lock_handle: Mutex<Option<Box<dyn LockHandle>>>,
}

impl RingBufferImpl {
  pub fn new<P: AsRef<Path>>(
    name: &str,
    volatile_size: u32,
    non_volatile_filename: P,
    non_volatile_size: u32,
    per_record_crc32_check: PerRecordCrc32Check,
    allow_overwrite: AllowOverwrite,
    volatile_stats: Arc<RingBufferStats>,
    non_volatile_stats: Arc<RingBufferStats>,
    on_record_evicted_cb: impl Fn(&[u8]) + Send + Sync + 'static,
  ) -> Result<Arc<Self>> {
    // For aggregate buffers, the size of the file (after subtracting header space) must be >= the
    // size of RAM. This is to avoid situations in which we accept a record into RAM but cannot ever
    // write it to disk.
    if non_volatile_size < std::mem::size_of::<FileHeader>().to_u32_lossy()
      || volatile_size > (non_volatile_size - std::mem::size_of::<FileHeader>().to_u32_lossy())
    {
      log::error!(
        "file size '{}' not big enough for header size '{}' or file size (minus header) not \
         bigger than memory size '{}'",
        non_volatile_size,
        std::mem::size_of::<FileHeader>(),
        volatile_size
      );
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "invalid file or memory size".to_string(),
      ));
    }

    let non_volatile_buffer = NonVolatileRingBuffer::new(
      format!("{name}-non-volatile"),
      non_volatile_filename,
      non_volatile_size,
      allow_overwrite,
      BlockWhenReservingIntoConcurrentRead::Yes,
      per_record_crc32_check,
      non_volatile_stats,
      |_| {},
    )?;

    let volatile_buffer = VolatileRingBuffer::new(
      format!("{name}-volatile"),
      volatile_size,
      volatile_stats,
      on_record_evicted_cb,
    );

    let shared_data = Arc::new(SharedData {
      volatile_buffer,
      non_volatile_buffer,
      shutdown_flush_thread: AtomicBool::new(false),
      allow_overwrite,

      #[cfg(test)]
      thread_synchronizer: ThreadSynchronizer::default().into(),
    });

    let cloned_shared_data = shared_data.clone();
    let flush_thread = std::thread::Builder::new()
      .name(format!("bd-buffer-{name}"))
      .spawn(move || cloned_shared_data.flush_thread_func())
      .map_err(|e| Error::ThreadStartFailure(e.to_string()))?;

    Ok(Arc::new(Self {
      shared_data,
      flush_thread: Some(flush_thread),
      shutdown_lock_handle: Mutex::default(),
    }))
  }

  #[must_use]
  pub fn non_volatile_buffer(&self) -> &NonVolatileRingBuffer {
    &self.shared_data.non_volatile_buffer
  }
}

impl Drop for RingBufferImpl {
  fn drop(&mut self) {
    self.shutdown();
    if let Some(flush_thread) = self.flush_thread.take() {
      let _ignored = flush_thread.join();
    }
  }
}

impl RingBuffer for RingBufferImpl {
  fn register_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferConsumer>> {
    self
      .shared_data
      .non_volatile_buffer
      .clone()
      .register_consumer()
  }

  fn register_cursor_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferCursorConsumer>> {
    self
      .shared_data
      .non_volatile_buffer
      .clone()
      .register_cursor_consumer()
  }

  fn register_producer(self: Arc<Self>) -> Result<Box<dyn RingBufferProducer>> {
    self.shared_data.volatile_buffer.clone().register_producer()
  }

  fn wait_for_drain(&self) {
    self.shared_data.non_volatile_buffer.wait_for_drain();
  }

  fn flush(&self) {
    self.shared_data.volatile_buffer.wait_for_drain();
  }

  fn shutdown(&self) {
    {
      let mut shutdown_lock_handle = self.shutdown_lock_handle.lock();

      #[cfg(test)]
      self.shared_data.thread_synchronizer.sync_point("shutdown");

      if shutdown_lock_handle.is_some() {
        return;
      }

      log::info!("shutting down ring buffer");
      *shutdown_lock_handle = Some(self.shared_data.volatile_buffer.clone().lock());

      // Make sure all reservations are drained. This should always be true at shutdown time but we
      // check anyway.
      if let Some(shutdown_lock_handle) = shutdown_lock_handle.as_ref() {
        shutdown_lock_handle.await_reservations_drained();
      }
    }

    if matches!(self.shared_data.allow_overwrite, AllowOverwrite::Yes) {
      // We are now locked for new writes so flush any volatile data to storage.
      // TODO(mattklein123): Currently this is only done if overwrites are allowed. Technically, we
      // could write until there is no space left. This edge case is not handled right now.
      self.flush();
    }

    // Once the flush is done we can now let the flush thread exit.
    self
      .shared_data
      .shutdown_flush_thread
      .store(true, Ordering::Relaxed);
    self.shared_data.volatile_buffer.shutdown();
    self.shared_data.non_volatile_buffer.shutdown();
  }

  fn lock(self: Arc<Self>) -> Box<dyn LockHandle> {
    self.shared_data.volatile_buffer.clone().lock()
  }

  #[cfg(test)]
  fn thread_synchronizer(&self) -> Arc<ThreadSynchronizer> {
    self.shared_data.thread_synchronizer.clone()
  }

  #[cfg(test)]
  fn as_any(&self) -> &dyn Any {
    self
  }
}
