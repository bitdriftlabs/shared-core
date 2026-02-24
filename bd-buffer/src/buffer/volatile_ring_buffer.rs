// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./volatile_ring_buffer_test.rs"]
mod volatile_ring_buffer_test;

use super::common_ring_buffer::{
  AllowOverwrite,
  CommonRingBuffer,
  Conditions,
  Cursor,
  LockHandleImpl as CommonLockHandleImpl,
  LockedData,
  Range,
};
use super::intrusive_queue_with_free_list::{Container, IntrusiveQueueWithFreeList};
#[cfg(test)]
use super::test::thread_synchronizer::ThreadSynchronizer;
use super::{
  LockHandle,
  RingBuffer,
  RingBufferConsumer,
  RingBufferCursorConsumer,
  RingBufferProducer,
  RingBufferStats,
};
use crate::{AbslCode, Error, Result};
use bd_client_common::error::InvariantError;
use parking_lot::{Condvar, MutexGuard};
#[cfg(test)]
use std::any::Any;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::{Arc, Weak};

//
// Reservation
//

#[derive(Default, Clone)]
struct Reservation {
  range: Range,
  committed: bool,
}

//
// ProducerImpl
//

struct SendPointer<T>(*mut T);
unsafe impl<T> Send for SendPointer<T> {}

// Producer implementation for the volatile buffer.
// TODO(mattklein123): Currently if the producer is dropped with an active reservation the
// reservation is not cleaned up. This was also not handled in the original C++ code.
struct ProducerImpl {
  buffer: Weak<RingBufferImpl>,
  reservation: SendPointer<Container<Reservation>>,
}

impl RingBufferProducer for ProducerImpl {
  fn reserve<'a>(&'a mut self, size: u32, block: bool) -> Result<&'a mut [u8]> {
    if !self.reservation.0.is_null() {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "attempt to re-reserve before commit".to_string(),
      ));
    }

    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    let actual_size = common_ring_buffer.start_reserve_common(size)?;
    let temp_reservation_data = common_ring_buffer.calculate_next_write_start(actual_size)?;

    // If there is a concurrent reader block the write and drop the data.
    let read_reservation = common_ring_buffer
      .extra_locked_data
      .consumer
      .as_ref()
      .and_then(|c| c.reservation.as_ref())
      .cloned();
    if read_reservation.map_or(Ok(false), |r| {
      common_ring_buffer.intersect_reservation(&temp_reservation_data, &r)
    })? {
      log::trace!(
        "({}) writing into concurrent read. Dropping due to no space left",
        common_ring_buffer.name
      );
      return Err(common_ring_buffer.return_record_refused(
        size,
        AbslCode::ResourceExhausted,
        "writing into concurrent read",
      ));
    }

    let first_reservation = common_ring_buffer
      .extra_locked_data
      .reservations
      .front()
      .cloned();
    if first_reservation.map_or(Ok(false), |r| {
      common_ring_buffer.intersect_reservation(&temp_reservation_data, &r.range)
    })? {
      log::trace!(
        "({}) writing into concurrent write. Dropping due to no space left",
        common_ring_buffer.name
      );
      return Err(common_ring_buffer.return_record_refused(
        size,
        AbslCode::ResourceExhausted,
        "writing into concurrent write",
      ));
    }

    LockedData::advance_next_read_due_to_write(
      &mut common_ring_buffer,
      &parent.common_ring_buffer.conditions,
      &temp_reservation_data,
      block,
    )?;

    // We delay all actual state changes to this point (other than moving next read start in the
    // previous stanza) because we don't want to make any state changes until we know for sure that
    // the reservation is not going to fail.
    let reservation = Reservation {
      range: temp_reservation_data.range,
      committed: false,
    };
    *common_ring_buffer.next_write_start() = temp_reservation_data.next_write_start;
    if temp_reservation_data.last_write_end_before_wrap.is_some() {
      *common_ring_buffer.last_write_end_before_wrap() =
        temp_reservation_data.last_write_end_before_wrap;
    }

    let memory = common_ring_buffer.finish_reservation(&reservation.range, size)?;
    let memory = unsafe {
      // Safety: This is safe insofar as we are theoretically not handing out overlapping ranges
      // at the same time. The transmute drops the shared mutability check and propagates the
      // lifetime to the caller. Really, the lifetime should be bound to the buffer but there is no
      // easy/cheap way to do that.
      std::mem::transmute::<&mut [u8], &'a mut [u8]>(memory)
    };

    self.reservation = SendPointer(
      common_ring_buffer
        .extra_locked_data
        .reservations
        .emplace_back(reservation),
    );
    Ok(memory)
  }

  fn commit(&mut self) -> Result<()> {
    if self.reservation.0.is_null() {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "commit before reserve".to_string(),
      ));
    }

    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    let reservation_range = common_ring_buffer.extra_locked_data.reservations.map_mut(
      self.reservation.0,
      |reservation| {
        debug_assert!(!reservation.committed);
        reservation.committed = true;
        reservation.range.clone()
      },
    );
    common_ring_buffer
      .finish_commit_common(&reservation_range.ok_or(InvariantError::Invariant)?)?;

    loop {
      let Some(front) = common_ring_buffer.extra_locked_data.reservations.front() else {
        break;
      };
      if !front.committed {
        break;
      }
      log::trace!(
        "({}) popping reservation {}",
        common_ring_buffer.name,
        front.range
      );
      let range = front.range.clone();

      common_ring_buffer
        .advance_next_commited_write_start(&parent.common_ring_buffer.conditions, &range)?;
      common_ring_buffer
        .extra_locked_data
        .reservations
        .pop_front();
    }

    self.reservation = SendPointer(std::ptr::null_mut());

    if common_ring_buffer.extra_locked_data.reservations.is_empty() {
      parent.no_reservations_condition.notify_all();
    }

    Ok(())
  }
}

//
// ConsumerImpl
//

struct ConsumerImpl {
  buffer: Weak<RingBufferImpl>,
  readable: tokio::sync::watch::Receiver<bool>,
}

impl ConsumerImpl {
  fn inner_start_read<'a>(
    mut common_ring_buffer: MutexGuard<'_, LockedData<ExtraLockedData>>,
    conditions: &Conditions,
    block: bool,
  ) -> Result<&'a [u8]> {
    let mut existing_reservation = common_ring_buffer
      .extra_locked_data
      .consumer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
      .reservation
      .clone();
    let start_read_data = LockedData::start_read(
      &mut common_ring_buffer,
      conditions,
      block,
      &mut existing_reservation,
      Cursor::No,
    )?;
    common_ring_buffer
      .extra_locked_data
      .consumer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
      .reservation = existing_reservation;

    Ok(unsafe {
      // Safety: This is safe insofar as we are theoretically not handing out overlapping ranges
      // at the same time. The transmute drops the shared mutability check and propagates the
      // lifetime to the caller. Really, the lifetime should be bound to the buffer but there is no
      // easy/cheap way to do that.
      std::mem::transmute::<&[u8], &'a [u8]>(start_read_data.record_data)
    })
  }
}

#[async_trait::async_trait]
impl RingBufferConsumer for ConsumerImpl {
  async fn read<'a>(&'a mut self) -> Result<&'a [u8]> {
    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    // TODO(snowp): It doesn't seem like this clone would be necessary, but there are some lifetime
    // issues to work through.
    let mut readable = self.readable.clone();
    loop {
      if *readable.borrow_and_update() {
        match Self::inner_start_read(
          parent.common_ring_buffer.locked_data.lock(),
          &parent.common_ring_buffer.conditions,
          false,
        ) {
          Ok(value) => return Ok(value),
          Err(Error::AbslStatus(AbslCode::Unavailable, _)) => {
            log::trace!(
              "({}) read resulted in no entry, waiting for more data",
              parent.common_ring_buffer.locked_data.lock().name
            );
            continue;
          },
          Err(e) => return Err(e),
        }
      }

      readable
        .changed()
        .await
        .map_err(|_| InvariantError::Invariant)?;
    }
  }

  fn start_read(&mut self, block: bool) -> Result<&[u8]> {
    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    Self::inner_start_read(
      common_ring_buffer,
      &parent.common_ring_buffer.conditions,
      block,
    )
  }

  fn finish_read(&mut self) -> Result<()> {
    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    let reservation = common_ring_buffer
      .extra_locked_data
      .consumer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
      .reservation
      .clone();

    if reservation.is_none() {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "finish read before starting".to_string(),
      ));
    }

    common_ring_buffer
      .extra_locked_data
      .consumer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
      .reservation = None;
    LockedData::finish_read_common(
      &mut common_ring_buffer,
      &parent.common_ring_buffer.conditions,
      &reservation.ok_or(InvariantError::Invariant)?,
    )?;
    Ok(())
  }
}

impl Drop for ConsumerImpl {
  fn drop(&mut self) {
    if let Some(parent) = self.buffer.upgrade() {
      let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
      debug_assert!(common_ring_buffer.extra_locked_data.consumer.is_some());
      common_ring_buffer.extra_locked_data.consumer = None;
    }
  }
}

//
// ExtraLockedData
//

struct ActiveConsumerData {
  reservation: Option<Range>,
}

#[derive(Default)]
struct ExtraLockedData {
  consumer: Option<ActiveConsumerData>,
  reservations: IntrusiveQueueWithFreeList<Reservation>,
}

//
// ControlData
//

// This structure contains the memory for the control variables needed by the common ring buffer.
// It must be pinned as the common ring buffer code uses pointers to access the control variables.
// (Thus making it work the same as the memory mapped file).
struct ControlData {
  memory_do_not_use: Vec<u8>,
  next_write_start_do_not_use: u32,
  committed_write_start_do_not_use: Option<u32>,
  last_write_end_before_wrap_do_not_use: Option<u32>,
  next_read_start_do_not_use: Option<u32>,
  _pinned: PhantomPinned,
}

//
// LockHandleImpl
//

struct LockHandleImpl {
  buffer: Weak<RingBufferImpl>,
  _lock_handle: CommonLockHandleImpl,
}

impl LockHandle for LockHandleImpl {
  fn await_reservations_drained(&self) {
    if let Some(parent) = self.buffer.upgrade() {
      let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
      if !common_ring_buffer.extra_locked_data.reservations.is_empty() {
        parent
          .no_reservations_condition
          .wait(&mut common_ring_buffer);
      }
    }
  }
}

//
// RingBufferImpl
//

// An implementation of RingBuffer with the following high level semantics:
// 1) Allows for multiple producers and a single consumer.
// 2) State is volatile, meaning that it is not meant to be persisted to disk.
// 3) No locks are held during memory copies. Locks are only held during accounting updates.
// 4) Producers can commit out of order, and the consumer will be appropriately notified when a new
// read is ready.
//
// High level limitations that are unlikely to be addressed:
// 1) If there is no space for a reservation due to existing reservations on concurrent producers, a
// reservation attempt will fail and the caller will likely need to drop the data.
pub struct RingBufferImpl {
  _control_data_do_not_use: Pin<Box<ControlData>>,
  common_ring_buffer: CommonRingBuffer<ExtraLockedData>,
  no_reservations_condition: Condvar,
}

impl RingBufferImpl {
  #[must_use]
  pub fn new(
    name: String,
    size: u32,
    stats: Arc<RingBufferStats>,
    on_record_evicted_cb: impl Fn(&[u8]) + Send + Sync + 'static,
  ) -> Arc<Self> {
    let mut memory_do_not_use = Vec::with_capacity(size as usize);
    memory_do_not_use.spare_capacity_mut(); // Appease clippy.
    unsafe {
      // Safety: The capacity was allocated above.
      memory_do_not_use.set_len(size as usize);
    }

    let control_data_do_not_use = Box::pin(ControlData {
      memory_do_not_use,
      next_write_start_do_not_use: 0,
      committed_write_start_do_not_use: None,
      last_write_end_before_wrap_do_not_use: None,
      next_read_start_do_not_use: None,
      _pinned: PhantomPinned,
    });

    let common_ring_buffer = CommonRingBuffer::new(
      name,
      (control_data_do_not_use.memory_do_not_use.as_slice()).into(),
      (&control_data_do_not_use.next_write_start_do_not_use).into(),
      (&control_data_do_not_use.committed_write_start_do_not_use).into(),
      (&control_data_do_not_use.last_write_end_before_wrap_do_not_use).into(),
      (&control_data_do_not_use.next_read_start_do_not_use).into(),
      0,
      ExtraLockedData::default(),
      AllowOverwrite::Yes,
      stats,
      |_| {},
      on_record_evicted_cb,
      |extra_locked_data| {
        extra_locked_data
          .consumer
          .as_ref()
          .and_then(|c| c.reservation.as_ref())
          .is_some()
      },
      |extra_locked_data| !extra_locked_data.reservations.is_empty(),
    );

    Arc::new(Self {
      _control_data_do_not_use: control_data_do_not_use,
      common_ring_buffer,
      no_reservations_condition: Condvar::default(),
    })
  }
}

impl RingBuffer for RingBufferImpl {
  fn register_producer(self: Arc<Self>) -> Result<Box<dyn RingBufferProducer>> {
    log::trace!(
      "({}) registering producer",
      self.common_ring_buffer.locked_data.lock().name
    );
    let producer = Box::new(ProducerImpl {
      buffer: Arc::<Self>::downgrade(&self),
      reservation: SendPointer(std::ptr::null_mut()),
    });
    Ok(producer)
  }

  fn register_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferConsumer>> {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    if common_ring_buffer.extra_locked_data.consumer.is_some() {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "consumer already registered".to_string(),
      ));
    }
    log::trace!("({}) registering consumer", common_ring_buffer.name);

    let consumer = Box::new(ConsumerImpl {
      buffer: Arc::<Self>::downgrade(&self),
      readable: common_ring_buffer.readable.subscribe(),
    });
    common_ring_buffer.extra_locked_data.consumer = Some(ActiveConsumerData { reservation: None });
    Ok(consumer)
  }

  fn register_cursor_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferCursorConsumer>> {
    Err(Error::AbslStatus(
      AbslCode::Unimplemented,
      "not supported".to_string(),
    ))
  }

  fn wait_for_drain(&self) {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    LockedData::wait_for_drain(&mut common_ring_buffer, &self.common_ring_buffer.conditions);
  }

  fn flush(&self) {}

  fn shutdown(&self) {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    LockedData::shutdown_common(&mut common_ring_buffer, &self.common_ring_buffer.conditions);
  }

  fn lock(self: Arc<Self>) -> Box<dyn LockHandle> {
    Box::new(LockHandleImpl {
      buffer: Arc::<Self>::downgrade(&self),
      _lock_handle: self.common_ring_buffer.locked_data.lock().lock_common(),
    })
  }

  #[cfg(test)]
  fn thread_synchronizer(&self) -> Arc<ThreadSynchronizer> {
    self
      .common_ring_buffer
      .locked_data
      .lock()
      .thread_synchronizer
      .clone()
  }

  #[cfg(test)]
  fn as_any(&self) -> &dyn Any {
    self
  }
}
