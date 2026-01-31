// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod aggregate_ring_buffer;
mod common_ring_buffer;
mod intrusive_queue_with_free_list;
mod non_volatile_ring_buffer;
mod stats_test_helper;
mod volatile_ring_buffer;

#[cfg(test)]
pub(crate) mod test;

use bd_client_stats_store::Counter;
use bd_log_primitives::LossyIntToU32;
#[cfg(test)]
use std::any::Any;
use std::sync::Arc;
#[cfg(test)]
use test::thread_synchronizer::ThreadSynchronizer;

//
// RingBufferStats
//

// Ring buffer statistics.
// records_written:
//  # of records written into the buffer.
// records_read
//  # of records read from the buffer.
// records_overwritten
//  # of records overwritten to make room for new data.
// records_refused
//  # of records refused due to lack of space and no ability to overwrite.
// records_corrupted
//  # of records corrupted during read back and thus dropped.
// bytes_written
//  # of bytes written into the buffer.
// bytes_read
//  # of bytes read from the buffer.
// total_bytes_written
//  # of bytes written including the per-reservation overhead.
// total_bytes_read
//  # of bytes read including the per-reservation overhead.
// bytes_overwritten
//  # of bytes overwritten to make room for new data.
// bytes_refused
//  # of bytes refused due to lack of space and no ability to overwrite.
// total_data_loss
//  This is incremented if unrecoverable corruption is found and all data must be dropped.
#[derive(Default)]
pub struct RingBufferStats {
  pub records_written: Option<Counter>,
  pub records_read: Option<Counter>,
  pub records_overwritten: Option<Counter>,
  pub records_refused: Option<Counter>,
  pub records_corrupted: Option<Counter>,
  pub bytes_written: Option<Counter>,
  pub bytes_read: Option<Counter>,
  pub total_bytes_written: Option<Counter>,
  pub total_bytes_read: Option<Counter>,
  pub bytes_overwritten: Option<Counter>,
  pub bytes_refused: Option<Counter>,
  pub total_data_loss: Option<Counter>,
}

//
// RingBufferProducer
//

// A producer (writer) for a ring buffer. Producers produce individual records to be consumed by
// consumers. Individual records are created via reserve()/commit() API call pairs.
//
// NOTE: reserve()/commit() has been chosen versus making reserve() return an RAII handle to avoid
// an extra allocation for every reservation. Same for startRead()/finishRead() below.
pub trait RingBufferProducer: Send {
  // Reserve space in the ring. An error is returned if reserve() is called before committing a
  // previous reservation.
  // @param size supplies the size of space to reserve.
  // @param block supplies whether to block or not during reserve. Blocking can happen if no
  //        overwrites are allowed and readers have not read.
  // @return either a span that specifies the reserved space to write into, or an error indicating
  // why the space could not be reserved. Note that the returned space is guaranteed to be
  // contiguous.
  fn reserve(&mut self, size: u32, block: bool) -> Result<&mut [u8]>;

  // Commit a previous reservation, making the data available to readers. It is an error to commit
  // without a previous reservation.
  fn commit(&mut self) -> Result<()>;

  // Writes a single record to the buffer by copying the provided data into the record reservation.
  fn write(&mut self, data: &[u8]) -> Result<()> {
    let reserved = self.reserve(data.len().to_u32_lossy(), true)?;
    reserved.copy_from_slice(data);
    self.commit()
  }
}

//
// RingBufferConsumer
//

// A consumer (reader) for a ring buffer. Consumers read individual records produced by producers.
// Every startRead()/finishRead() call pair will return a record that was originally produced via a
// reserve()/commit() call pair.
#[async_trait::async_trait]
pub trait RingBufferConsumer: Send {
  // Performs an async read, delaying the resolution of the future until there is an available
  // record.
  async fn read<'a>(&'a mut self) -> Result<&'a [u8]>;

  // Start a read on the buffer. An error is returned if startRead() is called before a previous
  // read is finished via finishRead().
  // @param block supplies whether this function will block if there is no data. If true, the call
  // will not return until there is data available or an unrelated error. If block is false, the
  // call will return the unavailable status.
  // @return either the span that specifies the data block to read, or an error indicating why the
  // read could not be started.
  fn start_read(&mut self, block: bool) -> Result<&[u8]>;

  // Finish a previously started read. It is an error to finish a read without previously starting
  // one.
  fn finish_read(&mut self) -> Result<()>;
}

// Required for assert_matches! and avoids leaking Debug everywhere.
#[cfg(test)]
impl std::fmt::Debug for dyn RingBufferConsumer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "ring buffer consumer")
  }
}

//
// RingBufferCursorConsumer
//

// A consumer (reader) for a ring buffer that offers a cursor interface as opposed to a concurrent
// reader interface. As such, by definition, any buffer that offers a cursor consumer will only
// provide a single consumer. It will also *not* allow data overwrites. Although this interface
// looks similar to RingBufferConsumer, it is semantically different and kept separate for type
// checking reasons.
#[async_trait::async_trait]
pub trait RingBufferCursorConsumer: Send {
  // Performs an async read, delaying the resolution of the future until there is an available
  // record.
  async fn read<'a>(&'a mut self) -> Result<&'a [u8]>;

  // Start a read on the buffer. This API can be called any number times while there is still data
  // to be read in the buffer.
  fn start_read(&mut self, block: bool) -> Result<&[u8]>;

  // Advance the read pointer forward and thus allow the advanced record to be overwritten. In
  // practice, this API is paired with startRead() and can only be called after a successful call to
  // startRead(), but does not have to interleave back and forth with that API. I.e., it is possible
  // to call startRead(), startRead(), advanceReadPointer(), startRead(), etc. Calling this releases
  // the oldest read data acquired with startRead().
  fn advance_read_pointer(&mut self) -> Result<()>;
}

// Required for assert_matches! and avoids leaking Debug everywhere.
#[cfg(test)]
impl std::fmt::Debug for dyn RingBufferCursorConsumer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "ring buffer cursor consumer")
  }
}

//
// LockHandle
//

// A lock handle acquired via the lock() function. Destroying the handle will unlock the buffer.
pub trait LockHandle: Send {
  // Locking a buffer prevents new reservations, but does not imply anything about existing
  // reservations. This function can be used to wait until all existing reservations are drained.
  fn await_reservations_drained(&self);
}

//
// RingBuffer
//

// A ring buffer interface that offers:
// 1) Registering 1 or more producers.
// 2) Registering 1 or more consumers.
// 3) No blocking writing (space will either be reserved or a reason given for space not being
// reserved). 4) Opaque records using variable record sizes. 5) Contiguous reservations. This means
// that buffers are guaranteed to not be split across the wrap point. This simplifies overall
// operation at the cost of not using all bytes in the buffer. The assumption is that in general the
// size of the buffer is much larger than the average reservation size.
//
// Different implementations of this interface have different performance and volatility
// characteristics and also may support a different number of producers and consumers (e.g.,
// multiple producer and single consumer, single producer and single consumer, etc.). See the
// documentation for specific implementations for more details.
#[allow(clippy::module_name_repetitions)]
pub trait RingBuffer: Send + Sync {
  // Register a producer. The returned handle allows for reservations and commits. If an additional
  // producer cannot be registered an error will be returned.
  fn register_producer(self: Arc<Self>) -> Result<Box<dyn RingBufferProducer>>;

  // Register a consumer. The returned handle allows for starting and finishing reads. If an
  // additional consumer cannot be registered an error will be returned.
  fn register_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferConsumer>>;

  // Register a cursor consumer. A buffer that supports this will only offer a single cursor
  // consumer and the buffer will not support data overwrites. If a cursor consumer cannot be
  // registered an error will be returned.
  fn register_cursor_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferCursorConsumer>>;

  // This function waits for data to be drained *at the point in which this function is invoked.*
  // Draining is defined as all pending data has been read from the buffer. This means that any new
  // writes that come in after this call will not count towards the wait time. A few notes: 1)
  // drain_complete_cb supplies the callback to invoke when the drain is complete. 2) The callback
  // may be invoked immediately in the context of this call if there is no data to drain. 3) The
  // callback may be invoked on a different thread from the thread that invoked this call. The
  // callback should be thread safe. 4) Only a single callback is stored at a given time and there
  // is no way currently to cancel a callback. If this function is called again before the previous
  // drain is completed, the previous callback will never be called and the new drain will start.
  fn wait_for_drain(&self);

  // This function flushes all data *at the point in which this function is invoked.* What flushing
  // means is dependent on the underlying buffer implementation. For example, the aggregate buffer
  // will make sure data has been persisted to the non-volatile buffer. All of the same notes
  // provided for waitForDrain() also apply to this function.
  fn flush(&self);

  // Shutdown the ring buffer. This will release any blocked readers and prevent any new writes from
  // being accepted. This function implicitly performs a lock() and flush() on the buffer and will
  // block until these operations are complete. It also cannot be undone and is meant to be called
  // prior to an orderly shutdown.
  fn shutdown(&self);

  // Locks the ring buffer. This means that the buffer will no longer accept new writes. All
  // attempted reservations will be failed with a FAILED_PRECONDITION error code. Once locked, the
  // lock handle can be used to access a read-only view of the buffer. When the lock handle is
  // destroyed the buffer will become unlocked. Locks can be stacked. Only when the last lock handle
  // is destroyed will the buffer be unlocked.
  fn lock(self: Arc<Self>) -> Box<dyn LockHandle>;

  // Test only: return the thread synchronizer used within the buffer.
  #[cfg(test)]
  fn thread_synchronizer(&self) -> Arc<ThreadSynchronizer>;

  // Test only: needed for dynamic dispatch in the common tests.
  #[cfg(test)]
  fn as_any(&self) -> &dyn Any;
}

use crate::Result;
#[allow(clippy::module_name_repetitions)]
pub use aggregate_ring_buffer::RingBufferImpl as AggregateRingBuffer;
pub use common_ring_buffer::AllowOverwrite;
#[allow(clippy::module_name_repetitions)]
pub use non_volatile_ring_buffer::{
  BlockWhenReservingIntoConcurrentRead,
  FileHeader as NonVolatileFileHeader,
  PerRecordCrc32Check,
  RingBufferImpl as NonVolatileRingBuffer,
};
// The following test helpers are used by the fuzz tests.
pub use stats_test_helper::{OptionalStatGetter, StatsTestHelper};
#[allow(clippy::module_name_repetitions)]
pub use volatile_ring_buffer::RingBufferImpl as VolatileRingBuffer;
