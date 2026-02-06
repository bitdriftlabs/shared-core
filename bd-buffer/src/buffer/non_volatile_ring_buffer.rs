// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./non_volatile_ring_buffer_test.rs"]
mod non_volatile_ring_buffer_test;

use super::common_ring_buffer::{
  AllowOverwrite,
  CommonRingBuffer,
  Conditions,
  Cursor,
  LockHandleImpl as CommonLockHandleImpl,
  LockedData,
  Range,
  SendSyncNonNull,
};
use super::intrusive_queue_with_free_list::IntrusiveQueueWithFreeList;
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
use bd_log_primitives::LossyIntToU32;
use crc32fast::Hasher;
use fs2::FileExt;
use intrusive_collections::offset_of;
use memmap2::MmapMut;
use parking_lot::{Condvar, MutexGuard};
use static_assertions::const_assert_eq;
#[cfg(test)]
use std::any::Any;
use std::cell::RefCell;
use std::fs::File;
use std::hash::Hash;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};

//
// PerRecordCrc32Check
//

pub enum PerRecordCrc32Check {
  Yes,
  No,
}

//
// BlockWhenReservingIntoConcurrentRead
//

pub enum BlockWhenReservingIntoConcurrentRead {
  Yes,
  No,
}

//
// ProducerImpl
//

struct ProducerImpl {
  buffer: Weak<RingBufferImpl>,
}

impl RingBufferProducer for ProducerImpl {
  fn reserve<'a>(&'a mut self, size: u32, block: bool) -> Result<&'a mut [u8]> {
    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    if common_ring_buffer
      .extra_locked_data
      .producer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
      .reservation
      .is_some()
    {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "attempt to re-reserve before commit".to_string(),
      ));
    }

    let actual_size = common_ring_buffer.start_reserve_common(size)?;
    let temp_reservation_data = common_ring_buffer.calculate_next_write_start(actual_size)?;

    // If there is a concurrent reader block the write and drop the data.
    loop {
      let read_reservation = common_ring_buffer
        .extra_locked_data
        .consumer
        .as_mut()
        .and_then(|c| match c {
          ConsumerType::Consumer(c) => c.clone(),
          ConsumerType::CursorConsumer(c) => c.front().cloned(),
        });

      match read_reservation {
        None => break,
        Some(read_reservation) => {
          if !common_ring_buffer.intersect_reservation(&temp_reservation_data, &read_reservation)? {
            break;
          }
        },
      }

      match common_ring_buffer
        .extra_locked_data
        .block_when_reserving_into_concurrent_read
      {
        BlockWhenReservingIntoConcurrentRead::No => {
          log::trace!(
            "({}) writing into concurrent read. Dropping due to no space left",
            common_ring_buffer.name
          );
          return Err(common_ring_buffer.return_record_refused(
            size,
            AbslCode::ResourceExhausted,
            "writing into concurrent read",
          ));
        },
        BlockWhenReservingIntoConcurrentRead::Yes => {
          log::trace!(
            "({}) blocking due to concurrent reader",
            common_ring_buffer.name
          );

          #[cfg(test)]
          common_ring_buffer
            .thread_synchronizer
            .sync_point("block_for_concurrent_read");

          parent
            .no_consumer_reservation_condition
            .wait(&mut common_ring_buffer);

          if common_ring_buffer.is_shutdown() {
            return Err(Error::AbslStatus(
              AbslCode::Aborted,
              "ring buffer shut down".to_string(),
            ));
          }
        },
      }
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
    *common_ring_buffer.next_write_start() = temp_reservation_data.next_write_start;
    if temp_reservation_data.last_write_end_before_wrap.is_some() {
      *common_ring_buffer.last_write_end_before_wrap() =
        temp_reservation_data.last_write_end_before_wrap;
    }

    let memory = common_ring_buffer.finish_reservation(&temp_reservation_data.range, size)?;
    let memory = unsafe {
      // Safety: This is safe insofar as we are theoretically not handing out overlapping ranges at
      // the same time. The transmute drops the shared mutability check and propagates the lifetime
      // to the caller. Really, the lifetime should be bound to the buffer but there is no
      // easy/cheap way to do that.
      std::mem::transmute::<&mut [u8], &'a mut [u8]>(memory)
    };

    common_ring_buffer
      .extra_locked_data
      .producer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
      .reservation = Some(temp_reservation_data.range);

    write_header_crc32(&mut common_ring_buffer);
    Ok(memory)
  }

  fn commit(&mut self) -> Result<()> {
    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    let reservation = common_ring_buffer
      .extra_locked_data
      .producer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
      .reservation
      .clone();
    if reservation.is_none() {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "commit before reserve".to_string(),
      ));
    }

    // TODO(mattklein123): Further file based verification tools:
    // 1) Potentially use msync() or similar to force records to disk either after every record or
    //    every N records. The performance cost of this will need to be measured especially since
    //    losing logs may not be considered critical for certain use cases.

    common_ring_buffer.advance_next_commited_write_start(
      &parent.common_ring_buffer.conditions,
      reservation.as_ref().ok_or(InvariantError::Invariant)?,
    )?;

    let extra_record_header_space = common_ring_buffer
      .finish_commit_common(reservation.as_ref().ok_or(InvariantError::Invariant)?)?;
    let extra_record_header_space: &mut [u8] = unsafe {
      // Safety: This is safe insofar as we should not be writing to the same region from
      // multiple threads.
      std::mem::transmute(extra_record_header_space)
    };

    if matches!(
      common_ring_buffer.extra_locked_data.per_record_crc32_check,
      PerRecordCrc32Check::Yes
    ) {
      debug_assert!(extra_record_header_space.len() == 4);
      let data_crc32 = per_record_crc32(
        &mut common_ring_buffer,
        reservation.as_ref().ok_or(InvariantError::Invariant)?,
        extra_record_header_space.len(),
      );
      extra_record_header_space.copy_from_slice(&data_crc32.to_ne_bytes());
    }
    common_ring_buffer
      .extra_locked_data
      .producer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
      .reservation = None;

    common_ring_buffer.maybe_do_total_data_loss_reset();
    write_header_crc32(&mut common_ring_buffer);
    parent.no_producer_reservation_condition.notify_all();
    Ok(())
  }
}

impl Drop for ProducerImpl {
  fn drop(&mut self) {
    if let Some(parent) = self.buffer.upgrade() {
      let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
      debug_assert!(common_ring_buffer.extra_locked_data.producer.is_some());
      common_ring_buffer.extra_locked_data.producer = None;
    }
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
    let mut existing_reservation = match common_ring_buffer
      .extra_locked_data
      .consumer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(c) => c.clone(),
      ConsumerType::CursorConsumer(_) => return Err(InvariantError::Invariant.into()),
    };

    let result = common_consumer_start_read(
      &mut common_ring_buffer,
      conditions,
      block,
      &mut existing_reservation,
      Cursor::No,
    );
    let result = unsafe {
      // Safety: This is safe insofar as we should not be writing to the same region from
      // multiple threads.
      std::mem::transmute::<Result<&[u8]>, Result<&'a [u8]>>(result)
    };

    match common_ring_buffer
      .extra_locked_data
      .consumer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(c) => *c = existing_reservation,
      ConsumerType::CursorConsumer(_) => return Err(InvariantError::Invariant.into()),
    }

    result
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
      if *readable.borrow() {
        match Self::inner_start_read(
          parent.common_ring_buffer.locked_data.lock(),
          &parent.common_ring_buffer.conditions,
          false,
        ) {
          Ok(value) => return Ok(value),
          Err(Error::AbslStatus(AbslCode::Unavailable, _)) => {},
          Err(e) => return Err(e),
        }
      }

      log::trace!(
        "({}) cursor not readable, waiting for data",
        parent.common_ring_buffer.locked_data.lock().name
      );
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

    Self::inner_start_read(
      parent.common_ring_buffer.locked_data.lock(),
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
    let reservation = match common_ring_buffer
      .extra_locked_data
      .consumer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(c) => c.clone(),
      ConsumerType::CursorConsumer(_) => return Err(InvariantError::Invariant.into()),
    };

    if reservation.is_none() {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "finish read before starting".to_string(),
      ));
    }

    match common_ring_buffer
      .extra_locked_data
      .consumer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(c) => *c = None,
      ConsumerType::CursorConsumer(_) => return Err(InvariantError::Invariant.into()),
    }
    LockedData::finish_read_common(
      &mut common_ring_buffer,
      &parent.common_ring_buffer.conditions,
      &reservation.ok_or(InvariantError::Invariant)?,
    )?;
    write_header_crc32(&mut common_ring_buffer);
    parent.no_consumer_reservation_condition.notify_all();

    Ok(())
  }
}

impl Drop for ConsumerImpl {
  fn drop(&mut self) {
    if let Some(parent) = self.buffer.upgrade() {
      parent
        .common_ring_buffer
        .locked_data
        .lock()
        .extra_locked_data
        .consumer = None;
    }
  }
}

//
// CursorConsumerImpl
//

struct CursorConsumerImpl {
  buffer: Weak<RingBufferImpl>,
  readable: tokio::sync::watch::Receiver<bool>,
}

impl CursorConsumerImpl {
  fn get_reservations<'a>(
    guard: &'a MutexGuard<'_, LockedData<ExtraLockedData>>,
  ) -> Result<&'a IntrusiveQueueWithFreeList<Range>> {
    match guard
      .extra_locked_data
      .consumer
      .as_ref()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(_) => Err(InvariantError::Invariant.into()),
      ConsumerType::CursorConsumer(c) => Ok(c),
    }
  }

  fn get_reservations_mut<'a>(
    guard: &'a mut MutexGuard<'_, LockedData<ExtraLockedData>>,
  ) -> Result<&'a mut IntrusiveQueueWithFreeList<Range>> {
    match guard
      .extra_locked_data
      .consumer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(_) => Err(InvariantError::Invariant.into()),
      ConsumerType::CursorConsumer(c) => Ok(c),
    }
  }

  #[allow(clippy::unused_self)]
  fn inner_start_read<'a>(
    &'a self,
    mut common_ring_buffer: MutexGuard<'_, LockedData<ExtraLockedData>>,
    conditions: &Conditions,
    block: bool,
  ) -> Result<&'a [u8]> {
    let mut reservation = None;
    let result = common_consumer_start_read(
      &mut common_ring_buffer,
      conditions,
      block,
      &mut reservation,
      Cursor::Yes,
    )?;
    let result = unsafe {
      // Safety: This is safe insofar as we should not be writing to the same region from
      // multiple threads.
      std::mem::transmute::<&[u8], &'a [u8]>(result)
    };

    match common_ring_buffer
      .extra_locked_data
      .consumer
      .as_mut()
      .ok_or(InvariantError::Invariant)?
    {
      ConsumerType::Consumer(_) => return Err(InvariantError::Invariant.into()),
      ConsumerType::CursorConsumer(c) => {
        c.emplace_back(reservation.ok_or(InvariantError::Invariant)?);
      },
    }

    Ok(result)
  }
}

#[async_trait::async_trait]
impl RingBufferCursorConsumer for CursorConsumerImpl {
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
      if *readable.borrow() {
        match self.inner_start_read(
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

      log::trace!(
        "({}) cursor not readable, waiting for data",
        parent.common_ring_buffer.locked_data.lock().name
      );
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

    self.inner_start_read(
      parent.common_ring_buffer.locked_data.lock(),
      &parent.common_ring_buffer.conditions,
      block,
    )
  }

  fn advance_read_pointer(&mut self) -> Result<()> {
    let Some(parent) = self.buffer.upgrade() else {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "shutdown".to_string(),
      ));
    };

    let mut common_ring_buffer = parent.common_ring_buffer.locked_data.lock();
    if Self::get_reservations(&common_ring_buffer)?.is_empty() {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "no previous read to advance".to_string(),
      ));
    }

    let next_reservation = Self::get_reservations(&common_ring_buffer)?
      .front()
      .ok_or(InvariantError::Invariant)?
      .clone();
    Self::get_reservations_mut(&mut common_ring_buffer)?.pop_front();
    LockedData::finish_read_common(
      &mut common_ring_buffer,
      &parent.common_ring_buffer.conditions,
      &next_reservation,
    )?;
    write_header_crc32(&mut common_ring_buffer);
    parent.no_consumer_reservation_condition.notify_all();
    Ok(())
  }
}

impl Drop for CursorConsumerImpl {
  fn drop(&mut self) {
    if let Some(parent) = self.buffer.upgrade() {
      parent
        .common_ring_buffer
        .locked_data
        .lock()
        .extra_locked_data
        .consumer = None;
    }
  }
}

//
// FileHeader
//

#[repr(C)]
#[derive(Default)]
pub struct FileHeader {
  version: u32,
  size: u32,
  next_write_start: u32,
  committed_write_start: Option<u32>,
  last_write_end_before_wrap: Option<u32>,
  next_read_start: Option<u32>,
  crc32: u32,
}

// Version 1: Original version.
// Version 2: Switched to protobuf encoding for records.
const FILE_HEADER_CURRENT_VERSION: u32 = 2;

//
// ConsumerType
//

enum ConsumerType {
  Consumer(Option<Range>),
  CursorConsumer(IntrusiveQueueWithFreeList<Range>),
}

//
// ExtraLockedData
//

struct ProducerData {
  reservation: Option<Range>,
}

struct ExtraLockedData {
  producer: Option<ProducerData>,
  consumer: Option<ConsumerType>,
  crc32: SendSyncNonNull<u32>,
  block_when_reserving_into_concurrent_read: BlockWhenReservingIntoConcurrentRead,
  per_record_crc32_check: PerRecordCrc32Check,
}

impl ExtraLockedData {
  fn on_total_data_loss(&mut self) {
    write_header_crc32_worker(self, 0, None, None, None);
  }
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
      if common_ring_buffer
        .extra_locked_data
        .producer
        .as_ref()
        .and_then(|p| p.reservation.as_ref())
        .is_some()
      {
        parent
          .no_producer_reservation_condition
          .wait(&mut common_ring_buffer);
      }
    }
  }
}

//
// RingBufferImpl
//

// An implementation of RingBuffer with the following high level semantics:
// 1) Allows for a single producer and a single consumer.
// 2) State is non-volatile, meaning that it is meant to be persisted to disk and reloaded later.
// 3) No locks are held during memory copies. Locks are only held during accounting updates.
//
// TODO(mattklein123): High level limitations that will be addressed:
// 1) Consider the use of msync() or similar to force flushing to disk periodically and/or at
// shutdown.
pub struct RingBufferImpl {
  _memory_do_not_use: MmapMut,
  common_ring_buffer: CommonRingBuffer<ExtraLockedData>,
  no_consumer_reservation_condition: Condvar,
  no_producer_reservation_condition: Condvar,
}

impl RingBufferImpl {
  pub fn new<P: AsRef<Path>>(
    name: String,
    filename: P,
    size: u32,
    allow_overwrite: AllowOverwrite,
    block_when_reserving_into_concurrent_read: BlockWhenReservingIntoConcurrentRead,
    per_record_crc32_check: PerRecordCrc32Check,
    stats: Arc<RingBufferStats>,
    on_record_evicted_cb: impl Fn(&[u8]) + Send + Sync + 'static,
  ) -> Result<Arc<Self>> {
    // The following static asserts verify that FileHeader is a known size with all field offsets
    // known. This is done to avoid the use of #pragma pack(1) which may lead to poor performance on
    // unaligned reads and writes, for fields that will be frequently updated. If any of these
    // asserts fail on any architecture we care about we will need to move to per architecture
    // manual alignment. Additionally, although extremely unlikely, if a compiler change causes
    // these offsets to fail, either manual alignment changes will be required to match these or the
    // file version will need to be bumped.
    const_assert_eq!(std::mem::size_of::<FileHeader>(), 40);
    const_assert_eq!(offset_of!(FileHeader, version), 0);
    const_assert_eq!(offset_of!(FileHeader, size), 4);
    const_assert_eq!(offset_of!(FileHeader, next_write_start), 8);
    const_assert_eq!(offset_of!(FileHeader, committed_write_start), 12);
    const_assert_eq!(offset_of!(FileHeader, last_write_end_before_wrap), 20);
    const_assert_eq!(offset_of!(FileHeader, next_read_start), 28);
    const_assert_eq!(offset_of!(FileHeader, crc32), 36);

    if size < std::mem::size_of::<FileHeader>().to_u32_lossy() {
      log::error!(
        "({name}) file size '{}' not big enough for header size '{}'",
        size,
        std::mem::size_of::<FileHeader>()
      );
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "file size not big enough for header".to_string(),
      ));
    }

    // First open or create the file.
    log::info!(
      "({name}) opening ring buffer file: {}",
      filename.as_ref().display()
    );
    let file = File::options()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(filename)
      .map_err(|e| {
        Error::AbslStatus(AbslCode::InvalidArgument, format!("cannot open file: {e}"))
      })?;

    // Make sure we can acquire an exclusive file lock. This is advisory but will at least prevent
    // basic programming errors.
    file.try_lock_exclusive().map_err(|e| {
      Error::AbslStatus(AbslCode::InvalidArgument, format!("cannot lock file: {e}"))
    })?;

    // See if the file is already the right size, otherwise truncate it.
    let created_file = if u64::from(size)
      == file
        .metadata()
        .map_err(|e| {
          Error::AbslStatus(
            AbslCode::InvalidArgument,
            format!("cannot get file size: {e}"),
          )
        })?
        .len()
    {
      log::info!("({name}) ring buffer file exists with size: {size}");
      false
    } else {
      log::info!("({name}) resizing new or wrong sized ring buffer file to size: {size}");
      file.set_len(size.into()).map_err(|e| {
        Error::AbslStatus(
          AbslCode::InvalidArgument,
          format!("cannot resize file: {e}"),
        )
      })?;
      true
    };

    // Now we can memory map the file.
    let mut memory = unsafe {
      MmapMut::map_mut(&file).map_err(|e| {
        Error::AbslStatus(AbslCode::InvalidArgument, format!("cannot map file: {e}"))
      })?
    };

    #[allow(clippy::ptr_as_ptr, clippy::cast_ptr_alignment)]
    let file_header_address = memory.as_mut_ptr() as *mut FileHeader;
    let file_header = unsafe {
      file_header_address
        .as_mut()
        .ok_or(InvariantError::Invariant)?
    };

    if created_file {
      // Fully initialize the file header for anything that needs initializing. This should not be
      // strictly necessary since the file is initialized to 0, but it's worth the sanity check.
      *file_header = FileHeader::default();
      file_header.version = FILE_HEADER_CURRENT_VERSION;
      file_header.size = size;
      file_header.crc32 = compute_header_crc_32(
        file_header.next_write_start,
        file_header.committed_write_start,
        file_header.last_write_end_before_wrap,
        file_header.next_read_start,
      );
    } else if file_header.version != FILE_HEADER_CURRENT_VERSION || file_header.size != size {
      log::error!(
        "({name}) file contains wrong version '{}' or size '{}'",
        file_header.version,
        file_header.size
      );
      return Err(Error::AbslStatus(
        AbslCode::DataLoss,
        "wrong version or size".to_string(),
      ));
    }

    // Verify that the file header indexes have not been corrupted.
    if compute_header_crc_32(
      file_header.next_write_start,
      file_header.committed_write_start,
      file_header.last_write_end_before_wrap,
      file_header.next_read_start,
    ) != file_header.crc32
    {
      log::error!("({name}) file corruption via bad header crc32");
      return Err(Error::AbslStatus(
        AbslCode::DataLoss,
        "file corruption via bad header crc32".to_string(),
      ));
    }

    // Advance past the header to get the actual record memory.
    let memory_ptr: NonNull<[u8]> = memory[std::mem::size_of::<FileHeader>() ..].into();

    Ok(Arc::new(Self {
      _memory_do_not_use: memory,
      no_consumer_reservation_condition: Condvar::default(),
      no_producer_reservation_condition: Condvar::default(),
      common_ring_buffer: CommonRingBuffer::new(
        name,
        memory_ptr,
        (&file_header.next_write_start).into(),
        (&file_header.committed_write_start).into(),
        (&file_header.last_write_end_before_wrap).into(),
        (&file_header.next_read_start).into(),
        match per_record_crc32_check {
          PerRecordCrc32Check::No => 0,
          PerRecordCrc32Check::Yes => 4,
        },
        ExtraLockedData {
          producer: None,
          consumer: None,
          crc32: SendSyncNonNull((&file_header.crc32).into()),
          block_when_reserving_into_concurrent_read,
          per_record_crc32_check,
        },
        allow_overwrite,
        stats,
        ExtraLockedData::on_total_data_loss,
        on_record_evicted_cb,
        |extra_locked_data| {
          extra_locked_data
            .consumer
            .as_ref()
            .is_some_and(|c| match c {
              ConsumerType::Consumer(c) => c.is_some(),
              ConsumerType::CursorConsumer(c) => !c.is_empty(),
            })
        },
        |extra_locked_data| {
          extra_locked_data
            .producer
            .as_ref()
            .and_then(|p| p.reservation.as_ref())
            .is_some()
        },
      ),
    }))
  }

  /// Runs `f` against the oldest record while holding the buffer lock.
  ///
  /// The closure must be non-blocking and must not await.
  pub fn peek_oldest_record<T>(&self, f: impl FnOnce(&[u8]) -> T) -> Result<Option<T>> {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    let Some(record_range) = common_ring_buffer.peek_next_read_record_range(Cursor::No)? else {
      return Ok(None);
    };

    let record_start = record_range.start as usize;
    let record_end = record_start + record_range.size as usize;
    let memory = common_ring_buffer.memory();
    if record_end > memory.len() {
      return Err(Error::AbslStatus(
        AbslCode::DataLoss,
        "corrupted record size".to_string(),
      ));
    }

    Ok(Some(f(&memory[record_start .. record_end])))
  }
}

impl RingBuffer for RingBufferImpl {
  fn register_producer(self: Arc<Self>) -> Result<Box<dyn RingBufferProducer>> {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    if common_ring_buffer.extra_locked_data.producer.is_some() {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "producer already registered".to_string(),
      ));
    }

    log::trace!("({}) registering producer", common_ring_buffer.name);
    let producer = Box::new(ProducerImpl {
      buffer: Arc::downgrade(&self),
    });
    common_ring_buffer.extra_locked_data.producer = Some(ProducerData { reservation: None });

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
      buffer: Arc::downgrade(&self),
      readable: common_ring_buffer.readable.subscribe(),
    });
    common_ring_buffer.extra_locked_data.consumer = Some(ConsumerType::Consumer(None));

    Ok(consumer)
  }

  fn register_cursor_consumer(self: Arc<Self>) -> Result<Box<dyn RingBufferCursorConsumer>> {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    if common_ring_buffer.extra_locked_data.consumer.is_some() {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "consumer already registered".to_string(),
      ));
    }

    if matches!(common_ring_buffer.allow_overwrite, AllowOverwrite::Yes) {
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "cursor consumer not allowed in non-blocking mode".to_string(),
      ));
    }

    // TODO(mattklein123): Currently if someone destroys and recreates a cursor consumer, or goes
    // back and forth between a cursor and regular consumer the cursor state is not reset and
    // incorrect results will be returned. This is not needed right now but potentially fix this
    // later and add tests for it.
    log::trace!("({}) registering cursor consumer", common_ring_buffer.name);
    let cursor_consumer = Box::new(CursorConsumerImpl {
      buffer: Arc::downgrade(&self),
      // We subscribe to the watch here to avoid having to lock the buffer just to clone out the
      // watch on read.
      readable: common_ring_buffer.readable.subscribe(),
    });
    common_ring_buffer.extra_locked_data.consumer = Some(ConsumerType::CursorConsumer(
      IntrusiveQueueWithFreeList::default(),
    ));

    Ok(cursor_consumer)
  }

  fn wait_for_drain(&self) {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    LockedData::wait_for_drain(&mut common_ring_buffer, &self.common_ring_buffer.conditions);
  }

  fn flush(&self) {}

  fn shutdown(&self) {
    let mut common_ring_buffer = self.common_ring_buffer.locked_data.lock();
    LockedData::shutdown_common(&mut common_ring_buffer, &self.common_ring_buffer.conditions);
    self.no_consumer_reservation_condition.notify_all();
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

thread_local! {
  static CRC32_HASHER: RefCell<Hasher> = RefCell::new(Hasher::new());
}

fn do_crc32(mut op: impl FnMut(&mut Hasher)) -> u32 {
  CRC32_HASHER.with(|hasher| {
    let mut hasher = hasher.borrow_mut();
    op(&mut hasher);
    let value = hasher.clone().finalize();
    hasher.reset();
    value
  })
}

// Compute the crc32 for a record.
// TODO(mattklein123): Optimally we should not be holding the common lock when doing this.
fn per_record_crc32(
  guard: &mut MutexGuard<'_, LockedData<ExtraLockedData>>,
  reservation: &Range,
  extra_space_size: usize,
) -> u32 {
  do_crc32(|hasher| {
    let start = reservation.start as usize + extra_space_size;
    hasher.update(&guard.memory()[start .. start + (reservation.size as usize - extra_space_size)]);
  })
}

// Compute the crc32 for the file header and write it.
fn write_header_crc32(guard: &mut MutexGuard<'_, LockedData<ExtraLockedData>>) {
  let next_write_start = *guard.next_write_start();
  let committed_write_start = *guard.committed_write_start();
  let last_write_end_before_wrap = *guard.last_write_end_before_wrap();
  let next_read_start = *guard.next_read_start();
  write_header_crc32_worker(
    &mut guard.extra_locked_data,
    next_write_start,
    committed_write_start,
    last_write_end_before_wrap,
    next_read_start,
  );
}

// Compute the crc32 for the file header given specific values and write it.
fn write_header_crc32_worker(
  extra_locked_data: &mut ExtraLockedData,
  next_write_start: u32,
  committed_write_start: Option<u32>,
  last_write_end_before_wrap: Option<u32>,
  next_read_start: Option<u32>,
) {
  let crc32 = compute_header_crc_32(
    next_write_start,
    committed_write_start,
    last_write_end_before_wrap,
    next_read_start,
  );
  unsafe {
    // Safety: We have exclusive access via &mut self.
    *extra_locked_data.crc32.0.as_mut() = crc32;
  }
}

// Compute the header crc32.
fn compute_header_crc_32(
  next_write_start: u32,
  committed_write_start: Option<u32>,
  last_write_end_before_wrap: Option<u32>,
  next_read_start: Option<u32>,
) -> u32 {
  do_crc32(|hasher| {
    next_write_start.hash(&mut *hasher);
    committed_write_start.hash(&mut *hasher);
    last_write_end_before_wrap.hash(&mut *hasher);
    next_read_start.hash(&mut *hasher);
  })
}

// Common start read handling for regular and cursor consumers.
fn common_consumer_start_read<'a>(
  common_ring_buffer: &'a mut MutexGuard<'_, LockedData<ExtraLockedData>>,
  conditions: &Conditions,
  block: bool,
  reservation: &mut Option<Range>,
  cursor: Cursor,
) -> Result<&'a [u8]> {
  loop {
    let result =
      LockedData::start_read(common_ring_buffer, conditions, block, reservation, cursor)?;

    if matches!(
      common_ring_buffer.extra_locked_data.per_record_crc32_check,
      PerRecordCrc32Check::Yes
    ) {
      debug_assert_eq!(result.extra_space.len(), 4);
      let data_crc32 = per_record_crc32(
        common_ring_buffer,
        reservation.as_ref().ok_or(InvariantError::Invariant)?,
        result.extra_space.len(),
      );
      let crc_equal = data_crc32
        == u32::from_ne_bytes(
          result
            .extra_space
            .try_into()
            .map_err(|_| InvariantError::Invariant)?,
        );

      // If we are not operating in cursor mode, zero out extra data (CRCs, etc.) so that this
      // record cannot be read again if we land on it again somehow due to corruption.
      if matches!(cursor, Cursor::No) {
        common_ring_buffer
          .zero_extra_data(reservation.as_ref().ok_or(InvariantError::Invariant)?.start);
      }

      if !crc_equal {
        log::warn!(
          "({}) dropping record due to corruption",
          common_ring_buffer.name
        );
        if let Some(stat) = &common_ring_buffer.stats.records_corrupted {
          stat.inc();
        }

        // What we do here depends on whether we are in cursor mode or not. If not in cursor mode
        // we need to finish the read so we advance past the record and can accept a new start
        // read reservation. If we are in cursor mode, we can simply null out the reservation as
        // it has not yet been committed to the reservation list.
        if matches!(cursor, Cursor::No) {
          LockedData::finish_read_common(
            common_ring_buffer,
            conditions,
            reservation.as_ref().ok_or(InvariantError::Invariant)?,
          )?;
          write_header_crc32(common_ring_buffer);
        }

        *reservation = None;
        continue;
      }
    }

    return Ok(result.record_data);
  }
}
