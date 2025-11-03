// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./common_ring_buffer_test.rs"]
mod common_ring_buffer_test;

use super::RingBufferStats;
#[cfg(test)]
use super::test::thread_synchronizer::ThreadSynchronizer;
use crate::{AbslCode, Error, Result};
use bd_client_common::error::InvariantError;
use bd_log_primitives::LossyIntToU32;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::fmt::Display;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

// Performing an intersection calculation (do a pair of start and size tuples overlap).
pub const fn intersect(a: &Range, b: &Range) -> bool {
  // (StartA <= EndB) and (EndA >= StartB)
  // https://stackoverflow.com/questions/3269434/
  // whats-the-most-efficient-way-to-test-two-integer-ranges-for-overlap
  let end_a = a.start + a.size - 1;
  let end_b = b.start + b.size - 1;
  a.start <= end_b && end_a >= b.start
}

//
// Cursor
//

// 2-state enum for whether or not to use the read cursor, for readability.
#[derive(Copy, Clone)]
pub enum Cursor {
  No,
  Yes,
}

//
// AllowOverwrite
//

#[derive(Copy, Clone)]
pub enum AllowOverwrite {
  Block,
  Yes,
}

//
// Range
//

// Basic wrapper for a start index and length.
#[derive(Default, Clone)]
pub struct Range {
  pub start: u32,
  pub size: u32,
}

impl Display for Range {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{{{}-{}}}", self.start, self.start + self.size - 1)
  }
}

//
// TempReservationData
//

// Wrapper for temporary reservation data that has not yet been committed.
#[derive(Default)]
pub struct TempReservationData {
  pub range: Range,
  pub next_write_start: u32,
  pub last_write_end_before_wrap: Option<u32>,
}

//
// LockHandle
//

pub struct LockHandleImpl {
  lock_count: Arc<AtomicU32>,
}

impl Drop for LockHandleImpl {
  fn drop(&mut self) {
    debug_assert!(self.lock_count.load(Ordering::Relaxed) > 0);
    self.lock_count.fetch_sub(1, Ordering::Relaxed);
  }
}

//
// SendSyncNonNull
//

// Wrapper to allow passing NonNull as Send/Sync. This allows proper Send/Sync for the rest of
// whatever this is contained in and then we take responsibility for the use of the pointer within.
pub struct SendSyncNonNull<T: ?Sized>(pub NonNull<T>);
unsafe impl<T: ?Sized> Send for SendSyncNonNull<T> {}
unsafe impl<T: ?Sized> Sync for SendSyncNonNull<T> {}

//
// WaitForDrainData
//

struct WaitForDrainData {
  committed_write_start: Option<u32>,
}

//
// LockedData
//

// All data that must be accessed under lock within the buffer.
pub struct LockedData<ExtraLockedData> {
  memory: SendSyncNonNull<[u8]>,
  // The index where the next new write will be reserved.
  next_write_start: SendSyncNonNull<u32>,
  // The index up to which there are contiguous writes that have been committed. Essentially
  // committed_write_start trails next_write_start as producers commit data, potentially out of
  // order.
  committed_write_start: SendSyncNonNull<Option<u32>>,
  // Tracks the end of the last write before the buffer wrapped. This is used for accounting during
  // out of order commits.
  last_write_end_before_wrap: SendSyncNonNull<Option<u32>>,
  // Tracks the first available read start. If the buffer wraps, this will be pushed forward by
  // next_write_start so that next_read_start always points at the oldest data.
  next_read_start: SendSyncNonNull<Option<u32>>,
  // Tracks the first available cursor read start. Cursor reads are in-memory only, and not
  // persisted to disk for non-volatile buffers. Cursor reads can advance beyond next_read_start in
  // order to allow for multiple in flight reads before advancing the backing store.
  next_cursor_read_start: Option<u32>,

  // Tracks whether there is a caller waiting for data to be drained.
  wait_for_drain_data: Option<WaitForDrainData>,

  // Tracks whether we are waiting to fully reset after total data loss due to outstanding readers
  // or writers.
  pending_total_data_loss_reset: bool,

  lock_count: Arc<AtomicU32>,
  shutdown_lock: Option<LockHandleImpl>,

  pub stats: Arc<RingBufferStats>,
  extra_bytes_per_record: u32,
  pub allow_overwrite: AllowOverwrite,
  #[allow(clippy::struct_field_names)]
  pub extra_locked_data: ExtraLockedData,

  // Callbacks used by concrete buffers for customizing some behavior.
  on_total_data_loss_cb: Box<dyn Fn(&mut ExtraLockedData) + Send>,
  has_read_reservation_cb: Box<dyn Fn(&ExtraLockedData) -> bool + Send>,
  has_write_reservation_cb: Box<dyn Fn(&ExtraLockedData) -> bool + Send>,

  // Indicates whether the buffer is in a readable state or not, i.e. if we expect a read() call to
  // immediately return an entry.
  pub readable: tokio::sync::watch::Sender<bool>,
  // We need to keep track of one of the receivers to avoid closing the channel.
  _readable_rx: tokio::sync::watch::Receiver<bool>,

  // For debugging only.
  pub name: String,

  #[cfg(test)]
  pub thread_synchronizer: Arc<ThreadSynchronizer>,
}

impl<ExtraLockedData> LockedData<ExtraLockedData> {
  // Return whether the buffer has been shutdown or not.
  pub const fn is_shutdown(&self) -> bool {
    self.shutdown_lock.is_some()
  }

  // Add an arbitrary number of numbers, checking for overflow.
  fn overflow_add(addends: &[u32]) -> Option<u32> {
    let mut total: u32 = 0;
    for i in addends {
      total = total.checked_add(*i)?;
    }
    Some(total)
  }

  // Return the next write start value.
  pub const fn next_write_start(&mut self) -> &mut u32 {
    // Safety: We have exclusive access due to &mut self.
    unsafe { self.next_write_start.0.as_mut() }
  }

  // Return the committed write start value.
  pub const fn committed_write_start(&mut self) -> &mut Option<u32> {
    // Safety: We have exclusive access due to &mut self.
    unsafe { self.committed_write_start.0.as_mut() }
  }

  // Return the last write end before wrap value.
  pub const fn last_write_end_before_wrap(&mut self) -> &mut Option<u32> {
    // Safety: We have exclusive access due to &mut self.
    unsafe { self.last_write_end_before_wrap.0.as_mut() }
  }

  // Return the next read start value.
  pub const fn next_read_start(&mut self) -> &mut Option<u32> {
    // Safety: We have exclusive access due to &mut self.
    unsafe { self.next_read_start.0.as_mut() }
  }

  // Return the backing memory slice.
  pub const fn memory(&mut self) -> &mut [u8] {
    // Safety: We have exclusive access due to &mut self.
    unsafe { self.memory.0.as_mut() }
  }

  // Load the next read start value to use, depending on the value of use_cursor.
  fn next_read_start_to_use(&mut self, cursor: Cursor) -> &mut Option<u32> {
    match cursor {
      Cursor::No => self.next_read_start(),
      Cursor::Yes => &mut self.next_cursor_read_start,
    }
  }

  // Computes the record size offset. The record layout is as follows: |<extra space>|<record size:
  // 4 bytes>|<record data: record size bytes> Thus, returns the index to he beginning of the 2nd
  // part of the record. The extra space is always fixed and is the value stored in
  // extra_bytes_per_record_.
  fn record_size_offset(&self, start: u32) -> u32 {
    debug_assert!(self.extra_bytes_per_record >= std::mem::size_of::<u32>().to_u32_lossy());
    start + (self.extra_bytes_per_record - std::mem::size_of::<u32>().to_u32_lossy())
  }

  // Returns any extra space at the beginning of the reservation. See recordSizeOffset() for more
  // information on the record layout.
  fn extra_data(&mut self, start: u32) -> &mut [u8] {
    debug_assert!(self.extra_bytes_per_record >= std::mem::size_of::<u32>().to_u32_lossy());
    let start = start as usize;
    let extra_bytes_per_record = self.extra_bytes_per_record as usize;
    &mut self.memory()[start .. start + extra_bytes_per_record - std::mem::size_of::<u32>()]
  }

  // Helper for zeroing out the extra data section of a record.
  pub fn zero_extra_data(&mut self, start: u32) {
    self.extra_data(start).fill(0);
  }

  // Finish a reservation by writing the size and returning the memory to the caller.
  pub fn finish_reservation(
    &mut self,
    reservation: &Range,
    original_size: u32,
  ) -> Result<&mut [u8]> {
    // Move the size into the beginning of the buffer.
    let offset = self.record_size_offset(reservation.start) as usize;
    self.memory()[offset .. offset + std::mem::size_of::<u32>()]
      .copy_from_slice(&original_size.to_ne_bytes());

    // We also zero out the extra data, if any, prior to finishing the reservation. This makes sure
    // that any stale extra data does not get carried over if the record is never committed. For,
    // example, an old crc and record with a matching size from this reservation.
    self.zero_extra_data(reservation.start);

    // Here we check to see if we are about to reserve a record that ends after an existing last
    // write before wrap value. If the end of this record is > than an existing value, the existing
    // value must be invalid and should be reset.
    if self.last_write_end_before_wrap().is_some()
      && reservation.start + original_size + self.extra_bytes_per_record - 1
        > self
          .last_write_end_before_wrap()
          .ok_or(InvariantError::Invariant)?
    {
      log::trace!(
        "({}) removing previous last write before wrap: {}",
        self.name.clone(),
        self
          .last_write_end_before_wrap()
          .ok_or(InvariantError::Invariant)?
      );
      *self.last_write_end_before_wrap() = None;
    }

    // Reduce the size to what the caller expects to remove the size prefix.
    log::trace!("({}) reserving {}", self.name, reservation);
    let data_start = (reservation.start + self.extra_bytes_per_record) as usize;
    Ok(&mut self.memory()[data_start .. data_start + original_size as usize])
  }

  // Load the next read size based on the position of next_read_start_ or next_cursor_read_start_,
  // depending on the value of use_cursor.
  fn load_next_read_size(&mut self, cursor: Cursor) -> Result<u32> {
    let next_read_start_to_use = self
      .next_read_start_to_use(cursor)
      .ok_or(InvariantError::Invariant)?;

    // This checks that we are attempting to read the record length from within the buffer address
    // space. This can overflow but as long as we are within the memory space crc checks will catch
    // further corruption.
    let size_index = self.record_size_offset(next_read_start_to_use);
    if size_index + std::mem::size_of::<u32>().to_u32_lossy() > self.memory.0.len().to_u32_lossy() {
      return Err(Error::AbslStatus(
        AbslCode::DataLoss,
        "corrupted record size index".to_string(),
      ));
    }

    let size_index_as_usize = size_index as usize;
    let size = u32::from_ne_bytes(
      self.memory()[size_index_as_usize .. size_index_as_usize + 4]
        .try_into()
        .map_err(|_| InvariantError::Invariant)?,
    );
    // The following makes sure that the next read size is actually within the buffer memory. If
    // it's not there is guaranteed corruption. If it is, non-volatile crc checks will catch an
    // individual corrupted record. This can overflow but as long as we are within the memory
    // space crc checks will catch further corruption.
    if size == 0
      || Self::overflow_add(&[next_read_start_to_use, self.extra_bytes_per_record, size])
        .is_none_or(|next_read_start_index| {
          next_read_start_index > self.memory.0.len().to_u32_lossy()
        })
    {
      return Err(Error::AbslStatus(
        AbslCode::DataLoss,
        "corrupted record size".to_string(),
      ));
    }
    Ok(size)
  }

  // Checks to see if the wrap gap (the space after last_write_end_before_wrap) in this reservation
  // (if applicable) intersects with another range.
  fn intersect_in_wrap_gap(
    &self,
    reservation_data: &TempReservationData,
    range: &Range,
  ) -> Result<bool> {
    if reservation_data.last_write_end_before_wrap.is_none() {
      return Ok(false);
    }
    let wrap_gap = Range {
      start: reservation_data
        .last_write_end_before_wrap
        .ok_or(InvariantError::Invariant)?
        + 1,
      size: self.memory.0.len().to_u32_lossy()
        - (reservation_data
          .last_write_end_before_wrap
          .ok_or(InvariantError::Invariant)?
          + 1),
    };
    Ok(intersect(&wrap_gap, range))
  }

  // Checks to see if a reservation's range or its wrap gap intersects with another range.
  pub fn intersect_reservation(
    &self,
    reservation_data: &TempReservationData,
    range: &Range,
  ) -> Result<bool> {
    Ok(
      intersect(range, &reservation_data.range)
        || self.intersect_in_wrap_gap(reservation_data, range)?,
    )
  }

  // If we are about to write into already written data (consumer is not consuming), we need to
  // advance read to the following record. We may need to advance multiple records and might run out
  // of things to read depending on record sizes.
  //
  // This function can block if the overwrite mode for the buffer is to block if there is no space.
  // If 'block' is set to false this function will error instead of blocking.
  //
  // This function can return an aborted error if advancing was blocked due to no overwrites and the
  // buffer was shutdown.
  pub fn advance_next_read_due_to_write(
    guard: &mut MutexGuard<'_, Self>,
    conditions: &Conditions,
    reservation_data: &TempReservationData,
    block: bool,
  ) -> Result<()> {
    while guard.next_read_start().is_some() {
      let next_read_size = match guard.load_next_read_size(Cursor::No) {
        Ok(next_read_size) => next_read_size,
        Err(e) => {
          debug_assert!(matches!(e, Error::AbslStatus(AbslCode::DataLoss, _)));
          guard.on_total_data_loss();
          // TODO(mattklein123): Technically we can retry this write if we did the total data loss
          // reset.
          return Err(guard.return_record_refused(
            reservation_data.range.size,
            AbslCode::Unavailable,
            "pending total data loss reset",
          ));
        },
      };
      let next_read_actual_size = next_read_size + guard.extra_bytes_per_record;

      let mut overwrite_record: bool = false;
      let next_read = Range {
        start: guard.next_read_start().ok_or(InvariantError::Invariant)?,
        size: next_read_actual_size,
      };

      // If we are wrapping, we need to advance through any records in the extra space, since we
      // are effectively overwriting them. Thus, this intersection check looks at whether the next
      // record to read is in the gap space between the last write end before wrap (the last
      // In the second case we have a direct intersection so we overwrite.
      if guard.intersect_reservation(reservation_data, &next_read)? {
        overwrite_record = true;
      }

      if overwrite_record {
        if matches!(guard.allow_overwrite, AllowOverwrite::Block) {
          if !block {
            return Err(Error::AbslStatus(
              AbslCode::Unavailable,
              "not blocking for read completion".to_string(),
            ));
          }

          log::trace!(
            "({}) blocking read advance until read completion",
            guard.name
          );

          #[cfg(test)]
          guard
            .thread_synchronizer
            .sync_point("block_advance_next_read");

          // TODO(mattklein123): This sequence is ugly, potentially refactor to use wait_while().
          if guard.shutdown_lock.is_none() {
            conditions.next_read_complete.wait(guard);
          }
          if guard.shutdown_lock.is_some() {
            return Err(Error::AbslStatus(
              AbslCode::Aborted,
              "ring buffer shut down".to_string(),
            ));
          }

          continue;
        }

        log::trace!("({}) overwriting {}", guard.name, next_read);
        if let Some(stat) = &guard.stats.records_overwritten {
          stat.inc();
        }
        if let Some(stat) = &guard.stats.bytes_overwritten {
          stat.inc_by(next_read_size.into());
        }
        // When overwriting we zero out any extra data, to make sure that CRCs, etc. become so the
        // overwritten record is skipped correctly if corruption lands us in it somehow.
        let next_read_start = guard.next_read_start().ok_or(InvariantError::Invariant)?;
        guard.zero_extra_data(next_read_start);
        guard.advance_next_read(next_read_actual_size, Cursor::No)?;
      } else {
        break;
      }
    }

    Ok(())
  }

  // Calculate the next write start position based on the write size, a current write start
  // position, and temporary reservation data.
  pub fn calculate_next_write_start(&mut self, write_size: u32) -> Result<TempReservationData> {
    let mut reservation_data = TempReservationData::default();
    let next_write_start = *self.next_write_start();

    if next_write_start + write_size <= self.memory.0.len().to_u32_lossy() {
      // It fits in the remainder without wrapping.
      reservation_data.range.start = next_write_start;
      reservation_data.next_write_start = next_write_start + write_size;
    } else {
      // Move to the beginning.
      reservation_data.last_write_end_before_wrap = Some(next_write_start - 1);
      log::trace!(
        "({}) wrapping: last write end before wrap: {}",
        self.name,
        reservation_data
          .last_write_end_before_wrap
          .as_ref()
          .ok_or(InvariantError::Invariant)?
      );
      reservation_data.range.start = 0;
      reservation_data.next_write_start = write_size;
    }
    reservation_data.range.size = write_size;
    Ok(reservation_data)
  }

  // Common handling for starting a write reservation. Returns the reserved index or an error.
  pub fn start_reserve_common(&self, size: u32) -> Result<u32> {
    if self.lock_count.load(Ordering::Relaxed) > 0 {
      if let Some(stat) = &self.stats.bytes_refused {
        stat.inc_by(size.into());
      }
      if let Some(stat) = &self.stats.records_refused {
        stat.inc();
      }
      return Err(Error::AbslStatus(
        AbslCode::FailedPrecondition,
        "buffer is locked".to_string(),
      ));
    }

    if self.pending_total_data_loss_reset {
      return Err(self.return_record_refused(
        size,
        AbslCode::Unavailable,
        "pending total data loss reset",
      ));
    }

    // We need space for the size prefix.
    let Some(actual_size) = Self::overflow_add(&[size, self.extra_bytes_per_record]) else {
      log::trace!("({}) invalid reservation size: {}", self.name, size);
      // Note that we don't record the bytes lost in this case, since a very large number is likely
      // to cause overflow.
      if let Some(stat) = &self.stats.records_refused {
        stat.inc();
      }
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "reservation size invalid".to_string(),
      ));
    };

    // If size is 0 or > then the ring buffer size we can't do anything.
    if size == 0 || actual_size > self.memory.0.len().to_u32_lossy() {
      log::trace!("({}) invalid reservation size: {}", self.name, size);
      // Note that we don't record the bytes lost in this case, since a very large number is likely
      // to cause overflow.
      if let Some(stat) = &self.stats.records_refused {
        stat.inc();
      }
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "reservation size invalid".to_string(),
      ));
    }

    Ok(actual_size)
  }

  // Common handling for finishing a write commit. Returns the extra space, if any.
  pub fn finish_commit_common(&mut self, reservation: &Range) -> Result<&mut [u8]> {
    log::trace!("({}) committing {}", self.name, reservation);

    // TODO(snowp): Consider moving this deeper into the ring buffer where we notify the condvars
    // if calling it repeatedly on every log ends up being expensive.
    self
      .readable
      .send(true)
      .map_err(|_| InvariantError::Invariant)?;

    // Return the extra space at the beginning (not counting the size) if there is any.
    Ok(self.extra_data(reservation.start))
  }

  // Advance committed write start and initialize next read start if applicable.
  pub fn advance_next_commited_write_start(
    &mut self,
    conditions: &Conditions,
    reservation: &Range,
  ) -> Result<()> {
    // The new committed write start is the start of this commit.
    *self.committed_write_start() = Some(reservation.start);
    // If there is no pending read, set pending read to this commit.
    let mut new_data_to_read = false;
    if self.next_read_start().is_none() {
      new_data_to_read = true;
      *self.next_read_start() = *self.committed_write_start();
      log::trace!(
        "({}) initializing next read start: {}",
        self.name.clone(),
        self.next_read_start().ok_or(InvariantError::Invariant)?
      );
    }
    if self.next_cursor_read_start.is_none() {
      new_data_to_read = true;
      self.next_cursor_read_start = *self.committed_write_start();
      log::trace!(
        "({}) initializing next cursor read start: {}",
        self.name,
        self
          .next_cursor_read_start
          .ok_or(InvariantError::Invariant)?
      );
    }

    log::trace!(
      "({}) new committed write start: {}",
      self.name.clone(),
      self
        .committed_write_start()
        .ok_or(InvariantError::Invariant)?
    );

    if new_data_to_read {
      conditions.data_available.notify_all();
    }

    if let Some(stat) = &self.stats.total_bytes_written {
      stat.inc_by(reservation.size.into());
    }
    if let Some(stat) = &self.stats.records_written {
      stat.inc();
    }
    if let Some(stat) = &self.stats.bytes_written {
      stat.inc_by((reservation.size - self.extra_bytes_per_record).into());
    }
    Ok(())
  }

  // Advance the next read index.
  fn advance_next_read(&mut self, size: u32, cursor: Cursor) -> Result<()> {
    // If read start is equal to committed write start, the consumer has read all writes, thus there
    // is nothing to do. Otherwise, advance to the next read (which may include a wrap). This needs
    // to take into account any gaps. Read gaps should be exact, otherwise another write would have
    // overwritten them.
    if self
      .next_read_start_to_use(cursor)
      .ok_or(InvariantError::Invariant)?
      != self
        .committed_write_start()
        .ok_or(InvariantError::Invariant)?
    {
      if self.last_write_end_before_wrap().is_some()
        && self
          .last_write_end_before_wrap()
          .ok_or(InvariantError::Invariant)?
          == self
            .next_read_start_to_use(cursor)
            .ok_or(InvariantError::Invariant)?
            + size
            - 1
      {
        if matches!(cursor, Cursor::No) {
          *self.last_write_end_before_wrap() = None;
        }
        *self.next_read_start_to_use(cursor) = Some(0);
      } else {
        *self.next_read_start_to_use(cursor) = Some(
          self
            .next_read_start_to_use(cursor)
            .ok_or(InvariantError::Invariant)?
            + size,
        );
      }
      log::trace!(
        "({}) advance next read start (cursor={}): {}",
        self.name.clone(),
        matches!(cursor, Cursor::Yes),
        self
          .next_read_start_to_use(cursor)
          .ok_or(InvariantError::Invariant)?
      );

      return Ok(());
    }
    log::trace!(
      "({}) advance next read start (cursor={}): no further data to read",
      self.name,
      matches!(cursor, Cursor::Yes)
    );
    *self.next_read_start_to_use(cursor) = None;
    if matches!(cursor, Cursor::No) {
      *self.committed_write_start() = None;
    }
    Ok(())
  }

  // Common handling for finishing a read.
  pub fn finish_read_common(
    guard: &mut MutexGuard<'_, Self>,
    conditions: &Conditions,
    reservation: &Range,
  ) -> Result<()> {
    guard.advance_next_read(reservation.size, Cursor::No)?;
    conditions.next_read_complete.notify_all();

    let record_size = reservation.size - guard.extra_bytes_per_record;
    if let Some(stat) = &guard.stats.total_bytes_read {
      stat.inc_by(reservation.size.into());
    }
    if let Some(stat) = &guard.stats.records_read {
      stat.inc();
    }
    if let Some(stat) = &guard.stats.bytes_read {
      stat.inc_by(record_size.into());
    }
    log::trace!("({}) finish read {}", guard.name, reservation);

    // TODO(mattklein123): Handle the case in which the target committed write start gets
    // overwritten.
    if let Some(wait_for_drain_data) = &guard.wait_for_drain_data
      && (wait_for_drain_data.committed_write_start.is_none()
        || wait_for_drain_data
          .committed_write_start
          .ok_or(InvariantError::Invariant)?
          == reservation.start)
    {
      conditions.drain_complete.notify_all();
      guard.wait_for_drain_data = None;
    }

    guard.maybe_do_total_data_loss_reset();
    Ok(())
  }

  // Finish a total data loss reset if there are no outstanding readers/writers.
  pub fn maybe_do_total_data_loss_reset(&mut self) {
    if !self.pending_total_data_loss_reset {
      return;
    }

    if (self.has_read_reservation_cb)(&self.extra_locked_data)
      || (self.has_write_reservation_cb)(&self.extra_locked_data)
    {
      return;
    }

    self.pending_total_data_loss_reset = false;
    self.total_data_loss_reset();
  }

  // Return an and increment appropriate stats if we are refusing a record.
  pub fn return_record_refused(&self, size: u32, code: AbslCode, message: &str) -> Error {
    if let Some(stat) = &self.stats.records_refused {
      stat.inc();
    }
    if let Some(stat) = &self.stats.bytes_refused {
      stat.inc_by(size.into());
    }
    Error::AbslStatus(code, message.to_string())
  }

  // Do a total data loss full reset.
  fn total_data_loss_reset(&mut self) {
    log::trace!("({}) performing total data loss reset", self.name);
    *self.next_write_start() = 0;
    *self.committed_write_start() = None;
    *self.last_write_end_before_wrap() = None;
    *self.next_read_start() = None;
    self.next_cursor_read_start = None;

    // In the case of total data loss we zero out memory. There are extreme edge cases in which
    // corruption after a total data loss event (2+ corruptions) can land us back on a valid
    // previously written record which we should not read again.
    self.memory().fill(0);
    (self.on_total_data_loss_cb)(&mut self.extra_locked_data);
  }

  // Common handling for total data loss.
  fn on_total_data_loss(&mut self) {
    if let Some(stat) = &self.stats.total_data_loss {
      stat.inc();
    }
    log::warn!(
      "({}) likely corruption loading next read size. Dropping all records",
      self.name
    );
    if (self.has_read_reservation_cb)(&self.extra_locked_data)
      || (self.has_write_reservation_cb)(&self.extra_locked_data)
    {
      log::trace!(
        "({}) deferring total data loss reset due to active readers/writers",
        self.name
      );
      self.pending_total_data_loss_reset = true;
    } else {
      self.total_data_loss_reset();
    }
  }

  // Common implementation for the consumer startRead() API. On return will fill reserved_read with
  // a reservation if in blocking mode.
  pub fn start_read<'a>(
    guard: &mut MutexGuard<'_, Self>,
    conditions: &Conditions,
    block: bool,
    reserved_read: &mut Option<Range>,
    cursor: Cursor,
  ) -> Result<StartReadReturn<'a>> {
    if reserved_read.is_some() {
      return Err(Error::AbslStatus(
        AbslCode::InvalidArgument,
        "start read without finishing previous read".to_string(),
      ));
    }

    // The following loop is used to handle the case in which reading the next read size is
    // impossible due to corruption. When we loop back around we will either block or not depending
    // on what the caller requested.
    let next_read_size = loop {
      // If we can't reset due to active reader/writer, the only thing we do at this point is
      // become unavailable until readers/writers clear.
      if guard.pending_total_data_loss_reset {
        return Err(Error::AbslStatus(
          AbslCode::Unavailable,
          "pending total data loss reset".to_string(),
        ));
      }

      if guard.next_read_start_to_use(cursor).is_none() {
        if block {
          #[cfg(test)]
          guard.thread_synchronizer.sync_point("block_for_read");

          log::trace!(
            "({}) blocking for data available to read (cursor={})",
            guard.name,
            matches!(cursor, Cursor::Yes)
          );

          conditions.data_available.wait_while(guard, |guard| {
            guard.next_read_start_to_use(cursor).is_none() && guard.shutdown_lock.is_none()
          });
          if guard.shutdown_lock.is_some() {
            return Err(Error::AbslStatus(
              AbslCode::Aborted,
              "ring buffer shut down".to_string(),
            ));
          }
        } else {
          log::trace!("({}) non-blocking no more data to read", guard.name);

          guard
            .readable
            .send(false)
            .map_err(|_| InvariantError::Invariant)?;

          return Err(Error::AbslStatus(
            AbslCode::Unavailable,
            "no data to read".to_string(),
          ));
        }
      }

      match guard.load_next_read_size(cursor) {
        Ok(next_read_size) => break next_read_size,
        Err(e) => {
          debug_assert!(matches!(e, Error::AbslStatus(AbslCode::DataLoss, _)));
          // In this case we are fully corrupted, so reset the next read index to nothing to read.
          guard.on_total_data_loss();
        },
      }
    };

    *reserved_read = Some(Range {
      start: guard
        .next_read_start_to_use(cursor)
        .ok_or(InvariantError::Invariant)?,
      size: next_read_size + guard.extra_bytes_per_record,
    });
    let reserved_read = reserved_read.as_ref().ok_or(InvariantError::Invariant)?;
    log::trace!(
      "({}) start read (cursor={}): {}",
      guard.name,
      matches!(cursor, Cursor::Yes),
      reserved_read
    );

    // In general we advance next read on read completion. The one exception to this is if we are
    // doing a cursor read, in which case we must advance the cursor now so that there can be
    // further cursor reads.
    if matches!(cursor, Cursor::Yes) {
      guard.advance_next_read(reserved_read.size, cursor)?;
    }

    // Return the fixed up data.
    // Safety: This is safe insofar as we are theoretically not handing out overlapping ranges
    // at the same time. The transmute drops the shared mutability check and propagates the
    // lifetime to the caller along with the other reference.
    let record_start_usize = (reserved_read.start + guard.extra_bytes_per_record) as usize;
    Ok(StartReadReturn {
      extra_space: unsafe {
        std::mem::transmute::<&mut [u8], &'a mut [u8]>(guard.extra_data(reserved_read.start))
      },
      record_data: unsafe {
        std::mem::transmute::<&mut [u8], &'a mut [u8]>(
          &mut guard.memory()[record_start_usize .. record_start_usize + next_read_size as usize],
        )
      },
    })
  }

  // Common handling for the waitForDrain() functionality.
  pub fn wait_for_drain(guard: &mut MutexGuard<'_, Self>, conditions: &Conditions) {
    if (guard.has_read_reservation_cb)(&guard.extra_locked_data)
      || guard.next_read_start().is_some()
    {
      log::trace!(
        "({}) waiting for drain: {}",
        guard.name.clone(),
        guard
          .committed_write_start()
          .map_or("next read complete".to_string(), |c| c.to_string())
      );
      guard.wait_for_drain_data = Some(WaitForDrainData {
        committed_write_start: *guard.committed_write_start(),
      });
      conditions.drain_complete.wait(guard);
    }
  }

  // Common handling for the ring buffer shutdown() function.
  pub fn shutdown_common(guard: &mut MutexGuard<'_, Self>, conditions: &Conditions) {
    if guard.shutdown_lock.is_some() {
      return;
    }

    guard.shutdown_lock = Some(guard.lock_common());
    conditions.data_available.notify_all();
    conditions.next_read_complete.notify_all();
  }

  // Common handling for lock().
  pub fn lock_common(&self) -> LockHandleImpl {
    self.lock_count.fetch_add(1, Ordering::Relaxed);
    log::debug!(
      "({}) locking buffer (count={})",
      self.name,
      self.lock_count.load(Ordering::Relaxed)
    );
    LockHandleImpl {
      lock_count: self.lock_count.clone(),
    }
  }
}

//
// StartReadReturn
//

// Return data for the start_read() call.
pub struct StartReadReturn<'a> {
  pub extra_space: &'a [u8],
  pub record_data: &'a [u8],
}

//
// Conditions
//

#[derive(Default)]
pub struct Conditions {
  data_available: Condvar,
  next_read_complete: Condvar,
  drain_complete: Condvar,
}

//
// CommonRingBuffer
//

pub struct CommonRingBuffer<ExtraLockedData> {
  pub locked_data: Mutex<LockedData<ExtraLockedData>>,
  pub conditions: Conditions,
}

impl<ExtraLockedData> CommonRingBuffer<ExtraLockedData> {
  // Create a new common ring buffer.
  pub fn new(
    name: String,
    memory: NonNull<[u8]>,
    next_write_start: NonNull<u32>,
    committed_write_start: NonNull<Option<u32>>,
    last_write_end_before_wrap: NonNull<Option<u32>>,
    next_read_start: NonNull<Option<u32>>,
    extra_bytes_per_record: u32,
    extra_locked_data: ExtraLockedData,
    allow_overwrite: AllowOverwrite,
    stats: Arc<RingBufferStats>,
    on_total_data_loss_cb: impl Fn(&mut ExtraLockedData) + Send + 'static,
    has_read_reservation_cb: impl Fn(&ExtraLockedData) -> bool + Send + 'static,
    has_write_reservation_cb: impl Fn(&ExtraLockedData) -> bool + Send + 'static,
  ) -> Self {
    // Initialize the channel to true on startup. If we end up reading from the buffer when it's
    // not ready the async read calls will still wait for the read to be available, and starting
    // with reads enabled allows us to make progress if the buffer is full on startup and unable
    // to receive new logs (which is how we normally adjust the readable flag).
    let (readable, readable_rx) = tokio::sync::watch::channel(true);

    Self {
      locked_data: Mutex::new(LockedData {
        memory: SendSyncNonNull(memory),
        next_write_start: SendSyncNonNull(next_write_start),
        committed_write_start: SendSyncNonNull(committed_write_start),
        last_write_end_before_wrap: SendSyncNonNull(last_write_end_before_wrap),
        next_read_start: SendSyncNonNull(next_read_start),
        next_cursor_read_start: *unsafe { next_read_start.as_ref() },
        lock_count: AtomicU32::default().into(),
        shutdown_lock: None,
        stats,
        extra_bytes_per_record: extra_bytes_per_record + std::mem::size_of::<u32>().to_u32_lossy(),
        extra_locked_data,
        allow_overwrite,
        on_total_data_loss_cb: Box::new(on_total_data_loss_cb),
        has_read_reservation_cb: Box::new(has_read_reservation_cb),
        has_write_reservation_cb: Box::new(has_write_reservation_cb),
        wait_for_drain_data: None,
        pending_total_data_loss_reset: false,
        readable,
        _readable_rx: readable_rx,
        name,

        #[cfg(test)]
        thread_synchronizer: ThreadSynchronizer::default().into(),
      }),
      conditions: Conditions::default(),
    }
  }
}
