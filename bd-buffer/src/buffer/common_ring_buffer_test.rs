// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::Cursor;
use crate::buffer::common_ring_buffer::AllowOverwrite;
use crate::buffer::non_volatile_ring_buffer::{
  BlockWhenReservingIntoConcurrentRead,
  FileHeader,
  PerRecordCrc32Check,
};
use crate::buffer::test::thread_synchronizer::ThreadSynchronizer;
use crate::buffer::test::{Helper as CommonHelper, read_and_verify, reserve_no_commit};
use crate::buffer::{
  AggregateRingBuffer,
  NonVolatileRingBuffer,
  OptionalStatGetter,
  RingBuffer,
  RingBufferStats,
  StatsTestHelper,
  VolatileRingBuffer,
  to_u32,
};
use crate::{AbslCode, Error};
use assert_matches::assert_matches;
use bd_client_stats_store::Collector;
use parameterized::parameterized;
use std::any::Any;
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Clone, Copy)]
enum TestType {
  Volatile,
  NonVolatile,
  Aggregate,
}

const fn records_refused_stats(test_type: TestType) -> bool {
  !matches!(test_type, TestType::Aggregate)
}

fn thread_synchronizer(test_type: TestType, buffer: &dyn Any) -> Arc<ThreadSynchronizer> {
  match test_type {
    TestType::Volatile => buffer
      .downcast_ref::<VolatileRingBuffer>()
      .unwrap()
      .thread_synchronizer(),
    TestType::NonVolatile => buffer
      .downcast_ref::<NonVolatileRingBuffer>()
      .unwrap()
      .thread_synchronizer(),
    TestType::Aggregate => buffer
      .downcast_ref::<AggregateRingBuffer>()
      .unwrap()
      .non_volatile_buffer()
      .thread_synchronizer(),
  }
}

struct Helper {
  #[allow(clippy::struct_field_names)]
  helper: CommonHelper,
  _temp_dir: TempDir,
  stats: StatsTestHelper,
}

impl Helper {
  fn new(size: u32, test_type: TestType) -> Self {
    let temp_dir = TempDir::with_prefix("buffer_test").unwrap();
    let stats = StatsTestHelper::new(&Collector::default().scope(""));
    let buffer = match test_type {
      TestType::Volatile => VolatileRingBuffer::new("test".to_string(), size, stats.stats.clone())
        as Arc<dyn RingBuffer>,
      TestType::NonVolatile => NonVolatileRingBuffer::new(
        "test".to_string(),
        temp_dir.path().join("buffer"),
        size + to_u32(std::mem::size_of::<FileHeader>()),
        AllowOverwrite::Yes,
        BlockWhenReservingIntoConcurrentRead::No,
        PerRecordCrc32Check::No,
        stats.stats.clone(),
      )
      .unwrap() as Arc<dyn RingBuffer>,
      TestType::Aggregate => AggregateRingBuffer::new(
        "test",
        size,
        temp_dir.path().join("buffer"),
        size + to_u32(std::mem::size_of::<FileHeader>()),
        PerRecordCrc32Check::No,
        AllowOverwrite::Yes,
        Arc::new(RingBufferStats::default()),
        stats.stats.clone(),
      )
      .unwrap() as Arc<dyn RingBuffer>,
    };
    Self {
      helper: CommonHelper::new(buffer, Cursor::No),
      _temp_dir: temp_dir,
      stats,
    }
  }
}

impl Drop for Helper {
  fn drop(&mut self) {
    assert_eq!(self.stats.stats.total_data_loss.get_value(), 0);
    assert_eq!(self.stats.stats.records_corrupted.get_value(), 0);
  }
}

// Test basic error cases.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn errors(test_type: TestType) {
  let mut root = Helper::new(100, test_type);
  let helper = &mut root.helper;

  assert_matches!(
    helper.producer().reserve(0, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message == "reservation size invalid"
  );
  if records_refused_stats(test_type) {
    assert_eq!(root.stats.stats.records_refused.get_value(), 1);
    assert_eq!(root.stats.stats.bytes_refused.get_value(), 0);
  }

  assert_matches!(
    helper.producer().reserve(97, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message == "reservation size invalid"
  );
  if records_refused_stats(test_type) {
    assert_eq!(root.stats.stats.records_refused.get_value(), 2);
    assert_eq!(root.stats.stats.bytes_refused.get_value(), 0);
  }

  assert_matches!(
    helper.producer().reserve(u32::MAX, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message == "reservation size invalid"
  );
  if records_refused_stats(test_type) {
    assert_eq!(root.stats.stats.records_refused.get_value(), 3);
    assert_eq!(root.stats.stats.bytes_refused.get_value(), 0);
  }

  assert_matches!(
    helper.buffer.clone().register_consumer(),
    Err(Error::AbslStatus(code, _)) if code == AbslCode::FailedPrecondition
  );

  assert_matches!(
    helper.producer().commit(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message == "commit before reserve"
  );

  assert_matches!(
    helper.consumer().finish_read(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message == "finish read before starting"
  );

  helper.producer().reserve(10, true).unwrap();
  assert_matches!(
    helper.producer().reserve(10, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message == "attempt to re-reserve before commit"
  );

  helper.producer().commit().unwrap();
  helper.consumer().start_read(true).unwrap();
  assert_matches!(
    helper.consumer().start_read(true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument &&
         message == "start read without finishing previous read"
  );
  helper.consumer().finish_read().unwrap();

  assert_matches!(
    helper.consumer().start_read(false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable &&
         message == "no data to read"
  );
}

// Make sure calling shutdown multiple times works.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn multiple_shutdown(test_type: TestType) {
  let helper = Helper::new(30, test_type);

  helper.helper.buffer.shutdown();
  helper.helper.buffer.shutdown();
}

// Basic test of aligned writes and reads that are interleaved.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn aligned_interleaved(test_type: TestType) {
  let mut root = Helper::new(100, test_type);
  let helper = &mut root.helper;

  for i in 0 .. 21 {
    helper.reserve_and_commit("123456");
    helper.read_and_verify("123456");

    assert_eq!(root.stats.stats.records_written.get_value(), i + 1);
    assert_eq!(root.stats.stats.bytes_written.get_value(), 6 * (i + 1));
    assert_eq!(
      root.stats.stats.total_bytes_written.get_value(),
      (6 + 4) * (i + 1)
    ); // 4 bytes overhead.
    assert_eq!(root.stats.stats.records_read.get_value(), i + 1);
    assert_eq!(root.stats.stats.bytes_read.get_value(), 6 * (i + 1));
    assert_eq!(
      root.stats.stats.total_bytes_read.get_value(),
      (6 + 4) * (i + 1)
    ); // 4 bytes overhead.
  }
}

// Aligned double writes and double reads (not interleaved).
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn aligned_non_interleaved(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Reserve and write 10-19.
  helper.reserve_and_commit("abcdef");
  root.stats.wait_for_total_records_written(2);

  // Read 0-9.
  helper.read_and_verify("123456");

  // Read 10-19.
  helper.read_and_verify("abcdef");

  // Reserve and write 20-29.
  helper.reserve_and_commit("123456");

  // Reserve and write 0-9.
  helper.reserve_and_commit("abcdef");
  root.stats.wait_for_total_records_written(4);

  // Read 20-29.
  helper.read_and_verify("123456");

  // Read 0-9.
  helper.read_and_verify("abcdef");
}

// Non-aligned interleaved writes and reads that also show wrapping when there is not enough space.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn non_aligned_interleaved(test_type: TestType) {
  let mut helper = Helper::new(30, test_type);
  let helper = &mut helper.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Read 0-9.
  helper.read_and_verify("123456");

  // Reserve and write 0-29. This will cause a wrap to fit a contiguous region.
  helper.buffer.flush();
  let test_string_2 = "a".repeat(26);
  helper.reserve_and_commit(&test_string_2);

  // Read 0-29
  helper.read_and_verify(&test_string_2);
}

// Non-aligned and non-interleaved reads and writes that wrap.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn non_aligned_non_interleaved(test_type: TestType) {
  let mut helper = Helper::new(30, test_type);
  let helper = &mut helper.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Read 0-9.
  helper.read_and_verify("123456");

  // Reserve and write 10-29.
  let test_string_2 = "a".repeat(15);
  helper.reserve_and_commit(&test_string_2);

  // Reserve and write 0-9 since this will not fit in the remainder of the space.
  helper.buffer.flush();
  helper.reserve_and_commit("123456");

  // Read 10-29.
  helper.read_and_verify(&test_string_2);

  // Read 0-9
  helper.read_and_verify("123456");
}

// Aligned writes until overwritten, and then read.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_then_read_aligned(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("aaaaaa");

  // Reserve and write 10-19.
  helper.reserve_and_commit("bbbbbb");

  // Reserve and write 20-29.
  helper.reserve_and_commit("cccccc");

  // Reserve and write 0-9.
  root.stats.wait_for_total_records_written(3);
  helper.reserve_and_commit("dddddd");

  // Read 10-19 since this is now the oldest data.
  root.stats.wait_for_total_records_written(4);
  assert_eq!(root.stats.stats.records_overwritten.get_value(), 1);
  assert_eq!(root.stats.stats.bytes_overwritten.get_value(), 6);
  helper.read_and_verify("bbbbbb");
}

// Unaligned writes until overwritten, and then read.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_then_read_unaligned(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Reserve and write 10-29.
  let test_string_2 = "a".repeat(15);
  helper.reserve_and_commit(&test_string_2);

  // Reserve and write 0-3. This should wrap and move next write to 10-29.
  root.stats.wait_for_total_records_written(2);
  helper.reserve_and_commit("b");

  // Read 10-29.
  root.stats.wait_for_total_records_written(3);
  helper.read_and_verify(&test_string_2);

  // Read 0-3.
  helper.read_and_verify("b");
}

// Using all aligned writes, overwrite though 2 cycles to make sure the next read wraps properly.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_at_wrap_then_read_aligned(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("aaaaaa");

  // Reserve and write 10-19.
  helper.reserve_and_commit("bbbbbb");

  // Reserve and write 20-29.
  helper.reserve_and_commit("cccccc");

  // Reserve and write 0-9.
  root.stats.wait_for_total_records_written(3);
  helper.reserve_and_commit("dddddd");

  // Reserve and write 10-19.
  helper.reserve_and_commit("eeeeee");

  // Reserve and write 20-29.
  helper.reserve_and_commit("ffffff");

  // Read 0-9 since this is now the oldest data.
  root.stats.wait_for_total_records_written(6);
  helper.read_and_verify("dddddd");
}

// Using unaligned writes, overwrite though 2 cycles to make sure the next read wraps properly.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_at_wrap_then_read_unaligned(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-9.
  helper.reserve_and_commit("aaaaaa");

  // Reserve and write 10-26.
  helper.reserve_and_commit("abcdefghijkl");

  // Reserve and write 0-5.
  root.stats.wait_for_total_records_written(2);
  helper.reserve_and_commit("1");

  // Reserve and write 6-11.
  helper.reserve_and_commit("23");

  // Read 0-5 since this is now the oldest data.
  root.stats.wait_for_total_records_written(4);
  helper.read_and_verify("1");
}

// Overwrite more than 1 record and show that we advance.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_multiple_records(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-4.
  helper.reserve_and_commit("a");

  // Reserve and write 5-9.
  helper.reserve_and_commit("b");

  // Reserve and write 10-24.
  helper.reserve_and_commit(&"c".repeat(10));

  // Reserve and write 0-6.
  root.stats.wait_for_total_records_written(3);
  helper.reserve_and_commit("ddd");

  // Read 10-24 and then 0-6.
  root.stats.wait_for_total_records_written(4);
  assert_eq!(root.stats.stats.records_overwritten.get_value(), 2);
  assert_eq!(root.stats.stats.bytes_overwritten.get_value(), 2);
  helper.read_and_verify(&"c".repeat(10));
  helper.read_and_verify("ddd");
}

// Overwrite multiple records at once which leads to no more data available to read.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_multiple_records_nothing_left_to_read(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;

  // Reserve and write 0-4.
  helper.reserve_and_commit("a");

  // Reserve and write 5-9.
  helper.reserve_and_commit("b");

  // Reserve and write 10-24.
  helper.reserve_and_commit(&"c".repeat(10));

  // Reserve and write 0-29.
  root.stats.wait_for_total_records_written(3);
  helper.reserve_and_commit(&"d".repeat(15));

  // Read 0-29.
  root.stats.wait_for_total_records_written(4);
  helper.read_and_verify(&"d".repeat(15));
}

// Makes sure that we correctly overwrite any small records in the wrap gap during a wrap. This is
// the same as the "spsc_buffer_corpus/crash_1" fuzz corpus.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn overwrite_short_record_during_wrap(test_type: TestType) {
  let mut root = Helper::new(36, test_type);
  let helper = &mut root.helper;

  helper.reserve_and_commit("aaaaaaaaa"); // 0-12
  helper.reserve_and_commit("bbbbbbbbb"); // 13-25
  helper.read_and_verify("aaaaaaaaa"); // 0-12
  helper.reserve_and_commit("c"); // 26-30
  root.stats.wait_for_total_records_written(3);
  helper.reserve_and_commit("ddddddddd"); // 0-12
  root.stats.wait_for_total_records_written(4);
  helper.reserve_and_commit("eeeeeeeee"); // 13-25. Move read to 26.
  root.stats.wait_for_total_records_written(5);
  helper.reserve_and_commit("fffffffff"); // 0-12. Move read to 13.
  root.stats.wait_for_total_records_written(6);
  helper.read_and_verify("eeeeeeeee"); // 13-25
  helper.read_and_verify("fffffffff"); // 0-12
}

// Test a blocking read using 2 threads.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn blocking_read(test_type: TestType) {
  let mut helper = Helper::new(30, test_type);
  let helper = &mut helper.helper;
  let mut consumer = helper.consumer.take().unwrap();
  let thread_synchronizer = thread_synchronizer(test_type, helper.buffer.as_any());
  thread_synchronizer.wait_on("block_for_read");

  let thread = std::thread::spawn(move || {
    read_and_verify(consumer.as_mut(), "aaaaaa");
  });

  thread_synchronizer.barrier_on("block_for_read");
  thread_synchronizer.signal("block_for_read");
  helper.reserve_and_commit("aaaaaa");
  thread.join().unwrap();
}

// Test a blocking read followed by a shutdown.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn blocking_read_then_shutdown(test_type: TestType) {
  let mut helper = Helper::new(30, test_type);
  let helper = &mut helper.helper;
  let mut consumer = helper.consumer.take().unwrap();
  let thread_synchronizer = thread_synchronizer(test_type, helper.buffer.as_any());
  thread_synchronizer.wait_on("block_for_read");

  let thread = std::thread::spawn(move || {
    assert_matches!(
      consumer.as_mut().start_read(true),
      Err(Error::AbslStatus(code, message))
        if code == AbslCode::Aborted && message == "ring buffer shut down"
    );
  });

  thread_synchronizer.barrier_on("block_for_read");
  thread_synchronizer.signal("block_for_read");
  helper.buffer.shutdown();
  thread.join().unwrap();
}

// Make sure buffer_->blockingFlush()/waitForDrain() work when there is nothing in the buffer.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn flush_wait_for_drain_no_data(test_type: TestType) {
  let mut helper = Helper::new(30, test_type);
  let helper = &mut helper.helper;
  helper.buffer.flush();
  helper.buffer.wait_for_drain();
}

// Basic verification of lock(), trying to lock() multiple times, etc.
#[parameterized(test_type = {TestType::Volatile, TestType::NonVolatile, TestType::Aggregate})]
fn lock(test_type: TestType) {
  let mut root = Helper::new(30, test_type);
  let helper = &mut root.helper;
  let handle = helper.buffer.clone().lock();
  assert_matches!(
    helper.producer().reserve(10, true),
    Err(Error::AbslStatus(AbslCode::FailedPrecondition, message))
      if message == "buffer is locked"
  );

  if records_refused_stats(test_type) {
    assert_eq!(root.stats.stats.records_refused.get_value(), 1);
    assert_eq!(root.stats.stats.bytes_refused.get_value(), 10);
  }
  std::mem::drop(handle);
  helper.reserve_and_commit("hello");
  helper.read_and_verify("hello");

  // Lock twice, make sure the buffer is locked.
  let handle = helper.buffer.clone().lock();
  let handle2 = helper.buffer.clone().lock();
  assert_matches!(
    helper.producer().reserve(10, true),
    Err(Error::AbslStatus(AbslCode::FailedPrecondition, message))
      if message == "buffer is locked"
  );
  if records_refused_stats(test_type) {
    assert_eq!(root.stats.stats.records_refused.get_value(), 2);
    assert_eq!(root.stats.stats.bytes_refused.get_value(), 20);
  }

  // Unlock once, make sure the buffer is still locked.
  std::mem::drop(handle);
  assert_matches!(
    helper.producer().reserve(10, true),
    Err(Error::AbslStatus(AbslCode::FailedPrecondition, message))
      if message == "buffer is locked"
  );
  if records_refused_stats(test_type) {
    assert_eq!(root.stats.stats.records_refused.get_value(), 3);
    assert_eq!(root.stats.stats.bytes_refused.get_value(), 30);
  }

  // Fully unlock and make sure the buffer is unlocked.
  std::mem::drop(handle2);
  helper.reserve_and_commit("world");
  helper.read_and_verify("world");

  // Start a reservation and make sure we don't fully lock until the reservation is finished.
  reserve_no_commit(helper.producer(), "123456");
  let handle = helper.buffer.clone().lock();
  let mut producer = helper.producer.take().unwrap();
  let thread = std::thread::spawn(move || producer.as_mut().commit().unwrap());
  handle.await_reservations_drained();
  thread.join().unwrap();
  helper.read_and_verify("123456");
}
