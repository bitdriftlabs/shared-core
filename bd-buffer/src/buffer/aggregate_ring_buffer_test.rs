// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::RingBufferImpl;
use crate::buffer::common_ring_buffer::{AllowOverwrite, Cursor};
use crate::buffer::non_volatile_ring_buffer::{FileHeader, PerRecordCrc32Check};
use crate::buffer::test::thread_synchronizer::ThreadSynchronizer;
use crate::buffer::test::{
  cursor_read_advance,
  cursor_read_and_verify,
  cursor_read_and_verify_and_advance,
  read_and_verify,
  start_read_and_verify,
  Helper as CommonHelper,
};
use crate::buffer::{to_u32, RingBuffer, RingBufferStats, StatsHelper};
use crate::Result;
use bd_client_stats_store::Collector;
use futures::poll;
use std::sync::Arc;
use tempdir::TempDir;

struct Helper {
  volatile_size: u32,
  non_volatile_size: u32,
  allow_overwrite: AllowOverwrite,
  cursor: Cursor,
  temp_dir: TempDir,
  #[allow(clippy::struct_field_names)]
  helper: Option<CommonHelper>,
  stats: StatsHelper,
}

impl Helper {
  fn new(
    volatile_size: u32,
    non_volatile_size: u32,
    allow_overwrite: AllowOverwrite,
    cursor: Cursor,
  ) -> Self {
    let temp_dir = TempDir::new("buffer_test").unwrap();
    let stats = StatsHelper::new(&Collector::default().scope(""));
    let buffer = RingBufferImpl::new(
      "test",
      volatile_size,
      temp_dir.path().join("buffer"),
      non_volatile_size + to_u32(std::mem::size_of::<FileHeader>()),
      PerRecordCrc32Check::Yes,
      allow_overwrite,
      Arc::new(RingBufferStats::default()),
      stats.stats.clone(),
    )
    .unwrap();
    Self {
      volatile_size,
      non_volatile_size,
      allow_overwrite,
      cursor,
      temp_dir,
      helper: Some(CommonHelper::new(buffer, cursor)),
      stats,
    }
  }

  fn helper(&mut self) -> &mut CommonHelper {
    self.helper.as_mut().unwrap()
  }

  fn close(&mut self) {
    self.helper = None;
  }

  fn open(&mut self) -> Result<()> {
    self.helper = Some(CommonHelper::new(
      RingBufferImpl::new(
        "test",
        self.volatile_size,
        self.temp_dir.path().join("buffer"),
        self.non_volatile_size + to_u32(std::mem::size_of::<FileHeader>()),
        PerRecordCrc32Check::Yes,
        self.allow_overwrite,
        Arc::new(RingBufferStats::default()),
        self.stats.stats.clone(),
      )?,
      self.cursor,
    ));
    Ok(())
  }

  fn reopen(&mut self) {
    self.helper = None;
    self.open().unwrap();
  }

  fn non_volatile_sync(&mut self) -> Arc<ThreadSynchronizer> {
    self
      .helper()
      .buffer
      .as_any()
      .downcast_ref::<RingBufferImpl>()
      .unwrap()
      .non_volatile_buffer()
      .thread_synchronizer()
  }
}

// TODO(mattklein123): Port AggregateMpScRingBufferImplFailureTest from the C++ code.

// Verify that writing into a concurrent read will block until the reader clears.
#[test]
fn write_into_concurrent_reader() {
  let mut helper = Helper::new(14, 14, AllowOverwrite::Block, Cursor::No);
  let mut consumer = helper.helper().consumer.take().unwrap();
  let non_volatile_sync = helper.non_volatile_sync();
  non_volatile_sync.wait_on("block_for_concurrent_read");

  helper.helper().reserve_and_commit("aaaaaa");
  helper.stats.wait_for_total_records_written(1);
  let reserved = start_read_and_verify(consumer.as_mut(), "aaaaaa");

  let thread = std::thread::spawn(move || {
    non_volatile_sync.barrier_on("block_for_concurrent_read");
    non_volatile_sync.signal("block_for_concurrent_read");
    reserved.verify_and_finish(consumer.as_mut(), "aaaaaa");
  });

  helper.helper().reserve_and_commit("bbbbbb");
  thread.join().unwrap();

  let mut consumer = helper.helper().buffer.clone().register_consumer().unwrap();
  read_and_verify(consumer.as_mut(), "bbbbbb");
}

// Make sure that shutdown does not deadlock if there are pending writes to flush at shutdown time.
#[test]
fn shutdown_with_pending_write() {
  let mut helper = Helper::new(50, 50, AllowOverwrite::Yes, Cursor::No);
  let aggregate_sync = helper.helper().buffer.thread_synchronizer();

  aggregate_sync.wait_on("thread_func_start_read");
  aggregate_sync.wait_on("shutdown");
  // We might have already passed thread_func_start read by the time we call the initial wait_on, so
  // write one entry to allow us to move to the next loop iteration.
  helper.helper().reserve_and_commit("aaaaaa");
  aggregate_sync.barrier_on("thread_func_start_read");

  helper.helper().reserve_and_commit("aaaaaa");
  helper.helper().reserve_and_commit("bbbbbb");
  let thread = std::thread::spawn(move || {
    aggregate_sync.barrier_on("shutdown");
    aggregate_sync.signal("shutdown");
    aggregate_sync.signal("thread_func_start_read");
  });
  helper.helper().buffer.shutdown();
  thread.join().unwrap();

  // Make sure everything was flushed out.
  helper.close();
  helper.reopen();
  helper.helper().read_and_verify("aaaaaa");
  helper.helper().read_and_verify("aaaaaa");
  helper.helper().read_and_verify("bbbbbb");
}

// Verify functionality when overwrites are blocked.
#[test]
fn no_overwrite() {
  // Volatile takes 3x 2 byte records, non-volatile also takes 3x 2 byte records with padding.
  let mut helper = Helper::new(18, 30, AllowOverwrite::Block, Cursor::No);
  let non_volatile_sync = helper.non_volatile_sync();

  non_volatile_sync.wait_on("block_advance_next_read");

  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.stats.wait_for_total_records_written(3);

  // Now fill RAM again, the flush thread will be blocked waiting for space.
  helper.helper().reserve_and_commit("dd"); // 0-9
  non_volatile_sync.barrier_on("block_advance_next_read");
  non_volatile_sync.signal("block_advance_next_read");

  helper.helper().reserve_and_commit("ee"); // 10-19
  helper.helper().reserve_and_commit("ff"); // 20-29

  // Make sure everything is flushed through after we start reading.
  helper.helper().read_and_verify("aa"); // 0-9
  helper.helper().read_and_verify("bb"); // 10-19
  helper.helper().read_and_verify("cc"); // 20-29
  helper.helper().read_and_verify("dd"); // 0-9
  helper.helper().read_and_verify("ee"); // 10-19
  helper.helper().read_and_verify("ff"); // 20-29
}

// Make sure that shutdown works if there is data in RAM but no space available on disk.
#[test]
fn no_overwrite_ram_data_on_shutdown() {
  // Volatile takes 3x 2 byte records, non-volatile also takes 3x 2 byte records with padding.
  let mut helper = Helper::new(18, 30, AllowOverwrite::Block, Cursor::No);

  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.stats.wait_for_total_records_written(3);

  // Now fill RAM again.
  helper.helper().reserve_and_commit("dd"); // 0-9

  // Shutting down with data in RAM that can't be flushed should not be blocked.
}

// Basic tests for the aggregate buffer when using the cursor consumer.
#[tokio::test]
async fn cursor() {
  // Volatile takes 3x 2 byte records, non-volatile also takes 3x 2 byte records with padding.
  let mut helper = Helper::new(18, 30, AllowOverwrite::Block, Cursor::Yes);
  let mut consumer = helper.helper().cursor_consumer.take().unwrap();
  let non_volatile_sync = helper.non_volatile_sync();

  non_volatile_sync.wait_on("block_for_read");

  // Verify blocking reads for cursors.
  let read_thread = std::thread::spawn(move || {
    cursor_read_and_verify_and_advance(consumer.as_mut(), "aa");
  });

  non_volatile_sync.barrier_on("block_for_read");
  non_volatile_sync.signal("block_for_read");
  helper.helper().reserve_and_commit("aa");
  read_thread.join().unwrap();
  helper.helper().buffer.flush();

  // Reading concurrently with writing.
  helper.helper().reserve_and_commit("bb");
  helper.helper().reserve_and_commit("cc");
  helper.helper().reserve_and_commit("dd");
  helper.stats.wait_for_total_records_written(4);
  let mut consumer = helper
    .helper()
    .buffer
    .clone()
    .register_cursor_consumer()
    .unwrap();
  cursor_read_and_verify(consumer.as_mut(), "bb");
  cursor_read_and_verify(consumer.as_mut(), "cc");
  cursor_read_advance(consumer.as_mut());
  cursor_read_advance(consumer.as_mut());
  helper.helper().reserve_and_commit("ee");
  helper.stats.wait_for_total_records_written(5);
  cursor_read_and_verify(consumer.as_mut(), "dd");
  cursor_read_and_verify(consumer.as_mut(), "ee");
  cursor_read_advance(consumer.as_mut());
  cursor_read_advance(consumer.as_mut());

  // Async reads
  assert!(poll!(consumer.as_mut().read()).is_pending());
  helper.helper().reserve_and_commit("ff");
  assert_eq!(
    consumer.as_mut().read().await.unwrap(),
    &"ff".bytes().collect::<Vec<_>>()
  );

  let mut future =
    tokio_test::task::spawn(async move { consumer.as_mut().read().await.map(|_| ()) });
  assert!(future.poll().is_pending());
  helper.helper().reserve_and_commit("ff");
  future.await.unwrap();
}
