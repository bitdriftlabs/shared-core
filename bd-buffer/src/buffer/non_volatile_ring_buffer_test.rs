// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{FileHeader, RingBufferImpl};
use crate::buffer::common_ring_buffer::{AllowOverwrite, Cursor};
use crate::buffer::test::{Helper as CommonHelper, reserve_and_commit};
use crate::buffer::{OptionalStatGetter, StatsTestHelper};
use crate::{AbslCode, Error, Result};
use assert_matches::assert_matches;
use bd_client_stats_store::Collector;
use bd_log_primitives::LossyIntToU32;
use fs2::FileExt;
use intrusive_collections::offset_of;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

struct Helper {
  size: u32,
  allow_overwrite: AllowOverwrite,
  cursor: Cursor,
  temp_dir: TempDir,
  buffer: Option<Arc<RingBufferImpl>>,
  #[allow(clippy::struct_field_names)]
  helper: Option<CommonHelper>,
  stats: StatsTestHelper,
}

impl Helper {
  fn new(size: u32, allow_overwrite: AllowOverwrite, cursor: Cursor) -> Self {
    let temp_dir = TempDir::with_prefix("buffer_test").unwrap();
    let stats = StatsTestHelper::new(&Collector::default().scope(""));
    let buffer = RingBufferImpl::new(
      "test".to_string(),
      temp_dir.path().join("buffer"),
      size + std::mem::size_of::<FileHeader>().to_u32_lossy(),
      allow_overwrite,
      super::BlockWhenReservingIntoConcurrentRead::No,
      super::PerRecordCrc32Check::Yes,
      stats.stats.clone(),
      |_| {},
      None::<fn()>,
    )
    .unwrap();
    Self {
      size,
      allow_overwrite,
      cursor,
      temp_dir,
      helper: Some(CommonHelper::new(buffer.clone(), cursor)),
      buffer: Some(buffer),
      stats,
    }
  }

  fn helper(&mut self) -> &mut CommonHelper {
    self.helper.as_mut().unwrap()
  }

  fn close(&mut self) {
    self.helper = None;
    self.buffer = None;
  }

  fn buffer(&self) -> &Arc<RingBufferImpl> {
    self.buffer.as_ref().unwrap()
  }

  fn file_to_vector(&self) -> Vec<u8> {
    let mut file = File::open(self.temp_dir.path().join("buffer")).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    buffer
  }

  fn vector_to_file(&self, buffer: &[u8]) {
    let mut file = File::create(self.temp_dir.path().join("buffer")).unwrap();
    file.write_all(buffer).unwrap();
  }

  fn unread_counters(&self) -> (u64, u64) {
    let buffer = self.file_to_vector();
    let unread_payload_bytes_offset = offset_of!(FileHeader, unread_payload_bytes);
    let unread_record_count_offset = offset_of!(FileHeader, unread_record_count);
    let unread_payload_bytes = u64::from_ne_bytes(
      buffer[unread_payload_bytes_offset .. unread_payload_bytes_offset + 8]
        .try_into()
        .unwrap(),
    );
    let unread_record_count = u64::from_ne_bytes(
      buffer[unread_record_count_offset .. unread_record_count_offset + 8]
        .try_into()
        .unwrap(),
    );
    (unread_payload_bytes, unread_record_count)
  }

  fn open(&mut self) -> Result<()> {
    self.open_with_reset_callback(None::<fn()>)
  }

  fn open_with_reset_callback(
    &mut self,
    on_reset: Option<impl Fn() + Send + Sync + 'static>,
  ) -> Result<()> {
    let buffer = RingBufferImpl::new(
      "test".to_string(),
      self.temp_dir.path().join("buffer"),
      self.size + std::mem::size_of::<FileHeader>().to_u32_lossy(),
      self.allow_overwrite,
      super::BlockWhenReservingIntoConcurrentRead::No,
      super::PerRecordCrc32Check::Yes,
      self.stats.stats.clone(),
      |_| {},
      on_reset,
    )?;
    self.helper = Some(CommonHelper::new(buffer.clone(), self.cursor));
    self.buffer = Some(buffer);
    Ok(())
  }

  fn reopen(&mut self) {
    self.close();
    self.open().unwrap();
  }
}

// TODO(mattklein123): Port NonVolatileSpScRingBufferImplFailureTest from the C++ code.

// Make sure cursor is only supported in blocking mode.
#[test]
fn no_cursor_non_blocking() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().consumer.take();
  assert_matches!(
    helper.helper().buffer.clone().register_cursor_consumer(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::FailedPrecondition &&
         message == "cursor consumer not allowed in non-blocking mode"
  );
}

// Verify that reopening the file reads data that was already written.
#[test]
fn reopen_file() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aaaaaa");
  helper.reopen();
  helper.helper().read_and_verify("aaaaaa");
}

#[test]
fn unread_counters_survive_restart_after_partial_consumption() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aaaaaa");
  helper.helper().reserve_and_commit("bbb");
  helper.helper().read_and_verify("aaaaaa");
  helper.close();

  assert_eq!(helper.unread_counters(), (3, 1));
  helper.open().unwrap();
  helper.helper().consumer.take();
  let locked_consumer = helper
    .buffer()
    .clone()
    .register_locked_cursor_consumer()
    .unwrap();
  assert_eq!(
    helper.buffer().locked_cursor_remaining_stats().unwrap(),
    (3, 1)
  );
  drop(locked_consumer);
}

#[test]
fn unread_counters_retire_overwritten_records() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aaaaaa");
  helper.helper().reserve_and_commit("bbbbbb");
  helper.helper().reserve_and_commit("cccccc");
  helper.helper().consumer.take();
  let locked_consumer = helper
    .buffer()
    .clone()
    .register_locked_cursor_consumer()
    .unwrap();

  assert_eq!(
    helper.buffer().locked_cursor_remaining_stats().unwrap(),
    (12, 2)
  );
  drop(locked_consumer);
}

// Verify that a correct-length existing file is physically allocated before it is memory mapped.
#[test]
fn preallocates_existing_file() {
  let temp_dir = TempDir::with_prefix("buffer_test").unwrap();
  let filename = temp_dir.path().join("buffer");
  let size = 4096_u32;
  let file = File::create(&filename).unwrap();
  file.set_len(size.into()).unwrap();
  drop(file);

  let stats = StatsTestHelper::new(&Collector::default().scope(""));
  let result = RingBufferImpl::new(
    "test".to_string(),
    &filename,
    size,
    AllowOverwrite::Yes,
    super::BlockWhenReservingIntoConcurrentRead::No,
    super::PerRecordCrc32Check::Yes,
    stats.stats,
    |_| {},
    None::<fn()>,
  );
  let Err(Error::AbslStatus(code, message)) = result else {
    panic!("expected invalid header to be rejected");
  };
  assert_eq!(code, AbslCode::DataLoss);
  assert_eq!(message, "wrong version or size");

  assert!(File::open(filename).unwrap().allocated_size().unwrap() >= u64::from(size));
}

// Make sure writing into a concurrent reader fails.
#[test]
fn write_into_concurrent_reader() {
  let mut helper = Helper::new(14, AllowOverwrite::Yes, Cursor::No);

  helper.helper().reserve_and_commit("aaaaaa");
  let reserved = helper.helper().start_read_and_verify("aaaaaa");
  assert_matches!(
    helper.helper().producer().reserve(6, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::ResourceExhausted && message == "writing into concurrent read"
  );
  reserved.verify_and_finish(helper.helper().consumer(), "aaaaaa");
  assert_eq!(helper.stats.stats.records_refused.get_value(), 1);
  assert_eq!(helper.stats.stats.bytes_refused.get_value(), 6);
}

// Make sure the file fails to open if the file header crc gets corrupted.
#[test]
fn corrupted_file_header() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aaaaaa"); // 0-13
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());

  // Corrupt the persisted unread record count.
  buffer[offset_of!(FileHeader, unread_record_count)] = 0xFF;
  helper.vector_to_file(&buffer);

  assert_matches!(
    helper.open(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::DataLoss && message == "file corruption via bad header crc32"
  );
}

// Make sure we skip a record that was corrupted.
#[test]
fn corrupted_record_contents() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());
  // Corrupt a byte in the record itself. This advances beyond the header, the 4 byte crc32 and the
  // 4 byte size.
  buffer[std::mem::size_of::<FileHeader>() + 8] = b'c';
  helper.vector_to_file(&buffer);

  helper.open().unwrap();
  // The first record is skipped due to corruption and we should be able to read the 2nd record.
  helper.helper().read_and_verify("bb");
  assert_eq!(1, helper.stats.stats.records_corrupted.get_value());
}

// Make sure we skip a record that was corrupted in cursor mode.
#[test]
fn corrupted_record_contents_cursor() {
  let mut helper = Helper::new(30, AllowOverwrite::Block, Cursor::Yes);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());
  // Corrupt a byte in the record itself. This advances beyond the header, the 4 byte crc32 and the
  // 4 byte size.
  buffer[std::mem::size_of::<FileHeader>() + 8] = b'c';
  helper.vector_to_file(&buffer);

  helper.open().unwrap();
  // The first record is skipped due to corruption and we should be able to read the 2nd record.
  helper.helper().cursor_read_and_verify("bb");
  assert_eq!(1, helper.stats.stats.records_corrupted.get_value());
}

// Corrupt a record size so that the corrupted size still points to within the buffer.
#[test]
fn corrupted_record_size_within_buffer() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());
  // Corrupt the size in the 2nd record. This will make the 2nd and 3rd record unreadable.
  let new_size: u32 = 3;
  let start = std::mem::size_of::<FileHeader>() + 14;
  buffer[start .. start + 4].copy_from_slice(&new_size.to_ne_bytes());
  helper.vector_to_file(&buffer);

  helper.open().unwrap();
  helper.helper().read_and_verify("aa");
  assert_matches!(
    helper.helper().consumer().start_read(false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "no data to read"
  );
  assert_eq!(1, helper.stats.stats.total_data_loss.get_value());
}

// A corrupted in-bounds record size can pass the range check but exceed the persisted unread
// payload total. Drop the buffer rather than retaining a stale consumer reservation.
#[test]
fn corrupted_record_size_exceeds_unread_payload_bytes() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.close();

  let mut buffer = helper.file_to_vector();
  let new_size: u32 = 12;
  let start = std::mem::size_of::<FileHeader>() + 14;
  buffer[start .. start + 4].copy_from_slice(&new_size.to_ne_bytes());
  helper.vector_to_file(&buffer);

  helper.open().unwrap();
  helper.helper().read_and_verify("aa");
  assert_matches!(
    helper.helper().consumer().start_read(false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "no data to read"
  );
  assert_eq!(1, helper.stats.stats.total_data_loss.get_value());

  helper.helper().reserve_and_commit("dd");
  helper.helper().read_and_verify("dd");
}

// Corrupt a record size so that the corrupted size points outside the buffer.
#[test]
fn corrupted_record_size_outside_buffer() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());
  // Corrupt the size in the 2nd record. This will make the 2nd and 3rd record unreadable.
  let new_size: u32 = 1000;
  let start = std::mem::size_of::<FileHeader>() + 14;
  buffer[start .. start + 4].copy_from_slice(&new_size.to_ne_bytes());
  helper.vector_to_file(&buffer);

  helper.open().unwrap();
  helper.helper().read_and_verify("aa");
  assert_matches!(
    helper.helper().consumer().start_read(false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "no data to read"
  );
  assert_eq!(1, helper.stats.stats.total_data_loss.get_value());
}

// Test the case where we have unrecoverable corruption during record read with an outstanding
// cursor. Make sure the cursor can correctly advance after the all records have been deleted.
#[test]
fn corruption_then_cursor_advance() {
  let mut helper = Helper::new(30, AllowOverwrite::Block, Cursor::Yes);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());
  // Corrupt the size in the 3rd record. This will make the 3rd record unreadable.
  let new_size: u32 = 1000;
  let start = std::mem::size_of::<FileHeader>() + 24;
  buffer[start .. start + 4].copy_from_slice(&new_size.to_ne_bytes());
  helper.vector_to_file(&buffer);

  let resets = Arc::new(AtomicUsize::new(0));
  helper
    .open_with_reset_callback(Some({
      let resets = resets.clone();
      move || {
        resets.fetch_add(1, Ordering::Relaxed);
      }
    }))
    .unwrap();
  helper.helper().cursor_read_and_verify("aa"); // 0-9
  helper.helper().cursor_read_and_verify("bb"); // 10-19
  assert_matches!(
    helper.helper().cursor_consumer().start_read(false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "pending total data loss reset"
  );
  assert_eq!(1, helper.stats.stats.total_data_loss.get_value());
  assert_eq!(resets.load(Ordering::Relaxed), 0);
  assert_matches!(
    helper.helper().producer().reserve(2, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "pending total data loss reset"
  );

  // This will clear the readers and do the full reset.
  helper.helper().cursor_read_advance();
  helper.helper().cursor_read_advance();
  assert_eq!(resets.load(Ordering::Relaxed), 1);

  // Make sure we can still add data and read it.
  helper.helper().reserve_and_commit("bb"); // 0-9
  helper.helper().cursor_read_and_verify_and_advance("bb"); // 0-9
}

// Attempt to write into a corrupted record size, causing all data to be dropped and for reads to
// continue after the next write.
#[test]
fn write_into_corrupted_record_size() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.close();

  let mut buffer = helper.file_to_vector();
  assert_eq!(buffer.len(), 30 + std::mem::size_of::<FileHeader>());
  // Corrupt the size in the 1st record. This will make the 2nd and 3rd record unreadable.
  let new_size: u32 = 1000;
  let start = std::mem::size_of::<FileHeader>() + 4;
  buffer[start .. start + 4].copy_from_slice(&new_size.to_ne_bytes());
  helper.vector_to_file(&buffer);

  helper.open().unwrap();

  assert_matches!(
    helper.helper().producer().reserve(2, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "pending total data loss reset"
  );
  assert_eq!(1, helper.stats.stats.total_data_loss.get_value());

  helper.helper().reserve_and_commit("dd"); // 0-9
  helper.helper().read_and_verify("dd");
}

// Make sure flock() is working.
#[test]
#[cfg(not(target_os = "macos"))]
fn cant_open_file_twice() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  // The errno value is platform specific, so avoid specifying it in test.
  assert_matches!(
    helper.open(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::InvalidArgument && message.starts_with("cannot lock file")
  );
}

// Verify blocking functionality if the buffer is configured to block during
// overwrites.
#[test]
fn block_on_reserve() {
  let mut helper = Helper::new(30, AllowOverwrite::Block, Cursor::No);
  let mut producer = helper.helper().producer.take().unwrap();
  let synchronizer = helper.helper().buffer.thread_synchronizer();
  synchronizer.wait_on("block_advance_next_read");

  reserve_and_commit(producer.as_mut(), &"a".repeat(22)); // 0-29
  std::mem::drop(producer);

  // This will block waiting for space.
  let cloned_buffer = helper.helper().buffer.clone();
  let thread = std::thread::spawn(|| {
    let mut producer = cloned_buffer.register_producer().unwrap();
    reserve_and_commit(producer.as_mut(), "bb"); // 0-9
  });

  // Wait for the thread to block waiting for space, then perform the read. This will allow the
  // thread to release and reserve and commit.
  synchronizer.barrier_on("block_advance_next_read");
  synchronizer.signal("block_advance_next_read");
  helper.helper().read_and_verify(&"a".repeat(22)); // 0-29
  thread.join().unwrap();
  helper.helper().read_and_verify("bb"); // 0-9

  // In this next sequence we make sure a large write that requires clearing multiple reads works.
  let mut producer = helper.helper().buffer.clone().register_producer().unwrap();
  reserve_and_commit(producer.as_mut(), "cc"); // 10-19
  reserve_and_commit(producer.as_mut(), "dd"); // 20-29
  std::mem::drop(producer);

  synchronizer.wait_on("block_advance_next_read");
  let cloned_buffer = helper.helper().buffer.clone();
  let thread = std::thread::spawn(|| {
    let mut producer = cloned_buffer.register_producer().unwrap();
    reserve_and_commit(producer.as_mut(), &"e".repeat(22)); // 0-29
  });
  synchronizer.barrier_on("block_advance_next_read");
  synchronizer.signal("block_advance_next_read");
  helper.helper().read_and_verify("cc"); // 10-19
  helper.helper().read_and_verify("dd"); // 20-29
  thread.join().unwrap();
  helper.helper().read_and_verify(&"e".repeat(22)); // 0-29
}

// Basic tests for using a cursor consumer.
#[test]
fn cursor_consumer() {
  let mut helper = Helper::new(30, AllowOverwrite::Block, Cursor::Yes);
  assert_matches!(
    helper.helper().buffer.clone().register_consumer(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::FailedPrecondition && message.starts_with("consumer already registered")
  );
  assert_matches!(
    helper.helper().cursor_consumer().advance_read_pointers(1),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::FailedPrecondition && message.starts_with("no previous read to advance")
  );

  helper.helper().reserve_and_commit("aa"); // 0-9
  helper.helper().cursor_read_and_verify_and_advance("aa"); // 0-9

  helper.helper().reserve_and_commit("bb"); // 10-19
  helper.helper().cursor_read_and_verify("bb"); // 10-19, no advance.

  // Destroy the buffer, recreate it, and make sure we get the same data back.
  helper.close();
  helper.open().unwrap();
  helper.helper().cursor_read_and_verify("bb"); // 10-19, no advance.

  // Now verify that the cursor can chase writing.
  helper.helper().reserve_and_commit("cc"); // 20-29
  helper.helper().reserve_and_commit("dd"); // 0-9
  helper.helper().cursor_read_and_verify("cc"); // 20-29
  helper.helper().cursor_read_advance(); // "bb", 10-19
  helper.helper().reserve_and_commit("ee"); // 10-19
  helper.helper().cursor_read_advance(); // "cc", 20-29
  helper.helper().cursor_read_and_verify_and_advance("dd"); // 0-9
  helper.helper().cursor_read_and_verify_and_advance("ee"); // 10-19
}

#[test]
fn peek_oldest_record_empty() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  let buffer = helper
    .helper()
    .buffer
    .as_any()
    .downcast_ref::<RingBufferImpl>()
    .unwrap();

  assert!(buffer.peek_oldest_record(<[u8]>::to_vec).unwrap().is_none());
}

#[test]
fn peek_oldest_record_does_not_consume() {
  let mut helper = Helper::new(30, AllowOverwrite::Yes, Cursor::No);
  helper.helper().reserve_and_commit("aa");
  helper.helper().reserve_and_commit("bb");

  {
    let buffer = helper
      .helper()
      .buffer
      .as_any()
      .downcast_ref::<RingBufferImpl>()
      .unwrap();
    let first_peek = buffer.peek_oldest_record(<[u8]>::to_vec).unwrap().unwrap();
    let second_peek = buffer.peek_oldest_record(<[u8]>::to_vec).unwrap().unwrap();
    assert_eq!(first_peek, b"aa");
    assert_eq!(second_peek, b"aa");
  }

  helper.helper().read_and_verify("aa");

  let third_peek = helper
    .helper()
    .buffer
    .as_any()
    .downcast_ref::<RingBufferImpl>()
    .unwrap()
    .peek_oldest_record(<[u8]>::to_vec)
    .unwrap()
    .unwrap();
  assert_eq!(third_peek, b"bb");
}
