// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use arbitrary::Arbitrary;
use bd_buffer::buffer::{
  AggregateRingBuffer,
  AllowOverwrite,
  BlockWhenReservingIntoConcurrentRead,
  NonVolatileFileHeader,
  NonVolatileRingBuffer,
  OptionalStatGetter,
  PerRecordCrc32Check,
  RingBuffer,
  RingBufferConsumer,
  RingBufferCursorConsumer,
  RingBufferProducer,
  RingBufferStats,
  StatsTestHelper,
  VolatileRingBuffer,
};
use bd_buffer::{AbslCode, Error};
use bd_client_stats_store::Collector;
use bd_log_primitives::LossyIntToU32;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tempfile::TempDir;

pub mod buffer_corruption_fuzz_test;
pub mod mpsc_buffer_fuzz_test;
pub mod spsc_buffer_fuzz_test;
pub mod versioned_kv_journal;
pub mod workflow_state;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

#[derive(Arbitrary, Debug)]
pub enum BufferType {
  Volatile,
  NonVolatile,
  Aggregate,
}

#[derive(Arbitrary, Debug)]
pub struct ReserveAndCommit {
  size: u32,
  pub producer_index: u32,
}

#[derive(Arbitrary, Debug)]
struct CorruptByte {
  offset: u32,
  new_byte: u8,
}

#[derive(Arbitrary, Debug)]
pub struct ReopenBuffer {
  corrupt_byte: Option<CorruptByte>,
}

#[derive(Arbitrary, Debug)]
pub enum CursorRead {
  Read,
  Advance,
}

#[derive(Arbitrary, Debug)]
pub enum Action {
  ReserveAndCommit(ReserveAndCommit),
  Read,
  ReopenBuffer(ReopenBuffer),
  CursorRead(CursorRead),
}

#[derive(Arbitrary, Debug)]
pub struct BufferFuzzTestCase {
  pub buffer_type: BufferType,
  buffer_size: u32,
  actions: Vec<Action>,
  pub num_producers: u32,
  pub interleaved_operations: bool,
  pub cursor_consumer: bool,
}

enum ConsumerType {
  Consumer(Box<dyn RingBufferConsumer>),
  CursorConsumer(Box<dyn RingBufferCursorConsumer>),
}

struct ProducerState {
  producer: Box<dyn RingBufferProducer>,
  write_reservation: Option<(*mut u8, usize)>,
  write_index: u32,
}

struct BufferState {
  _buffer: Arc<dyn RingBuffer>,
  producers: Vec<ProducerState>,
  consumer: ConsumerType,
  read_reservation: Option<(*const u8, usize)>,
  cursor_read_count: u32,
}

impl BufferState {
  fn new(
    test_case: &BufferFuzzTestCase,
    temp_dir: &TempDir,
    stats: Arc<RingBufferStats>,
    saw_file_header_corruption: bool,
  ) -> Option<Self> {
    let buffer = match test_case.buffer_type {
      BufferType::Volatile => {
        VolatileRingBuffer::new("test".to_string(), test_case.buffer_size, stats)
      },
      BufferType::NonVolatile => {
        // TODO(mattklein123): fuzz no overwrite in non-cursor mode.
        match NonVolatileRingBuffer::new(
          "test".to_string(),
          temp_dir.path().join("buffer"),
          test_case.buffer_size + std::mem::size_of::<NonVolatileFileHeader>().to_u32_lossy(),
          if test_case.cursor_consumer {
            AllowOverwrite::Block
          } else {
            AllowOverwrite::Yes
          },
          BlockWhenReservingIntoConcurrentRead::No,
          PerRecordCrc32Check::Yes,
          stats,
        ) {
          Ok(buffer) => buffer as Arc<dyn RingBuffer>,
          Err(e) => {
            if saw_file_header_corruption
              && (matches!(e, Error::AbslStatus(AbslCode::DataLoss, _))
                || matches!(e, Error::AbslStatus(AbslCode::InvalidArgument, ref message) if
                                                message == "wrong version or size"))
            {
              // This is expected.
              return None;
            }
            panic!("unexpected error: {e}")
          },
        }
      },
      BufferType::Aggregate => {
        // TODO(mattklein123): Have the volatile and non-volatile size be different. Also, make sure
        // that we allocate an additional 4 bytes for a per record crc32, so that a single record is
        // guaranteed to always fit in both buffers. Basically: <size volatile> == <size volatile> +
        // <sizeof file header> + <single crc32 record padding>
        match AggregateRingBuffer::new(
          "test",
          test_case.buffer_size,
          temp_dir.path().join("buffer"),
          test_case.buffer_size + std::mem::size_of::<NonVolatileFileHeader>().to_u32_lossy() + 4,
          PerRecordCrc32Check::Yes,
          if test_case.cursor_consumer {
            AllowOverwrite::Block
          } else {
            AllowOverwrite::Yes
          },
          stats.clone(),
          stats,
        ) {
          Ok(buffer) => buffer as Arc<dyn RingBuffer>,
          Err(e) => {
            if saw_file_header_corruption
              && (matches!(e, Error::AbslStatus(AbslCode::DataLoss, _))
                || matches!(e, Error::AbslStatus(AbslCode::InvalidArgument, ref message) if
                                                message == "wrong version or size"))
            {
              // This is expected.
              return None;
            }
            panic!("unexpected error: {e}")
          },
        }
      },
    };

    assert!(test_case.num_producers >= 1);
    let producers = (0 .. test_case.num_producers)
      .map(|_| ProducerState {
        producer: buffer.clone().register_producer().unwrap(),
        write_reservation: None,
        write_index: 0,
      })
      .collect();

    let consumer = if test_case.cursor_consumer {
      ConsumerType::CursorConsumer(buffer.clone().register_cursor_consumer().unwrap())
    } else {
      ConsumerType::Consumer(buffer.clone().register_consumer().unwrap())
    };

    Some(Self {
      _buffer: buffer,
      producers,
      consumer,
      read_reservation: None,
      cursor_read_count: 0,
    })
  }
}

pub struct BufferFuzzTest {
  test_case: BufferFuzzTestCase,
  temp_dir: TempDir,
  current_write_index: u32,
  last_read_value: u32,
  buffer_state: Option<BufferState>,
  stats: Arc<RingBufferStats>,
  saw_file_header_corruption: bool,
  saw_file_body_corruption: bool,
}

impl BufferFuzzTest {
  #[must_use]
  pub fn new(test_case: BufferFuzzTestCase) -> Self {
    let temp_dir = TempDir::with_prefix("buffer_fuzz").unwrap();
    let stats = StatsTestHelper::new(&Collector::default().scope(""));
    let buffer_state = BufferState::new(&test_case, &temp_dir, stats.stats.clone(), false);
    assert!(buffer_state.is_some());

    Self {
      test_case,
      temp_dir,
      buffer_state,
      current_write_index: 0,
      last_read_value: 0,
      stats: stats.stats,
      saw_file_header_corruption: false,
      saw_file_body_corruption: false,
    }
  }

  const fn state(&mut self) -> &mut BufferState {
    self.buffer_state.as_mut().unwrap()
  }

  fn action_read(&mut self) {
    let finish_read = if self.state().read_reservation.is_none() {
      if !self.start_read() {
        return;
      }
      !self.test_case.interleaved_operations
    } else {
      true
    };

    if finish_read {
      self.finish_read_and_verify();
      self.state().read_reservation = None;
    }
  }

  fn action_cursor_read(&mut self, cursor_read: &CursorRead) {
    match cursor_read {
      CursorRead::Read => {
        let reservation = match self.cursor_consumer().start_read(false) {
          Ok(reservation) => reservation,
          Err(Error::AbslStatus(AbslCode::Unavailable, _)) => return,
          Err(e) => panic!("unexpected error: {e}"),
        };
        let reservation = (reservation.as_ptr(), reservation.len());

        // TODO(mattklein123): Potentially wait to verify to look for other types of overwrite
        // issues.
        self.verify_read(reservation);
        self.state().cursor_read_count += 1;
      },
      CursorRead::Advance => {
        if self.state().cursor_read_count > 0 {
          self.cursor_consumer().advance_read_pointer().unwrap();
          self.state().cursor_read_count -= 1;
        }
      },
    }
  }

  fn action_reserve_and_commit(&mut self, reserve_and_commit: &ReserveAndCommit) {
    // Fixup producer index to always be within the number of producers.
    let producer_index = reserve_and_commit.producer_index as usize % self.state().producers.len();
    let finish_commit = if self.state().producers[producer_index]
      .write_reservation
      .is_none()
    {
      if !self.reserve(reserve_and_commit, producer_index) {
        return;
      }
      !self.test_case.interleaved_operations
    } else {
      true
    };

    if finish_commit {
      self.write_and_commit(producer_index);
      self.state().producers[producer_index].write_reservation = None;
    }
  }

  fn action_reopen_buffer(&mut self, reopen_buffer: &ReopenBuffer) {
    self.cleanup_before_close();
    self.buffer_state = None;

    if let Some(corrupt_byte) = &reopen_buffer.corrupt_byte {
      let mut file = File::open(self.temp_dir.path().join("buffer")).unwrap();
      let mut buffer = Vec::new();
      file.read_to_end(&mut buffer).unwrap();
      let offset = corrupt_byte.offset
        % (self.test_case.buffer_size
          + std::mem::size_of::<NonVolatileFileHeader>().to_u32_lossy());
      log::trace!("corrupting byte at offset {offset}");
      buffer[offset as usize] = corrupt_byte.new_byte;
      let mut file = File::create(self.temp_dir.path().join("buffer")).unwrap();
      file.write_all(&buffer).unwrap();

      if offset < std::mem::size_of::<NonVolatileFileHeader>().to_u32_lossy() {
        // In this case we should expect further reopens to fail so we can track that.
        self.saw_file_header_corruption = true;
      } else {
        self.saw_file_body_corruption = true;
      }
    }
    self.buffer_state = BufferState::new(
      &self.test_case,
      &self.temp_dir,
      self.stats.clone(),
      self.saw_file_header_corruption,
    );

    if self.buffer_state.is_none() {
      // In this case delete the file and recreate so we can keep fuzzing.
      assert!(self.saw_file_header_corruption);
      self.saw_file_header_corruption = false;
      std::fs::remove_file(self.temp_dir.path().join("buffer")).unwrap();
      self.buffer_state = BufferState::new(
        &self.test_case,
        &self.temp_dir,
        self.stats.clone(),
        self.saw_file_header_corruption,
      );
      assert!(self.buffer_state.is_some());
    }
  }

  pub fn run(&mut self, allow_corruption: bool) {
    for action in self.test_case.actions.split_off(0) {
      log::trace!("fuzz action: {action:?}");

      match action {
        Action::Read => self.action_read(),
        Action::CursorRead(cursor_read) => self.action_cursor_read(&cursor_read),
        Action::ReserveAndCommit(reserve_and_commit) => {
          self.action_reserve_and_commit(&reserve_and_commit);
        },
        Action::ReopenBuffer(reopen_buffer) => self.action_reopen_buffer(&reopen_buffer),
      }
    }

    self.cleanup_before_close();

    // Total data loss should only happen if we saw file body corruption.
    assert!(
      (allow_corruption && self.saw_file_body_corruption)
        || 0 == self.stats.total_data_loss.get_value()
    );

    // Record corruption can happen if we close after reserve but before commit.
    // TODO(mattklein123): Track that this actually happened or we saw file body corruption.
    assert!(allow_corruption || 0 == self.stats.records_corrupted.get_value());
  }

  fn cleanup_before_close(&mut self) {
    log::trace!("fuzz cleanup");
    // Flush out any pending writes and then read until the buffer is empty if using a regular
    // consumer.
    for i in 0 .. self.state().producers.len() {
      if self.state().producers[i].write_reservation.is_some() {
        log::trace!("fuzz cleanup flushing write");
        self.write_and_commit(i);
      }
    }
    match &mut self.state().consumer {
      ConsumerType::Consumer(_) => {
        if self.state().read_reservation.is_some() {
          log::trace!("fuzz cleanup flushing open read");
          self.finish_read_and_verify();
          self.state().read_reservation = None;
        }
        while self.start_read() {
          log::trace!("fuzz cleanup flushing read");
          self.finish_read_and_verify();
          self.state().read_reservation = None;
        }
      },
      ConsumerType::CursorConsumer(_) => {},
    }
  }

  fn consumer(&mut self) -> &mut dyn RingBufferConsumer {
    match &mut self.state().consumer {
      ConsumerType::Consumer(c) => c.as_mut(),
      ConsumerType::CursorConsumer(_) => unreachable!(),
    }
  }

  fn cursor_consumer(&mut self) -> &mut dyn RingBufferCursorConsumer {
    match &mut self.state().consumer {
      ConsumerType::Consumer(_) => unreachable!(),
      ConsumerType::CursorConsumer(c) => c.as_mut(),
    }
  }

  fn start_read(&mut self) -> bool {
    let reservation = match self.consumer().start_read(false) {
      Ok(reservation) => reservation,
      Err(Error::AbslStatus(AbslCode::Unavailable, _)) => return false,
      Err(e) => panic!("unexpected error: {e}"),
    };

    self.state().read_reservation = Some((reservation.as_ptr(), reservation.len()));
    true
  }

  fn finish_read_and_verify(&mut self) {
    let read_reservation = self.state().read_reservation.unwrap();
    self.verify_read(read_reservation);
    self.consumer().finish_read().unwrap();
  }

  fn verify_read(&mut self, read_reservation: (*const u8, usize)) {
    let (start, len) = read_reservation;
    let memory = unsafe { std::slice::from_raw_parts(start, len) };
    match len {
      1 => {
        assert_eq!(memory[0], b'a');
      },
      2 => {
        assert_eq!(memory[0], b'a');
        assert_eq!(memory[1], b'b');
      },
      3 => {
        assert_eq!(memory[0], b'a');
        assert_eq!(memory[1], b'b');
        assert_eq!(memory[2], b'c');
      },
      _ => {
        let write_index = u32::from_ne_bytes(memory[0 .. 4].try_into().unwrap());
        log::trace!(
          "fuzz read {} {} {}",
          write_index,
          self.current_write_index,
          self.last_read_value,
        );
        assert!(write_index < self.current_write_index);

        // TODO(mattklein123): The cursor consumer can read without advancing which causes this
        // logic to become out of sync. This is disabled for right now but should be turned
        // back on in all cases.
        if !self.test_case.cursor_consumer {
          assert!(write_index >= self.last_read_value);
        }

        self.last_read_value = write_index;

        // TODO(mattklein123): Verify remainder is 0?
      },
    }
  }

  fn reserve(&mut self, reserve_and_commit: &ReserveAndCommit, producer_index: usize) -> bool {
    // Never block when attempting to reserve.
    let reservation = match self.state().producers[producer_index]
      .producer
      .reserve(reserve_and_commit.size, false)
    {
      Ok(reservation) => reservation,
      // TODO(mattklein123): The old C++ code used to be more granular in what it would accept
      // here. See if we can do the same moving forward.
      Err(Error::AbslStatus(
        AbslCode::InvalidArgument | AbslCode::ResourceExhausted | AbslCode::Unavailable,
        _,
      )) => return false,
      Err(e) => panic!("unexpected error: {e}"),
    };

    self.state().producers[producer_index].write_reservation =
      Some((reservation.as_mut_ptr(), reservation.len()));
    self.state().producers[producer_index].write_index = self.current_write_index;
    self.current_write_index += 1;
    log::trace!(
      "fuzz reserve {} {}",
      self.current_write_index,
      self.last_read_value
    );
    true
  }

  fn write_and_commit(&mut self, producer_index: usize) {
    self.write_data(producer_index);
    self.state().producers[producer_index]
      .producer
      .commit()
      .unwrap();
  }

  fn write_data(&mut self, producer_index: usize) {
    // If size is 1-3, just write a basic pattern in. For sizes 4 and greater we write a sequence ID
    // that can be checked during read time.
    let (start, len) = self.state().producers[producer_index]
      .write_reservation
      .unwrap();
    let memory = unsafe { std::slice::from_raw_parts_mut(start, len) };
    match len {
      1 => memory[0] = b'a',
      2 => {
        memory[0] = b'a';
        memory[1] = b'b';
      },
      3 => {
        memory[0] = b'a';
        memory[1] = b'b';
        memory[2] = b'c';
      },
      _ => {
        // Write the 1st 4 bytes as the sequence, and any remainder as 0.
        memory[0 .. 4].copy_from_slice(
          &self.state().producers[producer_index]
            .write_index
            .to_ne_bytes(),
        );
        log::trace!(
          "fuzz write {} {}",
          self.current_write_index,
          self.last_read_value
        );
        if memory.len() > 4 {
          memory[5 ..].fill(0);
        }
      },
    }
  }
}

pub fn process_test_case(
  mut test_case: BufferFuzzTestCase,
  retain_action_if: impl Fn(&Action) -> bool,
  modify_reserve_and_commit: impl Fn(&mut ReserveAndCommit),
) -> BufferFuzzTestCase {
  // Clamp to a min of 1 byte and a max of 512B for the buffer size.
  test_case.buffer_size = std::cmp::max(1, test_case.buffer_size % ((512) + 1));

  test_case.actions.retain_mut(|action| {
    let mut retain = match action {
      Action::Read => !test_case.cursor_consumer,
      Action::CursorRead(_) => test_case.cursor_consumer,
      Action::ReserveAndCommit(reserve_and_commit) => {
        // Clamp to a min of 1 and a max of buffer size.
        reserve_and_commit.size = (reserve_and_commit.size % test_case.buffer_size) + 1;
        modify_reserve_and_commit(reserve_and_commit);
        true
      },
      Action::ReopenBuffer(_) => true,
    };

    if retain {
      retain = retain_action_if(action);
    }
    retain
  });

  test_case
}

pub fn run_all_corpus<T: for<'a> Arbitrary<'a>>(corpus_path: &str, fuzzer: impl Fn(T)) {
  for path in std::fs::read_dir(corpus_path).unwrap() {
    let path = path.unwrap().path();
    let mut file = std::fs::File::open(path.clone()).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    log::info!("running corpus file: {}", path.display());
    let corpus = T::arbitrary(&mut arbitrary::Unstructured::new(&buffer)).unwrap();
    fuzzer(corpus);
  }
}
