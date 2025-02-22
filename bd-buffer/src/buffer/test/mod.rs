// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod thread_synchronizer;

use crate::buffer::common_ring_buffer::Cursor;
use crate::buffer::{
  RingBuffer,
  RingBufferConsumer,
  RingBufferCursorConsumer,
  RingBufferProducer,
  to_u32,
};
use std::sync::Arc;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// StartRead
//

pub struct StartRead {
  start: *const u8,
  len: usize,
}

unsafe impl Send for StartRead {}

impl StartRead {
  pub fn verify_and_finish(self, consumer: &mut dyn RingBufferConsumer, data: &str) {
    let reserved = unsafe { std::slice::from_raw_parts(self.start, self.len) };
    assert_eq!(reserved, data.as_bytes());
    consumer.finish_read().unwrap();
  }
}

pub fn reserve_no_commit(producer: &mut dyn RingBufferProducer, data: &str) {
  let reserved = producer.reserve(to_u32(data.len()), true).unwrap();
  assert_eq!(reserved.len(), data.len());
  reserved.copy_from_slice(data.as_bytes());
}

pub fn reserve_and_commit(producer: &mut dyn RingBufferProducer, data: &str) {
  reserve_no_commit(producer, data);
  producer.commit().unwrap();
}

pub fn start_read_and_verify(consumer: &mut dyn RingBufferConsumer, data: &str) -> StartRead {
  let reserved = consumer.start_read(true).unwrap();
  assert_eq!(data.len(), reserved.len());
  assert_eq!(reserved, data.as_bytes());
  StartRead {
    start: reserved.as_ptr(),
    len: reserved.len(),
  }
}

pub fn read_and_verify(consumer: &mut dyn RingBufferConsumer, data: &str) {
  start_read_and_verify(consumer, data);
  consumer.finish_read().unwrap();
}

pub fn cursor_read_and_verify(
  consumer: &mut dyn RingBufferCursorConsumer,
  data: &str,
) -> StartRead {
  let reserved = consumer.start_read(true).unwrap();
  assert_eq!(data.len(), reserved.len());
  assert_eq!(reserved, data.as_bytes());
  StartRead {
    start: reserved.as_ptr(),
    len: reserved.len(),
  }
}

pub fn cursor_read_advance(consumer: &mut dyn RingBufferCursorConsumer) {
  consumer.advance_read_pointer().unwrap();
}

pub fn cursor_read_and_verify_and_advance(consumer: &mut dyn RingBufferCursorConsumer, data: &str) {
  cursor_read_and_verify(consumer, data);
  cursor_read_advance(consumer);
}

//
// Helper
//

pub struct Helper {
  pub buffer: Arc<dyn RingBuffer>,
  pub producer: Option<Box<dyn RingBufferProducer>>,
  pub consumer: Option<Box<dyn RingBufferConsumer>>,
  pub cursor_consumer: Option<Box<dyn RingBufferCursorConsumer>>,
}

impl Helper {
  pub fn new(buffer: Arc<dyn RingBuffer>, cursor: Cursor) -> Self {
    let producer = Some(buffer.clone().register_producer().unwrap());
    let mut consumer = None;
    let mut cursor_consumer = None;

    match cursor {
      Cursor::No => consumer = Some(buffer.clone().register_consumer().unwrap()),
      Cursor::Yes => cursor_consumer = Some(buffer.clone().register_cursor_consumer().unwrap()),
    }

    Self {
      buffer,
      producer,
      consumer,
      cursor_consumer,
    }
  }

  pub fn producer(&mut self) -> &mut dyn RingBufferProducer {
    self.producer.as_mut().unwrap().as_mut()
  }

  pub fn consumer(&mut self) -> &mut dyn RingBufferConsumer {
    self.consumer.as_mut().unwrap().as_mut()
  }

  pub fn cursor_consumer(&mut self) -> &mut dyn RingBufferCursorConsumer {
    self.cursor_consumer.as_mut().unwrap().as_mut()
  }

  pub fn reserve_and_commit(&mut self, data: &str) {
    reserve_and_commit(self.producer(), data);
  }

  pub fn start_read_and_verify(&mut self, data: &str) -> StartRead {
    start_read_and_verify(self.consumer.as_mut().unwrap().as_mut(), data)
  }

  pub fn read_and_verify(&mut self, data: &str) {
    read_and_verify(self.consumer.as_mut().unwrap().as_mut(), data);
  }

  pub fn cursor_read_and_verify(&mut self, data: &str) -> StartRead {
    cursor_read_and_verify(self.cursor_consumer(), data)
  }

  pub fn cursor_read_advance(&mut self) {
    cursor_read_advance(self.cursor_consumer());
  }

  pub fn cursor_read_and_verify_and_advance(&mut self, data: &str) {
    cursor_read_and_verify_and_advance(self.cursor_consumer(), data);
  }
}
