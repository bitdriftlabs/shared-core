// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

pub mod buffer;
mod ffi;
mod ring_buffer;

pub use crate::buffer::{
  AggregateRingBuffer,
  AllowOverwrite,
  PerRecordCrc32Check,
  RingBuffer,
  RingBufferConsumer,
  RingBufferStats,
  StatsTestHelper,
};
pub use ffi::AbslCode;
pub use ring_buffer::{
  BufferEvent,
  BufferEventWithResponse,
  BuffersWithAck,
  Consumer,
  CursorConsumer,
  Manager,
  RingBuffer as Buffer,
};
use std::path::PathBuf;

pub type Producer = Box<dyn crate::buffer::RingBufferProducer>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("A absl::Status was returned: {0:?}:{1}")]
  AbslStatus(AbslCode, String),
  #[error("An error ocurred while attempting to create buffer '{0}': {1}")]
  BufferCreation(PathBuf, Box<Error>),
  #[error("Failed to convert a Path into a valid String")]
  InvalidFileName,
  #[error("Failed to start a thread: {0}")]
  ThreadStartFailure(String),
  #[error("Invariant violation")]
  Invariant,
}

type Result<T> = std::result::Result<T, Error>;
