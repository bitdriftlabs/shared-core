// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./writer_test.rs"]
mod writer_test;

use std::ffi::c_void;

use crate::serialize_primitives;
use crate::serialize_primitives::{Result, SerializationError};

pub struct Writer<W: std::io::Write + Send + Sync> {
  pub(crate) writer: W,
}

impl<W: std::io::Write + Send + Sync> Writer<W> {
  #[must_use]
  pub fn into_raw(self) -> *const c_void {
    Box::into_raw(Box::new(self)) as *const _
  }

  fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize> {
    self.writer.write(bytes).map_err(|_| SerializationError::Io)
  }

  pub fn write_null(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    let size = serialize_primitives::serialize_null(&mut buffer)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_boolean(&mut self, v: bool) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    let size = serialize_primitives::serialize_boolean(&mut buffer, v)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_signed(&mut self, v: i64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    let size = serialize_primitives::serialize_i64(&mut buffer, v)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_unsigned(&mut self, v: u64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    let size = serialize_primitives::serialize_u64(&mut buffer, v)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_float(&mut self, v: f64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    let size = serialize_primitives::serialize_f64(&mut buffer, v)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_f32(&mut self, v: f32) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    let size = serialize_primitives::serialize_f32(&mut buffer, v)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_str(&mut self, v: &str) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    let header_size = serialize_primitives::serialize_string_header(&mut buffer, v)?;
    self.write_bytes(&buffer[..header_size])?;
    self.write_bytes(v.as_bytes())?;
    Ok(header_size + v.len())
  }

  pub fn write_array_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    let size = serialize_primitives::serialize_array_begin(&mut buffer)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_map_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    let size = serialize_primitives::serialize_map_begin(&mut buffer)?;
    self.write_bytes(&buffer[..size])
  }

  pub fn write_container_end(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    let size = serialize_primitives::serialize_container_end(&mut buffer)?;
    self.write_bytes(&buffer[..size])
  }
}

impl<W: std::io::Write + Send + Sync> std::io::Write for Writer<W> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.writer.write(buf)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.writer.flush()
  }
}
