#[cfg(test)]
#[path = "./writer_test.rs"]
mod writer_test;

use crate::serialize_primitives;
use crate::serialize_primitives::{SerializationError, Result};

pub struct Writer<'a> {
  pub(crate) writer: &'a mut dyn std::io::Write,
}

impl Writer<'_> {
  pub fn write_null(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    serialize_primitives::serialize_null(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_boolean(&mut self, v: bool) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    serialize_primitives::serialize_boolean(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_integer(&mut self, v: i64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    serialize_primitives::serialize_i64(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_unsigned(&mut self, v: u64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    serialize_primitives::serialize_u64(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_float(&mut self, v: f64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    serialize_primitives::serialize_f64(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_f32(&mut self, v: f32) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    serialize_primitives::serialize_f32(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_str(&mut self, v: &str) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    serialize_primitives::serialize_string_header(&mut buffer, v).and_then(|header_size| {
      self.writer.write(&buffer[.. header_size]).and_then(|header_size| {
        self.writer.write(v.as_bytes())
          .and_then(|string_size| Ok(header_size + string_size))
      }).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_array_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    serialize_primitives::serialize_array_begin(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_map_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    serialize_primitives::serialize_map_begin(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }

  pub fn write_container_end(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    serialize_primitives::serialize_container_end(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| SerializationError::Io)
    })
  }
}

impl std::io::Write for Writer<'_> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.writer.write(buf)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.writer.flush()
  }
}
