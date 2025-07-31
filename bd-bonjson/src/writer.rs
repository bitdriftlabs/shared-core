use crate::{Error, Result, primitives};

pub struct Writer<'a> {
  writer: &'a mut dyn std::io::Write,
}

impl Writer<'_> {
  fn write_null(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    primitives::serialize_null(&mut buffer)?;
    self
      .writer
      .write(&buffer[.. 1])
      .map_err(|_| Error::Io { offset: 0 })
  }

  fn write_boolean(&mut self, v: bool) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    match primitives::serialize_boolean(&mut buffer, v) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_i64(&mut self, v: i64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    match primitives::serialize_i64(&mut buffer, v) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_u64(&mut self, v: u64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    match primitives::serialize_u64(&mut buffer, v) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_f64(&mut self, v: f64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    match primitives::serialize_f64(&mut buffer, v) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_f32(&mut self, v: f32) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    match primitives::serialize_f32(&mut buffer, v) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_str(&mut self, v: &str) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    match primitives::serialize_string_header(&mut buffer, v) {
      Ok(size) => self.writer.write(&buffer[.. size]).and_then(|header_size| {
        self
          .writer
          .write(v.as_bytes())
          .and_then(|string_size| Ok(header_size + string_size))
      }),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_array_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    match primitives::serialize_array_begin(&mut buffer) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_map_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    match primitives::serialize_map_begin(&mut buffer) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }

  fn write_container_end(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    match primitives::serialize_container_end(&mut buffer) {
      Ok(size) => self.writer.write(&buffer[.. size]),
      Err(e) => Err(std::io::Error::other(e)),
    }
  }
}

impl std::io::Write for Writer<'_> {
  fn write(&mut self, buf: &[u8]) -> Result<usize> {
    self.writer.write(buf)
  }

  fn flush(&mut self) -> Result<()> {
    self.writer.flush()
  }
}
