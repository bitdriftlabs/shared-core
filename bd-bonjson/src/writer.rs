use crate::{Error, Result, primitives};

pub struct Writer<'a> {
  writer: &'a mut dyn std::io::Write,
}

impl Writer<'_> {
  pub fn write_null(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    primitives::serialize_null(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_boolean(&mut self, v: bool) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    primitives::serialize_boolean(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_i64(&mut self, v: i64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    primitives::serialize_i64(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_u64(&mut self, v: u64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    primitives::serialize_u64(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_f64(&mut self, v: f64) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    primitives::serialize_f64(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_f32(&mut self, v: f32) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    primitives::serialize_f32(&mut buffer, v).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_str(&mut self, v: &str) -> Result<usize> {
    let mut buffer: [u8; 16] = [0; 16];
    primitives::serialize_string_header(&mut buffer, v).and_then(|header_size| {
      self.writer.write(&buffer[.. header_size]).and_then(|header_size| {
        self.writer.write(v.as_bytes())
          .and_then(|string_size| Ok(header_size + string_size))
      }).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_array_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    primitives::serialize_array_begin(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_map_begin(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    primitives::serialize_map_begin(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
    })
  }

  pub fn write_container_end(&mut self) -> Result<usize> {
    let mut buffer: [u8; 1] = [0; 1];
    primitives::serialize_container_end(&mut buffer).and_then(|size| {
      self.writer.write(&buffer[.. size]).map_err(|_| Error::Io { offset: 0 })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::io::Write;
    use assert_no_alloc::*;

    #[cfg(debug_assertions)]
    #[global_allocator]
    static A: AllocDisabler = AllocDisabler;

    #[test]
    fn writer_does_not_allocate() {
        writer_does_not_allocate_using_buff_size(1000);
        writer_does_not_allocate_using_buff_size(1);
        writer_does_not_allocate_using_buff_size(10);
    }

    fn writer_does_not_allocate_using_buff_size(buff_size:usize) {
        let mut buf = vec![0u8; buff_size];
        assert_no_alloc(|| {
            let mut cursor = Cursor::new(&mut buf[..]);
            let mut writer = Writer { writer: &mut cursor };

            // Test all writer methods
            writer.write_null().unwrap();
            writer.write_boolean(true).unwrap();
            writer.write_boolean(false).unwrap();
            writer.write_i64(-42).unwrap();
            writer.write_i64(0).unwrap();
            writer.write_u64(42).unwrap();
            writer.write_u64(0).unwrap();
            writer.write_f64(3.14159).unwrap();
            writer.write_f32(2.71f32).unwrap();
            writer.write_str("hello").unwrap();
            writer.write_str("").unwrap();
            writer.write_array_begin().unwrap();
            writer.write_map_begin().unwrap();
            writer.write_container_end().unwrap();

            // Also test the std::io::Write implementation
            writer.write(b"test").unwrap();
            writer.flush().unwrap();
        });
    }
}
