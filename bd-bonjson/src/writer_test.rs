// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::writer::Writer;
use assert_no_alloc::*;
use std::io::{Cursor, Write};

#[cfg(debug_assertions)]
#[global_allocator]
static A: AllocDisabler = AllocDisabler;

#[test]
fn writer_does_not_allocate() {
  writer_does_not_allocate_using_buff_size(1000);
  writer_does_not_allocate_using_buff_size(10000);
  // Note: This will fail. When using a Cursor, we must ensure that the buffer is large enough.
  // writer_does_not_allocate_using_buff_size(10);
}

fn writer_does_not_allocate_using_buff_size(buff_size: usize) {
  let buf = vec![0u8; buff_size];
  let mut cursor = Cursor::new(buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  assert_no_alloc(move || {
    // Test all writer methods
    writer.write_null().unwrap();
    writer.write_boolean(true).unwrap();
    writer.write_boolean(false).unwrap();
    writer.write_signed(-42).unwrap();
    writer.write_signed(0).unwrap();
    writer.write_unsigned(42).unwrap();
    writer.write_unsigned(0).unwrap();
    writer.write_float(std::f64::consts::PI).unwrap();
    writer.write_f32(2.71f32).unwrap();
    writer.write_str("hello").unwrap();
    writer.write_str("").unwrap();
    writer.write_array_begin().unwrap();
    writer.write_map_begin().unwrap();
    writer.write_container_end().unwrap();

    // Also test the std::io::Write implementation
    writer.write_all(b"test").unwrap();
    writer.flush().unwrap();
  });
}

use std::fs::OpenOptions;

#[test]
fn writer_does_not_allocate_writing_to_file() {
  use std::io::BufWriter;
  use tempfile::NamedTempFile;

  // Create a temporary file
  let temp_file = NamedTempFile::new().unwrap();
  let file = OpenOptions::new()
    .write(true)
    .open(temp_file.path())
    .unwrap();

  // Use BufWriter to avoid frequent system calls
  let buf_writer = Box::new(BufWriter::new(file));
  let mut writer = Writer { writer: buf_writer };

  assert_no_alloc(|| {
    for _ in 0 .. 100_000 {
      // Test all writer methods
      writer.write_null().unwrap();
      writer.write_boolean(true).unwrap();
      writer.write_boolean(false).unwrap();
      writer.write_signed(-42).unwrap();
      writer.write_signed(0).unwrap();
      writer.write_unsigned(42).unwrap();
      writer.write_unsigned(0).unwrap();
      writer.write_float(std::f64::consts::PI).unwrap();
      writer.write_f32(2.71f32).unwrap();
      writer.write_str("hello").unwrap();
      writer.write_str("").unwrap();
      writer.write_array_begin().unwrap();
      writer.write_map_begin().unwrap();
      writer.write_container_end().unwrap();

      // Also test the std::io::Write implementation
      writer.write_all(b"test").unwrap();
    }
    writer.flush().unwrap();
  });
}
