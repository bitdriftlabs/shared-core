// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::{ffi::c_void, fs::File};
use std::fs::OpenOptions;
use std::io;
use std::ptr::null_mut;
use crate::writer::Writer;
use std::io::{BufWriter};
use libc;

pub type WriterHandle = *mut *const c_void;
pub type WriterBufWriterFile = Writer<BufWriter<File>>;

pub fn new_writer(path: &str) -> io::Result<WriterBufWriterFile> {
  let file = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)?;
  Ok(Writer { writer: BufWriter::new(file) })
}

impl TryFrom<WriterHandle> for &mut WriterBufWriterFile {
  type Error = ();

  /// # Safety
  /// This function dereferences a raw pointer and must be called with a valid handle
  #[allow(clippy::not_unsafe_ptr_arg_deref)]
  fn try_from(value: WriterHandle) -> Result<Self, Self::Error> {
    unsafe {
      if value.is_null() || (*value).is_null() {
        return Err(());
      }
      (*value as *mut Writer<BufWriter<File>>).as_mut().ok_or(())
    }
  }
}

macro_rules! try_into_writer {
  ($handle_ptr_ptr:ident, $failure_ret_val:expr) => {{
    let writer: Result<&mut Writer<BufWriter<File>>, _> = $handle_ptr_ptr.try_into();
    if writer.is_err() {
      return $failure_ret_val;
    }
    writer.unwrap()
  }};
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn bdcrw_open_writer(path: *const libc::c_char) -> WriterHandle {
  if path.is_null() {
    return null_mut();
  }

  unsafe {
    let path_str: &str = match std::ffi::CStr::from_ptr(path).to_str() {
      Ok(s) => s,
      Err(e) => {
          println!("Path contains invalid UTF-8: {}", e);
          return null_mut();
      }
    };
    match new_writer(path_str) {
      Ok(writer) => {
        let boxed_writer = Box::new(writer);
        Box::into_raw(boxed_writer) as WriterHandle
      },
      Err(e) => {
        println!("Failed to open writer: {}", e);
        null_mut()
      }
    }
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_boolean(
  handle: WriterHandle,
  value: bool,
) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_boolean(value) {
    Ok(_) => true,
    Err(e) => {
      println!("Failed to write boolean: {:?}", e);
      false
    }
  }
}
