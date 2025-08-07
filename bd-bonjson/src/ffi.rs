// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::ptr::null;
use std::{ffi::c_void, fs::File};
use std::fs::OpenOptions;
use std::io::{self, Write};
use crate::writer::Writer;
use std::io::{BufWriter};
use libc;

pub type BDCrashWriterHandle = *mut *const c_void;
pub type WriterBufWriterFile = Writer<BufWriter<File>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFIError {
  HandleWasNull,
}

impl TryFrom<BDCrashWriterHandle> for &mut WriterBufWriterFile {
  type Error = FFIError;

  /// # Safety
  /// This function dereferences a raw pointer and must be called with a valid handle
  #[allow(clippy::not_unsafe_ptr_arg_deref)]
  fn try_from(handle: BDCrashWriterHandle) -> Result<Self, Self::Error> {
    unsafe {
      if handle.is_null() || (*handle).is_null() {
        return Err(FFIError::HandleWasNull);
      }
      (*handle as *mut WriterBufWriterFile).as_mut().ok_or(FFIError::HandleWasNull)
    }
  }
}

/// Attempt to convert a raw handle into a reference to a `WriterBufWriterFile`
///
/// * `$handle_ptr_ptr`  - raw handle
/// * `$failure_ret_val` - value to return upon failure to convert
macro_rules! try_into_writer {
  ($handle_ptr_ptr:ident, $failure_ret_val:expr) => {{
    let writer: Result<&mut WriterBufWriterFile, _> = $handle_ptr_ptr.try_into();
    if writer.is_err() {
      println!("Error converting handle to writer: {:?}", writer.err());
      return $failure_ret_val;
    }
    writer.unwrap()
  }};
}

pub fn new_writer(path: &str) -> io::Result<WriterBufWriterFile> {
  println!("### Creating new writer for path: {}", path);
  let file = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)?;
  Ok(Writer { writer: BufWriter::new(file) })
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn bdcrw_open_writer(handle: BDCrashWriterHandle, path: *const libc::c_char) -> bool {
  if path.is_null() {
    println!("Error: bdcrw_open_writer: path is null");
    return false;
  }

  let path_str: &str = match unsafe { std::ffi::CStr::from_ptr(path).to_str() } {
    Ok(s) => s,
    Err(e) => {
        println!("Error: bdcrw_open_writer: path contains invalid UTF-8: {e}");
        return false;
    }
  };

  let writer = match new_writer(path_str) {
    Ok(writer) => writer,
    Err(e) => {
      println!("Error: bdcrw_open_writer: failed to open writer: {e}");
      return false;
    }
  };

  unsafe {
    *handle = writer.into_raw();
    println!("### bdcrw_open_writer: handle: {:?}", *handle);
  }
  true
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn bdcrw_close_writer(handle: BDCrashWriterHandle) {
  unsafe {
    if handle.is_null() || (*handle).is_null() {
      return;
    }
    drop(Box::<WriterBufWriterFile>::from_raw(*handle as *mut _));
    *handle = null();
  }
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn bdcrw_flush_writer(handle: BDCrashWriterHandle) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.flush() {
    Ok(_) => true,
    Err(e) => {
      println!("Error: bdcrw_flush_writer: {e:?}");
      false
    }
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_boolean(handle: BDCrashWriterHandle, value: bool) -> bool {
  unsafe {println!("### bdcrw_write_boolean: handle: {:?}", *handle);}
  let writer = try_into_writer!(handle, false);
  match writer.write_boolean(value) {
    Ok(_) => true,
    Err(e) => {
      println!("Error: bdcrw_write_boolean: {e:?}");
      false
    }
  }
}
