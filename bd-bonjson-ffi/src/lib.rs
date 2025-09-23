// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_bonjson::writer::Writer;
use std::ffi::c_void;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::ops::{Deref, DerefMut};
use std::ptr::null;

pub type BDCrashWriterHandle = *mut *const c_void;

/// Newtype wrapper around `Writer<BufWriter<File>>` to allow implementing traits
pub struct WriterBufWriterFile(Writer<BufWriter<File>>);

impl WriterBufWriterFile {
  #[must_use]
  pub fn new(writer: Writer<BufWriter<File>>) -> Self {
    Self(writer)
  }

  #[must_use]
  pub fn into_raw(self) -> *const c_void {
    Box::into_raw(Box::new(self)) as *const c_void
  }
}

impl Deref for WriterBufWriterFile {
  type Target = Writer<BufWriter<File>>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for WriterBufWriterFile {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}



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
      (*handle as *mut WriterBufWriterFile)
        .as_mut()
        .ok_or(FFIError::HandleWasNull)
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
      log::error!("converting handle to writer: {:?}", writer.err());
      return $failure_ret_val;
    }
    writer.unwrap()
  }};
}

fn new_writer(path: &str) -> io::Result<WriterBufWriterFile> {
  let file = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)?;
  Ok(WriterBufWriterFile::new(Writer::new(BufWriter::new(file))))
}

/// Attempt to open a writer to the specified path.
/// The writer exists on the heap and must be closed with `bdcrw_close_writer`.
#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_open_writer(
  handle: BDCrashWriterHandle,
  path: *const libc::c_char,
) -> bool {
  if path.is_null() {
    log::error!("bdcrw_open_writer: path is null");
    return false;
  }

  let path_str: &str = match unsafe { std::ffi::CStr::from_ptr(path).to_str() } {
    Ok(s) => s,
    Err(e) => {
      log::error!("bdcrw_open_writer: path contains invalid UTF-8: {e}");
      return false;
    },
  };

  let writer = match new_writer(path_str) {
    Ok(writer) => writer,
    Err(e) => {
      log::error!("bdcrw_open_writer: failed to open writer: {e}");
      return false;
    },
  };

  unsafe {
    *handle = writer.into_raw();
  }
  true
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_close_writer(handle: BDCrashWriterHandle) {
  unsafe {
    if handle.is_null() || (*handle).is_null() {
      return;
    }
    drop(Box::<WriterBufWriterFile>::from_raw(*handle as *mut _));
    *handle = null();
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_flush_writer(handle: BDCrashWriterHandle) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.flush() {
    Ok(()) => true,
    Err(e) => {
      log::error!("Error: bdcrw_flush_writer: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_boolean(handle: BDCrashWriterHandle, value: bool) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_boolean(value) {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_boolean: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_null(handle: BDCrashWriterHandle) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_null() {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_null: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_signed(handle: BDCrashWriterHandle, value: i64) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_signed(value) {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_signed: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_unsigned(handle: BDCrashWriterHandle, value: u64) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_unsigned(value) {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_unsigned: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_float(handle: BDCrashWriterHandle, value: f64) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_float(value) {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_float: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_str(
  handle: BDCrashWriterHandle,
  value: *const libc::c_char,
) -> bool {
  if value.is_null() {
    log::error!("bdcrw_write_str: value is null");
    return false;
  }
  let cstr = unsafe { std::ffi::CStr::from_ptr(value) };
  let str_slice = match cstr.to_str() {
    Ok(s) => s,
    Err(e) => {
      log::error!("bdcrw_write_str: invalid UTF-8: {e}");
      return false;
    },
  };
  let writer = try_into_writer!(handle, false);
  match writer.write_str(str_slice) {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_str: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_array_begin(handle: BDCrashWriterHandle) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_array_begin() {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_array_begin: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_map_begin(handle: BDCrashWriterHandle) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_map_begin() {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_map_begin: {e:?}");
      false
    },
  }
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdcrw_write_container_end(handle: BDCrashWriterHandle) -> bool {
  let writer = try_into_writer!(handle, false);
  match writer.write_container_end() {
    Ok(_) => true,
    Err(e) => {
      log::error!("bdcrw_write_container_end: {e:?}");
      false
    },
  }
}
