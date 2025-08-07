// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::ptr::null;

use bd_bonjson::ffi::BDCrashWriterHandle;
use tempfile::NamedTempFile;
use libc;


unsafe extern "C-unwind" {
  fn open_writer(handle: BDCrashWriterHandle, path: *const libc::c_char) -> bool;
  fn close_writer(handle: BDCrashWriterHandle);
  fn flush_writer(handle: BDCrashWriterHandle) -> bool;
  fn write_boolean(handle: BDCrashWriterHandle, value: bool) -> bool;
}

fn new_temp_file_path() -> String {
  NamedTempFile::new().unwrap().path().to_str().unwrap().to_string()
}

#[test]
fn test_open_and_close_writer() {
  let temp_file_path = new_temp_file_path();
  let mut handle = null();

  unsafe {
    assert!(open_writer(&raw mut handle, temp_file_path.as_ptr() as *const libc::c_char));
    assert!(!handle.is_null());
    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
}

#[test]
fn test_flush() {
  let temp_file_path = new_temp_file_path();
  let mut handle = null();

  unsafe {
    assert!(open_writer(&raw mut handle, temp_file_path.as_ptr() as *const libc::c_char));
    assert!(!handle.is_null());

    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
}

#[test]
fn test_write_boolean() {
  let temp_file_path = new_temp_file_path();
  let mut handle = null();

  unsafe {
    assert!(open_writer(&raw mut handle, temp_file_path.as_ptr() as *const libc::c_char));
    assert!(!handle.is_null());

    let result = write_boolean(&raw mut handle, true);
    assert!(result);

    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
}
