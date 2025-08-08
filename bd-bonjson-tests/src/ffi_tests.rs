// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use assert_no_alloc::*;
use bd_bonjson::decoder::{Decoder, Value};
use bd_bonjson::ffi::BDCrashWriterHandle;
use std::ptr::null;
use tempfile::NamedTempFile;

#[cfg(debug_assertions)]
#[global_allocator]
static A: AllocDisabler = AllocDisabler;

unsafe extern "C-unwind" {
  fn open_writer(handle: BDCrashWriterHandle, path: *const libc::c_char) -> bool;
  fn close_writer(handle: BDCrashWriterHandle);
  fn flush_writer(handle: BDCrashWriterHandle) -> bool;
  fn write_boolean(handle: BDCrashWriterHandle, value: bool) -> bool;
  fn write_null(handle: BDCrashWriterHandle) -> bool;
  fn write_signed(handle: BDCrashWriterHandle, value: i64) -> bool;
  fn write_unsigned(handle: BDCrashWriterHandle, value: u64) -> bool;
  fn write_float(handle: BDCrashWriterHandle, value: f64) -> bool;
  fn write_str(handle: BDCrashWriterHandle, value: *const libc::c_char) -> bool;
  fn write_array_begin(handle: BDCrashWriterHandle) -> bool;
  fn write_map_begin(handle: BDCrashWriterHandle) -> bool;
  fn write_container_end(handle: BDCrashWriterHandle) -> bool;
}

fn new_temp_file_path() -> String {
  NamedTempFile::new()
    .unwrap()
    .path()
    .to_str()
    .unwrap()
    .to_string()
}

#[test]
fn test_open_and_close_writer() {
  let temp_file_path = new_temp_file_path();
  let mut handle = null();

  unsafe {
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
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
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
}

#[test]
fn test_write_boolean() {
  let temp_file_path = new_temp_file_path();

  let expected = true;

  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    let result = write_boolean(&raw mut handle, expected);
    assert!(result);

    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }

  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Bool(expected));
}

#[test]
fn test_write_null() {
  let temp_file_path = new_temp_file_path();
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(write_null(&raw mut handle));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Null);
}

#[test]
fn test_write_signed() {
  let temp_file_path = new_temp_file_path();
  let expected = -12345i64;
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(write_signed(&raw mut handle, expected));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Signed(expected));
}

#[test]
fn test_write_unsigned() {
  let temp_file_path = new_temp_file_path();
  let expected = u64::MAX;
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(write_unsigned(&raw mut handle, expected));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Unsigned(expected));
}

#[test]
fn test_write_float() {
  let temp_file_path = new_temp_file_path();
  let expected = 3.819_242_f64;
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(write_float(&raw mut handle, expected));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Float(expected));
}

#[test]
fn test_write_str() {
  let temp_file_path = new_temp_file_path();
  let expected = "hello ffi";
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    let cstr = std::ffi::CString::new(expected).unwrap();
    assert!(write_str(&raw mut handle, cstr.as_ptr()));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::String(expected.to_string()));
}

#[test]
fn test_write_array() {
  let temp_file_path = new_temp_file_path();
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(write_array_begin(&raw mut handle));
    assert!(write_signed(&raw mut handle, 1));
    assert!(write_signed(&raw mut handle, 2));
    assert!(write_signed(&raw mut handle, 3));
    assert!(write_container_end(&raw mut handle));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  assert_eq!(
    value,
    Value::Array(vec![Value::Signed(1), Value::Signed(2), Value::Signed(3),])
  );
}

#[test]
fn test_write_map() {
  let temp_file_path = new_temp_file_path();
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    assert!(write_map_begin(&raw mut handle));
    let key1 = std::ffi::CString::new("foo").unwrap();
    assert!(write_str(&raw mut handle, key1.as_ptr()));
    assert!(write_signed(&raw mut handle, 42));
    let key2 = std::ffi::CString::new("bar").unwrap();
    assert!(write_str(&raw mut handle, key2.as_ptr()));
    assert!(write_boolean(&raw mut handle, false));
    assert!(write_container_end(&raw mut handle));
    assert!(flush_writer(&raw mut handle));

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();
  let mut expected = std::collections::HashMap::new();
  expected.insert("foo".to_string(), Value::Signed(42));
  expected.insert("bar".to_string(), Value::Bool(false));
  assert_eq!(value, Value::Object(expected));
}

#[test]
fn test_write_deeply_nested_structure() {
  let temp_file_path = new_temp_file_path();
  unsafe {
    let mut handle = null();
    assert!(open_writer(
      &raw mut handle,
      temp_file_path.as_ptr().cast::<libc::c_char>()
    ));
    assert!(!handle.is_null());

    let outer_key = std::ffi::CString::new("outer").unwrap();
    let inner_key = std::ffi::CString::new("inner").unwrap();

    assert_no_alloc(|| {
      // Write: { "outer": [ { "inner": [1, 2, 3] } ] }
      assert!(write_map_begin(&raw mut handle));
      assert!(write_str(&raw mut handle, outer_key.as_ptr()));
      assert!(write_array_begin(&raw mut handle));
      assert!(write_map_begin(&raw mut handle));
      assert!(write_str(&raw mut handle, inner_key.as_ptr()));
      assert!(write_array_begin(&raw mut handle));
      assert!(write_signed(&raw mut handle, 1));
      assert!(write_signed(&raw mut handle, 2));
      assert!(write_signed(&raw mut handle, 3));
      assert!(write_container_end(&raw mut handle)); // end inner array
      assert!(write_container_end(&raw mut handle)); // end inner map
      assert!(write_container_end(&raw mut handle)); // end outer array
      assert!(write_container_end(&raw mut handle)); // end outer map
      assert!(flush_writer(&raw mut handle));
    });

    close_writer(&raw mut handle);
    assert!(handle.is_null());
  }
  let bytes = std::fs::read(temp_file_path).unwrap();
  let mut decoder = Decoder::new(&bytes);
  let value = decoder.decode().unwrap();

  // Build expected value
  let mut inner_map = std::collections::HashMap::new();
  inner_map.insert(
    "inner".to_string(),
    Value::Array(vec![Value::Signed(1), Value::Signed(2), Value::Signed(3)]),
  );
  let expected = {
    let mut outer_map = std::collections::HashMap::new();
    outer_map.insert(
      "outer".to_string(),
      Value::Array(vec![Value::Object(inner_map)]),
    );
    Value::Object(outer_map)
  };

  assert_eq!(value, expected);
}
