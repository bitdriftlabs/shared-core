// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::anyhow;
use std::ffi::{CStr, CString, c_char};
use std::slice;

unsafe extern "C" {
  fn bdrc_make_bin_from_json(
    schema_data: *const u8,
    json_data_path: *const c_char,
    length_or_err: *mut i32,
  ) -> *const u8;

  fn bdrc_alloc_json(bin_data_path: *const c_char) -> *const c_char;
  fn bdrc_json_free(json: *const c_char);
}

pub fn bin_to_json(input_path: &str) -> anyhow::Result<String> {
  let data_path = CString::new(input_path)?;
  let text = unsafe { bdrc_alloc_json(data_path.as_ptr()) };
  let text_ptr = unsafe { CStr::from_ptr(text) };

  let output = text_ptr.to_str().map(ToOwned::to_owned);
  unsafe {
    bdrc_json_free(text);
  }
  output.map_err(|err| anyhow!(err))
}

pub fn json_to_bin<'a>(input_path: &str) -> anyhow::Result<&'a [u8]> {
  let schema_data = concat!(
    include_str!("../../api/src/bitdrift_public/fbs/issue-reporting/v1/report.fbs"),
    "\0"
  );
  let data_path = CString::new(input_path)?;
  let mut length_or_err: i32 = 0;
  let buf = unsafe {
    bdrc_make_bin_from_json(
      schema_data.as_bytes().as_ptr(),
      data_path.as_ptr(),
      &raw mut length_or_err,
    )
  };
  if length_or_err > 0 {
    Ok(unsafe { slice::from_raw_parts(buf, length_or_err.unsigned_abs() as usize) })
  } else {
    anyhow::bail!("failed to parse {input_path} ({length_or_err})")
  }
}
