// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::ffi::{CStr, CString, c_char};
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::{env, fs, slice};


unsafe extern "C" {
  fn bdrc_make_bin_from_json(
    schema_data: *const u8,
    json_data_path: *const c_char,
    length_or_err: *mut i32,
  ) -> *const u8;

  fn bdrc_alloc_json(bin_data_path: *const c_char) -> *const c_char;
  fn bdrc_json_free(json: *const c_char);
}

fn print_usage(program: &str) -> ExitCode {
  eprintln!("converter for the bd issue report format\n");
  eprintln!("usage: {program} to-json REPORT_BIN_PATH");
  eprintln!(
    "       {} to-bin  REPORT_JSON_PATH DEST_PATH",
    " ".repeat(program.len())
  );
  ExitCode::from(64) // EX_USAGE error
}

pub fn main() -> Result<ExitCode, std::io::Error> {
  let args: Vec<String> = env::args().collect();
  if args.len() < 3 {
    return Ok(print_usage(&args[0]));
  }

  let (action, input_path) = (&args[1], &args[2]);
  if !Path::new(input_path).exists() {
    eprintln!("Input file not found: {input_path}");
    return Ok(ExitCode::from(74));
  }
  match (args.len(), action.as_str()) {
    (3, "to-json") => print_json(input_path.as_str()),
    (4, "to-bin") => write_bin(input_path.as_str(), &args[3]),
    _ => Ok(print_usage(&args[0])),
  }
}

fn print_json(input_path: &str) -> Result<ExitCode, std::io::Error> {
  let data_path = CString::new(input_path)?;
  let text = unsafe { bdrc_alloc_json(data_path.as_ptr()) };
  let text_ptr = unsafe { CStr::from_ptr(text) };

  match text_ptr.to_str() {
    Ok(json) => {
      println!("{json}");
      unsafe {
        bdrc_json_free(text);
      }
      Ok(ExitCode::SUCCESS)
    },
    Err(result) => {
      eprintln!("failed to convert file: {result}");
      Ok(ExitCode::FAILURE)
    },
  }
}

fn write_bin(input_path: &str, output_path: &str) -> Result<ExitCode, std::io::Error> {
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
      &mut length_or_err,
    )
  };
  if length_or_err > 0 {
    let mut file = fs::OpenOptions::new()
      .create(true)
      .truncate(true)
      .write(true)
      .open(output_path)?;
    let data = unsafe { slice::from_raw_parts(buf, length_or_err.unsigned_abs() as usize) };
    file.write_all(data)?;
    Ok(ExitCode::SUCCESS)
  } else {
    eprintln!("failed to create file: {length_or_err}");
    Ok(ExitCode::from(74))
  }
}
