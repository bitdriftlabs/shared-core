// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_report_convert::{bin_to_json, json_to_bin};
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::{env, fs};

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
    (3, "to-json") => Ok(print_json(input_path.as_str())),
    (4, "to-bin") => write_bin(input_path.as_str(), &args[3]),
    _ => Ok(print_usage(&args[0])),
  }
}

fn print_json(input_path: &str) -> ExitCode {
  match bin_to_json(input_path) {
    Ok(json) => {
      println!("{json}");
      ExitCode::SUCCESS
    },
    Err(result) => {
      eprintln!("failed to convert file: {result}");
      ExitCode::FAILURE
    },
  }
}

fn write_bin(input_path: &str, output_path: &str) -> Result<ExitCode, std::io::Error> {
  match json_to_bin(input_path) {
    Ok(data) => {
      let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(output_path)?;
      file.write_all(data)?;
      Ok(ExitCode::SUCCESS)
    },
    Err(code) => {
      eprintln!("failed to create file: {code}");
      Ok(ExitCode::from(74))
    },
  }
}
