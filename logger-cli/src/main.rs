// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::cli::Command;
use crate::logger::SESSION_FILE;
use bd_log::SwapLogger;
use clap::Parser;
use std::env;
use std::path::Path;

mod cli;
mod logger;
mod metadata;
mod storage;

fn main() -> anyhow::Result<()> {
  // initialize console logging
  SwapLogger::initialize();
  let args = crate::cli::Options::parse();

  let home = env::var("HOME")?;
  let sdk_directory = Path::new(&home).join(".local").join("bd-logger-cli");
  std::fs::create_dir_all(&sdk_directory)?;

  let mut logger = crate::logger::make_logger(&sdk_directory, &args)?;

  match args.command {
    Command::EnqueueArtifacts(cmd) => {
      let report_dir = &sdk_directory.join("reports/new");
      std::fs::create_dir_all(report_dir)?;
      for path in &cmd.path {
        let source_path = Path::new(&path);
        std::fs::copy(
          source_path,
          report_dir.join(source_path.file_name().unwrap()),
        )?;
      }
    },
    Command::UploadArtifacts => {
      logger.start();
      logger.process_crash_reports()?;
      logger.stop();
    },
    Command::Log(cmd) => {
      logger.start();
      logger.log(cmd, true);
      logger.stop();
    },
    Command::NewSession => {
      let session_config = sdk_directory.join(SESSION_FILE);
      std::fs::remove_file(session_config)?;
    },
  }

  Ok(())
}
