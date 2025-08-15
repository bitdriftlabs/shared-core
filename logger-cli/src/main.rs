// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::cli::{Command, Options, RuntimeValueType};
use crate::logger::{LoggerHolder, MaybeStaticSessionGenerator, SESSION_FILE};
use bd_log::SwapLogger;
use clap::Parser;
use std::env;
use std::path::Path;
use time::Duration;

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
    Command::UploadArtifacts => with_logger(&args, &sdk_directory, |logger| {
      logger.process_crash_reports()
    })?,
    Command::Log(ref cmd) => with_logger(&args, &sdk_directory, |logger| {
      logger.log(cmd, true);
      Ok(())
    })?,
    Command::NewSession => {
      let session_config = sdk_directory.join(SESSION_FILE);
      std::fs::remove_file(session_config)?;
    },
    Command::Timeline => {
      let config_path = sdk_directory.join(SESSION_FILE);
      let generator = MaybeStaticSessionGenerator { config_path };
      if let Ok(session_id) = generator.cached_session_id() {
        let base_url = args.api_url.replace("api.", "timeline.");
        let session_url = format!("{base_url}/session/{session_id}");
        std::process::Command::new("open")
          .arg(session_url)
          .output()?;
      } else {
        eprintln!("No session ID set");
      }
    },
    Command::LogRuntimeValue(ref cmd) => with_logger(&args, &sdk_directory, |logger| {
      let name = cmd.name.clone();
      let snapshot = logger.logger.runtime_snapshot();
      log::error!("Requested runtime value {name}: {}", match cmd.type_ {
        RuntimeValueType::Bool => format!("{}", snapshot.get_bool(&name, false)),
        RuntimeValueType::String => format!("'{}'", snapshot.get_string(&name, "".to_string())),
        RuntimeValueType::Int => format!("{}", snapshot.get_integer(&name, 0)),
        RuntimeValueType::Duration => {
          format!("{}", snapshot.get_duration(&name, Duration::seconds(0)))
        },
      });
      Ok(())
    })?,
  }

  Ok(())
}

fn with_logger<F>(args: &Options, sdk_directory: &Path, f: F) -> anyhow::Result<()>
where
  F: FnOnce(&mut LoggerHolder) -> anyhow::Result<()>,
{
  let mut logger = crate::logger::make_logger(sdk_directory, args)?;
  logger.start();
  f(&mut logger)?;
  logger.stop();
  Ok(())
}
