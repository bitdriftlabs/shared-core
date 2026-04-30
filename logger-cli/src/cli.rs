// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use clap::{ArgAction, Args, Parser, Subcommand};
use logger_cli::logger::LoggerArgs;
use logger_cli::types::{LogLevel, LogType, Platform, RuntimeValueType};
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::PathBuf;

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorMode {
  Auto,
  Always,
  Never,
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Options {
  /// Server connection host
  #[clap(
    env = "LOGGER_HOST",
    long,
    required = false,
    default_value = "localhost"
  )]
  pub host: String,

  /// Server connection port
  #[clap(env = "LOGGER_PORT", long, required = false, default_value = "5501")]
  pub port: u16,

  /// Override the SDK data directory used for local state.
  #[clap(env = "LOGGER_SDK_DIRECTORY", long, global = true)]
  pub sdk_directory: Option<PathBuf>,

  /// Control ANSI color in logger-cli output.
  #[clap(
    env = "LOGGER_LOG_COLOR",
    long,
    global = true,
    value_enum,
    default_value = "auto"
  )]
  pub log_color: ColorMode,

  /// Observe stats uploads for a single action ID and write matching events as JSONL.
  #[clap(env = "LOGGER_OBSERVE_STATS_ACTION_ID", long, global = true)]
  pub observe_stats_action_id: Option<String>,

  /// Override the JSONL output path for observed stats events.
  #[clap(env = "LOGGER_OBSERVE_STATS_OUTPUT", long, global = true)]
  pub observe_stats_output: Option<PathBuf>,

  /// Command to run
  #[command(subcommand)]
  pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
  /// Emit a log
  Log(LogCommand),

  /// Copy a file to the artifact upload directory
  EnqueueArtifacts(EnqueueCommand),

  /// Send any pending artifacts to the server
  UploadArtifacts,

  /// Generate a new session
  NewSession,

  /// Open the timeline for the current session in the default browser
  Timeline,

  /// Start server
  Start(StartCommand),

  /// Stop running server
  Stop,

  /// Break execution
  Trap,

  /// Log a runtime config value
  PrintRuntimeValue(RuntimeValueCommand),

  /// Toggle sleep mode
  SetSleepMode(SleepModeCommand),

  /// Set a feature flag exposure
  SetFeatureFlag(SetFeatureFlagCommand),
}

#[derive(Args, Debug, Clone)]
pub struct StartCommand {
  /// API key to use with emitted logs
  #[clap(env, long)]
  pub api_key: String,

  /// Remove the existing logger data directory before starting
  #[clap(long)]
  pub clean_data_dir: bool,

  /// Bitdrift URL to connect
  #[clap(env, long, required = false, default_value = "https://api.bitdrift.io")]
  pub api_url: String,

  /// Uniquely identify the app
  #[clap(env, long, required = false, default_value = "io.bitdrift.cli")]
  pub app_id: String,

  /// App version
  #[clap(long, required = false, default_value = "1.0.0")]
  pub app_version: String,

  /// App version code (build number)
  #[clap(long, required = false, default_value = "10")]
  pub app_version_code: String,

  /// Device platform
  #[clap(long, required = false, default_value = "apple")]
  pub platform: Platform,

  /// Device model
  #[clap(long, required = false, default_value = "iPhone12,1")]
  pub model: String,
}

impl From<StartCommand> for LoggerArgs {
  fn from(cmd: StartCommand) -> Self {
    Self {
      api_url: if cmd.api_url.contains("://") {
        cmd.api_url
      } else {
        format!("https://{}", cmd.api_url)
      },
      api_key: cmd.api_key,
      app_id: cmd.app_id,
      platform: cmd.platform,
      app_version: cmd.app_version,
      app_version_code: cmd.app_version_code,
      model: cmd.model,
    }
  }
}

#[derive(clap::ValueEnum, PartialEq, Eq, Debug, Clone)]
pub enum EnableFlag {
  On,
  Off,
}

#[derive(Args, Debug)]
pub struct SleepModeCommand {
  /// Toggle state
  pub enabled: EnableFlag,
}

#[derive(Args, Debug)]
pub struct LogCommand {
  /// Type of the log
  #[clap(long, required = false, value_enum, default_value = "normal")]
  pub log_type: LogType,

  /// Severity level of the log
  #[clap(long, required = false, value_enum, default_value = "info")]
  pub log_level: LogLevel,

  /// Additional field(s) to send with the log
  #[clap(long, num_args=2, value_names=["key", "value"], action=ArgAction::Append)]
  pub field: Vec<String>,

  /// Capture a session for this log.
  #[clap(long, action = ArgAction::SetTrue)]
  pub capture_session: bool,

  /// Block until the log has been processed or the timeout expires.
  #[clap(long, action = ArgAction::SetTrue)]
  pub block: bool,

  /// Log message
  pub message: String,
}

#[derive(Args, Debug)]
pub struct EnqueueCommand {
  /// Path(s) to the file(s) to upload
  #[clap(action=ArgAction::Append)]
  pub path: Vec<String>,
}


#[derive(Args, Debug)]
pub struct RuntimeValueCommand {
  /// Expected value type
  #[clap(long, required = false, value_enum, default_value = "bool")]
  pub type_: RuntimeValueType,

  pub name: String,
}

#[derive(Args, Debug)]
pub struct SetFeatureFlagCommand {
  /// Name of the feature flag
  pub name: String,

  /// Optional variant value for the feature flag
  #[clap(long)]
  pub variant: Option<String>,
}

pub struct FieldPairs<T>(pub Vec<T>);
impl<S: BuildHasher + Default> From<FieldPairs<String>> for HashMap<String, String, S> {
  fn from(value: FieldPairs<String>) -> Self {
    value
      .0
      .chunks_exact(2)
      .map(|pair| (pair[0].clone(), pair[1].clone()))
      .collect()
  }
}
