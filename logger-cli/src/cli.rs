// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_logger::{AnnotatedLogField, AnnotatedLogFields, LogLevel, LogType, log_level};
use clap::{ArgAction, Args, Parser, Subcommand};

#[derive(clap::ValueEnum, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CliLogType {
  Normal,
  Replay,
  Lifecycle,
  Resource,
  View,
  Device,
  UX,
  Span,
}

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum CliLogLevel {
  Trace,
  Debug,
  Info,
  Warn,
  Error,
}

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum CliPlatform {
  Android,
  Apple,
}

#[derive(clap::ValueEnum, Copy, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RuntimeValueType {
  Bool,
  String,
  Int,
  Duration,
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
}

#[derive(Args, Debug)]
pub struct StartCommand {
  /// API key to use with emitted logs
  #[clap(env, long)]
  pub api_key: String,

  /// Bitdrift URL to connect
  #[clap(env, long, required = false, default_value = "https://api.bitdrift.io")]
  pub api_url: String,

  /// Uniquely identify the app
  #[clap(env, long)]
  pub app_id: String,

  /// App version
  #[clap(long, required = false, default_value = "1.0.0")]
  pub app_version: String,

  /// Device platform
  #[clap(long, required = false, default_value = "apple")]
  pub platform: CliPlatform,

  /// Device model
  #[clap(long, required = false, default_value = "iPhone12,1")]
  pub model: String,
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
  pub log_type: CliLogType,

  /// Severity level of the log
  #[clap(long, required = false, value_enum, default_value = "info")]
  pub log_level: CliLogLevel,

  /// Additional field(s) to send with the log
  #[clap(long, num_args=2, value_names=["key", "value"], action=ArgAction::Append)]
  pub field: Vec<String>,

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

impl From<CliLogLevel> for LogLevel {
  fn from(value: CliLogLevel) -> Self {
    match value {
      CliLogLevel::Error => log_level::ERROR,
      CliLogLevel::Warn => log_level::WARNING,
      CliLogLevel::Info => log_level::INFO,
      CliLogLevel::Debug => log_level::DEBUG,
      CliLogLevel::Trace => log_level::TRACE,
    }
  }
}

impl From<CliLogType> for LogType {
  fn from(value: CliLogType) -> Self {
    match value {
      CliLogType::Device => Self::Device,
      CliLogType::Lifecycle => Self::Lifecycle,
      CliLogType::Normal => Self::Normal,
      CliLogType::Replay => Self::Replay,
      CliLogType::Resource => Self::Resource,
      CliLogType::Span => Self::Span,
      CliLogType::UX => Self::UX,
      CliLogType::View => Self::View,
    }
  }
}

impl From<CliPlatform> for bd_api::Platform {
  fn from(value: CliPlatform) -> Self {
    match value {
      CliPlatform::Apple => Self::Apple,
      CliPlatform::Android => Self::Android,
    }
  }
}

pub struct FieldPairs<T>(pub Vec<T>);
impl From<FieldPairs<String>> for AnnotatedLogFields {
  fn from(value: FieldPairs<String>) -> Self {
    value
      .0
      .chunks_exact(2)
      .map(|pair| {
        (
          pair[0].clone().into(),
          AnnotatedLogField {
            value: pair[1].clone().into(),
            kind: bd_logger::LogFieldKind::Ootb,
          },
        )
      })
      .collect()
  }
}
