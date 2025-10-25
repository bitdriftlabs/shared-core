// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_logger::log_level;
use rmcp::schemars;

/// A log type enum for categorizing logs.
///
/// This matches the `LogType` enum in `bd_logger`, but is redeclared here to allow additional
/// traits like `clap::ValueEnum`.
#[derive(
  clap::ValueEnum, Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum LogType {
  /// The default log type, used for most application logs that are not emitted through a well
  /// known integration.
  Normal,

  /// Logs emitted by the session replay system. These logs will have an empty message and will
  /// encode a session replay event field in the `_session_replay_event` field.
  Replay,

  /// Logs related to application lifecycle events, such as app start, app stop, and significant
  /// state transitions.
  Lifecycle,

  /// Logs used to report resouce utilization metrics, such as memory usage, CPU usage, and network
  /// usage. These are typically emitted at regular intervals by the platform layer.
  ///
  /// Each collected metric is reported as a single field, while the log message is empty.
  Resource,

  /// Logs related to view events, such as screen transitions and view appearances/disappearances.
  View,

  /// Logs related to device information and state, such as device model, OS version, and other
  Device,

  /// Logs related to user experience (UX) events, such as button taps, form submissions, and other
  UX,

  /// Logs related to spans started and stopped within the application. For example, network
  /// requests are modeled as a start span log when the request is initiated, and a stop span log
  /// when the response is received. A pair of spans can be correlated via a shared span ID included
  /// in the `_span_id` field.
  Span,
}

impl From<LogType> for bd_logger::LogType {
  fn from(value: LogType) -> Self {
    match value {
      LogType::Device => Self::Device,
      LogType::Lifecycle => Self::Lifecycle,
      LogType::Normal => Self::Normal,
      LogType::Replay => Self::Replay,
      LogType::Resource => Self::Resource,
      LogType::Span => Self::Span,
      LogType::UX => Self::UX,
      LogType::View => Self::View,
    }
  }
}

/// A log level enum for categorizing log severity.
///
/// This matches the `LogLevel` enum from `bd_logger`, but is redeclared here to allow additional
/// traits like `clap::ValueEnum`.
#[derive(
  clap::ValueEnum, Debug, Clone, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub enum LogLevel {
  Trace,
  Debug,
  Info,
  Warn,
  Error,
}

impl From<LogLevel> for bd_logger::LogLevel {
  fn from(value: LogLevel) -> Self {
    match value {
      LogLevel::Error => log_level::ERROR,
      LogLevel::Warn => log_level::WARNING,
      LogLevel::Info => log_level::INFO,
      LogLevel::Debug => log_level::DEBUG,
      LogLevel::Trace => log_level::TRACE,
    }
  }
}

/// A platform enum for categorizing platforms associated with the logger.
///
/// This matches the `Platform` enum defined in `bd_logger`, but is redeclared here to allow
/// additional traits like `clap::ValueEnum`.
#[derive(clap::ValueEnum, Debug, Clone)]
pub enum Platform {
  Android,
  Apple,
}

impl From<Platform> for bd_api::Platform {
  fn from(value: Platform) -> Self {
    match value {
      Platform::Apple => Self::Apple,
      Platform::Android => Self::Android,
    }
  }
}

#[derive(clap::ValueEnum, Copy, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RuntimeValueType {
  Bool,
  String,
  Int,
  Duration,
}
