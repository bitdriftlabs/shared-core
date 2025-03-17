// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub use bd_log_primitives::{log_level, LogFields, LogLevel, LogType};
use std::sync::Arc;

//
// Logger
//

/// A trait for writing log messages. This is used to provide a layer of indirection to break a
/// circular dependency between core/logger and core/api, allowing the api crate to write back into
/// the logger.
pub trait Logger: Send + Sync {
  /// Writes a single log line.
  fn log(&self, level: LogLevel, log_type: LogType, msg: &str, fields: LogFields);

  fn log_internal(&self, msg: &str) {
    self.log(
      log_level::DEBUG,
      LogType::InternalSDK,
      msg,
      LogFields::default(),
    );
  }
}

//
// NoopLogging
//

/// A logger that does nothing. This is used in situations where we don't actually want the
/// internal logs to go anywhere.
pub struct NoopLogger;

impl NoopLogger {
  #[must_use]
  pub fn new() -> Arc<Self> {
    Arc::new(Self {})
  }
}

impl Logger for NoopLogger {
  fn log(&self, _level: LogLevel, _log_type: LogType, _msg: &str, _fields: LogFields) {}
}
