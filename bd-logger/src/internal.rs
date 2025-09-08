// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::logger::{Block, CaptureSession, LoggerHandle};
use bd_log_primitives::{AnnotatedLogField, LogFields, LogLevel};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_runtime::runtime::ConfigLoader;

//
// InternalLogger
//

/// A wrapper around a `LoggerId` making it possible for any part of the system to log messages to
/// the logger specified by the ID. This is accomplished via a dyn trait, avoiding the possible
/// circular dependency between other crates and the logger crate.
pub struct InternalLogger {
  logger_handle: LoggerHandle,
  logging_enabled:
    bd_runtime::runtime::BoolWatch<bd_runtime::runtime::debugging::InternalLoggingFlag>,
}

impl InternalLogger {
  pub fn new(logger_handle: LoggerHandle, runtime: &ConfigLoader) -> Self {
    Self {
      logger_handle,
      logging_enabled: runtime.register_bool_watch(),
    }
  }
}

impl bd_internal_logging::Logger for InternalLogger {
  fn log(&self, log_level: LogLevel, log_type: LogType, msg: &str, fields: LogFields) {
    if !*self.logging_enabled.read() {
      return;
    }

    log::debug!("{msg}");

    self.logger_handle.log(
      log_level,
      log_type,
      msg.into(),
      fields
        .into_iter()
        .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
        .collect(),
      [].into(),
      None,
      Block::No,
      CaptureSession::default(),
    );
  }
}
