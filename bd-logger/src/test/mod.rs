// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod crash_handler_integration;
mod directory_lock_integration;
mod embedded_logger_integration;
mod logger_integration;
pub mod setup;
mod state_upload_integration;
mod stats_integration;

use crate::async_log_buffer::Sender;
use crate::device_command::DeviceCommandRegistry;
use bd_event_buffer::EventBuffer;
use bd_log_primitives::LogFields;
use bd_session::Strategy;
use bd_test_helpers::metadata_provider::LogMetadata;
use parking_lot::Mutex;
use std::sync::Arc;
use time::OffsetDateTime;

impl Sender {
  /// Uses normal admission with deterministic metadata and the fixture's real session state.
  pub(crate) fn from_event_buffer(
    event_buffer: EventBuffer,
    session_strategy: Arc<Strategy>,
  ) -> Self {
    Self::new(
      event_buffer,
      Arc::new(LogMetadata {
        timestamp: Mutex::new(OffsetDateTime::UNIX_EPOCH),
        custom_fields: LogFields::default(),
        ootb_fields: LogFields::default(),
      }),
      session_strategy,
      DeviceCommandRegistry::default(),
    )
  }
}
