// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

mod app_version;
mod async_log_buffer;
mod buffer_selector;
pub mod builder;
mod client_config;
mod consumer;
mod device_id;
mod directory_lock;
pub mod internal;
mod internal_report;
mod log_replay;
mod logger;
mod logging_state;
mod metadata;
mod network;
mod ordered_receiver;
mod pre_config_buffer;
mod service;

#[cfg(test)]
mod test;

pub use crate::app_version::AppVersionExtra;
pub use crate::logger::{ChannelPair, InitParams};
pub use async_log_buffer::LogAttributesOverrides;
pub use bd_api::{PlatformNetworkManager, PlatformNetworkStream};
use bd_buffer::AbslCode;
pub use bd_device::Device;
pub use bd_events::ListenerTarget as EventsListenerTarget;
pub use bd_log_metadata::MetadataProvider;
use bd_log_primitives::LossyIntToU32;
pub use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  FieldsRef,
  LogFieldKind,
  LogFieldValue,
  LogFields,
  LogLevel,
  LogMessage,
  log_level,
};
pub use bd_resource_utilization::Target as ResourceUtilizationTarget;
pub use bd_session_replay::Target as SessionReplayTarget;
pub use builder::LoggerBuilder;
pub use logger::{Block, CaptureSession, Logger, LoggerHandle, ReportProcessingSession};
pub use metadata::LogMetadata;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

fn write_log_to_buffer(
  producer: &mut bd_buffer::Producer,
  log: &mut bd_log_primitives::EncodableLog,
  action_ids: &[&str],
  stream_ids: &[&str],
) -> anyhow::Result<()> {
  match producer.reserve(
    log.compute_size(action_ids, stream_ids)?.to_u32_lossy(),
    true,
  ) {
    // If the buffer is locked, drop the error. This helps ensure that we are able to
    // log to all buffers even if one of them is locked.
    // TODO(snowp): Track how often logs are dropped due to locks.
    // If the buffer is out of space, drop the error.
    // TODO(mattklein123): Track this via stats.
    e @ Err(bd_buffer::Error::AbslStatus(
      AbslCode::FailedPrecondition | AbslCode::ResourceExhausted,
      _,
    )) => {
      log::debug!("failed to write log to buffer: {e:?}");
      Ok(())
    },
    Err(e) => Err(e),
    Ok(reservation) => {
      log.serialize_to_bytes(action_ids, stream_ids, reservation)?;
      producer.commit()
    },
  }?;
  Ok(())
}
