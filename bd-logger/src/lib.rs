// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod app_version;
mod async_log_buffer;
mod bounded_buffer;
mod builder;
mod client_config;
mod consumer;
pub mod internal;
mod internal_report;
mod log_replay;
mod logger;
mod logging_state;
mod metadata;
mod network;
mod pre_config_buffer;
mod service;

pub use crate::logger::{ChannelPair, InitParams};
pub use app_version::AppVersionExtra;
pub use async_log_buffer::LogAttributesOverridesPreviousRunSessionID;
pub use bd_api::{PlatformNetworkManager, PlatformNetworkStream};
pub use bd_device::Device;
pub use bd_events::ListenerTarget as EventsListenerTarget;
pub use bd_log_metadata::{AnnotatedLogFields, MetadataProvider};
pub use bd_log_primitives::{
  log_level,
  AnnotatedLogField,
  FieldsRef,
  LogField,
  LogFieldKind,
  LogFieldValue,
  LogFields,
  LogLevel,
  LogMessage,
  LogRef,
  StringOrBytes,
};
pub use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
pub use bd_resource_utilization::Target as ResourceUtilizationTarget;
pub use bd_session_replay::Target as SessionReplayTarget;
pub use builder::LoggerBuilder;
pub use logger::{Logger, LoggerHandle};
pub use metadata::LogMetadata;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}
