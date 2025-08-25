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

use bd_proto::protos::client::api::{ApiRequest, ApiResponse, HandshakeRequest};
use std::future::{Future, pending};
use tokio::time::Interval;

pub mod error;
pub mod fb;
pub mod file;
pub mod file_system;
pub mod payload_conversion;
pub mod safe_file_cache;
pub mod test;
pub mod zlib;

// This is a helper for use in tokio::select to avoid unwrap() in the typical pattern:
// async { foo.unwrap().await }, if foo.is_some().
pub async fn maybe_await<R, F: Future<Output = R> + Unpin>(future: &mut Option<F>) -> R {
  if let Some(f) = future {
    let result = f.await;
    *future = None;
    result
  } else {
    pending().await
  }
}

// Same as above but for an Interval that may or may not exist.
pub async fn maybe_await_interval(interval: Option<&mut Interval>) {
  if let Some(f) = interval {
    f.tick().await;
  } else {
    pending::<()>().await;
  }
}

// Flags used in the handshake to indicate which configs are up to date.
pub const HANDSHAKE_FLAG_CONFIG_UP_TO_DATE: u32 = 0x1;
pub const HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE: u32 = 0x2;

/// Used to define a configuration pipeline that receives configuration updates through the
/// multiplexing API. The configuration pipeline may optionally support disk persistence.
#[mockall::automock]
#[async_trait::async_trait]
pub trait ConfigurationUpdate: Send + Sync {
  /// Attempt to apply a new inbound configuration. Returns None if the response does not apply
  /// to this configuration type, otherwise returns the ack/nack after attempting to apply the
  /// config.
  async fn try_apply_config(&self, response: &ApiResponse) -> Option<ApiRequest>;

  /// Attempts to load persisted config from disk if supported by the configuration type.
  async fn try_load_persisted_config(&self);

  /// Fill a handshake with version nonce information if available.
  fn fill_handshake(&self, handshake: &mut HandshakeRequest);

  /// Called to allow the configuration pipeline to react to the server being available.
  async fn on_handshake_complete(&self, configuration_update_status: u32);

  /// Unconditionally mark any cached config as "safe" to use.
  async fn mark_safe(&self);
}
