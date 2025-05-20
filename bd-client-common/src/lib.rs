// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::client::api::{ApiRequest, ApiResponse, HandshakeRequest};
use error::handle_unexpected;
use std::future::Future;

pub mod error;
pub mod fb;
pub mod file;
pub mod payload_conversion;
pub mod safe_file_cache;
pub mod zlib;

pub fn spawn_error_handling_task<E: std::error::Error + Sync + Send + 'static>(
  f: impl Future<Output = std::result::Result<(), E>> + Send + 'static,
  description: &'static str,
) {
  tokio::spawn(async move { handle_unexpected(f.await, description) });
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
