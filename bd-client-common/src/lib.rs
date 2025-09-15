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

use bd_proto::protos::client::api::{self, ApiRequest, HandshakeRequest};
use std::future::{Future, pending};
use tokio::time::Interval;

pub mod error;
pub mod fb;
pub mod file;
pub mod file_system;
pub mod init_lifecycle;
pub mod payload_conversion;
pub mod safe_file_cache;
pub mod test;
pub mod zlib;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

// This is a helper for use in tokio::select to avoid unwrap() in the typical pattern:
// async { foo.unwrap().await }, if foo.is_some().
pub async fn maybe_await<R, F: Future<Output = R> + Unpin>(future: &mut Option<F>) -> R {
  let result = if let Some(f) = future {
    f.await
  } else {
    pending().await
  };
  *future = None;
  result
}

// Same as above but allows mapping into the option to get the future. This version does not
// clear the option since in the mapping version it's unlikely we want clearing if the option
// exists as this is generating some other future.
pub async fn maybe_await_map<'a, T, R, F: Future<Output = R>>(
  future: Option<&'a mut T>,
  map: impl FnOnce(&'a mut T) -> F,
) -> R {
  if let Some(f) = future {
    map(f).await
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

// TODO(mattklein123): Move this trait and the client config code into its own crate to break a
// a circular dependency.
#[mockall::automock]
#[async_trait::async_trait]
pub trait ClientConfigurationUpdate: Send + Sync {
  /// Attempts to load persisted config from disk if supported by the configuration type.
  async fn try_load_persisted_config(&self);

  /// Fill a handshake with version nonce information if available.
  fn fill_handshake(&self, handshake: &mut HandshakeRequest);

  /// Called to allow the configuration pipeline to react to the server being available.
  async fn on_handshake_complete(&self, configuration_update_status: u32);

  /// Unconditionally mark any cached config as "safe" to use.
  async fn mark_safe(&self);

  async fn try_apply_config(
    &self,
    configuration_update: api::ConfigurationUpdate,
  ) -> Option<ApiRequest>;

  async fn clear_cached_config(&self);
}
