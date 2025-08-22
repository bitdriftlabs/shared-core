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

use crate::reporter::handle_unexpected;

pub mod reporter;
pub mod test;

pub fn spawn_error_handling_task<E: std::error::Error + Sync + Send + 'static>(
  f: impl Future<Output = std::result::Result<(), E>> + Send + 'static,
  description: &'static str,
) {
  tokio::spawn(async move { handle_unexpected(f.await, description) });
}
