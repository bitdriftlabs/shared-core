// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use error::handle_unexpected;
use std::future::Future;

pub mod error;
pub mod fb;
pub mod file;
pub mod file_system;
pub mod zlib;

pub fn spawn_error_handling_task<E: std::error::Error + Sync + Send + 'static>(
  f: impl Future<Output = std::result::Result<(), E>> + Send + 'static,
  description: &'static str,
) {
  tokio::spawn(async move { handle_unexpected(f.await, description) });
}
