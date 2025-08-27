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

pub use bd_log_primitives::LogFields;

//
// MetadataProvider
//

/// Used to provide metadata that is typically included with each log message.
pub trait MetadataProvider {
  /// Returns the timestamp to associate with the log message.
  fn timestamp(&self) -> anyhow::Result<time::OffsetDateTime>;

  /// Returns custom and ootb fields to include with each log message.
  fn fields(&self) -> anyhow::Result<(LogFields, LogFields)>;
}
