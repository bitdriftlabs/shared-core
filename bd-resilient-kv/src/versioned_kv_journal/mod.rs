// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::state;

// Journal format constants
// These define the wire format for the versioned journal.
//
// Journal header structure (17 bytes):
// | Offset | Field            | Type | Size |
// |--------|------------------|------|------|
// | 0      | Format Version   | u64  | 8    |
// | 8      | Position         | u64  | 8    |
// | 16     | Reserved         | u8   | 1    |
pub const HEADER_SIZE: usize = 17;

pub mod cleanup;
mod file_manager;
pub mod framing;
mod journal;
mod memmapped_journal;
pub mod recovery;
pub mod retention;
pub mod store;

pub use store::PersistentStoreConfig;

/// Errors that can occur during store operations.
#[derive(thiserror::Error, Debug)]
pub enum UpdateError {
  /// The has exceeded its configured capacity limit.
  ///
  /// For the persistent store this indicates the journal buffer size has been exceeded despite
  /// attempts to rotate the journal.
  ///
  /// For the in-memory store this indicates the maximum memory usage has been exceeded.
  #[error("Capacity exeeded")]
  CapacityExceeded,

  /// An underlying system error occurred (I/O, serialization, etc.).
  #[error(transparent)]
  System(#[from] anyhow::Error),
}


/// Represents a value with its associated timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampedValue {
  /// The value stored in the key-value store.
  pub value: state::payload::StateValue,

  /// The timestamp (in microseconds since UNIX epoch) when this value was last written.
  pub timestamp: u64,
}

#[cfg(test)]
#[must_use]
pub fn make_string_value(s: &str) -> state::payload::StateValue {
  state::payload::StateValue {
    value_type: Some(state::payload::state_value::Value_type::StringValue(
      s.to_string(),
    )),
    ..Default::default()
  }
}
