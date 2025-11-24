// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::state;

pub mod cleanup;
mod file_manager;
mod framing;
mod journal;
mod memmapped_journal;
pub mod recovery;
pub mod retention;
pub mod store;

pub use store::PersistentStoreConfig;

/// Represents a value with its associated timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampedValue {
  /// The value stored in the key-value store.
  pub value: state::payload::StateValue,

  /// The timestamp (in microseconds since UNIX epoch) when this value was last written.
  pub timestamp: u64,
}

#[cfg(test)]
pub fn make_string_value(s: &str) -> state::payload::StateValue {
  state::payload::StateValue {
    value_type: Some(state::payload::state_value::Value_type::StringValue(
      s.to_string(),
    )),
    ..Default::default()
  }
}
