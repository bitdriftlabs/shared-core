// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  // TODO(snowp): Ideally we'll depend on bd-test-helpers here, but that would create a cyclic
  // dependency.
  bd_log::SwapLogger::initialize();
}

#[cfg(test)]
mod tests;

mod scope;
pub mod versioned_kv_journal;

/// Maximum decompressed state journal accepted by server-side readers.
pub const MAX_DECOMPRESSED_STATE_SNAPSHOT_BYTES: usize = 10 * 1024 * 1024;

/// Maximum compressed state journal accepted by server-side readers.
///
/// This limits request memory and decompressor input work independently of the expanded journal
/// limit above.
pub const MAX_COMPRESSED_STATE_SNAPSHOT_BYTES: usize = 10 * 1024 * 1024;

pub use bd_proto::protos::state::payload::StateValue;
pub use bd_proto::protos::state::payload::state_value::Value_type;
pub use scope::Scope;
pub use versioned_kv_journal::filename::SnapshotFilename;
pub use versioned_kv_journal::recovery::{
  DecodedStateChange,
  VersionedRecovery,
  decode_compressed_journal,
  decode_journal,
  extract_non_empty_string_values_from_compressed_journal,
};
pub use versioned_kv_journal::retention::{RetentionHandle, RetentionRegistry};
pub use versioned_kv_journal::store::{DataLoss, ScopedMaps, VersionedKVStore};
pub use versioned_kv_journal::{
  HEADER_SIZE as VERSIONED_JOURNAL_HEADER_SIZE,
  PersistentStoreConfig,
  TimestampedValue,
  UpdateError,
};
