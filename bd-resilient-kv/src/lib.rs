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

#[cfg(test)]
mod tests;

pub mod kv_journal;
pub mod kv_store;
pub mod snapshot_cleanup;
pub mod versioned_kv_store;
pub mod versioned_recovery;

pub use kv_journal::{
  DoubleBufferedKVJournal,
  InMemoryKVJournal,
  KVJournal,
  MemMappedKVJournal,
  MemMappedVersionedKVJournal,
  VersionedKVJournal,
};
pub use kv_store::KVStore;
pub use snapshot_cleanup::{SnapshotCleanup, SnapshotInfo};
pub use versioned_kv_store::{RotationCallback, VersionedKVStore};
pub use versioned_recovery::VersionedRecovery;
