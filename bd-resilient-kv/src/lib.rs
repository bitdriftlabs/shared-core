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
#[ctor::ctor]
fn test_global_init() {
  // TODO(snowp): Ideally we'll depend on bd-test-helpers here, but that would create a cyclic
  // dependency.
  bd_log::SwapLogger::initialize();
}

#[cfg(test)]
mod tests;

pub mod kv_journal;
pub mod kv_store;
mod scope;
mod versioned_kv_journal;

pub use kv_journal::{DoubleBufferedKVJournal, InMemoryKVJournal, KVJournal, MemMappedKVJournal};
pub use kv_store::KVStore;
pub use scope::Scope;
pub use versioned_kv_journal::recovery::VersionedRecovery;
pub use versioned_kv_journal::retention::{RetentionHandle, RetentionRegistry};
pub use versioned_kv_journal::store::{DataLoss, ScopedMaps, VersionedKVStore};
pub use versioned_kv_journal::{PersistentStoreConfig, TimestampedValue, UpdateError};
