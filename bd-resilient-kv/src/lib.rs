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

pub mod double_buffered;
pub mod in_memory;
pub mod kv_store;
pub mod kvjournal;
pub mod memmapped;

// Re-export the main trait and types for convenience
// Re-export the in-memory implementation for convenience
pub use double_buffered::DoubleBufferedKVJournal;
pub use in_memory::InMemoryKVJournal;
pub use kv_store::KVStore;
pub use kvjournal::{HighWaterMarkCallback, KVJournal};
pub use memmapped::MemMappedKVJournal;
