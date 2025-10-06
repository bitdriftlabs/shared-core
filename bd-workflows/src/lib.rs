// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::indexing_slicing,
  clippy::panic,
  clippy::string_slice,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use std::ops::Deref;

pub mod actions_flush_buffers;
pub mod config;
pub mod engine;
mod generate_log;
pub mod metrics;
mod sankey_diagram;
pub mod test;
pub mod workflow;

enum OwnedOrRef<'a, T> {
  Owned(T),
  Ref(&'a T),
}

impl<T> Deref for OwnedOrRef<'_, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    match self {
      OwnedOrRef::Owned(s) => s,
      OwnedOrRef::Ref(s) => s,
    }
  }
}
