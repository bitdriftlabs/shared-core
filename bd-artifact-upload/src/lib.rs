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

mod uploader;

pub use uploader::{Client, MockClient, SnappedFeatureFlag, Uploader};

#[cfg(test)]
#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
}
