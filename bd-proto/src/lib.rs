// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(
  elided_lifetimes_in_paths,
  unused_extern_crates,
  explicit_outlives_requirements
)]

#[rustfmt::skip]
#[allow(
  clippy::nursery,
  clippy::pedantic,
  clippy::style,
  unsafe_op_in_unsafe_fn,
// Below needed for nightly as of 1.88
  unknown_lints,
  mismatched_lifetime_syntaxes
)]
#[cfg(not(tarpaulin_include))]
pub mod flatbuffers;
#[allow(
  clippy::nursery,
  clippy::pedantic,
  clippy::style,
  renamed_and_removed_lints
)]
#[cfg(not(tarpaulin_include))]
pub mod protos;
