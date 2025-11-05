// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Test modules for bd-resilient-kv

#![allow(
  clippy::unwrap_used,
  clippy::panic,
  clippy::manual_assert,
  clippy::ignored_unit_patterns,
  clippy::uninlined_format_args,
  clippy::len_zero,
  clippy::cast_lossless,
  clippy::cast_possible_wrap,
  clippy::stable_sort_primitive,
  clippy::manual_range_contains,
  clippy::approx_constant,
  clippy::items_after_statements
)]

pub mod boundary_test;
pub mod concurrency_test;
pub mod double_buffered_automatic_switching_test;
pub mod double_buffered_memmapped_new_test;
pub mod double_buffered_retry_test;
pub mod double_buffered_selection_test;
pub mod double_buffered_test;
pub mod error_handling_test;
pub mod kv_store_test;
pub mod kv_test;
pub mod memmapped_test;
pub mod snapshot_cleanup_test;
pub mod versioned_kv_store_test;
pub mod versioned_recovery_test;
