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

use crate::DataValue;

pub mod versioned_kv_store_dynamic_growth_test;
pub mod versioned_kv_store_test;
pub mod versioned_recovery_error_test;
pub mod versioned_recovery_test;

/// Helper function to decompress zlib-compressed data.
pub fn decompress_zlib(data: &[u8]) -> anyhow::Result<Vec<u8>> {
  use flate2::read::ZlibDecoder;
  use std::io::Read;

  let mut decoder = ZlibDecoder::new(data);
  let mut decompressed = Vec::new();
  decoder.read_to_end(&mut decompressed)?;
  Ok(decompressed)
}

pub fn make_string_value(s: &str) -> DataValue {
  DataValue::String(s.to_string())
}
