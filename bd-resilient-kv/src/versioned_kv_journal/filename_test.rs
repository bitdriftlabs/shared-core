// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn parse_valid_filename() {
  let result = SnapshotFilename::parse("state.jrn.g1.t1234567890.zz");
  assert_eq!(result, Some(SnapshotFilename { timestamp_micros: 1_234_567_890 }));
}

#[test]
fn parse_large_values() {
  let result = SnapshotFilename::parse("state.jrn.g999.t1737933600000000.zz");
  assert_eq!(result, Some(SnapshotFilename { timestamp_micros: 1_737_933_600_000_000 }));
}

#[test]
fn parse_different_journal_names() {
  let result = SnapshotFilename::parse("other.jrn.g5.t999999.zz");
  assert_eq!(result, Some(SnapshotFilename { timestamp_micros: 999_999 }));
}

#[test]
fn parse_invalid_missing_zz_extension() {
  assert_eq!(SnapshotFilename::parse("state.jrn.g1.t1234567890"), None);
}

#[test]
fn parse_missing_generation_still_parses() {
  // generation is not extracted — only timestamp matters
  assert_eq!(
    SnapshotFilename::parse("state.jrn.t1234567890.zz"),
    Some(SnapshotFilename { timestamp_micros: 1_234_567_890 })
  );
}

#[test]
fn parse_invalid_missing_timestamp() {
  assert_eq!(SnapshotFilename::parse("state.jrn.g1.zz"), None);
}

#[test]
fn parse_non_numeric_generation_still_parses() {
  // generation is not extracted — only timestamp matters
  assert_eq!(
    SnapshotFilename::parse("state.jrn.gabc.t1234567890.zz"),
    Some(SnapshotFilename { timestamp_micros: 1_234_567_890 })
  );
}

#[test]
fn parse_invalid_non_numeric_timestamp() {
  assert_eq!(SnapshotFilename::parse("state.jrn.g1.tabc.zz"), None);
}

#[test]
fn parse_invalid_completely_wrong_format() {
  assert_eq!(SnapshotFilename::parse("random_file.txt"), None);
  assert_eq!(SnapshotFilename::parse(""), None);
  assert_eq!(SnapshotFilename::parse(".zz"), None);
}
