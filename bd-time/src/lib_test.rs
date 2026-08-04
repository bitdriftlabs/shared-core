// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::OffsetDateTimeExt as _;
use time::ext::NumericalDuration;
use time::macros::datetime;

#[test]
fn timestamp_floor() {
  let test_cases = &[
    (
      datetime!(2021-01-01 00:00:03 UTC),
      datetime!(2021-01-01 00:00:00 UTC),
      5.seconds(),
    ),
    (
      datetime!(2021-01-01 00:00:03 UTC),
      datetime!(2021-01-01 00:00:00 UTC),
      1.minutes(),
    ),
    (
      datetime!(2021-01-01 00:01:03 UTC),
      datetime!(2021-01-01 00:01:00 UTC),
      1.minutes(),
    ),
    (
      datetime!(2021-01-01 00:01:00 UTC),
      datetime!(2021-01-01 00:01:00 UTC),
      1.minutes(),
    ),
  ];

  for (input, expected, interval) in test_cases {
    assert_eq!(
      input.floor(*interval),
      *expected,
      "{input} should round to {expected} with interval {interval}",
    );
  }
}

#[test]
fn timestamp_ceil() {
  let test_cases = &[
    (
      datetime!(2021-01-01 00:00:03 UTC),
      datetime!(2021-01-01 00:00:05 UTC),
      5.seconds(),
    ),
    (
      datetime!(2021-01-01 00:00:03 UTC),
      datetime!(2021-01-01 00:01:00 UTC),
      1.minutes(),
    ),
    (
      datetime!(2021-01-01 00:01:03 UTC),
      datetime!(2021-01-01 00:02:00 UTC),
      1.minutes(),
    ),
    (
      datetime!(2021-01-01 00:01:00 UTC),
      datetime!(2021-01-01 00:01:00 UTC),
      1.minutes(),
    ),
  ];

  for (input, expected, interval) in test_cases {
    assert_eq!(
      input.ceil(*interval),
      *expected,
      "{input} should ceil to {expected} with interval {interval}",
    );
  }
}

#[test]
fn from_unix_timestamp_micros() {
  use time::OffsetDateTime;

  // Test zero
  let result = OffsetDateTime::from_unix_timestamp_micros(0).unwrap();
  assert_eq!(result.unix_timestamp(), 0);
  assert_eq!(result.unix_timestamp_nanos(), 0);

  // Test positive microseconds: 1,500,000 micros = 1.5 seconds
  let result = OffsetDateTime::from_unix_timestamp_micros(1_500_000).unwrap();
  assert_eq!(result.unix_timestamp(), 1);
  assert_eq!(result.unix_timestamp_nanos(), 1_500_000_000);

  // Test negative microseconds: -1,500,000 micros = -1.5 seconds
  let result = OffsetDateTime::from_unix_timestamp_micros(-1_500_000).unwrap();
  assert_eq!(result.unix_timestamp(), -2);
  assert_eq!(result.unix_timestamp_nanos(), -1_500_000_000);

  // Test exact second boundary: 1,000,000 micros = 1 second
  let result = OffsetDateTime::from_unix_timestamp_micros(1_000_000).unwrap();
  assert_eq!(result.unix_timestamp(), 1);
  assert_eq!(result.unix_timestamp_nanos(), 1_000_000_000);

  // Test sub-microsecond precision is properly handled
  // 1,234,567 micros = 1.234567 seconds = 1_234_567_000 nanos
  let result = OffsetDateTime::from_unix_timestamp_micros(1_234_567).unwrap();
  assert_eq!(result.unix_timestamp_nanos(), 1_234_567_000);

  // Test large value
  // Jan 1, 2024 00:00:00 UTC ≈ 1,704,067,200 seconds = 1,704,067,200,000,000 micros
  let micros_2024 = 1_704_067_200_000_000_i64;
  let result = OffsetDateTime::from_unix_timestamp_micros(micros_2024).unwrap();
  assert_eq!(result.unix_timestamp(), 1_704_067_200);
}
