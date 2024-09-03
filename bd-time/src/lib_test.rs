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
      "{} should round to {} with interval {}",
      input,
      expected,
      interval
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
      "{} should ceil to {} with interval {}",
      input,
      expected,
      interval
    );
  }
}
