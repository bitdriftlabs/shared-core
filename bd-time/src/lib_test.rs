// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::{
  DynamicIntervalTicker,
  OffsetDateTimeExt as _,
  ProtoDurationExt as _,
  TestTimeProvider,
  Ticker,
  TimeProvider,
  TimestampExt as _,
};
use protobuf::well_known_types::duration::Duration as ProtoDuration;
use protobuf::well_known_types::timestamp::Timestamp;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
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

#[tokio::test]
async fn dynamic_interval_ticker_reloads_interval_provider() {
  let interval_seconds = Arc::new(AtomicI64::new(10));
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2026-01-01 00:00:00 UTC)));

  let mut ticker = DynamicIntervalTicker::new({
    let interval_seconds = Arc::clone(&interval_seconds);
    move || time::Duration::seconds(interval_seconds.load(Ordering::Relaxed))
  })
  .with_time_provider(time_provider.clone());

  let first_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(time_provider.now(), first_now + time::Duration::seconds(10));

  interval_seconds.store(1, Ordering::Relaxed);
  let second_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(time_provider.now(), second_now + time::Duration::seconds(1));
}

fn timestamp(seconds: i64, nanos: i32) -> Timestamp {
  Timestamp {
    seconds,
    nanos,
    ..Default::default()
  }
}

fn proto_duration(seconds: i64, nanos: i32) -> ProtoDuration {
  ProtoDuration {
    seconds,
    nanos,
    ..Default::default()
  }
}

#[test]
fn conversions_saturate_instead_of_panicking() {
  assert_eq!(
    proto_duration(i64::MAX, 2_000_000_000).to_time_duration(),
    time::Duration::MAX
  );
  assert_eq!(
    proto_duration(i64::MIN, -2_000_000_000).to_time_duration(),
    time::Duration::MIN
  );
  // One nanosecond past the latest date `OffsetDateTime` can hold.
  assert_eq!(
    timestamp(253_402_300_799, 1_000_000_000).to_offset_date_time(),
    datetime!(9999-12-31 23:59:59.999999999 UTC)
  );
}

#[test]
fn conversions_keep_their_old_output() {
  assert_eq!(
    timestamp(1_609_459_200, 123_456_789).to_offset_date_time(),
    datetime!(2021-01-01 00:00:00.123456789 UTC)
  );
  // Negative nanos have always counted as zero.
  assert_eq!(
    timestamp(1_609_459_200, -5).to_offset_date_time(),
    datetime!(2021-01-01 00:00:00 UTC)
  );
  assert_eq!(
    proto_duration(-3, -500_000_000).to_time_duration(),
    time::Duration::new(-3, -500_000_000)
  );
}
