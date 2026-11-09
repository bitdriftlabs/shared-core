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
  TimeDurationExt as _,
  TimeProvider,
  TimestampExt as _,
};
use protobuf::well_known_types::duration::Duration as ProtoDuration;
use protobuf::well_known_types::timestamp::Timestamp;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use time::ext::NumericalDuration;
use time::macros::datetime;
use tokio::time::{Instant, MissedTickBehavior};

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
fn timestamp_checked_accepts_the_spec_range() {
  assert_eq!(
    timestamp(-62_135_596_800, 0).to_offset_date_time_checked(),
    Some(datetime!(0001-01-01 00:00:00 UTC))
  );
  assert_eq!(
    timestamp(253_402_300_799, 999_999_999).to_offset_date_time_checked(),
    Some(datetime!(9999-12-31 23:59:59.999999999 UTC))
  );
  assert_eq!(
    timestamp(1_609_459_200, 500).to_offset_date_time_checked(),
    Some(timestamp(1_609_459_200, 500).to_offset_date_time())
  );
}

#[test]
fn timestamp_checked_refuses_values_past_the_spec_range() {
  for (seconds, nanos) in [
    (-62_135_596_801, 0),
    (253_402_300_800, 0),
    (0, -1),
    (0, 1_000_000_000),
    (i64::MAX, 0),
    (i64::MIN, 0),
  ] {
    assert_eq!(
      timestamp(seconds, nanos).to_offset_date_time_checked(),
      None,
      "seconds {seconds}, nanos {nanos}"
    );
  }
}

#[test]
fn duration_checked_accepts_the_spec_range() {
  for (seconds, nanos) in [
    (315_576_000_000, 999_999_999),
    (-315_576_000_000, -999_999_999),
    (0, 999_999_999),
    (0, -999_999_999),
    (5, 0),
    (-5, 0),
  ] {
    assert_eq!(
      proto_duration(seconds, nanos).to_time_duration_checked(),
      Some(time::Duration::new(seconds, nanos)),
      "seconds {seconds}, nanos {nanos}"
    );
  }
}

#[test]
fn duration_checked_refuses_values_past_the_spec_range() {
  for (seconds, nanos) in [
    (315_576_000_001, 0),
    (-315_576_000_001, 0),
    (0, 1_000_000_000),
    (0, -1_000_000_000),
    (1, -1),
    (-1, 1),
    (i64::MAX, 0),
    (i64::MIN, 0),
  ] {
    assert_eq!(
      proto_duration(seconds, nanos).to_time_duration_checked(),
      None,
      "seconds {seconds}, nanos {nanos}"
    );
  }
}

#[test]
fn unchecked_conversions_saturate_instead_of_panicking() {
  assert_eq!(
    proto_duration(i64::MAX, 2_000_000_000).to_time_duration(),
    time::Duration::MAX
  );
  assert_eq!(
    proto_duration(i64::MIN, -2_000_000_000).to_time_duration(),
    time::Duration::MIN
  );

  // Seconds out of range still fall back to the Unix epoch, as before.
  assert_eq!(
    timestamp(i64::MAX, 2_000_000_000).to_offset_date_time(),
    datetime!(1970-01-01 00:00:02 UTC)
  );
  assert_eq!(
    timestamp(i64::MIN, -2_000_000_000).to_offset_date_time(),
    datetime!(1970-01-01 00:00:00 UTC)
  );

  let latest = datetime!(9999-12-31 23:59:59.999999999 UTC);
  assert_eq!(
    timestamp(253_402_300_799, 999_999_999).to_offset_date_time(),
    latest
  );
  // One nanosecond more would be past 9999-12-31, so the result stops there.
  assert_eq!(
    timestamp(253_402_300_799, 1_000_000_000).to_offset_date_time(),
    latest
  );
}

#[test]
fn unchecked_conversions_keep_their_output_for_valid_input() {
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

#[test]
fn rounding_to_an_interval_under_one_second_leaves_the_time_unchanged() {
  let time = datetime!(2021-01-01 00:00:03.5 UTC);
  for interval in [
    time::Duration::ZERO,
    500.milliseconds(),
    (-500).milliseconds(),
  ] {
    assert_eq!(time.floor(interval), time, "floor to {interval}");
    assert_eq!(time.ceil(interval), time, "ceil to {interval}");
  }
}

#[test]
fn rounding_to_a_negative_interval_uses_its_absolute_value() {
  let time = datetime!(2021-01-01 00:00:03 UTC);
  assert_eq!(
    time.floor((-5).seconds()),
    datetime!(2021-01-01 00:00:00 UTC)
  );
  assert_eq!(
    time.ceil((-5).seconds()),
    datetime!(2021-01-01 00:00:05 UTC)
  );

  // The absolute value of `time::Duration::MIN` doesn't fit in an `i64`.
  let epoch = time::OffsetDateTime::UNIX_EPOCH;
  assert_eq!(time.floor(time::Duration::MIN), epoch);
  assert_eq!(time.ceil(time::Duration::MIN), epoch);
}

#[test]
fn rounding_past_the_representable_dates_falls_back_to_the_epoch() {
  // Rounding to `i64::MAX` seconds gives either zero or a date out of range.
  let epoch = time::OffsetDateTime::UNIX_EPOCH;
  let after_epoch = datetime!(2021-01-01 00:00:03 UTC);
  let before_epoch = datetime!(1900-01-01 00:00:03 UTC);
  assert_eq!(after_epoch.floor(time::Duration::MAX), epoch);
  assert_eq!(after_epoch.ceil(time::Duration::MAX), epoch);
  assert_eq!(before_epoch.floor(time::Duration::MAX), epoch);
  assert_eq!(before_epoch.ceil(time::Duration::MAX), epoch);
}

// About 30 years, the longest duration the timer helpers pass to tokio.
const MAX_TIMER_DURATION: std::time::Duration = std::time::Duration::from_hours(24 * 365 * 30);

#[tokio::test(start_paused = true)]
async fn timer_helpers_cap_huge_durations_instead_of_panicking() {
  let now = Instant::now();
  for duration in [time::Duration::MAX, time::Duration::MIN] {
    assert_eq!(duration.add_tokio_now(), now + MAX_TIMER_DURATION);
    assert_eq!(duration.add_tokio_instant(now), now + MAX_TIMER_DURATION);
  }
  assert!(time::Duration::MAX.jittered() <= MAX_TIMER_DURATION);

  // The second tick adds the period to the first deadline. Without the cap, that overflows
  // `Instant`.
  for mut interval in [
    time::Duration::MAX.interval(MissedTickBehavior::Burst),
    time::Duration::MAX.interval_at(MissedTickBehavior::Burst),
    time::Duration::MAX.jittered_interval_at(MissedTickBehavior::Burst),
  ] {
    assert_eq!(interval.period(), MAX_TIMER_DURATION);
    interval.tick().await;
    interval.tick().await;
  }
}

#[tokio::test(start_paused = true)]
async fn a_zero_interval_ticks_every_millisecond_instead_of_panicking() {
  for mut interval in [
    time::Duration::ZERO.interval(MissedTickBehavior::Burst),
    time::Duration::ZERO.interval_at(MissedTickBehavior::Burst),
    time::Duration::ZERO.jittered_interval_at(MissedTickBehavior::Burst),
  ] {
    assert_eq!(interval.period(), std::time::Duration::from_millis(1));
    interval.tick().await;
    interval.tick().await;
  }
}

#[tokio::test(start_paused = true)]
async fn timer_helpers_keep_their_output_for_ordinary_durations() {
  let now = Instant::now();
  assert_eq!(
    5.seconds().add_tokio_now(),
    now + std::time::Duration::from_secs(5)
  );
  assert_eq!(
    (-5).seconds().add_tokio_instant(now),
    now + std::time::Duration::from_secs(5)
  );
  assert_eq!(
    5.seconds().interval(MissedTickBehavior::Burst).period(),
    std::time::Duration::from_secs(5)
  );
  assert_eq!((-5).seconds().jittered(), std::time::Duration::ZERO);
  assert!(5.seconds().jittered() <= std::time::Duration::from_secs(5));
}
