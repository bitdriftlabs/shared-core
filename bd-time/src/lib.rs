// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use parking_lot::Mutex;
use protobuf::well_known_types::timestamp::Timestamp;
use protobuf::MessageField;
use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::time::{interval, interval_at, Interval, Timeout};

//
// OffsetDateTimeExt
//

pub trait OffsetDateTimeExt {
  fn into_proto(self) -> MessageField<Timestamp>;
  fn unix_timestamp_ms(&self) -> i64;
}

impl OffsetDateTimeExt for OffsetDateTime {
  fn into_proto(self) -> MessageField<Timestamp> {
    Some(Timestamp {
      seconds: self.unix_timestamp(),
      // unix_timetsamp gives us seconds since epoch, unix_timestamp_nanos gives us nanos since
      // epoch. Compute the sub-second nanos by converting unix_timestamps to ns and subtracting it
      // from the exact value.
      #[allow(clippy::cast_possible_truncation)]
      nanos: (self.unix_timestamp_nanos() - i128::from(self.unix_timestamp()) * 1_000_000_000)
        as i32,
      ..Default::default()
    })
    .into()
  }

  #[must_use]
  fn unix_timestamp_ms(&self) -> i64 {
    self.unix_timestamp() * 1_000 + i64::from(self.nanosecond() / 1_000_000)
  }
}

//
// TimestampExt
//

pub trait TimestampExt {
  fn to_offset_date_time(&self) -> OffsetDateTime;
}

impl TimestampExt for Timestamp {
  fn to_offset_date_time(&self) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(self.seconds).unwrap()
      + std::time::Duration::from_nanos(self.nanos.try_into().unwrap_or_default())
  }
}

//
// TimeDurationExt
//

pub trait TimeDurationExt {
  fn advance(self) -> impl Future<Output = ()>;
  fn sleep(self) -> impl Future<Output = ()>;
  fn interval(self) -> Interval;
  fn interval_at(self) -> Interval;
  fn timeout<F: IntoFuture>(self, f: F) -> Timeout<F::IntoFuture>;
  fn add_tokio_now(self) -> tokio::time::Instant;
  fn add_tokio_instant(self, instant: tokio::time::Instant) -> tokio::time::Instant;
}

impl TimeDurationExt for time::Duration {
  fn advance(self) -> impl Future<Output = ()> {
    tokio::time::advance(self.unsigned_abs())
  }

  fn sleep(self) -> impl Future<Output = ()> {
    tokio::time::sleep(self.unsigned_abs())
  }

  fn interval(self) -> Interval {
    interval(self.unsigned_abs())
  }

  fn interval_at(self) -> Interval {
    interval_at(self.add_tokio_now(), self.unsigned_abs())
  }

  fn timeout<F: IntoFuture>(self, f: F) -> Timeout<F::IntoFuture> {
    tokio::time::timeout(self.unsigned_abs(), f)
  }

  fn add_tokio_now(self) -> tokio::time::Instant {
    tokio::time::Instant::now() + self.unsigned_abs()
  }

  fn add_tokio_instant(self, instant: tokio::time::Instant) -> tokio::time::Instant {
    instant + self.unsigned_abs()
  }
}

//
// ProtoDurationExt
//

pub trait ProtoDurationExt {
  /// Convert into to a std Duration. `std::time::Duration` is an absolute
  /// duration, so this returns the absolute value of the protobuf duration.
  fn to_std_duration_absolute(&self) -> Duration;

  /// Convert into to a std Duration. `std::time::Duration` is an absolute duration, so this
  /// function returns None if the duration is negative.
  fn to_std_duration_checked(&self) -> Option<Duration>;

  /// Convert to a ``time::Duration`` which can be negative.
  fn to_time_duration(&self) -> time::Duration;
}

impl ProtoDurationExt for protobuf::well_known_types::duration::Duration {
  fn to_std_duration_absolute(&self) -> Duration {
    // Nanos are only negative if the value is between -1 and 0, in which case self.seconds
    // should be zero so the abs() value ends up being the right thing.
    Duration::from_secs(self.seconds.unsigned_abs())
      + Duration::from_nanos(u64::from(self.nanos.unsigned_abs()))
  }

  fn to_std_duration_checked(&self) -> Option<Duration> {
    if self.seconds < 0 {
      return None;
    }
    if self.nanos < 0 {
      return None;
    }

    Some(self.to_std_duration_absolute())
  }

  fn to_time_duration(&self) -> time::Duration {
    time::Duration::seconds(self.seconds) + time::Duration::nanoseconds(self.nanos.into())
  }
}

//
// ToProtoDuration
//

pub trait ToProtoDuration {
  fn into_proto(self) -> MessageField<protobuf::well_known_types::duration::Duration>;
}

impl ToProtoDuration for Duration {
  fn into_proto(self) -> MessageField<protobuf::well_known_types::duration::Duration> {
    Some(self.into()).into()
  }
}

impl ToProtoDuration for time::Duration {
  fn into_proto(self) -> MessageField<protobuf::well_known_types::duration::Duration> {
    Some(protobuf::well_known_types::duration::Duration {
      seconds: self.whole_seconds(),
      nanos: self.subsec_nanoseconds(),
      ..Default::default()
    })
    .into()
  }
}

//
// TimeProvider
//

#[async_trait::async_trait]
pub trait TimeProvider: Send + Sync {
  fn now(&self) -> OffsetDateTime;
  async fn sleep(&self, duration: time::Duration);
}

//
// SystemTimeProvider
//

pub struct SystemTimeProvider;

#[async_trait::async_trait]
impl TimeProvider for SystemTimeProvider {
  fn now(&self) -> OffsetDateTime {
    OffsetDateTime::now_utc()
  }
  async fn sleep(&self, duration: time::Duration) {
    tokio::time::sleep(duration.unsigned_abs()).await;
  }
}

//
// TestTimeChangeGuard
//

pub struct TestTimeChangeGuard<'a> {
  time_provider: &'a TestTimeProvider,
  original_time: OffsetDateTime,
}

impl<'a> Drop for TestTimeChangeGuard<'a> {
  fn drop(&mut self) {
    *self.time_provider.now.lock() = self.original_time;
  }
}

//
// TestTimeProvider
//

#[derive(Clone)]
pub struct TestTimeProvider {
  now: Arc<Mutex<OffsetDateTime>>,
}

impl TestTimeProvider {
  #[must_use]
  pub fn new(now: OffsetDateTime) -> Self {
    Self {
      now: Arc::new(Mutex::new(now)),
    }
  }

  pub fn advance(&self, duration: time::Duration) {
    *self.now.lock() += duration;
  }

  pub fn set_time(&self, new_time: OffsetDateTime) {
    *self.now.lock() = new_time;
  }

  #[must_use]
  pub fn temp_set_time(&self, new_time: OffsetDateTime) -> TestTimeChangeGuard<'_> {
    let mut now = self.now.lock();
    let original_time = *now;
    *now = new_time;
    TestTimeChangeGuard {
      time_provider: self,
      original_time,
    }
  }
}

#[async_trait::async_trait]
impl TimeProvider for TestTimeProvider {
  fn now(&self) -> OffsetDateTime {
    *self.now.lock()
  }

  async fn sleep(&self, duration: time::Duration) {
    // For testing purposes we don't actually want to sleep, just advance the time. Advancing the
    // clock is required for cases where were the calling code expects that the wall clock
    // advances as a result of sleeping.
    *self.now.lock() += duration;

    // Yield to simulate the async nature of sleep. Without this, tests have a chance to spin as
    // sleeping doesn't yield to the exeuctor like it normally would.
    tokio::task::yield_now().await;
  }
}
