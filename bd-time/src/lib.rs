// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./lib_test.rs"]
mod test;

#[cfg(not(target_family = "wasm"))]
mod no_wasm;
#[cfg(target_family = "wasm")]
mod wasm;

#[cfg(not(target_family = "wasm"))]
pub use no_wasm::*;
use parking_lot::Mutex;
use protobuf::well_known_types::timestamp::Timestamp;
use protobuf::MessageField;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
#[cfg(target_family = "wasm")]
pub use wasm::*;

//
// OffsetDateTimeExt
//

pub trait OffsetDateTimeExt {
  /// Convert into a protobuf Timestamp.
  fn into_proto(self) -> MessageField<Timestamp>;

  /// Convert into a unix timestamp in milliseconds with millisecond precision.
  fn unix_timestamp_ms(&self) -> i64;

  /// Rounds down the given timestamp to the nearest interval.
  ///
  /// For example, if the interval is 5 minutes, then 12:03:00 would be rounded down to 12:00:00.
  ///
  /// Note that `interval` will be rounded down to the nearest second and should be positive.
  fn floor(&self, interval: time::Duration) -> OffsetDateTime;

  /// Rounds up the given timestamp to the nearest interval.
  ///
  /// For example, if the interval is 5 minutes, then 12:03:00 would be rounded up to 12:05:00.
  ///
  /// Note that `interval` will be rounded down to the nearest second and should be positive.
  fn ceil(&self, interval: time::Duration) -> OffsetDateTime;
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

  fn floor(&self, interval: time::Duration) -> OffsetDateTime {
    debug_assert!(interval.whole_seconds() >= 0);

    let unix_timestamp = self.unix_timestamp();
    let rounded_down = unix_timestamp - unix_timestamp.rem_euclid(interval.whole_seconds());
    Self::from_unix_timestamp(rounded_down).unwrap()
  }

  fn ceil(&self, interval: time::Duration) -> OffsetDateTime {
    debug_assert!(interval.whole_seconds() >= 0);

    let unix_timestamp = self.unix_timestamp();
    let rem = unix_timestamp.rem_euclid(interval.whole_seconds());
    if rem == 0 {
      return *self;
    }

    let rounded_up = unix_timestamp + interval.whole_seconds()
      - unix_timestamp.rem_euclid(interval.whole_seconds());
    Self::from_unix_timestamp(rounded_up).unwrap()
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
// MissedTickBehavior
//

pub enum MissedTickBehavior {
  Delay,
}

//
// TimeDurationExt
//

pub trait TimeDurationExt {
  fn advance(self) -> impl Future<Output = ()>;
  fn sleep(self) -> Sleep;
  fn interval(self, behavior: MissedTickBehavior) -> Interval;
  fn interval_at(self, behavior: MissedTickBehavior) -> Interval;
  fn jittered_interval_at(self, behavior: MissedTickBehavior) -> Interval;
  fn timeout<F: IntoFuture>(self, f: F) -> Timeout<F::IntoFuture>;
  fn add_now(self) -> Instant;
  fn add_instant(self, instant: Instant) -> Instant;
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

pub trait TimeProvider: Send + Sync {
  fn now(&self) -> OffsetDateTime;
  fn sleep(&self, duration: time::Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
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
  fn sleep(&self, duration: time::Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(duration.sleep())
  }
}

//
// TestTimeChangeGuard
//

pub struct TestTimeChangeGuard<'a> {
  time_provider: &'a TestTimeProvider,
  original_time: OffsetDateTime,
}

impl Drop for TestTimeChangeGuard<'_> {
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

impl TimeProvider for TestTimeProvider {
  fn now(&self) -> OffsetDateTime {
    *self.now.lock()
  }

  fn sleep(&self, duration: time::Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // For testing purposes we don't actually want to sleep, just advance the time. Advancing the
    // clock is required for cases where were the calling code expects that the wall clock
    // advances as a result of sleeping.
    *self.now.lock() += duration;

    // Yield to simulate the async nature of sleep. Without this, tests have a chance to spin as
    // sleeping doesn't yield to the executor like it normally would.
    #[cfg(not(target_family = "wasm"))]
    return Box::pin(tokio::task::yield_now());
    #[cfg(target_family = "wasm")]
    Box::pin(async {})
  }
}
